#[cfg(test)]
use independent_download_manager::downloader::DownloadResult;
use independent_download_manager::{
    downloader::{DownloadRequest, MAX_SEGMENT_COUNT},
    filename::filename_from_url,
    integrity::normalize_sha256,
    native_messaging::{self, PendingUrlClaim},
    queue::{
        DownloadQueue, JobSnapshot, JobState, Priority, QueueError,
        MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND,
    },
    schedule::DailyWindow,
    url_policy::validate_http_url,
};
use serde::{Deserialize, Serialize};
use slint::{ModelRc, SharedString, TimerMode, VecModel};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;
use uuid::Uuid;

slint::include_modules!();

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
struct Settings {
    max_concurrent_downloads: usize,
    default_speed_kib_per_second: u64,
    global_bandwidth_limit_bytes_per_second: u64,
    max_download_mib: u64,
    max_duration_minutes: u64,
    max_segments: u8,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_concurrent_downloads: 3,
            default_speed_kib_per_second: 0,
            global_bandwidth_limit_bytes_per_second: 0,
            max_download_mib: 4096,
            max_duration_minutes: 360,
            max_segments: 1,
        }
    }
}

const HISTORY_LIMIT: usize = 500;
const SETTINGS_LOAD_WARNING: &str =
    "Settings could not be read or are invalid; safe defaults are in use.";

/// Categories shown in the left tree. The identifiers match the ones used by
/// the Slint side (`ui/main.slint`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Category {
    #[default]
    All,
    Incomplete,
    Complete,
    Tasks,
    Documents,
    Music,
    Programs,
    Video,
}

impl Category {
    fn from_id(id: &str) -> Category {
        match id {
            "incomplete" => Category::Incomplete,
            "complete" => Category::Complete,
            "tasks" => Category::Tasks,
            "documents" => Category::Documents,
            "music" => Category::Music,
            "programs" => Category::Programs,
            "video" => Category::Video,
            _ => Category::All,
        }
    }

    fn id(self) -> &'static str {
        match self {
            Category::All => "all",
            Category::Incomplete => "incomplete",
            Category::Complete => "complete",
            Category::Tasks => "tasks",
            Category::Documents => "documents",
            Category::Music => "music",
            Category::Programs => "programs",
            Category::Video => "video",
        }
    }
}

/// State shared between the UI thread and the background queue: the last
/// snapshot of the queue plus the view selection.
#[derive(Default)]
struct UiState {
    snapshots: Vec<JobSnapshot>,
    category: Category,
    selected_id: String,
}

fn can_pause_job(state: &JobState) -> bool {
    matches!(
        state,
        JobState::Queued | JobState::Scheduled | JobState::Downloading
    )
}

fn can_resume_job(state: &JobState) -> bool {
    matches!(state, JobState::Paused)
}

fn can_cancel_job(state: &JobState) -> bool {
    can_pause_job(state) || can_resume_job(state)
}

/// File type bucket used by the category tree. Archives are intentionally not
/// mapped: the application has no archive category.
fn job_file_category(job: &JobSnapshot) -> Category {
    let extension = job
        .destination
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| Path::new(name).extension().and_then(|value| value.to_str()))
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "pdf" | "doc" | "docx" | "odt" | "rtf" | "txt" | "md" | "epub" | "mobi" | "xls"
        | "xlsx" | "ods" | "csv" | "ppt" | "pptx" | "odp" => Category::Documents,
        "mp3" | "flac" | "wav" | "ogg" | "oga" | "m4a" | "aac" | "opus" | "wma" => Category::Music,
        "exe" | "msi" | "apk" | "deb" | "rpm" | "appimage" | "jar" | "bat" | "cmd" | "sh" => {
            Category::Programs
        }
        "mp4" | "mkv" | "avi" | "mov" | "webm" | "flv" | "m4v" | "wmv" | "mpg" | "mpeg" => {
            Category::Video
        }
        _ => Category::All,
    }
}

fn job_matches_category(job: &JobSnapshot, category: Category) -> bool {
    match category {
        Category::All => true,
        Category::Incomplete => !is_terminal_state(&job.state),
        Category::Complete => matches!(job.state, JobState::Completed(_)),
        Category::Tasks => matches!(job.state, JobState::Queued | JobState::Scheduled),
        other => job_file_category(job) == other,
    }
}

fn describe_state(state: &JobState) -> (&'static str, &'static str) {
    match state {
        JobState::Queued => ("Na fila", "queued"),
        JobState::Scheduled => ("Agendado", "queued"),
        JobState::Downloading => ("Baixando", "active"),
        JobState::Paused => ("Pausado", "paused"),
        JobState::Completed(_) => ("Concluído", "done"),
        JobState::Failed(_) | JobState::FailedCleanupPending { .. } => ("Falhou", "error"),
        JobState::Cancelled | JobState::CancelledCleanupPending(_) => ("Cancelado", "muted"),
    }
}

fn priority_label(priority: Priority) -> &'static str {
    match priority {
        Priority::Low => "Baixa",
        Priority::Normal => "Normal",
        Priority::High => "Alta",
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_speed(bytes_per_second: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_second))
}

/// Builds the rows shown in the download list for the given category.
fn build_job_rows(snapshots: &[JobSnapshot], category: Category) -> Vec<JobRow> {
    snapshots
        .iter()
        .filter(|job| job_matches_category(job, category))
        .map(|job| {
            let name = job
                .destination
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("download")
                .to_owned();
            let can_pause = can_pause_job(&job.state);
            let can_resume = can_resume_job(&job.state);
            let can_cancel = can_cancel_job(&job.state);
            let can_set_priority = can_change_job_priority(&job.state);
            let bytes = match &job.state {
                JobState::Completed(result) => result.bytes,
                _ => job.bytes_received,
            };
            let total = job.total_bytes.filter(|total| *total > 0);
            let progress = match total {
                Some(total) => (bytes as f32 / total as f32).clamp(0.0, 1.0),
                None => -1.0f32,
            };
            let progress_text = match total {
                Some(total) => format!("{}%", (bytes as f64 * 100.0 / total as f64) as u64),
                None => "—".to_owned(),
            };
            let show_estimate = matches!(&job.state, JobState::Downloading | JobState::Paused);
            let speed = if show_estimate {
                job.speed_bytes_per_second.filter(|speed| *speed > 0)
            } else {
                None
            };
            let speed_text = speed.map(format_speed).unwrap_or_else(|| "—".to_owned());
            let eta_text = match (total, speed, job.eta_seconds) {
                (Some(total), Some(_), Some(seconds)) if total > bytes => format_eta(seconds),
                _ => "—".to_owned(),
            };
            let size_text = match total {
                Some(total) => format_bytes(total),
                None => format_bytes(bytes),
            };
            let (status_text, status_kind) = describe_state(&job.state);
            let progress_details = format_job_progress(
                bytes,
                job.total_bytes,
                speed,
                if speed.is_some() {
                    job.eta_seconds
                } else {
                    None
                },
            );
            let details = match &job.state {
                JobState::Completed(result) => {
                    let cleanup = result
                        .cleanup_pending
                        .as_ref()
                        .map(|message| format!(" · cleanup pending: {message}"))
                        .unwrap_or_default();
                    format!(
                        "Completed · {} bytes · staging SHA-256 {}{}",
                        result.bytes, result.sha256, cleanup
                    )
                }
                JobState::CancelledCleanupPending(error) => {
                    format!("Cancelled · {progress_details} · cleanup pending: {error}")
                }
                JobState::FailedCleanupPending {
                    operation,
                    cleanup_error,
                } => format!("{operation} · {progress_details} · cleanup pending: {cleanup_error}"),
                JobState::Failed(error) => format!("Failed · {progress_details} · {error}"),
                state => format!("{} · {progress_details}", state.label()),
            };
            JobRow {
                id: job.id.to_string().into(),
                name: name.clone().into(),
                url: safe_display_url(&job.url).into(),
                details: SharedString::from(format!("{details} — {name}")),
                size_text: size_text.into(),
                status_text: status_text.into(),
                status_kind: status_kind.into(),
                progress,
                progress_text: progress_text.into(),
                eta_text: eta_text.into(),
                speed_text: speed_text.into(),
                priority: priority_label(job.priority).into(),
                category: job_file_category(job).id().into(),
                can_set_priority,
                can_pause,
                can_resume,
                can_cancel,
            }
        })
        .collect()
}

/// Pushes the cached queue state into the window: rows, counters, status bar
/// and the capabilities of the selected row.
fn apply_ui_state(window: &MainWindow, ui_state: &Arc<Mutex<UiState>>) {
    let (rows, counts, active, queued, completed, speed, selection, bulk) = {
        let state = ui_state.lock().expect("ui state lock");
        let rows = build_job_rows(&state.snapshots, state.category);
        let mut counts = [0usize; 8];
        for job in &state.snapshots {
            for (index, category) in [
                Category::All,
                Category::Incomplete,
                Category::Complete,
                Category::Tasks,
                Category::Documents,
                Category::Music,
                Category::Programs,
                Category::Video,
            ]
            .into_iter()
            .enumerate()
            {
                if job_matches_category(job, category) {
                    counts[index] += 1;
                }
            }
        }
        let active = state
            .snapshots
            .iter()
            .filter(|job| matches!(job.state, JobState::Downloading))
            .count();
        let queued = state
            .snapshots
            .iter()
            .filter(|job| matches!(job.state, JobState::Queued | JobState::Scheduled))
            .count();
        let completed = state
            .snapshots
            .iter()
            .filter(|job| matches!(job.state, JobState::Completed(_)))
            .count();
        let speed: u64 = state
            .snapshots
            .iter()
            .filter(|job| matches!(job.state, JobState::Downloading))
            .filter_map(|job| job.speed_bytes_per_second)
            .sum();
        let selected = state
            .snapshots
            .iter()
            .find(|job| job.id.to_string() == state.selected_id)
            .cloned();
        let index = rows
            .iter()
            .position(|row| row.id.as_str() == state.selected_id)
            .map(|index| index as i32)
            .unwrap_or(-1);
        let details = rows
            .iter()
            .find(|row| row.id.as_str() == state.selected_id)
            .map(|row| row.details.to_string())
            .unwrap_or_default();
        let bulk = (
            state.snapshots.iter().any(|job| can_pause_job(&job.state)),
            state.snapshots.iter().any(|job| can_resume_job(&job.state)),
            state.snapshots.iter().any(|job| can_cancel_job(&job.state)),
        );
        (
            rows,
            counts,
            active,
            queued,
            completed,
            speed,
            (selected, index, details),
            bulk,
        )
    };

    window.set_jobs(ModelRc::new(VecModel::from(rows)));
    window.set_count_all_text(count_badge(counts[0]));
    window.set_count_incomplete_text(count_badge(counts[1]));
    window.set_count_complete_text(count_badge(counts[2]));
    window.set_count_tasks_text(count_badge(counts[3]));
    window.set_count_documents_text(count_badge(counts[4]));
    window.set_count_music_text(count_badge(counts[5]));
    window.set_count_programs_text(count_badge(counts[6]));
    window.set_count_video_text(count_badge(counts[7]));
    window.set_active_count_text(active.to_string().into());
    window.set_queued_count_text(queued.to_string().into());
    window.set_completed_count_text(completed.to_string().into());
    window.set_global_speed_text(format_speed(speed).into());

    let (can_pause_all, can_resume_all, can_cancel_all) = bulk;
    window.set_can_pause_all(can_pause_all);
    window.set_can_resume_all(can_resume_all);
    window.set_can_cancel_all(can_cancel_all);
    let (selected, index, details) = selection;
    let selected = selected.as_ref();
    window.set_selected_index(index);
    window.set_selected_job_details(details.into());
    window.set_selected_job_can_pause(selected.is_some_and(|job| can_pause_job(&job.state)));
    window.set_selected_job_can_resume(selected.is_some_and(|job| can_resume_job(&job.state)));
    window.set_selected_job_can_cancel(selected.is_some_and(|job| can_cancel_job(&job.state)));
    window.set_selected_job_can_set_priority(
        selected.is_some_and(|job| can_change_job_priority(&job.state)),
    );
}

fn count_badge(count: usize) -> SharedString {
    if count == 0 {
        SharedString::default()
    } else {
        SharedString::from(count.to_string())
    }
}

fn parse_priority(raw: &str) -> Result<Priority, String> {
    match raw.trim() {
        "Low" | "Baixa" => Ok(Priority::Low),
        "Normal" => Ok(Priority::Normal),
        "High" | "Alta" => Ok(Priority::High),
        _ => Err("Unknown download priority.".to_owned()),
    }
}

fn parse_segments(raw: &str) -> Result<u8, String> {
    let value: u8 = raw
        .trim()
        .parse()
        .map_err(|_| "The segment limit must be a number between 1 and 4.".to_owned())?;
    if !(1..=MAX_SEGMENT_COUNT).contains(&value) {
        return Err(format!(
            "The segment limit must be between 1 and {MAX_SEGMENT_COUNT}."
        ));
    }
    Ok(value)
}

/// Parses the per-download speed cap expressed in KiB/s. `None` means the
/// instance default applies.
fn parse_speed_kib(raw: &str) -> Result<Option<u64>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let value: u64 = trimmed
        .parse()
        .map_err(|_| "The speed limit must be a number in KB/s.".to_owned())?;
    if value > 1_000_000 {
        return Err("The speed limit must be at most 1,000,000 KB/s.".to_owned());
    }
    Ok((value > 0).then_some(value.saturating_mul(1024)))
}

struct LoadedSettings {
    settings: Settings,
    warning: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct HistoryDocument {
    generation: String,
    entries: Vec<HistoryEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct HistoryEntry {
    job_id: String,
    url: String,
    status: String,
    destination: String,
    bytes: u64,
    sha256: Option<String>,
    finished_at_utc: String,
}

#[derive(Clone, Default)]
struct HistoryStore {
    generation: String,
    entries: Vec<HistoryEntry>,
    observed_terminal_states: HashMap<Uuid, JobState>,
    suppressed_after_clear: HashSet<Uuid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NotificationKind {
    Completed,
    Failed,
}

#[derive(Default)]
struct NotificationTracker {
    announced: HashSet<Uuid>,
}

impl NotificationTracker {
    fn observe(&mut self, job: &JobSnapshot) -> Option<NotificationKind> {
        let kind = match &job.state {
            JobState::Completed(_) => NotificationKind::Completed,
            JobState::Failed(_) | JobState::FailedCleanupPending { .. } => NotificationKind::Failed,
            _ => return None,
        };
        self.announced.insert(job.id).then_some(kind)
    }
}

#[derive(Default)]
struct ProgressAnnouncementTracker {
    observed: HashMap<Uuid, AnnouncedProgress>,
}

#[derive(Clone, PartialEq, Eq)]
struct AnnouncedProgress {
    state: JobState,
    percentage_bucket: Option<u8>,
}

impl ProgressAnnouncementTracker {
    fn observe(&mut self, job: &JobSnapshot) -> Option<String> {
        let percentage_bucket = job.total_bytes.filter(|total| *total > 0).map(|total| {
            let percentage = (u128::from(job.bytes_received) * 100 / u128::from(total)).min(100);
            ((percentage as u8) / 10) * 10
        });
        let current = AnnouncedProgress {
            state: job.state.clone(),
            percentage_bucket,
        };
        let previous = self.observed.insert(job.id, current.clone());
        let should_announce = previous.as_ref().is_none_or(|previous| {
            previous.state != current.state
                || (current.percentage_bucket.is_some()
                    && previous.percentage_bucket != current.percentage_bucket)
        });
        if !should_announce {
            return None;
        }

        let filename = job
            .destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("download");
        let progress = percentage_bucket
            .map(|percentage| format!(" · {percentage}%"))
            .unwrap_or_else(|| format!(" · {} bytes", job.bytes_received));
        Some(format!("{filename}: {}{progress}", job.state.label()))
    }
}

impl HistoryStore {
    fn record_terminal_snapshots(&mut self, jobs: &[JobSnapshot], timestamp: &str) -> bool {
        let mut changed = false;
        for job in jobs {
            if !is_terminal_state(&job.state) {
                continue;
            }
            if job.history_generation != self.generation {
                self.observed_terminal_states
                    .insert(job.id, job.state.clone());
                continue;
            }
            if self.suppressed_after_clear.contains(&job.id)
                || self.observed_terminal_states.get(&job.id) == Some(&job.state)
            {
                continue;
            }
            let Some(mut entry) = history_entry(job, timestamp) else {
                continue;
            };
            if let Some(existing) = self
                .entries
                .iter_mut()
                .find(|entry| entry.job_id == job.id.to_string())
            {
                entry.finished_at_utc = existing.finished_at_utc.clone();
                if *existing != entry {
                    *existing = entry;
                    changed = true;
                }
            } else {
                self.entries.push(entry);
                changed = true;
            }
            self.observed_terminal_states
                .insert(job.id, job.state.clone());
        }
        if self.entries.len() > HISTORY_LIMIT {
            let excess = self.entries.len() - HISTORY_LIMIT;
            self.entries.drain(..excess);
            changed = true;
        }
        changed
    }

    fn clear_for_jobs(&mut self, jobs: &[JobSnapshot], generation: String) {
        self.entries.clear();
        self.generation = generation;
        for job in jobs {
            if is_terminal_state(&job.state) {
                self.suppressed_after_clear.insert(job.id);
                self.observed_terminal_states
                    .insert(job.id, job.state.clone());
            }
        }
    }
}

#[derive(Clone, Default)]
struct InboxPollGate(Arc<tokio::sync::Mutex<()>>);

impl InboxPollGate {
    fn try_enter(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.0.clone().try_lock_owned().ok()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args_os().any(|argument| argument.to_string_lossy() == "--native-host") {
        return native_messaging::run_stdio().map_err(Into::into);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let data_dir = native_messaging::application_data_dir().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "application data directory is unavailable",
        )
    })?;
    std::fs::create_dir_all(&data_dir)?;
    let settings_path = data_dir.join("settings.json");
    let history_dir = prepare_history_directory(&data_dir)?;
    let history_path = history_dir.join("history.json");
    migrate_legacy_history(&data_dir.join("history.json"), &history_path)?;
    let LoadedSettings {
        settings,
        warning: settings_warning,
    } = load_settings(&settings_path);
    let queue = DownloadQueue::new_with_global_bandwidth_limit(
        settings.max_concurrent_downloads,
        settings.global_bandwidth_limit_bytes_per_second,
    )?;
    queue.set_max_segments(settings.max_segments)?;
    let settings = Arc::new(Mutex::new(settings));
    let history_document = load_history(&history_path)?;
    let history_store = Arc::new(Mutex::new(HistoryStore {
        generation: history_document.generation,
        entries: history_document.entries,
        observed_terminal_states: HashMap::new(),
        suppressed_after_clear: HashSet::new(),
    }));
    let notification_tracker = Arc::new(Mutex::new(NotificationTracker::default()));
    let progress_announcement_tracker =
        Arc::new(Mutex::new(ProgressAnnouncementTracker::default()));
    let downloads_dir = user_download_directory();
    std::fs::create_dir_all(&downloads_dir)?;

    let window = MainWindow::new()?;
    if let Some(warning) = settings_warning {
        window.set_status(warning.into());
    }
    window.set_history(ModelRc::new(VecModel::from(history_rows(
        &history_store.lock().expect("history lock").entries,
    ))));
    window.set_max_active(
        settings
            .lock()
            .expect("settings lock")
            .max_concurrent_downloads
            .to_string()
            .into(),
    );
    window.set_speed_limit(
        settings
            .lock()
            .expect("settings lock")
            .default_speed_kib_per_second
            .to_string()
            .into(),
    );
    window.set_global_bandwidth_limit(
        settings
            .lock()
            .expect("settings lock")
            .global_bandwidth_limit_bytes_per_second
            .to_string()
            .into(),
    );
    window.set_max_download_mib(
        settings
            .lock()
            .expect("settings lock")
            .max_download_mib
            .to_string()
            .into(),
    );
    window.set_max_duration_minutes(
        settings
            .lock()
            .expect("settings lock")
            .max_duration_minutes
            .to_string()
            .into(),
    );
    window.set_max_segments(
        settings
            .lock()
            .expect("settings lock")
            .max_segments
            .to_string()
            .into(),
    );
    let handle = runtime.handle().clone();
    let weak = window.as_weak();
    let ui_state = Arc::new(Mutex::new(UiState::default()));
    window.set_downloads_dir(downloads_dir.to_string_lossy().to_string().into());
    {
        let ui_state = ui_state.clone();
        let weak = weak.clone();
        window.on_select_category(move |raw_category| {
            let category = Category::from_id(raw_category.as_ref());
            {
                let mut state = ui_state.lock().expect("ui state lock");
                state.category = category;
            }
            let ui_state = ui_state.clone();
            let weak = weak.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    window.set_selected_category(category.id().into());
                    apply_ui_state(&window, &ui_state);
                }
            });
        });
    }
    {
        let ui_state = ui_state.clone();
        let weak = weak.clone();
        window.on_select_job(move |raw_id| {
            let id = raw_id.to_string();
            {
                let mut state = ui_state.lock().expect("ui state lock");
                state.selected_id = id.clone();
            }
            let ui_state = ui_state.clone();
            let weak = weak.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    window.set_selected_job_id(id.into());
                    apply_ui_state(&window, &ui_state);
                }
            });
        });
    }
    {
        let ui_state = ui_state.clone();
        let weak = weak.clone();
        window.on_move_selection(move |raw_index| {
            let requested = raw_index;
            let selected = {
                let mut state = ui_state.lock().expect("ui state lock");
                let rows = build_job_rows(&state.snapshots, state.category);
                if rows.is_empty() {
                    None
                } else {
                    let index = requested.clamp(0, rows.len() as i32 - 1) as usize;
                    let id = rows[index].id.to_string();
                    state.selected_id = id.clone();
                    Some(id)
                }
            };
            let Some(selected) = selected else { return };
            let ui_state = ui_state.clone();
            let weak = weak.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    window.set_selected_job_id(selected.clone().into());
                    apply_ui_state(&window, &ui_state);
                }
            });
        });
    }
    {
        let queue = queue.clone();
        let weak = weak.clone();
        let handle = runtime.handle().clone();
        window.on_bulk_action(move |raw_action| {
            let action = raw_action.to_string();
            let queue = queue.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let snapshots = queue.snapshots().await;
                let mut affected = 0usize;
                let mut failures: Vec<String> = Vec::new();
                for job in &snapshots {
                    let outcome = match action.as_str() {
                        "pause-all" if can_pause_job(&job.state) => queue.pause(job.id).await,
                        "resume-all" if can_resume_job(&job.state) => queue.resume(job.id).await,
                        "cancel-all" if can_cancel_job(&job.state) => queue.cancel(job.id).await,
                        _ => continue,
                    };
                    match outcome {
                        Ok(()) => affected += 1,
                        Err(error) => failures.push(format!("{}: {error}", job.id)),
                    }
                }
                let message = match action.as_str() {
                    "pause-all" => format!("Paused {affected} download(s)."),
                    "resume-all" => format!("Resumed {affected} download(s)."),
                    "cancel-all" => format!("Cancelled {affected} download(s)."),
                    other => format!("Unknown bulk action: {other}"),
                };
                let message = if failures.is_empty() {
                    message
                } else {
                    format!("{message} Failed: {}", failures.join("; "))
                };
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_status(message.into());
                    }
                });
            });
        });
    }
    {
        window.on_quit_app(move || {
            let _ = slint::quit_event_loop();
        });
    }
    {
        let queue = queue.clone();
        let settings = settings.clone();
        let downloads_dir = downloads_dir.clone();
        let history_path = history_path.clone();
        let add_settings_path = settings_path.clone();
        window.on_add_url(
            move |raw_url,
                  raw_sha256,
                  raw_priority,
                  raw_segments,
                  raw_speed,
                  schedule_enabled,
                  schedule_start,
                  schedule_end| {
                let url = raw_url.to_string();
                let expected_hash = raw_sha256.to_string();
                let priority = raw_priority.to_string();
                let segments = raw_segments.to_string();
                let speed = raw_speed.to_string();
                let schedule_start = schedule_start.to_string();
                let schedule_end = schedule_end.to_string();
                let destination_dir = downloads_dir.clone();
                let queue = queue.clone();
                let settings = settings.clone();
                let history_path = history_path.clone();
                let settings_path = add_settings_path.clone();
                let weak = weak.clone();
                handle.spawn(async move {
                    let result = async {
                        let url = validate_http_url(&url)
                            .map_err(|error| error.to_string())?
                            .to_string();
                        let expected_sha256 = parse_expected_sha256(&expected_hash)?;
                        let priority = parse_priority(&priority)?;
                        let segments = parse_segments(&segments)?;
                        let requested_speed = parse_speed_kib(&speed)?;
                        let schedule = if schedule_enabled {
                            Some(
                                DailyWindow::parse_utc_range(&schedule_start, &schedule_end)
                                    .ok_or_else(|| {
                                        "Schedule must use valid 24-hour HH:MM values in UTC."
                                            .to_owned()
                                    })?,
                            )
                        } else {
                            None
                        };
                        let filename = filename_from_url(&url);
                        let destination = unique_destination(&destination_dir, &filename);
                        let current_settings = settings
                            .lock()
                            .map_err(|_| "settings unavailable".to_owned())?
                            .clone();
                        // The engine applies the segment limit to the whole instance, so the
                        // value chosen in the dialog is stored as the instance setting.
                        queue
                            .set_max_segments(segments)
                            .map_err(|error| error.to_string())?;
                        let mut next_settings = current_settings.clone();
                        next_settings.max_segments = segments;
                        if let Err(error) = save_settings(&settings_path, &next_settings) {
                            return Err(format!("Could not save the segment limit: {error}"));
                        }
                        replace_settings_state(&settings, next_settings);
                        let bytes_per_second = requested_speed.or_else(|| {
                            (current_settings.default_speed_kib_per_second > 0).then(|| {
                                current_settings
                                    .default_speed_kib_per_second
                                    .saturating_mul(1024)
                            })
                        });
                        let request = DownloadRequest {
                            url: url.clone(),
                            destination,
                            expected_sha256,
                            max_retries: 3,
                            bytes_per_second,
                            max_bytes: current_settings
                                .max_download_mib
                                .saturating_mul(1024 * 1024),
                            max_duration: Duration::from_secs(
                                current_settings.max_duration_minutes.saturating_mul(60),
                            ),
                        };
                        let id = enqueue_with_history_generation(
                            &queue,
                            &history_path,
                            request,
                            schedule,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        if let Err(error) = queue.set_priority(id, priority).await {
                            return Err(format!(
                                "Queued, but the priority could not be applied: {error}"
                            ));
                        }
                        Ok(filename)
                    }
                    .await;
                    let (message, failed) = match result {
                        Ok(filename) => (format!("Queued: {filename}"), false),
                        Err(error) => (error, true),
                    };
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_status(message.clone().into());
                            if failed {
                                window.set_add_dialog_error(message.into());
                            } else {
                                window.set_add_dialog_error("".into());
                                window.set_add_dialog_open(false);
                                window.set_max_segments(segments.to_string().into());
                            }
                        }
                    });
                });
            },
        );
    }
    {
        let queue = queue.clone();
        let weak = window.as_weak();
        let handle = runtime.handle().clone();
        window.on_job_action(move |raw_id, raw_action| {
            let action = raw_action.to_string();
            let raw_id = raw_id.to_string();
            let id = match Uuid::parse_str(&raw_id) {
                Ok(id) => id,
                Err(_) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_status("Invalid queue item identifier.".into());
                    }
                    return;
                }
            };
            if !matches!(action.as_str(), "pause" | "resume" | "cancel") {
                if let Some(window) = weak.upgrade() {
                    window.set_status("Unknown queue action.".into());
                }
                return;
            }
            if let Some(window) = weak.upgrade() {
                window.set_status(format!("{action} requested…").into());
            }
            let queue = queue.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let result = match action.as_str() {
                    "pause" => queue.pause(id).await,
                    "resume" => queue.resume(id).await,
                    "cancel" => queue.cancel(id).await,
                    _ => unreachable!("action was checked before spawning"),
                };
                let snapshots = queue.snapshots().await;
                let current_state = snapshots
                    .iter()
                    .find(|job| job.id == id)
                    .map(|job| job.state.clone());
                let message = queue_action_message(
                    &action,
                    result.map_err(|error| error.to_string()),
                    current_state.as_ref(),
                )
                .into();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_status(message);
                    }
                });
            });
        });
    }
    {
        let queue = queue.clone();
        let weak = window.as_weak();
        let handle = runtime.handle().clone();
        window.on_set_job_priority(move |raw_id, raw_priority| {
            let raw_id = raw_id.to_string();
            let raw_priority = raw_priority.to_string();
            let id = match Uuid::parse_str(&raw_id) {
                Ok(id) => id,
                Err(_) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_status("Select a valid queue item first.".into());
                    }
                    return;
                }
            };
            let priority = match raw_priority.as_str() {
                "Low" => Priority::Low,
                "Normal" => Priority::Normal,
                "High" => Priority::High,
                _ => {
                    if let Some(window) = weak.upgrade() {
                        window.set_status("Unknown download priority.".into());
                    }
                    return;
                }
            };
            let queue = queue.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let result = queue.set_priority(id, priority).await;
                let message = match result {
                    Ok(()) => format!("Priority set to {}.", priority.label()),
                    Err(error) => format!("Could not change priority: {error}"),
                };
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_status(message.into());
                    }
                });
            });
        });
    }
    {
        let settings = settings.clone();
        let weak = window.as_weak();
        let settings_path = settings_path.clone();
        let queue = queue.clone();
        window.on_save_limits(
            move |concurrency, speed, global_bandwidth, max_size, max_duration, max_segments| {
                let parsed = (
                    concurrency.parse::<usize>(),
                    speed.parse::<u64>(),
                    global_bandwidth.parse::<u64>(),
                    max_size.parse::<u64>(),
                    max_duration.parse::<u64>(),
                    max_segments.parse::<u8>(),
                );
                let message = match parsed {
                    (
                        Ok(concurrency),
                        Ok(speed),
                        Ok(global_bandwidth),
                        Ok(max_size),
                        Ok(max_duration),
                        Ok(max_segments),
                    ) if (1..=8).contains(&concurrency)
                        && speed <= 1_000_000
                        && global_bandwidth <= MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND
                        && (1..=1_048_576).contains(&max_size)
                        && (1..=10_080).contains(&max_duration)
                        && (1..=MAX_SEGMENT_COUNT).contains(&max_segments) =>
                    {
                        let next = Settings {
                            max_concurrent_downloads: concurrency,
                            default_speed_kib_per_second: speed,
                            global_bandwidth_limit_bytes_per_second: global_bandwidth,
                            max_download_mib: max_size,
                            max_duration_minutes: max_duration,
                            max_segments,
                        };
                        match save_settings(&settings_path, &next) {
                            Ok(()) => {
                                let global_bandwidth =
                                    next.global_bandwidth_limit_bytes_per_second;
                                match queue.set_global_bandwidth_limit(global_bandwidth) {
                                    Ok(()) => {
                                        if let Err(error) = queue.set_max_segments(max_segments) {
                                            let message = format!(
                                                "Settings were saved, but the segment limit could not be applied: {error}"
                                            );
                                            let weak = weak.clone();
                                            let _ = slint::invoke_from_event_loop(move || {
                                                if let Some(window) = weak.upgrade() {
                                                    window.set_status(message.into());
                                                }
                                            });
                                            return;
                                        }
                                        replace_settings_state(&settings, next);
                                        "Saved. Shared pacing and segment limits apply to this instance; per-download speed applies to new downloads; restart to apply job concurrency.".to_owned()
                                    }
                                    Err(error) => format!(
                                        "Settings were saved, but the global bandwidth limit could not be applied: {error}"
                                    ),
                                }
                            }
                            Err(error) => format!("Could not save settings: {error}"),
                        }
                    }
                    _ => "Limits must be concurrency 1–8, segments 1–4, per-download speed 0–1,000,000 KB/s, global speed 0–1,000,000,000 bytes/s, size 1–1,048,576 MiB, and duration 1–10,080 minutes.".to_owned(),
                };
                let weak = weak.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_status(message.into());
                    }
                });
            },
        );
    }
    {
        let history_store = history_store.clone();
        let history_path = history_path.clone();
        let queue = queue.clone();
        let weak = window.as_weak();
        let handle = runtime.handle().clone();
        window.on_clear_history(move || {
            let history_store = history_store.clone();
            let history_path = history_path.clone();
            let queue = queue.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let snapshots = queue.snapshots().await;
                let (rows, message) = {
                    let mut store = history_store.lock().expect("history lock");
                    match clear_history(&history_path, &mut store, &snapshots) {
                        Ok(()) => (history_rows(&store.entries), "History cleared.".to_owned()),
                        Err(error) => (
                            history_rows(&store.entries),
                            format!("Could not clear history: {error}"),
                        ),
                    }
                };
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_history(ModelRc::new(VecModel::from(rows)));
                        window.set_status(message.into());
                    }
                });
            });
        });
    }
    {
        let weak = window.as_weak();
        window.on_dismiss_notification(move || {
            if let Some(window) = weak.upgrade() {
                window.set_in_app_notification(SharedString::default());
            }
        });
    }
    {
        let queue = queue.clone();
        let weak = window.as_weak();
        let handle = runtime.handle().clone();
        window.on_retry_pending_cleanup(move || {
            if let Some(window) = weak.upgrade() {
                window.set_cleanup_retry_in_progress(true);
            }
            let queue = queue.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let pending_ids = queue
                    .snapshots()
                    .await
                    .into_iter()
                    .filter(|job| job_has_pending_cleanup(&job.state))
                    .map(|job| job.id)
                    .collect::<Vec<_>>();
                for id in pending_ids.iter().copied() {
                    let _ = queue.retry_cleanup(id).await;
                }
                let pending = queue
                    .snapshots()
                    .await
                    .into_iter()
                    .filter_map(|job| {
                        cleanup_diagnostic(&job.state)
                            .map(|diagnostic| format!("{}: {diagnostic}", job.id))
                    })
                    .collect::<Vec<_>>();
                let message = if !pending.is_empty() {
                    format!("Cleanup remains pending: {}", pending.join("; "))
                } else if pending_ids.is_empty() {
                    "No pending cleanup was found.".to_owned()
                } else {
                    "Cleanup completed for the previously pending jobs.".to_owned()
                };
                let has_pending_cleanup = !pending.is_empty();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        window.set_status(message.into());
                        window.set_has_pending_cleanup(has_pending_cleanup);
                        window.set_cleanup_retry_in_progress(false);
                    }
                });
            });
        });
    }

    let timer = slint::Timer::default();
    {
        let queue = queue.clone();
        let settings = settings.clone();
        let data_dir = data_dir.clone();
        let downloads_dir = downloads_dir.clone();
        let history_path = history_path.clone();
        let history_store = history_store.clone();
        let notification_tracker = notification_tracker.clone();
        let progress_announcement_tracker = progress_announcement_tracker.clone();
        let ui_state = ui_state.clone();
        let weak = window.as_weak();
        let handle: Handle = runtime.handle().clone();
        let inbox_poll_gate = InboxPollGate::default();
        timer.start(TimerMode::Repeated, Duration::from_secs(1), move || {
            let Some(poll_guard) = inbox_poll_gate.try_enter() else {
                return;
            };
            let queue = queue.clone();
            let settings = settings.clone();
            let data_dir = data_dir.clone();
            let downloads_dir = downloads_dir.clone();
            let history_path = history_path.clone();
            let history_store = history_store.clone();
            let notification_tracker = notification_tracker.clone();
            let progress_announcement_tracker = progress_announcement_tracker.clone();
            let ui_state = ui_state.clone();
            let weak = weak.clone();
            handle.spawn(async move {
                let _poll_guard = poll_guard;
                let mut inbox_status = None;
                loop {
                    let claim = match native_messaging::claim_next_pending_url(&data_dir) {
                        Ok(Some(claim)) => claim,
                        Ok(None) => break,
                        Err(error) => {
                            inbox_status =
                                Some(format!("Could not claim Chrome inbox item: {error}"));
                            break;
                        }
                    };
                    let url = claim.url().to_owned();
                    let destination = unique_destination(&downloads_dir, &filename_from_url(&url));
                    let current_settings = settings
                        .lock()
                        .map(|value| value.clone())
                        .unwrap_or_default();
                    let request = DownloadRequest {
                        url,
                        destination,
                        expected_sha256: None,
                        max_retries: 3,
                        bytes_per_second: (current_settings.default_speed_kib_per_second > 0).then(
                            || {
                                current_settings
                                    .default_speed_kib_per_second
                                    .saturating_mul(1024)
                            },
                        ),
                        max_bytes: current_settings
                            .max_download_mib
                            .saturating_mul(1024 * 1024),
                        max_duration: Duration::from_secs(
                            current_settings.max_duration_minutes.saturating_mul(60),
                        ),
                    };
                    if let Err(error) =
                        enqueue_pending_claim_with_history(&queue, &history_path, claim, request)
                            .await
                    {
                        inbox_status = Some(error);
                        break;
                    }
                }
                let snapshots = queue.snapshots().await;
                let progress_announcement = {
                    let mut tracker = progress_announcement_tracker
                        .lock()
                        .expect("progress announcement tracker lock");
                    let messages = snapshots
                        .iter()
                        .filter_map(|job| tracker.observe(job))
                        .collect::<Vec<_>>();
                    (!messages.is_empty()).then(|| messages.join("; "))
                };
                let history_save_error = {
                    let mut current = history_store.lock().expect("history lock");
                    persist_terminal_history(
                        &history_path,
                        &mut current,
                        &snapshots,
                        &current_utc_timestamp(),
                    )
                    .err()
                    .map(|error| error.to_string())
                };
                let history_rows = {
                    let history = history_store.lock().expect("history lock");
                    history_rows(&history.entries)
                };
                let notification_messages = {
                    let mut tracker = notification_tracker
                        .lock()
                        .expect("notification tracker lock");
                    snapshots
                        .iter()
                        .filter_map(|job| {
                            tracker
                                .observe(job)
                                .map(|kind| notification_message(job, kind))
                        })
                        .collect::<Vec<_>>()
                };
                let notification =
                    (!notification_messages.is_empty()).then(|| notification_messages.join("\n"));
                let status_message = match (inbox_status, history_save_error) {
                    (Some(inbox), Some(history)) => Some(format!("{inbox}; {history}")),
                    (Some(inbox), None) => Some(inbox),
                    (None, Some(history)) => Some(format!("Could not save history: {history}")),
                    (None, None) => None,
                };
                let has_pending_cleanup = snapshots
                    .iter()
                    .any(|job| job_has_pending_cleanup(&job.state));
                {
                    let mut state = ui_state.lock().expect("ui state lock");
                    state.snapshots = snapshots.clone();
                }
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = weak.upgrade() {
                        if let Some(message) = status_message {
                            window.set_status(message.into());
                        }
                        if let Some(message) = notification {
                            window.set_in_app_notification(message.into());
                        }
                        if let Some(message) = progress_announcement {
                            window.set_progress_announcement(message.into());
                        }
                        window.set_has_pending_cleanup(has_pending_cleanup);
                        window.set_history(ModelRc::new(VecModel::from(history_rows)));
                        apply_ui_state(&window, &ui_state);
                    }
                });
            });
        });
    }
    window.run()?;
    drop(timer);
    drop(runtime);
    Ok(())
}

fn job_has_pending_cleanup(state: &JobState) -> bool {
    cleanup_diagnostic(state).is_some()
}

fn format_job_progress(
    bytes_received: u64,
    total_bytes: Option<u64>,
    speed_bytes_per_second: Option<u64>,
    eta_seconds: Option<u64>,
) -> String {
    let progress = match total_bytes {
        Some(total) if total > 0 => {
            let percentage = (u128::from(bytes_received) * 100 / u128::from(total)).min(100);
            format!("{bytes_received}/{total} bytes ({percentage}%)")
        }
        Some(total) => format!("{bytes_received}/{total} bytes"),
        None => format!("{bytes_received} bytes"),
    };
    let stable_speed = speed_bytes_per_second.filter(|speed| *speed > 0);
    let speed = stable_speed
        .map(|speed| format!(" · {speed} B/s"))
        .unwrap_or_default();
    let eta = if total_bytes.is_some_and(|total| total > bytes_received) && stable_speed.is_some() {
        eta_seconds
            .map(|seconds| format!(" · ETA ~ {}", format_eta(seconds)))
            .unwrap_or_default()
    } else {
        String::new()
    };
    format!("{progress}{speed}{eta}")
}

fn format_eta(total_seconds: u64) -> String {
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn is_terminal_state(state: &JobState) -> bool {
    matches!(
        state,
        JobState::Completed(_)
            | JobState::Failed(_)
            | JobState::FailedCleanupPending { .. }
            | JobState::Cancelled
            | JobState::CancelledCleanupPending(_)
    )
}

fn can_change_job_priority(state: &JobState) -> bool {
    !is_terminal_state(state)
}

fn history_entry(job: &JobSnapshot, finished_at_utc: &str) -> Option<HistoryEntry> {
    if !is_terminal_state(&job.state) {
        return None;
    }
    let (bytes, sha256) = match &job.state {
        JobState::Completed(result) => (result.bytes, Some(result.sha256.clone())),
        _ => (job.bytes_received, None),
    };
    let status = match &job.state {
        JobState::Completed(result) if result.cleanup_pending.is_some() => {
            "Completed (cleanup pending)".to_owned()
        }
        _ => job.state.label().to_owned(),
    };
    Some(HistoryEntry {
        job_id: job.id.to_string(),
        url: redact_history_url(&job.url),
        status,
        destination: history_destination_basename(&job.destination.to_string_lossy()),
        bytes,
        sha256,
        finished_at_utc: finished_at_utc.to_owned(),
    })
}

fn redact_history_url(raw_url: &str) -> String {
    url::Url::parse(raw_url)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .map(|url| format!("{}{}", url.origin().ascii_serialization(), url.path()))
        .unwrap_or_else(|| "Invalid URL".to_owned())
}

fn history_destination_basename(destination: &str) -> String {
    destination
        .rsplit(['/', '\\'])
        .next()
        .filter(|basename| !basename.is_empty() && *basename != "." && *basename != "..")
        .unwrap_or("download")
        .to_owned()
}

fn format_utc_timestamp(unix_seconds: u64) -> String {
    let days = (unix_seconds / 86_400) as i64;
    let seconds_of_day = unix_seconds % 86_400;
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn current_utc_timestamp() -> String {
    format_utc_timestamp(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

fn prepare_history_directory(data_dir: &Path) -> io::Result<PathBuf> {
    let history_dir = data_dir.join("history");
    fs::create_dir_all(&history_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&history_dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(history_dir)
}

fn acquire_history_lock(history_path: &Path) -> io::Result<File> {
    let parent = history_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let lock_path = parent.join("history.lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock_file = options.open(lock_path)?;
    set_history_file_private(&lock_file)?;
    lock_file.lock()?;
    Ok(lock_file)
}

#[cfg(unix)]
fn set_history_file_private(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_history_file_private(_file: &File) -> io::Result<()> {
    // Windows ACLs are inherited from the per-user app-data directory and are not verified here.
    Ok(())
}

enum HistoryFileState {
    Missing,
    Valid {
        document: HistoryDocument,
        needs_rewrite: bool,
    },
    Corrupt,
}

fn read_history_unlocked(path: &Path) -> io::Result<HistoryFileState> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(HistoryFileState::Missing);
        }
        Err(error) => return Err(error),
    };
    set_history_file_private(&file)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    drop(file);

    if let Ok(mut document) = serde_json::from_slice::<HistoryDocument>(&bytes) {
        let Some(generation) = Uuid::parse_str(&document.generation).ok() else {
            return Ok(HistoryFileState::Corrupt);
        };
        let original = document.clone();
        document.generation = generation.to_string();
        document.entries = normalize_history_entries(document.entries);
        return Ok(HistoryFileState::Valid {
            needs_rewrite: document != original,
            document,
        });
    }

    match serde_json::from_slice::<Vec<HistoryEntry>>(&bytes) {
        Ok(entries) => Ok(HistoryFileState::Valid {
            document: HistoryDocument {
                generation: Uuid::new_v4().to_string(),
                entries: normalize_history_entries(entries),
            },
            needs_rewrite: true,
        }),
        Err(_) => Ok(HistoryFileState::Corrupt),
    }
}

fn normalize_history_entries(mut entries: Vec<HistoryEntry>) -> Vec<HistoryEntry> {
    let mut seen_ids = HashSet::new();
    let mut normalized = entries
        .drain(..)
        .rev()
        .filter_map(|mut entry| {
            if !seen_ids.insert(entry.job_id.clone()) {
                return None;
            }
            entry.url = redact_history_url(&entry.url);
            entry.destination = history_destination_basename(&entry.destination);
            Some(entry)
        })
        .collect::<Vec<_>>();
    normalized.reverse();
    if normalized.len() > HISTORY_LIMIT {
        let excess = normalized.len() - HISTORY_LIMIT;
        normalized.drain(..excess);
    }
    normalized
}

fn load_history(path: &Path) -> io::Result<HistoryDocument> {
    let _lock = acquire_history_lock(path)?;
    load_or_initialize_history_unlocked(path)
}

fn load_or_initialize_history_unlocked(path: &Path) -> io::Result<HistoryDocument> {
    match read_history_unlocked(path)? {
        HistoryFileState::Missing => {
            let document = HistoryDocument {
                generation: Uuid::new_v4().to_string(),
                entries: Vec::new(),
            };
            write_history_unlocked(path, &document)?;
            Ok(document)
        }
        HistoryFileState::Valid {
            document,
            needs_rewrite,
        } => {
            if needs_rewrite {
                write_history_unlocked(path, &document)?;
            }
            Ok(document)
        }
        HistoryFileState::Corrupt => Err(invalid_history_error()),
    }
}

fn invalid_history_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "history data is malformed")
}

fn migrate_legacy_history(legacy_path: &Path, history_path: &Path) -> io::Result<()> {
    let _lock = acquire_history_lock(history_path)?;
    let current = read_history_unlocked(history_path)?;
    let legacy = read_history_unlocked(legacy_path)?;
    match (current, legacy) {
        (
            HistoryFileState::Valid {
                document,
                needs_rewrite,
            },
            legacy,
        ) => {
            let legacy_is_valid = matches!(legacy, HistoryFileState::Valid { .. });
            if needs_rewrite || legacy_is_valid {
                write_history_unlocked(history_path, &document)?;
            }
            if legacy_is_valid {
                remove_legacy_history(legacy_path)?;
            }
            Ok(())
        }
        (HistoryFileState::Missing, HistoryFileState::Valid { document, .. })
        | (HistoryFileState::Corrupt, HistoryFileState::Valid { document, .. }) => {
            write_history_unlocked(history_path, &document)?;
            write_history_unlocked(legacy_path, &document)?;
            remove_legacy_history(legacy_path)
        }
        (HistoryFileState::Missing, HistoryFileState::Missing) => Ok(()),
        (HistoryFileState::Missing, HistoryFileState::Corrupt)
        | (HistoryFileState::Corrupt, HistoryFileState::Missing | HistoryFileState::Corrupt) => {
            Err(invalid_history_error())
        }
    }
}

fn remove_legacy_history(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn persist_terminal_history(
    path: &Path,
    store: &mut HistoryStore,
    jobs: &[JobSnapshot],
    timestamp: &str,
) -> io::Result<()> {
    let _lock = acquire_history_lock(path)?;
    let document = load_or_initialize_history_unlocked(path)?;
    let mut candidate = HistoryStore {
        generation: document.generation,
        entries: document.entries,
        observed_terminal_states: store.observed_terminal_states.clone(),
        suppressed_after_clear: store.suppressed_after_clear.clone(),
    };
    let changed = candidate.record_terminal_snapshots(jobs, timestamp);
    if changed {
        write_history_unlocked(
            path,
            &HistoryDocument {
                generation: candidate.generation.clone(),
                entries: candidate.entries.clone(),
            },
        )?;
    }
    *store = candidate;
    Ok(())
}

fn clear_history(path: &Path, store: &mut HistoryStore, jobs: &[JobSnapshot]) -> io::Result<()> {
    let _lock = acquire_history_lock(path)?;
    let document = load_or_initialize_history_unlocked(path)?;
    let generation = Uuid::new_v4().to_string();
    let mut candidate = HistoryStore {
        generation: document.generation,
        entries: document.entries,
        observed_terminal_states: store.observed_terminal_states.clone(),
        suppressed_after_clear: store.suppressed_after_clear.clone(),
    };
    candidate.clear_for_jobs(jobs, generation.clone());
    write_history_unlocked(
        path,
        &HistoryDocument {
            generation,
            entries: candidate.entries.clone(),
        },
    )?;
    *store = candidate;
    Ok(())
}

fn write_history_unlocked(path: &Path, document: &HistoryDocument) -> io::Result<()> {
    let generation = Uuid::parse_str(&document.generation)
        .map_err(|_| invalid_history_error())?
        .to_string();
    let normalized = HistoryDocument {
        generation,
        entries: normalize_history_entries(document.entries.clone()),
    };
    let contents = serde_json::to_vec_pretty(&normalized)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let (temporary, mut file) = create_unique_history_temp(path)?;
    let write_result = file.write_all(&contents).and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = write_result {
        return Err(cleanup_data_temp(&temporary, error));
    }
    if let Err(error) = replace_settings_file(&temporary, path) {
        return Err(cleanup_data_temp(&temporary, error));
    }
    match read_history_unlocked(path)? {
        HistoryFileState::Valid { document, .. } if document == normalized => Ok(()),
        HistoryFileState::Missing | HistoryFileState::Valid { .. } | HistoryFileState::Corrupt => {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "history replacement could not be verified",
            ))
        }
    }
}

fn create_unique_history_temp(path: &Path) -> io::Result<(PathBuf, File)> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "history path has no file name")
    })?;
    for _ in 0..16 {
        let temporary = parent.join(format!(
            ".{}.tmp-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary) {
            Ok(file) => {
                if let Err(error) = set_history_file_private(&file) {
                    drop(file);
                    return Err(cleanup_data_temp(&temporary, error));
                }
                return Ok((temporary, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique history temporary file",
    ))
}

fn history_rows(entries: &[HistoryEntry]) -> Vec<HistoryRow> {
    entries
        .iter()
        .rev()
        .map(|entry| {
            let sha256 = entry.sha256.as_deref().unwrap_or("not available");
            HistoryRow {
                details: SharedString::from(format!(
                    "{} · {} · {} bytes · SHA-256 {} · {} · {}",
                    entry.finished_at_utc,
                    entry.status,
                    entry.bytes,
                    sha256,
                    entry.destination,
                    entry.url
                )),
            }
        })
        .collect()
}

fn notification_message(job: &JobSnapshot, kind: NotificationKind) -> String {
    let filename = job
        .destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    let outcome = match kind {
        NotificationKind::Completed => "completed",
        NotificationKind::Failed => "failed",
    };
    format!(
        "Download {outcome}: {filename} · {}",
        redact_history_url(&job.url)
    )
}

fn parse_expected_sha256(value: &str) -> Result<Option<String>, String> {
    if value.is_empty() {
        return Ok(None);
    }
    normalize_sha256(value).map(Some).ok_or_else(|| {
        "Expected SHA-256 must contain exactly 64 hexadecimal characters.".to_owned()
    })
}

fn queue_action_message(
    action: &str,
    result: Result<(), String>,
    current_state: Option<&JobState>,
) -> String {
    if matches!(current_state, Some(JobState::Completed(_))) {
        return format!("Download already completed; {action} was not applied.");
    }
    match result {
        Err(error) => match current_state {
            Some(state) => format!(
                "Could not {action} download: {error} (current status: {}).",
                state.label()
            ),
            None => format!("Could not {action} download: {error}; job is no longer in the queue."),
        },
        Ok(()) => match (action, current_state) {
            ("pause", Some(JobState::Paused)) => "Download paused.".to_owned(),
            ("resume", Some(JobState::Queued)) => {
                "Download resumed; current status: Queued.".to_owned()
            }
            ("resume", Some(JobState::Scheduled)) => {
                "Download resumed; current status: Scheduled.".to_owned()
            }
            ("resume", Some(JobState::Downloading)) => {
                "Download resumed; current status: Downloading.".to_owned()
            }
            ("cancel", Some(JobState::Cancelled)) => "Download cancelled.".to_owned(),
            ("cancel", Some(JobState::CancelledCleanupPending(error))) => {
                format!("Download cancelled; cleanup pending: {error}")
            }
            (_, Some(state)) => format!(
                "{action} was not applied; current status: {}.",
                state.label()
            ),
            (_, None) => format!("{action} returned without a current queue item."),
        },
    }
}

#[derive(Debug)]
enum EnqueueError {
    History(io::Error),
    Queue(QueueError),
}

impl std::fmt::Display for EnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::History(error) => write!(formatter, "history storage unavailable: {error}"),
            Self::Queue(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<QueueError> for EnqueueError {
    fn from(error: QueueError) -> Self {
        Self::Queue(error)
    }
}

async fn enqueue_with_history_generation(
    queue: &DownloadQueue,
    history_path: &Path,
    request: DownloadRequest,
    window: Option<DailyWindow>,
) -> Result<Uuid, EnqueueError> {
    let _lock = acquire_history_lock(history_path).map_err(EnqueueError::History)?;
    let document =
        load_or_initialize_history_unlocked(history_path).map_err(EnqueueError::History)?;
    queue
        .enqueue_with_history_generation(request, window, document.generation)
        .await
        .map_err(EnqueueError::Queue)
}

#[cfg(test)]
async fn enqueue_pending_claim(
    queue: &DownloadQueue,
    claim: PendingUrlClaim,
    request: DownloadRequest,
) -> Result<(), String> {
    let url = claim.url().to_owned();
    let destination = request.destination.clone();
    let result = queue
        .enqueue(request, None)
        .await
        .map_err(EnqueueError::Queue);
    settle_pending_claim(claim, url, destination, result, queue.snapshots()).await
}

async fn enqueue_pending_claim_with_history(
    queue: &DownloadQueue,
    history_path: &Path,
    claim: PendingUrlClaim,
    request: DownloadRequest,
) -> Result<(), String> {
    let url = claim.url().to_owned();
    let destination = request.destination.clone();
    let result = enqueue_with_history_generation(queue, history_path, request, None).await;
    settle_pending_claim(claim, url, destination, result, queue.snapshots()).await
}

async fn settle_pending_claim(
    claim: PendingUrlClaim,
    url: String,
    destination: PathBuf,
    result: Result<Uuid, EnqueueError>,
    snapshots: impl std::future::Future<Output = Vec<JobSnapshot>>,
) -> Result<(), String> {
    match result {
        Ok(_) => claim
            .acknowledge()
            .map_err(|error| format!("URL was queued, but inbox acknowledgement failed: {error}")),
        Err(error @ EnqueueError::Queue(QueueError::DuplicateDestination)) => {
            let equivalent_job_is_queued = snapshots
                .await
                .iter()
                .any(|job| is_proven_duplicate_job(job, &url, &destination));
            if equivalent_job_is_queued {
                claim.acknowledge().map_err(|error| {
                    format!(
                        "equivalent queued URL found, but inbox acknowledgement failed: {error}"
                    )
                })
            } else {
                Err(restore_pending_claim(
                    claim,
                    format!("{error}; no equivalent URL-and-destination job exists in the queue"),
                ))
            }
        }
        Err(error) => Err(restore_pending_claim(claim, error.to_string())),
    }
}

fn restore_pending_claim(claim: PendingUrlClaim, cause: String) -> String {
    match claim.restore(cause.clone()) {
        Ok(()) => format!("{cause}; inbox item restored for retry"),
        Err(error) => format!("{cause}; claim recovery requires attention: {error}"),
    }
}

fn is_proven_duplicate_job(job: &JobSnapshot, url: &str, destination: &Path) -> bool {
    job.url.as_str() == url
        && job.destination.as_path() == destination
        && matches!(
            &job.state,
            JobState::Queued
                | JobState::Scheduled
                | JobState::Downloading
                | JobState::Paused
                | JobState::Completed(_)
        )
}

fn cleanup_diagnostic(state: &JobState) -> Option<&str> {
    match state {
        JobState::Completed(result) => result.cleanup_pending.as_deref(),
        JobState::CancelledCleanupPending(error) => Some(error),
        JobState::FailedCleanupPending { cleanup_error, .. } => Some(cleanup_error),
        _ => None,
    }
}

fn load_settings(path: &Path) -> LoadedSettings {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return LoadedSettings {
                settings: Settings::default(),
                warning: None,
            };
        }
        Err(_) => {
            return LoadedSettings {
                settings: Settings::default(),
                warning: Some(SETTINGS_LOAD_WARNING),
            };
        }
    };
    let settings = match serde_json::from_slice::<Settings>(&bytes) {
        Ok(settings) if valid_settings(&settings) => settings,
        _ => {
            return LoadedSettings {
                settings: Settings::default(),
                warning: Some(SETTINGS_LOAD_WARNING),
            };
        }
    };
    LoadedSettings {
        settings,
        warning: None,
    }
}

fn valid_settings(settings: &Settings) -> bool {
    (1..=8).contains(&settings.max_concurrent_downloads)
        && settings.default_speed_kib_per_second <= 1_000_000
        && settings.global_bandwidth_limit_bytes_per_second <= MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND
        && (1..=1_048_576).contains(&settings.max_download_mib)
        && (1..=10_080).contains(&settings.max_duration_minutes)
        && (1..=MAX_SEGMENT_COUNT).contains(&settings.max_segments)
}

fn replace_settings_state(settings: &Mutex<Settings>, next: Settings) {
    let mut current = match settings.lock() {
        Ok(current) => current,
        Err(poisoned) => {
            // Settings is plain data; replacing the full value repairs any interrupted update.
            let current = poisoned.into_inner();
            settings.clear_poison();
            current
        }
    };
    *current = next;
}

fn save_settings(path: &Path, settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    save_settings_with_replace(path, settings, replace_settings_file)
}

fn save_settings_with_replace(
    path: &Path,
    settings: &Settings,
    replace: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    save_serialized_with_replace(path, settings, replace)
}

fn save_serialized_with_replace<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
    replace: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let contents = serde_json::to_vec_pretty(value)?;
    let (temporary, mut file) = create_unique_data_temp(path)?;
    let write_result = file.write_all(&contents).and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = write_result {
        return Err(cleanup_data_temp(&temporary, error).into());
    }
    if let Err(error) = replace(&temporary, path) {
        return Err(cleanup_data_temp(&temporary, error).into());
    }
    Ok(())
}

fn create_unique_data_temp(path: &Path) -> io::Result<(PathBuf, File)> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "data file path has no file name",
        )
    })?;

    for _ in 0..16 {
        let temporary = parent.join(format!(
            ".{}.tmp-{}",
            name.to_string_lossy(),
            Uuid::new_v4()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique data temporary file",
    ))
}

fn cleanup_data_temp(temporary: &Path, error: io::Error) -> io::Error {
    match fs::remove_file(temporary) {
        Ok(()) => error,
        Err(cleanup_error) if cleanup_error.kind() == io::ErrorKind::NotFound => error,
        Err(cleanup_error) => io::Error::new(
            error.kind(),
            format!("{error}; temporary-file cleanup failed: {cleanup_error}"),
        ),
    }
}

#[cfg(not(windows))]
fn replace_settings_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_settings_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    if !destination.exists() {
        match fs::rename(source, destination) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && destination.exists() => {}
            Err(error) => return Err(error),
        }
    }
    let destination_wide = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let source_wide = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();

    #[link(name = "Kernel32")]
    unsafe extern "system" {
        #[link_name = "ReplaceFileW"]
        fn replace_file_w(
            replaced_file: *const u16,
            replacement_file: *const u16,
            backup_file: *const u16,
            flags: u32,
            exclude: *mut std::ffi::c_void,
            reserved: *mut std::ffi::c_void,
        ) -> i32;
    }

    let replaced = unsafe {
        replace_file_w(
            destination_wide.as_ptr(),
            source_wide.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn user_download_directory() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        .join("Downloads")
}

fn unique_destination(directory: &Path, filename: &str) -> PathBuf {
    let original = directory.join(filename);
    if destination_is_available(&original) {
        return original;
    }
    let path = Path::new(filename);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let extension = path.extension().and_then(|value| value.to_str());
    for index in 2..u32::MAX {
        let candidate_name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = directory.join(candidate_name);
        if destination_is_available(&candidate) {
            return candidate;
        }
    }
    original
}

fn destination_is_available(path: &Path) -> bool {
    !path.exists()
}

fn safe_display_url(raw_url: &str) -> String {
    url::Url::parse(raw_url)
        .map(|mut url| {
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        })
        .unwrap_or_else(|_| "Invalid URL".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::oneshot,
    };

    #[test]
    fn default_resource_limits_are_finite() {
        let settings = Settings::default();
        assert_eq!(settings.global_bandwidth_limit_bytes_per_second, 0);
        assert_eq!(settings.max_download_mib, 4096);
        assert_eq!(settings.max_duration_minutes, 360);
        assert_eq!(settings.max_segments, 1);
    }

    #[test]
    fn global_bandwidth_setting_persists_and_old_settings_default_to_unlimited() {
        let legacy = serde_json::from_str::<Settings>(
            r#"{"max_concurrent_downloads":2,"default_speed_kib_per_second":64,"max_download_mib":512,"max_duration_minutes":120}"#,
        )
        .unwrap();
        assert_eq!(legacy.global_bandwidth_limit_bytes_per_second, 0);
        assert_eq!(legacy.max_segments, 1);

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings.json");
        let settings = Settings {
            global_bandwidth_limit_bytes_per_second: 123_456,
            max_segments: 4,
            ..Settings::default()
        };
        save_settings(&path, &settings).unwrap();

        assert_eq!(
            load_settings(&path)
                .settings
                .global_bandwidth_limit_bytes_per_second,
            123_456
        );
        assert_eq!(load_settings(&path).settings.max_segments, 4);
        assert!(!valid_settings(&Settings {
            max_segments: 5,
            ..Settings::default()
        }));
    }

    #[test]
    fn settings_load_warns_for_corrupt_or_invalid_data_but_not_for_missing_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings.json");

        let missing = load_settings(&path);
        assert!(missing.warning.is_none());
        assert_eq!(missing.settings.max_concurrent_downloads, 3);

        let invalid_inputs: [&[u8]; 2] = [b"not-json", br#"{"max_concurrent_downloads":0}"#];
        for invalid_contents in invalid_inputs {
            std::fs::write(&path, invalid_contents).unwrap();
            let loaded = load_settings(&path);
            assert_eq!(loaded.warning, Some(SETTINGS_LOAD_WARNING));
            assert_eq!(loaded.settings.max_concurrent_downloads, 3);
        }

        let unreadable = load_settings(directory.path());
        assert_eq!(unreadable.warning, Some(SETTINGS_LOAD_WARNING));
        assert_eq!(unreadable.settings.max_concurrent_downloads, 3);
    }

    #[test]
    fn settings_update_recovers_and_clears_a_poisoned_plain_data_mutex() {
        let settings = Mutex::new(Settings::default());
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = settings.lock().unwrap();
            panic!("poison settings lock for recovery test");
        }));
        assert!(panic_result.is_err());

        replace_settings_state(
            &settings,
            Settings {
                global_bandwidth_limit_bytes_per_second: 456_789,
                ..Settings::default()
            },
        );
        assert_eq!(
            settings
                .lock()
                .unwrap()
                .global_bandwidth_limit_bytes_per_second,
            456_789
        );
    }

    #[test]
    fn destination_selection_does_not_treat_legacy_partial_paths_as_final_files() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("package.zip.dm-part"), b"partial").unwrap();
        let selected = unique_destination(directory.path(), "package.zip");
        assert_eq!(
            selected.file_name().unwrap().to_str().unwrap(),
            "package.zip"
        );
    }

    #[test]
    fn displayed_urls_do_not_reveal_query_parameters_or_fragments() {
        assert_eq!(
            safe_display_url("https://example.com/file?token=secret#part"),
            "https://example.com/file"
        );
    }

    #[test]
    fn job_progress_omits_eta_without_a_known_remaining_total() {
        let unknown_total = format_job_progress(250, None, Some(100), Some(8));
        assert!(unknown_total.contains("250 bytes"));
        assert!(!unknown_total.contains("ETA"));
    }

    #[test]
    fn priority_controls_are_enabled_only_for_nonterminal_jobs() {
        for state in [
            JobState::Queued,
            JobState::Scheduled,
            JobState::Downloading,
            JobState::Paused,
        ] {
            assert!(can_change_job_priority(&state));
        }

        let destination = PathBuf::from("download.bin");
        for state in [
            JobState::Completed(DownloadResult {
                destination: destination.clone(),
                bytes: 0,
                sha256: "00".repeat(32),
                resumed: false,
                cleanup_pending: None,
            }),
            JobState::Completed(DownloadResult {
                destination,
                bytes: 0,
                sha256: "00".repeat(32),
                resumed: false,
                cleanup_pending: Some("cleanup pending".to_owned()),
            }),
            JobState::Failed("transfer failed".to_owned()),
            JobState::FailedCleanupPending {
                operation: "download".to_owned(),
                cleanup_error: "cleanup pending".to_owned(),
            },
            JobState::Cancelled,
            JobState::CancelledCleanupPending("cleanup pending".to_owned()),
        ] {
            assert!(!can_change_job_priority(&state));
        }
    }

    #[test]
    fn progress_live_announcements_throttle_status_and_ten_percent_without_unknown_size_spam() {
        let mut tracker = ProgressAnnouncementTracker::default();
        let mut job = history_test_snapshot(
            Uuid::from_u128(900),
            "https://example.com/progress",
            PathBuf::from("progress.bin"),
            JobState::Queued,
            0,
        );
        job.total_bytes = Some(1_000);
        assert!(tracker.observe(&job).is_some());

        job.state = JobState::Downloading;
        assert!(tracker.observe(&job).is_some());
        job.bytes_received = 99;
        assert!(tracker.observe(&job).is_none());
        job.bytes_received = 100;
        assert!(tracker
            .observe(&job)
            .is_some_and(|message| message.contains("10%")));
        job.bytes_received = 199;
        assert!(tracker.observe(&job).is_none());

        job.state = JobState::Paused;
        assert!(tracker.observe(&job).is_some());

        let mut unknown_size_job = history_test_snapshot(
            Uuid::from_u128(901),
            "https://example.com/unknown-size",
            PathBuf::from("unknown-size.bin"),
            JobState::Queued,
            0,
        );
        assert_eq!(unknown_size_job.total_bytes, None);
        assert!(tracker.observe(&unknown_size_job).is_some());
        unknown_size_job.state = JobState::Downloading;
        assert!(tracker.observe(&unknown_size_job).is_some());
        unknown_size_job.bytes_received = 12_345;
        assert!(tracker.observe(&unknown_size_job).is_none());
        unknown_size_job.bytes_received = 67_890;
        assert!(tracker.observe(&unknown_size_job).is_none());
    }

    #[test]
    fn expected_sha256_field_is_optional_but_rejects_invalid_values() {
        assert_eq!(parse_expected_sha256("").unwrap(), None);
        assert!(parse_expected_sha256("  ").is_err());
        assert_eq!(
            parse_expected_sha256(&"A".repeat(64)).unwrap(),
            Some("a".repeat(64))
        );
        assert!(parse_expected_sha256(&format!(" {} ", "a".repeat(64))).is_err());
        assert!(parse_expected_sha256("not-a-digest").is_err());
    }

    #[test]
    fn persistent_history_roundtrips_terminal_fields_and_utc_timestamp() {
        assert_eq!(
            history_destination_basename(r"C:\Users\alice\Projects\archive.zip"),
            "archive.zip"
        );
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.json");
        let timestamp = "2025-03-04T05:06:07Z";
        let mut store = load_test_history_store(&path);
        let destination = PathBuf::from("/home/alice/Projects/Private/archive.zip");
        let job = history_test_snapshot_for_generation(
            Uuid::from_u128(1),
            "https://example.com/files/archive.zip?token=not-stored#section",
            destination.clone(),
            JobState::Completed(DownloadResult {
                destination: destination.clone(),
                bytes: 42,
                sha256: "ab".repeat(32),
                resumed: false,
                cleanup_pending: None,
            }),
            42,
            store.generation.clone(),
        );

        persist_terminal_history(&path, &mut store, std::slice::from_ref(&job), timestamp).unwrap();
        let loaded = load_history(&path).unwrap();

        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].status, "Completed");
        assert_eq!(loaded.entries[0].destination, "archive.zip");
        assert_eq!(loaded.entries[0].bytes, 42);
        let expected_sha256 = "ab".repeat(32);
        assert_eq!(
            loaded.entries[0].sha256.as_deref(),
            Some(expected_sha256.as_str())
        );
        assert_eq!(loaded.entries[0].finished_at_utc, timestamp);
        assert_eq!(
            loaded.entries[0].url,
            "https://example.com/files/archive.zip"
        );
        let persisted = fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains("alice"));
        assert!(!persisted.contains("/home/"));
        assert!(!persisted.contains("Projects"));
        assert!(!persisted.contains("Private"));
        assert_eq!(job.destination, destination);
    }

    #[test]
    fn persisted_history_redacts_userinfo_query_and_fragment() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.json");
        let mut store = load_test_history_store(&path);
        let job = history_test_snapshot_for_generation(
            Uuid::from_u128(2),
            "https://user:password@example.com/path/file?access_token=query-secret#fragment-secret",
            PathBuf::from("file.bin"),
            JobState::Failed("transfer failed".to_owned()),
            12,
            store.generation.clone(),
        );
        persist_terminal_history(&path, &mut store, &[job], "2025-03-04T05:06:07Z").unwrap();

        let serialized = std::fs::read_to_string(&path).unwrap();
        assert!(!serialized.contains("user"));
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("query-secret"));
        assert!(!serialized.contains("fragment-secret"));
        assert!(!serialized.contains("transfer failed"));
        assert_eq!(
            load_history(&path).unwrap().entries[0].url,
            "https://example.com/path/file"
        );
    }

    #[test]
    fn loading_preexisting_history_redacts_and_caps_the_persisted_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.json");
        let seeded = (1..=503_u128)
            .map(|index| HistoryEntry {
                job_id: Uuid::from_u128(index).to_string(),
                url: format!(
                    "https://user:password@example.com/file-{index}?token=secret-{index}#fragment-{index}"
                ),
                status: "Completed".to_owned(),
                destination: format!("/home/example-user/private-project/file-{index}.bin"),
                bytes: index as u64,
                sha256: Some("ab".repeat(32)),
                finished_at_utc: "2025-03-04T05:06:07Z".to_owned(),
            })
            .collect::<Vec<_>>();
        fs::write(&path, serde_json::to_vec(&seeded).unwrap()).unwrap();

        let loaded = load_history(&path).unwrap();
        let persisted = fs::read_to_string(&path).unwrap();

        assert_eq!(loaded.entries.len(), HISTORY_LIMIT);
        assert_eq!(loaded.entries[0].job_id, Uuid::from_u128(4).to_string());
        assert_eq!(loaded.entries[0].url, "https://example.com/file-4");
        assert_eq!(loaded.entries[0].destination, "file-4.bin");
        assert!(!persisted.contains("user"));
        assert!(!persisted.contains("password"));
        assert!(!persisted.contains("token=secret"));
        assert!(!persisted.contains("fragment-"));
        assert!(!persisted.contains("example-user"));
        assert!(!persisted.contains("private-project"));
        let from_disk = serde_json::from_str::<HistoryDocument>(&persisted).unwrap();
        assert_eq!(from_disk.entries.len(), HISTORY_LIMIT);
        assert!(from_disk.entries.iter().all(|entry| {
            entry.url.starts_with("https://example.com/")
                && !entry.url.contains('?')
                && !entry.url.contains('#')
                && !entry.url.contains('@')
        }));
    }

    #[test]
    fn legacy_history_migration_sanitizes_before_removing_the_old_file() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let legacy_path = directory.path().join("history.json");
        let history_path = history_dir.join("history.json");
        let legacy = vec![HistoryEntry {
            job_id: Uuid::from_u128(600).to_string(),
            url: "https://user:password@example.com/file?token=secret#fragment".to_owned(),
            status: "Completed".to_owned(),
            destination: "file.bin".to_owned(),
            bytes: 7,
            sha256: None,
            finished_at_utc: "2025-03-04T05:06:07Z".to_owned(),
        }];
        fs::write(&legacy_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        migrate_legacy_history(&legacy_path, &history_path).unwrap();

        assert!(!legacy_path.exists());
        assert_eq!(
            load_history(&history_path).unwrap().entries[0].url,
            "https://example.com/file"
        );
    }

    #[test]
    fn corrupt_dedicated_history_migrates_valid_legacy_data() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let legacy_path = directory.path().join("history.json");
        let history_path = history_dir.join("history.json");
        let corrupt_current = b"{malformed-current";
        fs::write(&history_path, corrupt_current).unwrap();
        let legacy = vec![HistoryEntry {
            job_id: Uuid::from_u128(601).to_string(),
            url: "https://user:pass@example.com/legacy?token=legacy-secret#fragment".to_owned(),
            status: "Completed".to_owned(),
            destination: "legacy.bin".to_owned(),
            bytes: 17,
            sha256: None,
            finished_at_utc: "2025-03-04T05:06:07Z".to_owned(),
        }];
        fs::write(&legacy_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        migrate_legacy_history(&legacy_path, &history_path).unwrap();

        assert!(!legacy_path.exists());
        let migrated = load_history(&history_path).unwrap();
        assert_eq!(migrated.entries.len(), 1);
        assert_eq!(migrated.entries[0].url, "https://example.com/legacy");
        let persisted = fs::read_to_string(&history_path).unwrap();
        assert!(!persisted.contains("legacy-secret"));
        assert!(!persisted.contains("user:pass"));
    }

    #[test]
    fn malformed_history_files_are_preserved_when_no_valid_document_exists() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let legacy_path = directory.path().join("history.json");
        let history_path = history_dir.join("history.json");
        let current_bytes = b"{malformed-current";
        let legacy_bytes = b"not-json-legacy";
        fs::write(&history_path, current_bytes).unwrap();
        fs::write(&legacy_path, legacy_bytes).unwrap();

        let error = migrate_legacy_history(&legacy_path, &history_path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&history_path).unwrap().as_slice(), current_bytes);
        assert_eq!(fs::read(&legacy_path).unwrap().as_slice(), legacy_bytes);
        assert_eq!(
            load_history(&history_path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(&history_path).unwrap().as_slice(), current_bytes);
    }

    #[test]
    fn persistent_history_keeps_only_the_newest_500_terminal_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.json");
        let mut store = load_test_history_store(&path);
        let generation = store.generation.clone();
        let jobs = (1..=501_u128)
            .map(|index| {
                history_test_snapshot_for_generation(
                    Uuid::from_u128(index),
                    "https://example.com/file",
                    PathBuf::from(format!("file-{index}.bin")),
                    JobState::Cancelled,
                    index as u64,
                    generation.clone(),
                )
            })
            .collect::<Vec<_>>();

        persist_terminal_history(&path, &mut store, &jobs, "2025-03-04T05:06:07Z").unwrap();
        assert_eq!(store.entries.len(), HISTORY_LIMIT);
        let loaded = load_history(&path).unwrap();
        assert_eq!(loaded.entries.len(), HISTORY_LIMIT);
        assert_eq!(loaded.entries[0].job_id, Uuid::from_u128(2).to_string());
        assert_eq!(
            loaded.entries.last().unwrap().job_id,
            Uuid::from_u128(501).to_string()
        );
    }

    #[test]
    fn clearing_history_does_not_reinsert_already_terminal_jobs_after_cleanup_updates() {
        let id = Uuid::from_u128(502);
        let pending = history_test_snapshot(
            id,
            "https://example.com/file",
            PathBuf::from("file.bin"),
            JobState::Completed(DownloadResult {
                destination: PathBuf::from("file.bin"),
                bytes: 1,
                sha256: "ef".repeat(32),
                resumed: false,
                cleanup_pending: Some("cleanup pending".to_owned()),
            }),
            1,
        );
        let resolved = history_test_snapshot(
            id,
            "https://example.com/file",
            PathBuf::from("file.bin"),
            JobState::Completed(DownloadResult {
                destination: PathBuf::from("file.bin"),
                bytes: 1,
                sha256: "ef".repeat(32),
                resumed: false,
                cleanup_pending: None,
            }),
            1,
        );
        let mut store = HistoryStore::default();
        assert!(
            store.record_terminal_snapshots(std::slice::from_ref(&pending), "2025-03-04T05:06:07Z")
        );

        store.clear_for_jobs(
            std::slice::from_ref(&pending),
            Uuid::from_u128(505).to_string(),
        );

        assert!(!store.record_terminal_snapshots(&[resolved], "2025-03-04T05:06:08Z"));
        assert!(store.entries.is_empty());
    }

    #[test]
    fn clear_generation_blocks_old_completion_and_accepts_new_generation_job() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let path = history_dir.join("history.json");
        let mut old_store = load_test_history_store(&path);
        let old_generation = old_store.generation.clone();
        let old_job_id = Uuid::from_u128(503);
        let old_terminal = history_test_snapshot_for_generation(
            old_job_id,
            "https://example.com/old",
            PathBuf::from("old.bin"),
            JobState::Failed("old failure".to_owned()),
            9,
            old_generation.clone(),
        );
        persist_terminal_history(
            &path,
            &mut old_store,
            std::slice::from_ref(&old_terminal),
            "2025-03-04T05:06:07Z",
        )
        .unwrap();
        assert_eq!(old_store.entries.len(), 1);

        let mut clear_store = old_store.clone();
        let clear_snapshot = history_test_snapshot_for_generation(
            old_job_id,
            "https://example.com/old",
            PathBuf::from("old.bin"),
            JobState::Failed("old failure".to_owned()),
            9,
            old_generation.clone(),
        );
        clear_history(
            &path,
            &mut clear_store,
            std::slice::from_ref(&clear_snapshot),
        )
        .unwrap();
        let new_generation = clear_store.generation.clone();
        assert_ne!(old_generation, new_generation);

        let old_completion = history_test_snapshot_for_generation(
            old_job_id,
            "https://example.com/old",
            PathBuf::from("old.bin"),
            JobState::Completed(DownloadResult {
                destination: PathBuf::from("old.bin"),
                bytes: 9,
                sha256: "ab".repeat(32),
                resumed: false,
                cleanup_pending: None,
            }),
            9,
            old_generation,
        );
        persist_terminal_history(
            &path,
            &mut old_store,
            std::slice::from_ref(&old_completion),
            "2025-03-04T05:06:08Z",
        )
        .unwrap();

        let new_job = history_test_snapshot_for_generation(
            Uuid::from_u128(504),
            "https://example.com/new",
            PathBuf::from("new.bin"),
            JobState::Cancelled,
            3,
            new_generation.clone(),
        );
        persist_terminal_history(
            &path,
            &mut clear_store,
            std::slice::from_ref(&new_job),
            "2025-03-04T05:06:09Z",
        )
        .unwrap();

        let final_document = load_history(&path).unwrap();
        assert_eq!(final_document.generation, new_generation);
        assert_eq!(final_document.entries.len(), 1);
        assert_eq!(
            final_document.entries[0].job_id,
            Uuid::from_u128(504).to_string()
        );
    }

    #[tokio::test]
    async fn accepted_queue_job_retains_its_history_generation() {
        let directory = tempfile::tempdir().unwrap();
        let queue = DownloadQueue::new(1).unwrap();
        let generation = Uuid::new_v4().to_string();
        let id = queue
            .enqueue_with_history_generation(
                test_download_request(
                    "https://example.com/generation",
                    directory.path().join("generation.bin"),
                ),
                Some(upcoming_daily_window()),
                generation.clone(),
            )
            .await
            .unwrap();

        let snapshot = queue
            .snapshots()
            .await
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        assert_eq!(snapshot.history_generation, generation);
        queue.cancel(id).await.unwrap();
    }

    #[test]
    fn history_file_lock_excludes_a_second_handle_until_release() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let path = history_dir.join("history.json");
        let held_lock = acquire_history_lock(&path).unwrap();
        let contender = OpenOptions::new()
            .read(true)
            .write(true)
            .open(history_dir.join("history.lock"))
            .unwrap();
        assert!(contender.try_lock().is_err());
        drop(held_lock);
        contender.try_lock().unwrap();
    }

    #[test]
    fn valid_empty_dedicated_history_is_not_replaced_by_legacy_entries() {
        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        let legacy_path = directory.path().join("history.json");
        let history_path = history_dir.join("history.json");
        let current = HistoryDocument {
            generation: Uuid::new_v4().to_string(),
            entries: Vec::new(),
        };
        write_history_unlocked(&history_path, &current).unwrap();
        let legacy = vec![HistoryEntry {
            job_id: Uuid::from_u128(602).to_string(),
            url: "https://example.com/old".to_owned(),
            status: "Completed".to_owned(),
            destination: "old.bin".to_owned(),
            bytes: 1,
            sha256: None,
            finished_at_utc: "2025-03-04T05:06:07Z".to_owned(),
        }];
        fs::write(&legacy_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        migrate_legacy_history(&legacy_path, &history_path).unwrap();

        assert_eq!(load_history(&history_path).unwrap(), current);
        assert!(!legacy_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn history_directory_lock_data_and_temporary_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let history_dir = prepare_history_directory(directory.path()).unwrap();
        assert_eq!(
            fs::metadata(&history_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let path = history_dir.join("history.json");
        let (temporary, file) = create_unique_history_temp(&path).unwrap();
        assert_eq!(
            fs::metadata(&temporary).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(file);
        fs::remove_file(&temporary).unwrap();

        let lock = acquire_history_lock(&path).unwrap();
        let lock_path = history_dir.join("history.lock");
        assert_eq!(
            fs::metadata(&lock_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(lock);

        write_history_unlocked(
            &path,
            &HistoryDocument {
                generation: Uuid::new_v4().to_string(),
                entries: Vec::new(),
            },
        )
        .unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn notification_tracker_emits_once_for_completion_or_failure_only() {
        let mut tracker = NotificationTracker::default();
        let completed = history_test_snapshot(
            Uuid::from_u128(10),
            "https://example.com/complete",
            PathBuf::from("complete.bin"),
            JobState::Completed(DownloadResult {
                destination: PathBuf::from("complete.bin"),
                bytes: 1,
                sha256: "cd".repeat(32),
                resumed: false,
                cleanup_pending: None,
            }),
            1,
        );
        assert_eq!(
            tracker.observe(&completed),
            Some(NotificationKind::Completed)
        );
        assert_eq!(tracker.observe(&completed), None);

        let failed = history_test_snapshot(
            Uuid::from_u128(11),
            "https://example.com/fail",
            PathBuf::from("failed.bin"),
            JobState::Failed("network failure".to_owned()),
            0,
        );
        assert_eq!(tracker.observe(&failed), Some(NotificationKind::Failed));
        let cancelled = history_test_snapshot(
            Uuid::from_u128(12),
            "https://example.com/cancel",
            PathBuf::from("cancel.bin"),
            JobState::Cancelled,
            0,
        );
        assert_eq!(tracker.observe(&cancelled), None);
    }

    #[test]
    fn utc_timestamp_format_uses_utc_epoch() {
        assert_eq!(format_utc_timestamp(0), "1970-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn cancelled_duplicate_restores_the_native_claim() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("app-data");
        let url = "https://example.com/cancelled";
        let destination = directory.path().join("cancelled.bin");
        let queue = DownloadQueue::new(1).unwrap();
        let existing_id = queue
            .enqueue(
                test_download_request(url, destination.clone()),
                Some(upcoming_daily_window()),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let scheduled =
                    queue.snapshots().await.iter().any(|job| {
                        job.id == existing_id && matches!(&job.state, JobState::Scheduled)
                    });
                if scheduled {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("future job should be scheduled without making a request");
        queue.cancel(existing_id).await.unwrap();
        assert!(queue
            .snapshots()
            .await
            .iter()
            .any(|job| { job.id == existing_id && matches!(&job.state, JobState::Cancelled) }));

        native_messaging::accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/cancelled"}"#,
        )
        .unwrap();
        let claim = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        let error = enqueue_pending_claim(&queue, claim, test_download_request(url, destination))
            .await
            .unwrap_err();

        assert!(error.contains("inbox item restored for retry"));
        let restored = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(restored.url(), url);
        restored.acknowledge().unwrap();
    }

    #[tokio::test]
    async fn failed_cleanup_duplicate_restores_the_native_claim() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("app-data");
        let url = "https://example.com/cleanup-pending";
        let destination = directory.path().join("cleanup-pending.bin");
        native_messaging::accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/cleanup-pending"}"#,
        )
        .unwrap();
        let claim = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        let duplicate_job = JobSnapshot {
            id: Uuid::new_v4(),
            url: url.to_owned(),
            destination: destination.clone(),
            priority: Priority::Normal,
            state: JobState::FailedCleanupPending {
                operation: "transfer failed".to_owned(),
                cleanup_error: "cleanup still pending".to_owned(),
            },
            bytes_received: 0,
            total_bytes: None,
            speed_bytes_per_second: None,
            eta_seconds: None,
            history_generation: String::new(),
        };
        let error = settle_pending_claim(
            claim,
            url.to_owned(),
            destination,
            Err(QueueError::DuplicateDestination.into()),
            std::future::ready(vec![duplicate_job]),
        )
        .await
        .unwrap_err();

        assert!(error.contains("inbox item restored for retry"));
        let restored = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(restored.url(), url);
        restored.acknowledge().unwrap();
    }

    #[tokio::test]
    async fn acknowledgement_failure_keeps_claim_payload_recoverable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
            }
            let _ = request_seen_tx.send(());
            std::future::pending::<()>().await;
        });

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("app-data");
        let inbox = data_dir.join("incoming");
        let url = format!("http://{address}/ack-failure");
        let message = format!(r#"{{"action":"download","url":"{url}"}}"#);
        native_messaging::accept_message(&data_dir, message.as_bytes()).unwrap();
        let claim = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();

        let claim_dir = std::fs::read_dir(&inbox)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".processing-"))
            })
            .unwrap();
        let claimed_message = claim_dir.join("message.json");
        let original_message = std::fs::read(&claimed_message).unwrap();
        std::fs::remove_file(&claimed_message).unwrap();
        std::fs::create_dir(&claimed_message).unwrap();
        let recoverable_message = claimed_message.join("preserved-message.json");
        std::fs::write(&recoverable_message, &original_message).unwrap();

        let queue = DownloadQueue::new(1).unwrap();
        let error = enqueue_pending_claim(
            &queue,
            claim,
            test_download_request(&url, directory.path().join("ack-failure.bin")),
        )
        .await
        .unwrap_err();
        assert!(error.contains("inbox acknowledgement failed"));
        tokio::time::timeout(Duration::from_secs(3), request_seen_rx)
            .await
            .expect("the accepted queue job should reach the local test server")
            .unwrap();
        assert!(claim_dir.exists());
        assert_eq!(
            std::fs::read(recoverable_message).unwrap(),
            original_message
        );
        assert!(native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .is_none());

        let job_id = queue
            .snapshots()
            .await
            .into_iter()
            .find(|job| job.url == url)
            .unwrap()
            .id;
        queue.cancel(job_id).await.unwrap();
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn platform_replace_failure_preserves_existing_settings_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings.json");
        let original = b"previous settings";
        std::fs::write(&path, original).unwrap();
        let mut temporary = None;

        let result =
            save_settings_with_replace(&path, &Settings::default(), |source, destination| {
                assert_eq!(destination, path.as_path());
                assert!(source.exists());
                temporary = Some(source.to_path_buf());
                let missing_source = source.with_file_name(format!("missing-{}", Uuid::new_v4()));
                replace_settings_file(&missing_source, destination)
            });

        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!temporary.unwrap().exists());
    }

    #[test]
    fn successful_settings_replace_writes_complete_new_settings() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings.json");
        std::fs::write(&path, serde_json::to_vec(&Settings::default()).unwrap()).unwrap();
        let updated = Settings {
            max_concurrent_downloads: 7,
            max_segments: 1,
            default_speed_kib_per_second: 512,
            global_bandwidth_limit_bytes_per_second: 0,
            max_download_mib: 2048,
            max_duration_minutes: 90,
        };
        let expected = serde_json::to_vec_pretty(&updated).unwrap();

        save_settings(&path, &updated).unwrap();

        assert_eq!(std::fs::read(path).unwrap(), expected);
    }

    #[test]
    fn concurrent_settings_writers_use_independent_atomic_temporaries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("settings.json");
        std::fs::write(&path, serde_json::to_vec(&Settings::default()).unwrap()).unwrap();
        let first = Settings {
            max_concurrent_downloads: 2,
            max_segments: 1,
            default_speed_kib_per_second: 128,
            global_bandwidth_limit_bytes_per_second: 0,
            max_download_mib: 1024,
            max_duration_minutes: 30,
        };
        let second = Settings {
            max_concurrent_downloads: 6,
            max_segments: 1,
            default_speed_kib_per_second: 256,
            global_bandwidth_limit_bytes_per_second: 0,
            max_download_mib: 3072,
            max_duration_minutes: 120,
        };
        let expected_first = serde_json::to_vec_pretty(&first).unwrap();
        let expected_second = serde_json::to_vec_pretty(&second).unwrap();
        let start = Arc::new(std::sync::Barrier::new(3));
        let temporary_paths = Arc::new(Mutex::new(Vec::new()));
        let first_path = path.clone();
        let first_start = start.clone();
        let first_temporaries = temporary_paths.clone();
        let first_writer = std::thread::spawn(move || {
            first_start.wait();
            save_settings_with_replace(&first_path, &first, move |source, destination| {
                first_temporaries.lock().unwrap().push(source.to_path_buf());
                replace_settings_file(source, destination)
            })
            .is_ok()
        });
        let second_start = start.clone();
        let second_temporaries = temporary_paths.clone();
        let second_writer = std::thread::spawn(move || {
            second_start.wait();
            save_settings_with_replace(&path, &second, move |source, destination| {
                second_temporaries
                    .lock()
                    .unwrap()
                    .push(source.to_path_buf());
                replace_settings_file(source, destination)
            })
            .is_ok()
        });
        start.wait();

        assert!(first_writer.join().unwrap());
        assert!(second_writer.join().unwrap());
        let final_settings = std::fs::read(directory.path().join("settings.json")).unwrap();
        assert!(final_settings == expected_first || final_settings == expected_second);
        let temporary_paths = temporary_paths.lock().unwrap();
        assert_eq!(temporary_paths.len(), 2);
        assert_ne!(temporary_paths[0], temporary_paths[1]);
        assert!(temporary_paths.iter().all(|temporary| !temporary.exists()));
    }

    #[test]
    fn inbox_poll_gate_rejects_overlapping_pollers() {
        let gate = InboxPollGate::default();
        let active_poll = gate.try_enter().unwrap();
        assert!(gate.try_enter().is_none());
        drop(active_poll);
        assert!(gate.try_enter().is_some());
    }

    #[test]
    fn independent_inbox_poll_gates_cannot_claim_one_item_twice() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        let expected = "https://example.com/opted-in";
        native_messaging::accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/opted-in"}"#,
        )
        .unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let consumers = [InboxPollGate::default(), InboxPollGate::default()].map(|gate| {
            let data_dir = data_dir.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let _local_guard = gate.try_enter().unwrap();
                barrier.wait();
                native_messaging::claim_next_pending_url(&data_dir)
                    .unwrap()
                    .map(|claim| {
                        let url = claim.url().to_owned();
                        claim.acknowledge().unwrap();
                        vec![url]
                    })
                    .unwrap_or_default()
            })
        });
        let delivered = consumers
            .into_iter()
            .flat_map(|consumer| consumer.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(delivered, vec![expected.to_owned()]);
        assert_eq!(
            std::fs::read_dir(data_dir.join("incoming"))
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn inbox_claims_survive_partial_enqueue_failures_and_ack_only_proven_duplicates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 512];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&buffer[..count]);
            }
            assert!(String::from_utf8_lossy(&request).starts_with("GET /one HTTP/1.1"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().join("app-data");
        let inbox = data_dir.join("incoming");
        std::fs::create_dir_all(&inbox).unwrap();
        let urls = [
            format!("http://{address}/one"),
            format!("http://{address}/one"),
            format!("http://{address}/two"),
            format!("http://{address}/different"),
            format!("http://{address}/three"),
        ];
        for (index, url) in urls.iter().enumerate() {
            let message = format!(r#"{{"action":"download","url":"{url}"}}"#);
            std::fs::write(inbox.join(format!("{:03}.json", index + 1)), message).unwrap();
        }
        let queue = DownloadQueue::new(2).unwrap();
        let one_destination = directory.path().join("one.bin");
        let first = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(first.url(), urls[0]);
        enqueue_pending_claim(
            &queue,
            first,
            DownloadRequest {
                url: urls[0].clone(),
                destination: one_destination.clone(),
                expected_sha256: None,
                max_retries: 0,
                bytes_per_second: None,
                max_bytes: 1024,
                max_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();

        // A duplicate is acknowledged only when both URL and destination match
        // the existing queue entry.
        let equivalent_duplicate = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(equivalent_duplicate.url(), urls[1]);
        enqueue_pending_claim(
            &queue,
            equivalent_duplicate,
            DownloadRequest {
                url: urls[1].clone(),
                destination: one_destination.clone(),
                expected_sha256: None,
                max_retries: 0,
                bytes_per_second: None,
                max_bytes: 1024,
                max_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap();

        let rejected = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(rejected.url(), urls[2]);
        let enqueue_error = enqueue_pending_claim(
            &queue,
            rejected,
            DownloadRequest {
                url: urls[2].clone(),
                destination: directory.path().join("two.bin"),
                expected_sha256: None,
                max_retries: 0,
                bytes_per_second: None,
                max_bytes: 0,
                max_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap_err();
        assert!(enqueue_error.contains("inbox item restored for retry"));

        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("local test server should receive the one accepted queue job")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let completed =
                    queue.snapshots().await.iter().any(|job| {
                        job.url == urls[0] && matches!(&job.state, JobState::Completed(_))
                    });
                if completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the accepted item should remain represented in the queue");

        let retry_two = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(retry_two.url(), urls[2]);
        let different_url_duplicate = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(different_url_duplicate.url(), urls[3]);
        let duplicate_error = enqueue_pending_claim(
            &queue,
            different_url_duplicate,
            DownloadRequest {
                url: urls[3].clone(),
                destination: one_destination,
                expected_sha256: None,
                max_retries: 0,
                bytes_per_second: None,
                max_bytes: 1024,
                max_duration: Duration::from_secs(30),
            },
        )
        .await
        .unwrap_err();
        assert!(duplicate_error.contains("no equivalent URL-and-destination job"));

        let retry_different = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(retry_different.url(), urls[3]);
        let untouched = native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .unwrap();
        assert_eq!(untouched.url(), urls[4]);
        assert!(native_messaging::claim_next_pending_url(&data_dir)
            .unwrap()
            .is_none());
        assert_eq!(
            queue
                .snapshots()
                .await
                .iter()
                .filter(|job| job.url == urls[0])
                .count(),
            1
        );

        retry_two
            .restore("test verifies recoverable partial failure")
            .unwrap();
        retry_different
            .restore("test verifies non-equivalent duplicate recovery")
            .unwrap();
        untouched
            .restore("test verifies unprocessed inbox item remains recoverable")
            .unwrap();
    }

    #[test]
    fn queue_action_reports_completed_state_when_commit_wins_a_pause_race() {
        let completed = JobState::Completed(DownloadResult {
            destination: PathBuf::from("finished.bin"),
            bytes: 3,
            sha256: "digest".to_owned(),
            resumed: false,
            cleanup_pending: None,
        });
        assert_eq!(
            queue_action_message("pause", Ok(()), Some(&completed)),
            "Download already completed; pause was not applied."
        );
    }

    fn history_test_snapshot(
        id: Uuid,
        url: &str,
        destination: PathBuf,
        state: JobState,
        bytes_received: u64,
    ) -> JobSnapshot {
        history_test_snapshot_for_generation(
            id,
            url,
            destination,
            state,
            bytes_received,
            String::new(),
        )
    }

    fn history_test_snapshot_for_generation(
        id: Uuid,
        url: &str,
        destination: PathBuf,
        state: JobState,
        bytes_received: u64,
        history_generation: String,
    ) -> JobSnapshot {
        JobSnapshot {
            id,
            url: url.to_owned(),
            destination,
            priority: Priority::Normal,
            state,
            bytes_received,
            total_bytes: None,
            speed_bytes_per_second: None,
            eta_seconds: None,
            history_generation,
        }
    }

    fn load_test_history_store(path: &Path) -> HistoryStore {
        let document = load_history(path).unwrap();
        HistoryStore {
            generation: document.generation,
            entries: document.entries,
            observed_terminal_states: HashMap::new(),
            suppressed_after_clear: HashSet::new(),
        }
    }

    fn test_download_request(url: &str, destination: PathBuf) -> DownloadRequest {
        DownloadRequest {
            url: url.to_owned(),
            destination,
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(30),
        }
    }

    fn upcoming_daily_window() -> DailyWindow {
        let minute = ((std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            / 60)
            % 1440) as u16;
        let start = (minute + 60) % 1440;
        DailyWindow::new(start, (start + 1) % 1440).unwrap()
    }
}
