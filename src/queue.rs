use crate::{
    downloader::{
        committed_result, discard_download_session, discard_download_session_until,
        download_with_session_managed_with_limiter_segments_and_slots, DownloadError,
        DownloadRequest, DownloadResult, DownloadSession, ProgressSnapshot, SharedBandwidthLimiter,
        MAX_SEGMENT_COUNT,
    },
    schedule::DailyWindow,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::{watch, Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND: u64 =
    crate::downloader::MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND;

/// Scheduling priority for a job waiting for this queue instance's transfer permit.
/// Higher priorities are selected first; equal priorities keep FIFO arrival order.
/// There is no aging, so sustained High-priority arrivals may defer Low-priority jobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
}

impl Priority {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Low,
            2 => Self::High,
            _ => Self::Normal,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Low => "Low",
            Self::Normal => "Normal",
            Self::High => "High",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Scheduled,
    Downloading,
    Paused,
    Completed(DownloadResult),
    Failed(String),
    FailedCleanupPending {
        operation: String,
        cleanup_error: String,
    },
    Cancelled,
    CancelledCleanupPending(String),
}

impl JobState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Scheduled => "Scheduled",
            Self::Downloading => "Downloading",
            Self::Paused => "Paused",
            Self::Completed(_) => "Completed",
            Self::Failed(_) => "Failed",
            Self::FailedCleanupPending { .. } => "Failed (cleanup pending)",
            Self::Cancelled => "Cancelled",
            Self::CancelledCleanupPending(_) => "Cancelled (cleanup pending)",
        }
    }
}

#[derive(Clone, Debug)]
pub struct JobSnapshot {
    pub id: Uuid,
    pub url: String,
    pub destination: PathBuf,
    pub state: JobState,
    pub priority: Priority,
    pub bytes_received: u64,
    pub total_bytes: Option<u64>,
    pub speed_bytes_per_second: Option<u64>,
    pub eta_seconds: Option<u64>,
    pub history_generation: String,
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("maximum concurrent downloads must be at least one")]
    InvalidConcurrency,
    #[error("maximum segments per download must be between one and four")]
    InvalidSegmentLimit,
    #[error("global bandwidth limit must be at most 1,000,000,000 bytes per second")]
    InvalidGlobalBandwidthLimit,
    #[error("a job for this destination is already queued")]
    DuplicateDestination,
    #[error("job does not exist")]
    UnknownJob,
    #[error("operation is invalid for the job's current state")]
    InvalidState,
    #[error(transparent)]
    Download(#[from] DownloadError),
}

struct JobEntry {
    request: DownloadRequest,
    destination_key: PathBuf,
    history_generation: String,
    window: Option<DailyWindow>,
    state: JobState,
    priority: Arc<std::sync::atomic::AtomicU8>,
    cancel: CancellationToken,
    run_id: Uuid,
    transfer_lock: Arc<Mutex<()>>,
    download_session: DownloadSession,
}

struct Inner {
    jobs: Mutex<HashMap<Uuid, JobEntry>>,
    permits: Arc<PriorityPermitPool>,
    schedule_clock: ScheduleClock,
    bandwidth_limiter: SharedBandwidthLimiter,
    connection_slots: Arc<Semaphore>,
    max_connections: usize,
    segment_limit: std::sync::atomic::AtomicU8,
}

impl Inner {
    fn active_connection_count(&self) -> u8 {
        self.segment_limit
            .load(Ordering::Acquire)
            .min(self.max_connections.min(usize::from(MAX_SEGMENT_COUNT)) as u8)
    }
}

struct PriorityPermitPool {
    state: StdMutex<PermitPoolState>,
    changed: Notify,
    #[cfg(test)]
    registered: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

struct PermitPoolState {
    available: usize,
    next_ticket: u64,
    waiters: Vec<PermitWaiter>,
}

struct PermitWaiter {
    ticket: u64,
    priority: Arc<std::sync::atomic::AtomicU8>,
}

struct PriorityPermit {
    pool: Arc<PriorityPermitPool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermitWaitError {
    Cancelled,
    DeadlineElapsed,
}

impl PriorityPermitPool {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: StdMutex::new(PermitPoolState {
                available: capacity,
                next_ticket: 0,
                waiters: Vec::new(),
            }),
            changed: Notify::new(),
            #[cfg(test)]
            registered: None,
        })
    }

    #[cfg(test)]
    fn new_observed(capacity: usize) -> (Arc<Self>, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (registered, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut pool = Self::new(capacity);
        Arc::get_mut(&mut pool)
            .expect("new permit pool has a unique owner")
            .registered = Some(registered);
        (pool, receiver)
    }

    async fn acquire(
        self: &Arc<Self>,
        priority: Arc<std::sync::atomic::AtomicU8>,
        cancel: &CancellationToken,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<PriorityPermit, PermitWaitError> {
        let ticket = {
            let mut state = self.lock_state();
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiters.push(PermitWaiter { ticket, priority });
            ticket
        };
        #[cfg(test)]
        if let Some(registered) = &self.registered {
            let _ = registered.send(());
        }

        loop {
            if cancel.is_cancelled() {
                self.remove_waiter(ticket);
                return Err(PermitWaitError::Cancelled);
            }
            if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                self.remove_waiter(ticket);
                return Err(PermitWaitError::DeadlineElapsed);
            }

            // Register before checking state so a release or reprioritization
            // between the check and await cannot be lost.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let acquired = {
                let mut state = self.lock_state();
                let index = state
                    .waiters
                    .iter()
                    .position(|waiter| waiter.ticket == ticket);
                if let Some(index) = index.filter(|index| {
                    state.available > 0 && best_waiter_index(&state) == Some(*index)
                }) {
                    state.waiters.remove(index);
                    state.available -= 1;
                    true
                } else {
                    false
                }
            };
            if acquired {
                let permit = PriorityPermit { pool: self.clone() };
                if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                    drop(permit);
                    return Err(PermitWaitError::DeadlineElapsed);
                }
                return Ok(permit);
            }

            let wait_result = if let Some(deadline) = deadline {
                tokio::select! {
                    _ = cancel.cancelled() => Err(PermitWaitError::Cancelled),
                    _ = tokio::time::sleep_until(deadline) => Err(PermitWaitError::DeadlineElapsed),
                    _ = &mut notified => continue,
                }
            } else {
                tokio::select! {
                    _ = cancel.cancelled() => Err(PermitWaitError::Cancelled),
                    _ = &mut notified => continue,
                }
            };
            self.remove_waiter(ticket);
            return wait_result;
        }
    }

    fn wake_waiters(&self) {
        self.changed.notify_waiters();
    }

    fn remove_waiter(&self, ticket: u64) {
        let mut state = self.lock_state();
        state.waiters.retain(|waiter| waiter.ticket != ticket);
        drop(state);
        self.changed.notify_waiters();
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, PermitPoolState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.lock_state().waiters.len()
    }

    #[cfg(test)]
    fn available_count(&self) -> usize {
        self.lock_state().available
    }
}

fn best_waiter_index(state: &PermitPoolState) -> Option<usize> {
    state
        .waiters
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| {
            let left_priority = left.priority.load(Ordering::Acquire);
            let right_priority = right.priority.load(Ordering::Acquire);
            right_priority
                .cmp(&left_priority)
                .then_with(|| left.ticket.cmp(&right.ticket))
        })
        .map(|(index, _)| index)
}

impl Drop for PriorityPermit {
    fn drop(&mut self) {
        let mut state = self.pool.lock_state();
        state.available += 1;
        drop(state);
        self.pool.changed.notify_waiters();
    }
}

#[derive(Default)]
struct ScheduleClock {
    test_minute: Option<Arc<AtomicU16>>,
    updates: Option<watch::Sender<()>>,
    #[cfg(test)]
    transfer_lock_waiting: Option<Arc<tokio::sync::Notify>>,
}

impl ScheduleClock {
    fn current_minute(&self) -> u16 {
        self.test_minute
            .as_ref()
            .map_or_else(utc_minute, |minute| minute.load(Ordering::Acquire))
    }

    fn subscribe(&self) -> Option<watch::Receiver<()>> {
        self.updates.as_ref().map(|updates| updates.subscribe())
    }

    async fn wait_for_next_check(
        &self,
        updates: &mut Option<watch::Receiver<()>>,
        cancel: &CancellationToken,
    ) -> bool {
        if let Some(updates) = updates.as_mut() {
            tokio::select! {
                _ = cancel.cancelled() => false,
                changed = updates.changed() => changed.is_ok(),
            }
        } else {
            tokio::select! {
                _ = cancel.cancelled() => false,
                _ = tokio::time::sleep(Duration::from_secs(30)) => true,
            }
        }
    }
}

#[derive(Clone)]
pub struct DownloadQueue {
    inner: Arc<Inner>,
}

impl DownloadQueue {
    pub fn new(max_concurrent: usize) -> Result<Self, QueueError> {
        Self::new_with_global_bandwidth_limit(max_concurrent, 0)
    }

    pub fn new_with_global_bandwidth_limit(
        max_concurrent: usize,
        global_bandwidth_limit_bytes_per_second: u64,
    ) -> Result<Self, QueueError> {
        if max_concurrent == 0 {
            return Err(QueueError::InvalidConcurrency);
        }
        if global_bandwidth_limit_bytes_per_second > MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND {
            return Err(QueueError::InvalidGlobalBandwidthLimit);
        }
        let bandwidth_limiter =
            SharedBandwidthLimiter::new(global_bandwidth_limit_bytes_per_second)
                .map_err(|_| QueueError::InvalidGlobalBandwidthLimit)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs: Mutex::new(HashMap::new()),
                permits: PriorityPermitPool::new(max_concurrent),
                schedule_clock: ScheduleClock::default(),
                bandwidth_limiter,
                connection_slots: Arc::new(Semaphore::new(max_concurrent)),
                max_connections: max_concurrent,
                segment_limit: std::sync::atomic::AtomicU8::new(1),
            }),
        })
    }

    /// Caps simultaneous segments for range-capable transfers. A range probe and
    /// every single-stream fallback use one connection slot; confirmed ranges
    /// atomically reserve their effective segment count.
    pub fn set_max_segments(&self, max_segments: u8) -> Result<(), QueueError> {
        if !(1..=MAX_SEGMENT_COUNT).contains(&max_segments) {
            return Err(QueueError::InvalidSegmentLimit);
        }
        self.inner
            .segment_limit
            .store(max_segments, Ordering::Release);
        Ok(())
    }

    pub fn set_global_bandwidth_limit(&self, bytes_per_second: u64) -> Result<(), QueueError> {
        if bytes_per_second > MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND {
            return Err(QueueError::InvalidGlobalBandwidthLimit);
        }
        self.inner
            .bandwidth_limiter
            .set_limit(bytes_per_second)
            .map_err(|_| QueueError::InvalidGlobalBandwidthLimit)?;
        Ok(())
    }

    #[cfg(test)]
    fn new_with_test_utc_minute(
        max_concurrent: usize,
        initial_minute: u16,
    ) -> Result<Self, QueueError> {
        let mut queue = Self::new(max_concurrent)?;
        let (updates, _receiver) = watch::channel(());
        let inner = Arc::get_mut(&mut queue.inner).expect("new queue has a unique inner");
        inner.schedule_clock.test_minute = Some(Arc::new(AtomicU16::new(initial_minute)));
        inner.schedule_clock.updates = Some(updates);
        Ok(queue)
    }

    #[cfg(test)]
    fn set_test_utc_minute(&self, minute: u16) {
        if let Some(current) = &self.inner.schedule_clock.test_minute {
            current.store(minute, Ordering::Release);
        }
        if let Some(updates) = &self.inner.schedule_clock.updates {
            updates.send_replace(());
        }
    }

    #[cfg(test)]
    fn observe_transfer_lock_waiting(&mut self) -> Arc<tokio::sync::Notify> {
        let notification = Arc::new(tokio::sync::Notify::new());
        Arc::get_mut(&mut self.inner)
            .expect("test observer must be installed before queue cloning")
            .schedule_clock
            .transfer_lock_waiting = Some(notification.clone());
        notification
    }

    pub async fn enqueue(
        &self,
        request: DownloadRequest,
        window: Option<DailyWindow>,
    ) -> Result<Uuid, QueueError> {
        self.enqueue_with_history_generation(request, window, String::new())
            .await
    }

    pub async fn enqueue_with_history_generation(
        &self,
        request: DownloadRequest,
        window: Option<DailyWindow>,
        history_generation: String,
    ) -> Result<Uuid, QueueError> {
        request.validate()?;
        let id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let destination_key = destination_key(&request.destination);
        {
            let mut jobs = self.inner.jobs.lock().await;
            if jobs.values().any(|entry| {
                same_destination(&entry.destination_key, &destination_key)
                    && !matches!(&entry.state, JobState::Failed(_))
            }) {
                return Err(QueueError::DuplicateDestination);
            }
            jobs.insert(
                id,
                JobEntry {
                    request,
                    destination_key,
                    history_generation,
                    window,
                    state: JobState::Queued,
                    priority: Arc::new(std::sync::atomic::AtomicU8::new(Priority::Normal as u8)),
                    cancel: cancel.clone(),
                    run_id,
                    transfer_lock: Arc::new(Mutex::new(())),
                    download_session: DownloadSession::default(),
                },
            );
        }
        self.launch(id, run_id, cancel);
        Ok(id)
    }

    pub async fn pause(&self, id: Uuid) -> Result<(), QueueError> {
        let (session, run_id) = {
            let jobs = self.inner.jobs.lock().await;
            let job = jobs.get(&id).ok_or(QueueError::UnknownJob)?;
            if !matches!(
                &job.state,
                JobState::Queued | JobState::Scheduled | JobState::Downloading
            ) {
                return Err(QueueError::InvalidState);
            }
            (job.download_session.clone(), job.run_id)
        };
        let committed = session.lock_commit_state().await;
        if *committed {
            // The gate establishes that commit won. Release it before reading
            // session state or taking the queue lock to avoid lock inversion.
            drop(committed);
            publish_committed_result(&self.inner, id, run_id, &session).await;
            return Ok(());
        }
        let mut jobs = self.inner.jobs.lock().await;
        let job = jobs.get_mut(&id).ok_or(QueueError::UnknownJob)?;
        if job.run_id != run_id
            || !matches!(
                &job.state,
                JobState::Queued | JobState::Scheduled | JobState::Downloading
            )
        {
            return Err(QueueError::InvalidState);
        }
        job.state = JobState::Paused;
        job.download_session.pause_progress();
        job.cancel.cancel();
        Ok(())
    }

    pub async fn resume(&self, id: Uuid) -> Result<(), QueueError> {
        let (run_id, cancel) = {
            let mut jobs = self.inner.jobs.lock().await;
            let job = jobs.get_mut(&id).ok_or(QueueError::UnknownJob)?;
            if job.state != JobState::Paused {
                return Err(QueueError::InvalidState);
            }
            job.cancel = CancellationToken::new();
            job.run_id = Uuid::new_v4();
            job.state = JobState::Queued;
            job.download_session.resume_progress();
            (job.run_id, job.cancel.clone())
        };
        self.launch(id, run_id, cancel);
        Ok(())
    }

    /// Set a job's scheduling priority. Active transfers are not preempted;
    /// changing their priority affects only a later resource wait.
    pub async fn set_priority(&self, id: Uuid, priority: Priority) -> Result<(), QueueError> {
        let priority_ref = {
            let jobs = self.inner.jobs.lock().await;
            let job = jobs.get(&id).ok_or(QueueError::UnknownJob)?;
            if matches!(
                &job.state,
                JobState::Completed(_)
                    | JobState::Failed(_)
                    | JobState::FailedCleanupPending { .. }
                    | JobState::Cancelled
                    | JobState::CancelledCleanupPending(_)
            ) {
                return Err(QueueError::InvalidState);
            }
            job.priority.clone()
        };
        priority_ref.store(priority as u8, Ordering::Release);
        self.inner.permits.wake_waiters();
        Ok(())
    }

    pub async fn cancel(&self, id: Uuid) -> Result<(), QueueError> {
        let (session, run_id) = {
            let jobs = self.inner.jobs.lock().await;
            let job = jobs.get(&id).ok_or(QueueError::UnknownJob)?;
            if !matches!(
                &job.state,
                JobState::Queued | JobState::Scheduled | JobState::Downloading | JobState::Paused
            ) {
                return Err(QueueError::InvalidState);
            }
            (job.download_session.clone(), job.run_id)
        };
        let committed = session.lock_commit_state().await;
        if *committed {
            // The gate establishes that commit won. Release it before reading
            // session state or taking the queue lock to avoid lock inversion.
            drop(committed);
            publish_committed_result(&self.inner, id, run_id, &session).await;
            return Ok(());
        }
        let session = {
            let mut jobs = self.inner.jobs.lock().await;
            let job = jobs.get_mut(&id).ok_or(QueueError::UnknownJob)?;
            if job.run_id != run_id
                || !matches!(
                    &job.state,
                    JobState::Queued
                        | JobState::Scheduled
                        | JobState::Downloading
                        | JobState::Paused
                )
            {
                return Err(QueueError::InvalidState);
            }
            job.cancel.cancel();
            job.state = JobState::Cancelled;
            job.download_session.clone()
        };
        drop(committed);
        let cleanup_result = match session.absolute_deadline() {
            Some(deadline) => discard_download_session_until(&session, deadline)
                .await
                .map_err(|cleanup_error| DownloadError::CleanupPending {
                    operation: "cancelled".to_owned(),
                    cleanup_error,
                }),
            None => discard_download_session(&session).await.map_err(|error| {
                DownloadError::CleanupPending {
                    operation: "cancelled".to_owned(),
                    cleanup_error: error.to_string(),
                }
            }),
        };
        match cleanup_result {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Some(job) = self.inner.jobs.lock().await.get_mut(&id) {
                    if let JobState::Completed(result) = &mut job.state {
                        result.cleanup_pending = Some(error.to_string());
                    }
                    if matches!(
                        &job.state,
                        JobState::Cancelled | JobState::CancelledCleanupPending(_)
                    ) {
                        job.state = JobState::CancelledCleanupPending(error.to_string());
                    }
                }
                Err(error.into())
            }
        }
    }

    /// Retry temporary-file cleanup after a previous cancel or completed transfer
    /// reported cleanup as pending. A failed retry keeps the same session handle.
    pub async fn retry_cleanup(&self, id: Uuid) -> Result<(), QueueError> {
        let session = {
            let jobs = self.inner.jobs.lock().await;
            let job = jobs.get(&id).ok_or(QueueError::UnknownJob)?;
            let may_retry = matches!(
                &job.state,
                JobState::Cancelled
                    | JobState::CancelledCleanupPending(_)
                    | JobState::Failed(_)
                    | JobState::FailedCleanupPending { .. }
            ) || matches!(&job.state, JobState::Completed(result) if result.cleanup_pending.is_some());
            if !may_retry {
                return Err(QueueError::InvalidState);
            }
            job.download_session.clone()
        };
        match discard_download_session(&session).await {
            Ok(()) => {
                if let Some(job) = self.inner.jobs.lock().await.get_mut(&id) {
                    match &mut job.state {
                        JobState::Completed(result) => result.cleanup_pending = None,
                        JobState::FailedCleanupPending { .. } => {}
                        _ => {}
                    }
                    if matches!(&job.state, JobState::CancelledCleanupPending(_)) {
                        job.state = JobState::Cancelled;
                    } else {
                        let failed_operation = match &job.state {
                            JobState::FailedCleanupPending { operation, .. } => {
                                Some(operation.clone())
                            }
                            _ => None,
                        };
                        if let Some(operation) = failed_operation {
                            job.state = JobState::Failed(operation);
                        }
                    }
                }
                Ok(())
            }
            Err(error) => {
                if let Some(job) = self.inner.jobs.lock().await.get_mut(&id) {
                    match &mut job.state {
                        JobState::Completed(result) => {
                            result.cleanup_pending = Some(error.to_string())
                        }
                        JobState::FailedCleanupPending { cleanup_error, .. } => {
                            *cleanup_error = error.to_string()
                        }
                        _ => {}
                    }
                    if matches!(&job.state, JobState::CancelledCleanupPending(_)) {
                        job.state = JobState::CancelledCleanupPending(error.to_string());
                    }
                }
                Err(error.into())
            }
        }
    }

    pub async fn snapshots(&self) -> Vec<JobSnapshot> {
        let jobs = self.inner.jobs.lock().await;
        let mut snapshots = jobs
            .iter()
            .map(|(id, job)| {
                let progress: ProgressSnapshot = job.download_session.progress_snapshot();
                JobSnapshot {
                    id: *id,
                    url: job.request.url.clone(),
                    destination: job.request.destination.clone(),
                    state: job.state.clone(),
                    priority: Priority::from_u8(job.priority.load(Ordering::Acquire)),
                    bytes_received: progress.bytes_received,
                    total_bytes: progress.total_bytes,
                    speed_bytes_per_second: progress.speed_bytes_per_second,
                    eta_seconds: progress.eta_seconds,
                    history_generation: job.history_generation.clone(),
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|job| job.id);
        snapshots
    }

    fn launch(&self, id: Uuid, run_id: Uuid, cancel: CancellationToken) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            if !is_runnable(&inner, id, run_id).await {
                return;
            }
            let (request, window, transfer_lock, download_session, priority) = {
                let jobs = inner.jobs.lock().await;
                let Some(job) = jobs.get(&id) else { return };
                if job.run_id != run_id {
                    return;
                }
                (
                    job.request.clone(),
                    job.window,
                    job.transfer_lock.clone(),
                    job.download_session.clone(),
                    job.priority.clone(),
                )
            };
            let mut schedule_updates = window.and_then(|_| inner.schedule_clock.subscribe());
            if let Some(window) = window {
                if !wait_for_scheduled_window(
                    &inner,
                    id,
                    run_id,
                    window,
                    &cancel,
                    &mut schedule_updates,
                )
                .await
                {
                    return;
                }
            }

            loop {
                if cancel.is_cancelled() || !is_runnable(&inner, id, run_id).await {
                    return;
                }
                // A resumed worker must wait for the paused worker to finish using
                // its open partial-file handle before issuing another range request.
                #[cfg(test)]
                if let Some(waiting) = &inner.schedule_clock.transfer_lock_waiting {
                    waiting.notify_one();
                }
                let transfer_guard = tokio::select! {
                    _ = cancel.cancelled() => return,
                    guard = transfer_lock.clone().lock_owned() => guard,
                };
                if cancel.is_cancelled() || !is_runnable(&inner, id, run_id).await {
                    return;
                }
                if let Some(result) = committed_result(&download_session).await {
                    set_completed(&inner, id, run_id, result).await;
                    return;
                }
                if let Some(closed_window) = window
                    .filter(|window| !window.contains_minute(inner.schedule_clock.current_minute()))
                {
                    drop(transfer_guard);
                    if !wait_for_scheduled_window(
                        &inner,
                        id,
                        run_id,
                        closed_window,
                        &cancel,
                        &mut schedule_updates,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }

                let permit = match inner
                    .permits
                    .acquire(
                        priority.clone(),
                        &cancel,
                        download_session.absolute_deadline(),
                    )
                    .await
                {
                    Ok(permit) => permit,
                    Err(PermitWaitError::Cancelled) => return,
                    Err(PermitWaitError::DeadlineElapsed) => {
                        drop(transfer_guard);
                        fail_owned_run(
                            &inner,
                            id,
                            run_id,
                            &download_session,
                            DownloadError::DurationLimitExceeded,
                        )
                        .await;
                        return;
                    }
                };
                if cancel.is_cancelled() || !is_runnable(&inner, id, run_id).await {
                    return;
                }
                if let Some(closed_window) = window
                    .filter(|window| !window.contains_minute(inner.schedule_clock.current_minute()))
                {
                    drop(permit);
                    drop(transfer_guard);
                    if !wait_for_scheduled_window(
                        &inner,
                        id,
                        run_id,
                        closed_window,
                        &cancel,
                        &mut schedule_updates,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }

                let max_segments = inner.active_connection_count();
                let connection_slots = inner.connection_slots.clone();
                if cancel.is_cancelled() || !is_runnable(&inner, id, run_id).await {
                    return;
                }
                if let Some(closed_window) = window
                    .filter(|window| !window.contains_minute(inner.schedule_clock.current_minute()))
                {
                    drop(permit);
                    drop(transfer_guard);
                    if !wait_for_scheduled_window(
                        &inner,
                        id,
                        run_id,
                        closed_window,
                        &cancel,
                        &mut schedule_updates,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }

                set_state(&inner, id, run_id, JobState::Downloading).await;
                // Recheck after all resource waits and the queue-state update,
                // immediately before starting the transfer.
                if let Some(closed_window) = window
                    .filter(|window| !window.contains_minute(inner.schedule_clock.current_minute()))
                {
                    drop(permit);
                    drop(transfer_guard);
                    if !wait_for_scheduled_window(
                        &inner,
                        id,
                        run_id,
                        closed_window,
                        &cancel,
                        &mut schedule_updates,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }
                if cancel.is_cancelled() {
                    return;
                }

                let result = download_with_session_managed_with_limiter_segments_and_slots(
                    request,
                    cancel.clone(),
                    &download_session,
                    &inner.bandwidth_limiter,
                    max_segments,
                    connection_slots,
                )
                .await;
                drop(permit);
                // Once this attempt has stopped using the session, let the next run
                // proceed before any generation-checked failure bookkeeping.
                drop(transfer_guard);
                match result {
                    Ok(result) => set_completed(&inner, id, run_id, result).await,
                    // Pause/cancel owners decide whether the session is preserved or discarded.
                    // A stale worker must never infer that decision from a newer global state.
                    Err(DownloadError::Cancelled) => {}
                    Err(DownloadError::CleanupPending {
                        operation,
                        cleanup_error,
                    }) => {
                        fail_owned_run(
                            &inner,
                            id,
                            run_id,
                            &download_session,
                            DownloadError::CleanupPending {
                                operation,
                                cleanup_error,
                            },
                        )
                        .await
                    }
                    Err(error) => {
                        fail_owned_run(&inner, id, run_id, &download_session, error).await
                    }
                }
                return;
            }
        });
    }
}

fn destination_key(path: &Path) -> PathBuf {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    canonical_parent.join(path.file_name().unwrap_or_default())
}

fn same_destination(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy().to_lowercase() == right.to_string_lossy().to_lowercase()
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

async fn is_runnable(inner: &Inner, id: Uuid, run_id: Uuid) -> bool {
    inner.jobs.lock().await.get(&id).is_some_and(|job| {
        job.run_id == run_id && matches!(&job.state, JobState::Queued | JobState::Scheduled)
    })
}

async fn set_state(inner: &Inner, id: Uuid, run_id: Uuid, state: JobState) {
    if let Some(job) = inner.jobs.lock().await.get_mut(&id) {
        if job.run_id == run_id
            && !matches!(
                &job.state,
                JobState::Paused | JobState::Cancelled | JobState::CancelledCleanupPending(_)
            )
        {
            job.state = state;
        }
    }
}

async fn wait_for_scheduled_window(
    inner: &Inner,
    id: Uuid,
    run_id: Uuid,
    window: DailyWindow,
    cancel: &CancellationToken,
    updates: &mut Option<watch::Receiver<()>>,
) -> bool {
    set_state(inner, id, run_id, JobState::Scheduled).await;
    loop {
        if cancel.is_cancelled() || !is_runnable(inner, id, run_id).await {
            return false;
        }
        if window.contains_minute(inner.schedule_clock.current_minute()) {
            return true;
        }
        if !inner
            .schedule_clock
            .wait_for_next_check(updates, cancel)
            .await
        {
            return false;
        }
    }
}

async fn fail_owned_run(
    inner: &Inner,
    id: Uuid,
    run_id: Uuid,
    session: &DownloadSession,
    error: DownloadError,
) {
    let operation = match error {
        DownloadError::CleanupPending { operation, .. } => operation,
        error => error.to_string(),
    };

    {
        let mut jobs = inner.jobs.lock().await;
        let Some(job) = jobs.get_mut(&id) else { return };
        if job.run_id != run_id
            || !matches!(
                &job.state,
                JobState::Downloading | JobState::Queued | JobState::Scheduled
            )
        {
            return;
        }
        // Claim this generation's failure before releasing the queue lock. The
        // slow filesystem cleanup then cannot block pause/resume or a newer run.
        job.state = JobState::Failed(operation.clone());
    }

    let cleanup_error = match session.absolute_deadline() {
        Some(deadline) => discard_download_session_until(session, deadline)
            .await
            .err(),
        None => discard_download_session(session)
            .await
            .err()
            .map(|error| error.to_string()),
    };
    if let Some(cleanup_error) = cleanup_error {
        let mut jobs = inner.jobs.lock().await;
        if let Some(job) = jobs.get_mut(&id) {
            if job.run_id == run_id
                && matches!(&job.state, JobState::Failed(current) if current == &operation)
            {
                job.state = JobState::FailedCleanupPending {
                    operation,
                    cleanup_error,
                };
            }
        }
    }
}

async fn publish_committed_result(
    inner: &Inner,
    id: Uuid,
    run_id: Uuid,
    session: &DownloadSession,
) {
    if let Some(result) = committed_result(session).await {
        if let Some(job) = inner.jobs.lock().await.get_mut(&id) {
            if job.run_id == run_id
                && matches!(
                    &job.state,
                    JobState::Queued | JobState::Scheduled | JobState::Downloading
                )
            {
                job.state = JobState::Completed(result);
            }
        }
    }
}

async fn set_completed(inner: &Inner, id: Uuid, run_id: Uuid, result: DownloadResult) {
    if let Some(job) = inner.jobs.lock().await.get_mut(&id) {
        // A completed disk commit is reported only for the still-active generation.
        if job.run_id == run_id
            && matches!(
                &job.state,
                JobState::Queued | JobState::Scheduled | JobState::Downloading
            )
        {
            job.state = JobState::Completed(result);
        }
    }
}

fn utc_minute() -> u16 {
    ((SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 60)
        % 1440) as u16
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downloader::{install_test_committed_result, install_test_partial};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot},
    };

    /// A boundary reported by the stalled Range fixture.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum FixtureEvent {
        /// A request was received on a connection.
        Request(String),
        /// A connection was closed.
        Closed,
        /// The fixture stopped accepting new connections.
        Held,
    }

    /// A command for the stalled Range fixture.
    #[derive(Debug, Clone, Copy)]
    enum FixtureCommand {
        /// Keep new connections in the listen backlog.
        Hold,
        /// Accept the pending connections again.
        Resume,
    }

    /// A local origin that serves stalled Range responses.
    ///
    /// The fixture reports every request and every closed connection, so a
    /// test can synchronize on an exact boundary instead of polling a
    /// counter. It can also hold new connections in the listen backlog, so a
    /// cancelled transfer's sockets are reaped before the next transfer
    /// connects and the observed peak measures the client's own concurrency.
    struct StalledRangeServer {
        base_url: String,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<()>,
        events: mpsc::UnboundedReceiver<FixtureEvent>,
        control: mpsc::UnboundedSender<FixtureCommand>,
        seen: Vec<FixtureEvent>,
        open: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl StalledRangeServer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let shutdown = CancellationToken::new();
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let (control_tx, mut control_rx) = mpsc::unbounded_channel();
            let open = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let server_shutdown = shutdown.clone();
            let server_open = open.clone();
            let server_peak = peak.clone();
            let task = tokio::spawn(async move {
                let mut held = false;
                'server: loop {
                    if held {
                        tokio::select! {
                            biased;
                            _ = server_shutdown.cancelled() => break 'server,
                            command = control_rx.recv() => match command {
                                Some(FixtureCommand::Hold) => {}
                                Some(FixtureCommand::Resume) => held = false,
                                None => break 'server,
                            },
                        }
                        continue;
                    }
                    let accepted = tokio::select! {
                        biased;
                        _ = server_shutdown.cancelled() => break 'server,
                        command = control_rx.recv() => match command {
                            Some(FixtureCommand::Hold) => {
                                held = true;
                                let _ = event_tx.send(FixtureEvent::Held);
                                continue 'server;
                            }
                            Some(FixtureCommand::Resume) => continue 'server,
                            None => break 'server,
                        },
                        accepted = listener.accept() => accepted,
                    };
                    let Ok((mut stream, _)) = accepted else { break };
                    let now_open = server_open.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                    server_peak.fetch_max(now_open, AtomicOrdering::AcqRel);
                    let request = read_request_text(&mut stream).await;
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_owned();
                    let range = request.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("range")
                            .then(|| value.trim().strip_prefix("bytes=").map(str::to_owned))
                            .flatten()
                    });
                    let Some((start, end)) = range
                        .as_deref()
                        .and_then(|range| range.split_once('-'))
                        .and_then(|(start, end)| {
                            Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?))
                        })
                    else {
                        drop(stream);
                        server_open.fetch_sub(1, AtomicOrdering::AcqRel);
                        let _ = event_tx.send(FixtureEvent::Closed);
                        continue;
                    };
                    let total: u64 = 8 * 1024 * 1024;
                    let length = end - start + 1;
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes {start}-{end}/{total}\r\nETag: \"fixture-v1\"\r\nConnection: close\r\n\r\n"
                    );
                    let _ = event_tx.send(FixtureEvent::Request(path));
                    if start == 0 && end == 0 {
                        // The probe is a complete, short exchange. Answering it
                        // in the accept loop keeps its connection closed before
                        // the next accept, so a probe and a segment are never
                        // counted as concurrent.
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(b"x").await;
                        drop(stream);
                        server_open.fetch_sub(1, AtomicOrdering::AcqRel);
                        let _ = event_tx.send(FixtureEvent::Closed);
                        continue;
                    }
                    // A segment connection stays open until the client closes it.
                    let stop = server_shutdown.clone();
                    let event_tx = event_tx.clone();
                    let server_open = server_open.clone();
                    tokio::spawn(async move {
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(b"x").await;
                        let mut client_closed = [0u8; 1];
                        tokio::select! {
                            _ = stop.cancelled() => {},
                            _ = stream.read(&mut client_closed) => {},
                        }
                        drop(stream);
                        server_open.fetch_sub(1, AtomicOrdering::AcqRel);
                        let _ = event_tx.send(FixtureEvent::Closed);
                    });
                }
            });
            Self {
                base_url: format!("http://{address}"),
                shutdown,
                task,
                events: event_rx,
                control: control_tx,
                seen: Vec::new(),
                open,
                peak,
            }
        }

        /// Waits until `count` requests for `path` have been received.
        async fn wait_for_requests(&mut self, path: &str, count: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.request_count(path) < count {
                    let event = self
                        .events
                        .recv()
                        .await
                        .expect("the fixture server is still running");
                    self.seen.push(event);
                }
            })
            .await
            .expect("the fixture server did not report the expected requests");
        }

        /// Waits until `count` connections have been closed.
        async fn wait_for_closed(&mut self, count: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.closed_count() < count {
                    let event = self
                        .events
                        .recv()
                        .await
                        .expect("the fixture server is still running");
                    self.seen.push(event);
                }
            })
            .await
            .expect("the fixture server did not report the expected closures");
        }

        /// Waits until the fixture stopped accepting new connections.
        async fn wait_for_held(&mut self) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.held_count() == 0 {
                    let event = self
                        .events
                        .recv()
                        .await
                        .expect("the fixture server is still running");
                    self.seen.push(event);
                }
            })
            .await
            .expect("the fixture server did not confirm the hold");
        }

        fn request_count(&self, path: &str) -> usize {
            self.seen
                .iter()
                .filter(|event| match event {
                    FixtureEvent::Request(requested) => requested == path,
                    FixtureEvent::Closed | FixtureEvent::Held => false,
                })
                .count()
        }

        fn closed_count(&self) -> usize {
            self.seen
                .iter()
                .filter(|event| matches!(event, FixtureEvent::Closed))
                .count()
        }

        fn held_count(&self) -> usize {
            self.seen
                .iter()
                .filter(|event| matches!(event, FixtureEvent::Held))
                .count()
        }

        /// The highest number of connections that were open at the same time.
        fn peak(&self) -> usize {
            self.peak.load(AtomicOrdering::Acquire)
        }

        fn open_count(&self) -> usize {
            self.open.load(AtomicOrdering::Acquire)
        }

        /// Keeps new connections in the listen backlog.
        fn hold(&self) {
            let _ = self.control.send(FixtureCommand::Hold);
        }

        /// Accepts the pending connections again.
        fn resume(&self) {
            let _ = self.control.send(FixtureCommand::Resume);
        }

        async fn stop(self) {
            self.shutdown.cancel();
            let _ = self.task.await;
        }
    }

    /// The segmented path only engages for bodies of at least 8 MiB, so the
    /// fixture origin serves exactly that much.
    fn range_payload() -> Vec<u8> {
        (0..8 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect()
    }

    /// A Range-capable origin with a strong ETag. It counts the requests that
    /// arrived with and without a `Range` header, so a test can tell a segmented
    /// transfer apart from a single-stream fallback.
    async fn start_range_server() -> (
        String,
        CancellationToken,
        tokio::task::JoinHandle<()>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let range_hits = Arc::new(AtomicUsize::new(0));
        let full_hits = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new(range_payload());
        let server_shutdown = shutdown.clone();
        let server_range_hits = range_hits.clone();
        let server_full_hits = full_hits.clone();
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    biased;
                    _ = server_shutdown.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _)) = accepted else { break };
                let body = payload.clone();
                let ranges = server_range_hits.clone();
                let full = server_full_hits.clone();
                tokio::spawn(async move {
                    serve_range_request(stream, body, ranges, full).await;
                });
            }
        });
        (
            format!("http://{address}"),
            shutdown,
            task,
            range_hits,
            full_hits,
        )
    }

    async fn serve_range_request(
        mut stream: TcpStream,
        payload: Arc<Vec<u8>>,
        range_hits: Arc<AtomicUsize>,
        full_hits: Arc<AtomicUsize>,
    ) {
        let request = read_request_text(&mut stream).await;
        let total = payload.len();
        let range = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("range")
                .then(|| value.trim().strip_prefix("bytes=").map(str::to_owned))
                .flatten()
        });
        let Some(range) = range else {
            full_hits.fetch_add(1, AtomicOrdering::AcqRel);
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nETag: \"fixture-v1\"\r\nConnection: close\r\n\r\n"
            );
            if stream.write_all(head.as_bytes()).await.is_ok() {
                let _ = stream.write_all(payload.as_slice()).await;
            }
            return;
        };
        let Some((start, end)) = range.split_once('-') else {
            return;
        };
        let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) else {
            return;
        };
        if start > end || end >= total {
            return;
        }
        range_hits.fetch_add(1, AtomicOrdering::AcqRel);
        let head = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nETag: \"fixture-v1\"\r\nConnection: close\r\n\r\n",
            end - start + 1
        );
        if stream.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        let _ = stream.write_all(&payload[start..=end]).await;
    }

    fn range_request(url: String, destination: &Path) -> DownloadRequest {
        DownloadRequest {
            url,
            destination: destination.to_path_buf(),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 8 * 1024 * 1024,
            max_duration: Duration::from_secs(30),
        }
    }

    #[test]
    fn concurrency_limit_must_be_positive() {
        assert!(matches!(
            DownloadQueue::new(0),
            Err(QueueError::InvalidConcurrency)
        ));
        assert!(DownloadQueue::new(1).is_ok());
    }

    #[test]
    fn segment_connections_are_capped_by_the_instance_job_quota() {
        let queue = DownloadQueue::new(2).unwrap();
        assert!(matches!(
            queue.set_max_segments(0),
            Err(QueueError::InvalidSegmentLimit)
        ));
        assert!(matches!(
            queue.set_max_segments(5),
            Err(QueueError::InvalidSegmentLimit)
        ));
        queue.set_max_segments(4).unwrap();
        assert_eq!(queue.inner.active_connection_count(), 2);

        queue.set_max_segments(1).unwrap();
        assert_eq!(queue.inner.active_connection_count(), 1);
    }

    #[test]
    fn global_bandwidth_api_accepts_maximum_and_rejects_maximum_plus_one() {
        let maximum = MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND;
        assert!(DownloadQueue::new_with_global_bandwidth_limit(1, maximum).is_ok());
        assert!(matches!(
            DownloadQueue::new_with_global_bandwidth_limit(1, maximum + 1),
            Err(QueueError::InvalidGlobalBandwidthLimit)
        ));

        let queue = DownloadQueue::new(1).unwrap();
        assert!(queue.set_global_bandwidth_limit(maximum).is_ok());
        assert!(matches!(
            queue.set_global_bandwidth_limit(maximum + 1),
            Err(QueueError::InvalidGlobalBandwidthLimit)
        ));
        assert_eq!(
            queue.inner.bandwidth_limiter.configured_rate_for_test(),
            maximum
        );
    }

    #[tokio::test]
    async fn permit_waiters_are_ordered_high_to_low_with_fifo_ties() {
        let (pool, mut registered) = PriorityPermitPool::new_observed(1);
        let held = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let waiters = [
            (Uuid::new_v4(), Priority::Low),
            (Uuid::new_v4(), Priority::Normal),
            (Uuid::new_v4(), Priority::High),
        ];
        let mut releases = Vec::new();
        let mut tasks = Vec::new();
        for (id, priority) in waiters {
            let (release_tx, release_rx) = oneshot::channel();
            tasks.push(spawn_permit_waiter(
                pool.clone(),
                id,
                priority_ref(priority),
                started_tx.clone(),
                release_rx,
            ));
            assert_eq!(registered.recv().await, Some(()));
            releases.push((id, Some(release_tx)));
        }

        drop(held);
        for expected in [waiters[2].0, waiters[1].0, waiters[0].0] {
            assert_eq!(started_rx.recv().await, Some(expected));
            releases
                .iter_mut()
                .find(|(id, _)| *id == expected)
                .unwrap()
                .1
                .take()
                .unwrap()
                .send(())
                .unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn permit_waiters_keep_fifo_order_at_the_same_priority() {
        let (pool, mut registered) = PriorityPermitPool::new_observed(1);
        let held = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let mut releases = Vec::new();
        let mut tasks = Vec::new();
        for id in ids {
            let (release_tx, release_rx) = oneshot::channel();
            tasks.push(spawn_permit_waiter(
                pool.clone(),
                id,
                priority_ref(Priority::Normal),
                started_tx.clone(),
                release_rx,
            ));
            assert_eq!(registered.recv().await, Some(()));
            releases.push((id, Some(release_tx)));
        }

        drop(held);
        for expected in ids {
            assert_eq!(started_rx.recv().await, Some(expected));
            releases
                .iter_mut()
                .find(|(id, _)| *id == expected)
                .unwrap()
                .1
                .take()
                .unwrap()
                .send(())
                .unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn changing_waiting_job_priority_reorders_its_permit_position() {
        let directory = tempfile::tempdir().unwrap();
        let (mut queue, low_id) = queue_with_entry(
            directory.path().join("low.bin"),
            JobState::Queued,
            DownloadSession::default(),
        )
        .await;
        let normal_id = Uuid::new_v4();
        let normal_priority_ref = priority_ref(Priority::Normal);
        let (registered_tx, mut registered) = mpsc::unbounded_channel();
        let inner = Arc::get_mut(&mut queue.inner).expect("test queue has a unique inner");
        Arc::get_mut(&mut inner.permits)
            .expect("test queue has a unique permit pool")
            .registered = Some(registered_tx);
        let pool = inner.permits.clone();
        let held = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        let low_priority_ref = queue
            .inner
            .jobs
            .lock()
            .await
            .get(&low_id)
            .unwrap()
            .priority
            .clone();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (low_release_tx, low_release_rx) = oneshot::channel();
        let low_task = spawn_permit_waiter(
            pool.clone(),
            low_id,
            low_priority_ref,
            started_tx.clone(),
            low_release_rx,
        );
        assert_eq!(registered.recv().await, Some(()));
        let (normal_release_tx, normal_release_rx) = oneshot::channel();
        let normal_task = spawn_permit_waiter(
            pool.clone(),
            normal_id,
            normal_priority_ref,
            started_tx,
            normal_release_rx,
        );
        assert_eq!(registered.recv().await, Some(()));

        queue.set_priority(low_id, Priority::High).await.unwrap();
        assert_eq!(
            queue
                .snapshots()
                .await
                .into_iter()
                .find(|snapshot| snapshot.id == low_id)
                .unwrap()
                .priority,
            Priority::High
        );
        drop(held);
        assert_eq!(started_rx.recv().await, Some(low_id));
        low_release_tx.send(()).unwrap();
        assert_eq!(started_rx.recv().await, Some(normal_id));
        normal_release_tx.send(()).unwrap();
        low_task.await.unwrap();
        normal_task.await.unwrap();
    }

    #[tokio::test]
    async fn range_jobs_compete_for_connection_quota_with_priority_without_preemption() {
        let mut server = StalledRangeServer::start().await;
        let base_url = server.base_url.clone();
        let directory = tempfile::tempdir().unwrap();
        let mut queue = DownloadQueue::new(2).unwrap();
        queue.set_max_segments(2).unwrap();
        let (pool, mut registered) = PriorityPermitPool::new_observed(2);
        Arc::get_mut(&mut queue.inner)
            .expect("test queue is uniquely owned")
            .permits = pool.clone();

        // Occupy both scheduler permits so priority can be changed before either
        // Range-capable transfer starts. The connection-slot quota remains two.
        let held_first = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        let held_second = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        assert_eq!(registered.recv().await, Some(()));

        let make_request = |path: &str, filename: &str| DownloadRequest {
            url: format!("{base_url}/{path}"),
            destination: directory.path().join(filename),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 8 * 1024 * 1024,
            max_duration: Duration::from_secs(60),
        };
        let low_id = queue
            .enqueue(make_request("low", "low.bin"), None)
            .await
            .unwrap();
        let high_id = queue
            .enqueue(make_request("high", "high.bin"), None)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            registered.recv().await.unwrap();
            registered.recv().await.unwrap();
        })
        .await
        .expect("both Range jobs should wait for a scheduler permit");
        queue.set_priority(low_id, Priority::Low).await.unwrap();
        queue.set_priority(high_id, Priority::High).await.unwrap();

        drop(held_first);
        // The High-priority job takes the first scheduler permit and opens its
        // Range probe plus both segment streams.
        server.wait_for_requests("/high", 3).await;
        let high_session = queue
            .inner
            .jobs
            .lock()
            .await
            .get(&high_id)
            .unwrap()
            .download_session
            .clone();
        tokio::time::timeout(Duration::from_secs(5), high_session.wait_for_bytes(2))
            .await
            .expect("the first transfer should hold the complete segment reservation");
        assert_eq!(queue.inner.connection_slots.available_permits(), 0);
        assert_eq!(server.request_count("/high"), 3);
        assert_eq!(server.request_count("/low"), 0);
        assert!(server.peak() <= 2);

        // A priority update is for waiting jobs only: it must not preempt the
        // active high-priority transfer or its atomic two-slot reservation.
        queue.set_priority(low_id, Priority::High).await.unwrap();
        drop(held_second);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let jobs = queue.snapshots().await;
                let low_started = jobs
                    .iter()
                    .any(|job| job.id == low_id && job.state == JobState::Downloading);
                if low_started {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second transfer should reach its connection-slot wait");
        assert_eq!(server.request_count("/low"), 0);
        assert_eq!(queue.inner.connection_slots.available_permits(), 0);
        assert!(queue
            .snapshots()
            .await
            .iter()
            .any(|job| job.id == high_id && job.state == JobState::Downloading));
        let high_cancel = queue
            .inner
            .jobs
            .lock()
            .await
            .get(&high_id)
            .unwrap()
            .cancel
            .clone();
        assert!(!high_cancel.is_cancelled());

        // Hold new connections so the cancelled transfer's sockets are reaped
        // before the waiting transfer connects. The observed peak then
        // measures the client's own concurrency instead of a handover race.
        server.hold();
        server.wait_for_held().await;
        // Releasing the first transfer lets the second obtain its probe and
        // full two-slot reservation; this also guards against holding a probe
        // slot while waiting atomically for all segment slots.
        match queue.cancel(high_id).await {
            Ok(()) => {}
            // A local cleanup that cannot finish yet is a retryable state,
            // not a failed transfer.
            Err(QueueError::Download(DownloadError::CleanupPending { .. })) => {}
            Err(error) => panic!("unexpected cancel outcome: {error}"),
        }
        server.wait_for_closed(3).await;
        server.resume();
        server.wait_for_requests("/low", 3).await;
        let low_session = queue
            .inner
            .jobs
            .lock()
            .await
            .get(&low_id)
            .unwrap()
            .download_session
            .clone();
        tokio::time::timeout(Duration::from_secs(5), low_session.wait_for_bytes(2))
            .await
            .expect("the second transfer should finish opening both Range streams");
        assert!(server.peak() <= 2);

        match queue.cancel(low_id).await {
            Ok(()) => {}
            Err(QueueError::Download(DownloadError::CleanupPending { .. })) => {}
            Err(error) => panic!("unexpected cancel outcome: {error}"),
        }
        // The cancelled transfer gives its connection slots back even when its
        // local cleanup is still pending.
        assert_eq!(queue.inner.connection_slots.available_permits(), 2);
        server.wait_for_closed(6).await;
        assert_eq!(server.open_count(), 0);
        let peak = server.peak();
        server.stop().await;
        assert!(peak <= 2);
    }

    /// A cancelled transfer must give its connection slots back, so a waiting
    /// job is never blocked by a transfer that is no longer running.
    #[tokio::test]
    async fn cancelling_a_transfer_releases_its_connection_slots() {
        let mut server = StalledRangeServer::start().await;
        let base_url = server.base_url.clone();
        let directory = tempfile::tempdir().unwrap();
        let queue = DownloadQueue::new(2).unwrap();
        queue.set_max_segments(2).unwrap();
        let request = DownloadRequest {
            url: format!("{base_url}/stuck"),
            destination: directory.path().join("stuck.bin"),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 8 * 1024 * 1024,
            max_duration: Duration::from_secs(60),
        };
        let id = queue.enqueue(request, None).await.unwrap();
        server.wait_for_requests("/stuck", 3).await;
        assert_eq!(queue.inner.connection_slots.available_permits(), 0);

        // The local cleanup of a cancelled transfer may still be pending, but
        // the connection slots are released either way.
        let outcome = queue.cancel(id).await;
        assert!(matches!(
            outcome,
            Ok(()) | Err(QueueError::Download(DownloadError::CleanupPending { .. }))
        ));
        assert_eq!(queue.inner.connection_slots.available_permits(), 2);
        server.wait_for_closed(3).await;
        server.stop().await;
    }

    /// The queue worker must take the segmented path for a Range-capable origin
    /// once more than one segment is allowed, and must stay on a single stream
    /// when only one segment is allowed.
    #[tokio::test]
    async fn queue_worker_segments_a_range_capable_origin_and_streams_it_with_one_segment() {
        let payload = range_payload();
        let (base_url, shutdown, server, range_hits, full_hits) = start_range_server().await;
        let directory = tempfile::tempdir().unwrap();
        let timeout = Duration::from_secs(30);

        let segmented_queue = DownloadQueue::new(2).unwrap();
        segmented_queue.set_max_segments(2).unwrap();
        let segmented_path = directory.path().join("segmented.bin");
        let request = range_request(format!("{base_url}/segmented"), &segmented_path);
        let id = segmented_queue.enqueue(request, None).await.unwrap();
        let segmented = wait_for_completion_within(&segmented_queue, id, timeout).await;
        assert_eq!(segmented.bytes, payload.len() as u64);
        let published = std::fs::read(&segmented_path).unwrap();
        assert_eq!(published, payload.as_slice());
        // One Range probe plus two segment bodies, and no single-stream fallback.
        assert_eq!(range_hits.load(AtomicOrdering::Acquire), 3);
        assert_eq!(full_hits.load(AtomicOrdering::Acquire), 0);

        let single_queue = DownloadQueue::new(2).unwrap();
        single_queue.set_max_segments(1).unwrap();
        let single_path = directory.path().join("single.bin");
        let request = range_request(format!("{base_url}/single"), &single_path);
        let id = single_queue.enqueue(request, None).await.unwrap();
        let single = wait_for_completion_within(&single_queue, id, timeout).await;
        assert_eq!(single.bytes, payload.len() as u64);
        let published = std::fs::read(&single_path).unwrap();
        assert_eq!(published, payload.as_slice());
        // One segment never probes for ranges: the body arrives in one plain request.
        assert_eq!(range_hits.load(AtomicOrdering::Acquire), 3);
        assert_eq!(full_hits.load(AtomicOrdering::Acquire), 1);

        shutdown.cancel();
        let _ = server.await;
    }

    #[tokio::test]
    async fn cancelled_waiter_is_removed_and_raii_release_restores_capacity() {
        let (pool, mut registered) = PriorityPermitPool::new_observed(1);
        let held = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        let cancel = CancellationToken::new();
        let waiter_pool = pool.clone();
        let waiter_cancel = cancel.clone();
        let waiter = tokio::spawn(async move {
            waiter_pool
                .acquire(priority_ref(Priority::Low), &waiter_cancel, None)
                .await
        });
        registered.recv().await.unwrap();
        cancel.cancel();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(PermitWaitError::Cancelled)
        ));
        assert_eq!(pool.waiter_count(), 0);

        drop(held);
        assert_eq!(pool.available_count(), 1);
        let acquired = pool
            .acquire(priority_ref(Priority::Low), &CancellationToken::new(), None)
            .await
            .unwrap();
        assert_eq!(pool.available_count(), 0);
        drop(acquired);
        assert_eq!(pool.available_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_deadline_removes_waiter_without_consuming_a_permit() {
        let (pool, mut registered) = PriorityPermitPool::new_observed(1);
        let held = pool
            .acquire(
                priority_ref(Priority::Normal),
                &CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(registered.recv().await, Some(()));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let waiter_pool = pool.clone();
        let waiter = tokio::spawn(async move {
            waiter_pool
                .acquire(
                    priority_ref(Priority::Low),
                    &CancellationToken::new(),
                    Some(deadline),
                )
                .await
        });
        registered.recv().await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(
            waiter.await.unwrap(),
            Err(PermitWaitError::DeadlineElapsed)
        ));
        assert_eq!(pool.waiter_count(), 0);
        drop(held);
        assert_eq!(pool.available_count(), 1);
    }

    fn priority_ref(priority: Priority) -> Arc<std::sync::atomic::AtomicU8> {
        Arc::new(std::sync::atomic::AtomicU8::new(priority as u8))
    }

    fn spawn_permit_waiter(
        pool: Arc<PriorityPermitPool>,
        id: Uuid,
        priority: Arc<std::sync::atomic::AtomicU8>,
        started: mpsc::UnboundedSender<Uuid>,
        release: oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let Ok(_permit) = pool
                .acquire(priority, &CancellationToken::new(), None)
                .await
            else {
                return;
            };
            let _ = started.send(id);
            let _ = release.await;
        })
    }

    #[tokio::test]
    async fn snapshots_default_priority_to_normal_and_allow_live_changes() {
        let directory = tempfile::tempdir().unwrap();
        let (queue, id) = queue_with_entry(
            directory.path().join("priority-api.bin"),
            JobState::Queued,
            DownloadSession::default(),
        )
        .await;
        assert_eq!(queue.snapshots().await[0].priority, Priority::Normal);
        queue.set_priority(id, Priority::High).await.unwrap();
        assert_eq!(queue.snapshots().await[0].priority, Priority::High);
    }

    #[tokio::test]
    async fn changing_an_active_jobs_priority_does_not_cancel_its_transfer() {
        let directory = tempfile::tempdir().unwrap();
        let (queue, id) = queue_with_entry(
            directory.path().join("active-priority.bin"),
            JobState::Downloading,
            DownloadSession::default(),
        )
        .await;
        let cancel = queue
            .inner
            .jobs
            .lock()
            .await
            .get(&id)
            .unwrap()
            .cancel
            .clone();

        queue.set_priority(id, Priority::High).await.unwrap();

        assert!(!cancel.is_cancelled());
        assert_eq!(
            queue.snapshots().await[0].state.clone(),
            JobState::Downloading
        );
    }

    #[tokio::test]
    async fn snapshots_expose_bytes_written_for_queue_progress() {
        let directory = tempfile::tempdir().unwrap();
        let partial_path = directory.path().join("owned-partial.bin");
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .await
            .unwrap();
        let mut file = file;
        file.write_all(&[0; 37]).await.unwrap();
        let session = DownloadSession::default();
        install_test_partial(&session, partial_path, file, 37).await;
        let destination = directory.path().join("output.bin");
        let (queue, id) = queue_with_entry(destination, JobState::Downloading, session).await;

        let snapshot = queue
            .snapshots()
            .await
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        assert_eq!(snapshot.bytes_received, 37);
    }

    #[tokio::test]
    async fn queue_pause_or_cancel_publishes_an_already_committed_result() {
        for action in ["pause", "cancel"] {
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("committed.bin");
            tokio::fs::write(&destination, b"committed").await.unwrap();
            let session = DownloadSession::default();
            let committed_result = DownloadResult {
                destination: destination.clone(),
                bytes: 9,
                sha256: "fixture".to_owned(),
                resumed: false,
                cleanup_pending: None,
            };
            install_test_committed_result(&session, committed_result.clone()).await;
            let (queue, id) =
                queue_with_entry(destination.clone(), JobState::Downloading, session).await;
            let cancel = {
                let jobs = queue.inner.jobs.lock().await;
                let job = jobs.get(&id).unwrap();
                job.cancel.clone()
            };

            match action {
                "pause" => queue.pause(id).await.unwrap(),
                "cancel" => queue.cancel(id).await.unwrap(),
                _ => unreachable!(),
            }
            assert!(!cancel.is_cancelled());
            let state = queue
                .inner
                .jobs
                .lock()
                .await
                .get(&id)
                .unwrap()
                .state
                .clone();
            assert_eq!(state, JobState::Completed(committed_result));
            assert_eq!(tokio::fs::read(destination).await.unwrap(), b"committed");
        }
    }

    #[tokio::test]
    async fn completion_does_not_overwrite_pause_or_cancel_in_same_generation() {
        for initial_state in [JobState::Paused, JobState::Cancelled] {
            let directory = tempfile::tempdir().unwrap();
            let destination = directory.path().join("not-committed.bin");
            let (queue, id) = queue_with_entry(
                destination.clone(),
                initial_state.clone(),
                DownloadSession::default(),
            )
            .await;
            let run_id = queue.inner.jobs.lock().await.get(&id).unwrap().run_id;
            set_completed(
                &queue.inner,
                id,
                run_id,
                DownloadResult {
                    destination: destination.clone(),
                    bytes: 1,
                    sha256: "fixture".to_owned(),
                    resumed: false,
                    cleanup_pending: None,
                },
            )
            .await;

            let state = queue
                .inner
                .jobs
                .lock()
                .await
                .get(&id)
                .unwrap()
                .state
                .clone();
            assert_eq!(state, initial_state);
            assert!(!tokio::fs::try_exists(destination).await.unwrap());
        }
    }

    #[tokio::test]
    async fn invalid_urls_never_enter_the_queue() {
        let queue = DownloadQueue::new(1).unwrap();
        let request = DownloadRequest {
            url: "file:///private/data".into(),
            destination: PathBuf::from("output.bin"),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(60),
        };
        assert!(matches!(
            queue.enqueue(request, None).await,
            Err(QueueError::Download(DownloadError::Url(_)))
        ));
        assert!(queue.snapshots().await.is_empty());
    }

    #[tokio::test]
    async fn scheduled_jobs_wait_for_utc_window_and_pause_or_cancel_prevents_start() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connected, mut connected_notification) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
            let _ = connected.send(());
        });
        let window = DailyWindow::new(61, 62).unwrap();

        for action in ["pause", "cancel"] {
            let directory = tempfile::tempdir().unwrap();
            let request = DownloadRequest {
                url: format!("http://{address}/scheduled"),
                destination: directory.path().join("scheduled.bin"),
                expected_sha256: None,
                max_retries: 0,
                bytes_per_second: None,
                max_bytes: 1024,
                max_duration: Duration::from_secs(30),
            };
            let queue = DownloadQueue::new_with_test_utc_minute(1, 60).unwrap();
            let id = queue.enqueue(request, Some(window)).await.unwrap();

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let scheduled = queue
                        .snapshots()
                        .await
                        .into_iter()
                        .any(|job| job.id == id && job.state == JobState::Scheduled);
                    if scheduled {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("future UTC job should enter Scheduled state");
            assert_eq!(
                queue.inner.permits.waiter_count(),
                0,
                "a Scheduled job does not join permit ordering before its window opens"
            );

            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            assert!(
                matches!(
                    connected_notification.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "scheduled job must not connect before its UTC window"
            );
            match action {
                "pause" => queue.pause(id).await.unwrap(),
                "cancel" => queue.cancel(id).await.unwrap(),
                _ => unreachable!(),
            }
            let state = queue
                .snapshots()
                .await
                .into_iter()
                .find(|job| job.id == id)
                .unwrap()
                .state;
            match action {
                "pause" => assert_eq!(state, JobState::Paused),
                "cancel" => assert_eq!(state, JobState::Cancelled),
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    connected_notification.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "paused/cancelled scheduled job must not start a request"
            );
            queue.set_test_utc_minute(61);
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            assert!(
                matches!(
                    connected_notification.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "paused/cancelled job must remain stopped when the UTC window opens"
            );
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn scheduled_job_starts_and_completes_after_test_utc_clock_enters_window() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connected, mut connected_notification) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = connected.send(stream);
        });
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("scheduled-complete.bin");
        let request = DownloadRequest {
            url: format!("http://{address}/scheduled"),
            destination: destination.clone(),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(30),
        };
        let queue = DownloadQueue::new_with_test_utc_minute(1, 60).unwrap();
        let id = queue
            .enqueue(request, Some(DailyWindow::new(61, 62).unwrap()))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if queue
                    .snapshots()
                    .await
                    .into_iter()
                    .any(|job| job.id == id && job.state == JobState::Scheduled)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("job should wait in Scheduled state before UTC minute 61");
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            connected_notification.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        queue.set_test_utc_minute(61);
        let mut stream = tokio::time::timeout(Duration::from_secs(3), &mut connected_notification)
            .await
            .expect("job should connect after the UTC window opens")
            .unwrap();
        server.await.unwrap();
        let request = read_request_text(&mut stream).await;
        assert!(request.starts_with("GET /scheduled HTTP/1.1"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
            .await
            .unwrap();

        let completed = wait_for_completion(&queue, id).await;
        assert_eq!(queue.inner.schedule_clock.current_minute(), 61);
        assert_eq!(completed.bytes, 5);
        assert_eq!(tokio::fs::read(destination).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn scheduled_job_rechecks_utc_window_after_waiting_for_transfer_permit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (first_request_tx, first_request_rx) = oneshot::channel();
        let (release_first_tx, release_first_rx) = oneshot::channel();
        let (second_accepting_tx, second_accepting_rx) = oneshot::channel();
        let (second_stream_tx, mut second_stream_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let first_request = read_request_text(&mut first).await;
            let _ = first_request_tx.send(first_request);
            release_first_rx.await.unwrap();
            first
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfirst",
                )
                .await
                .unwrap();
            drop(first);

            let _ = second_accepting_tx.send(());
            let (second, _) = listener.accept().await.unwrap();
            let _ = second_stream_tx.send(second);
        });

        let directory = tempfile::tempdir().unwrap();
        let queue = DownloadQueue::new_with_test_utc_minute(1, 60).unwrap();
        let first_destination = directory.path().join("first.bin");
        let first_id = queue
            .enqueue(
                DownloadRequest {
                    url: format!("http://{address}/first"),
                    destination: first_destination.clone(),
                    expected_sha256: None,
                    max_retries: 0,
                    bytes_per_second: None,
                    max_bytes: 1024,
                    max_duration: Duration::from_secs(30),
                },
                None,
            )
            .await
            .unwrap();
        let first_request = tokio::time::timeout(Duration::from_secs(3), first_request_rx)
            .await
            .expect("first job should acquire the only transfer permit")
            .unwrap();
        assert!(first_request.starts_with("GET /first HTTP/1.1"));

        let scheduled_destination = directory.path().join("scheduled-after-wait.bin");
        let scheduled_id = queue
            .enqueue(
                DownloadRequest {
                    url: format!("http://{address}/scheduled"),
                    destination: scheduled_destination.clone(),
                    expected_sha256: None,
                    max_retries: 0,
                    bytes_per_second: None,
                    max_bytes: 1024,
                    max_duration: Duration::from_secs(30),
                },
                Some(DailyWindow::new(60, 61).unwrap()),
            )
            .await
            .unwrap();
        let transfer_lock = {
            queue
                .inner
                .jobs
                .lock()
                .await
                .get(&scheduled_id)
                .unwrap()
                .transfer_lock
                .clone()
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if transfer_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduled worker should hold its transfer lock while waiting for the permit");

        queue.set_test_utc_minute(61);
        release_first_tx.send(()).unwrap();
        let first_result = wait_for_completion(&queue, first_id).await;
        assert_eq!(first_result.bytes, 5);
        second_accepting_rx.await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if transfer_lock.try_lock().is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("out-of-window worker should release its transfer lock and permit");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = queue
                    .snapshots()
                    .await
                    .into_iter()
                    .find(|job| job.id == scheduled_id)
                    .unwrap()
                    .state;
                if matches!(state, JobState::Scheduled) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker should wait for the next window after releasing resources");
        assert!(matches!(
            second_stream_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // The injected clock uses minute-of-day only; returning to minute 60
        // represents the next daily occurrence of this window.
        queue.set_test_utc_minute(60);
        let mut second = tokio::time::timeout(Duration::from_secs(3), &mut second_stream_rx)
            .await
            .expect("scheduled worker should resume at the next UTC window")
            .unwrap();
        let request = read_request_text(&mut second).await;
        assert!(request.starts_with("GET /scheduled HTTP/1.1"));
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlater")
            .await
            .unwrap();
        let second_result = wait_for_completion(&queue, scheduled_id).await;
        assert_eq!(second_result.bytes, 5);
        assert_eq!(
            tokio::fs::read(scheduled_destination).await.unwrap(),
            b"later"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scheduled_job_rechecks_window_after_waiting_for_transfer_lock() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stream_tx, mut stream_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream_tx.send(stream);
        });

        let directory = tempfile::tempdir().unwrap();
        let mut queue = DownloadQueue::new_with_test_utc_minute(1, 60).unwrap();
        let waiting_for_lock = queue.observe_transfer_lock_waiting();
        let request = DownloadRequest {
            url: format!("http://{address}/lock-scheduled"),
            destination: directory.path().join("lock-scheduled.bin"),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(30),
        };
        request.validate().unwrap();
        let destination = request.destination.clone();
        let id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let transfer_lock = Arc::new(Mutex::new(()));
        let held_lock = transfer_lock.clone().lock_owned().await;
        queue.inner.jobs.lock().await.insert(
            id,
            JobEntry {
                destination_key: destination_key(&destination),
                history_generation: Uuid::from_u128(0x1251).to_string(),
                request,
                window: Some(DailyWindow::new(60, 61).unwrap()),
                state: JobState::Queued,
                priority: Arc::new(std::sync::atomic::AtomicU8::new(Priority::Normal as u8)),
                cancel: cancel.clone(),
                run_id,
                transfer_lock,
                download_session: DownloadSession::default(),
            },
        );
        queue.launch(id, run_id, cancel);

        tokio::time::timeout(Duration::from_secs(1), waiting_for_lock.notified())
            .await
            .expect("scheduled worker should reach the held transfer lock");
        queue.set_test_utc_minute(61);
        drop(held_lock);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = queue
                    .snapshots()
                    .await
                    .into_iter()
                    .find(|job| job.id == id)
                    .unwrap()
                    .state;
                if matches!(state, JobState::Scheduled) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker should reschedule after acquiring the lock outside its window");
        assert!(matches!(
            stream_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        queue.set_test_utc_minute(60);
        let mut stream = tokio::time::timeout(Duration::from_secs(3), &mut stream_rx)
            .await
            .expect("job should connect when the next scheduled window opens")
            .unwrap();
        let request = read_request_text(&mut stream).await;
        assert!(request.starts_with("GET /lock-scheduled HTTP/1.1"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnext")
            .await
            .unwrap();
        let completed = wait_for_completion(&queue, id).await;
        assert_eq!(completed.bytes, 4);
        assert_eq!(tokio::fs::read(destination).await.unwrap(), b"next");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn immediate_pause_resume_preserves_partial_and_original_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (range_seen, range_received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let _ = read_request_text(&mut first).await;
            first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: keep-alive\r\n\r\nhello").await.unwrap();

            let (mut resumed, _) = listener.accept().await.unwrap();
            let request = read_request_text(&mut resumed).await.to_ascii_lowercase();
            assert!(request.contains("range: bytes=5-"));
            assert!(request.contains("if-range: \"v1\""));
            let _ = range_seen.send(request);
            resumed.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 5-9/10\r\nContent-Length: 5\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nworld").await.unwrap();
            drop(first);
        });

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("queue-resume.bin");
        let request = DownloadRequest {
            url: format!("http://{address}/file"),
            destination: destination.clone(),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(5),
        };
        let queue = DownloadQueue::new(1).unwrap();
        let id = queue.enqueue(request, None).await.unwrap();
        let (session, old_cancel) = {
            let jobs = queue.inner.jobs.lock().await;
            let job = jobs.get(&id).unwrap();
            (job.download_session.clone(), job.cancel.clone())
        };
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_bytes(5))
            .await
            .unwrap();
        let original_deadline = session.absolute_deadline().unwrap();

        queue.pause(id).await.unwrap();
        assert!(old_cancel.is_cancelled());
        queue.resume(id).await.unwrap();
        let ranged_request = tokio::time::timeout(Duration::from_secs(3), range_received)
            .await
            .unwrap()
            .unwrap();
        assert!(ranged_request.contains("range: bytes=5-"));
        let completed = wait_for_completion(&queue, id).await;
        assert!(completed.resumed);
        assert_eq!(completed.bytes, 10);
        assert_eq!(
            session.absolute_deadline(),
            Some(original_deadline),
            "resume must preserve the original absolute deadline"
        );
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"helloworld");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stale_worker_failure_cannot_discard_the_resumed_generation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (range_seen, range_received) = oneshot::channel();
        let (close_old_response, old_response_close) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let _ = read_request_text(&mut first).await;
            first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nhello").await.unwrap();
            old_response_close.await.unwrap();
            drop(first);

            let (mut resumed, _) = listener.accept().await.unwrap();
            let request = read_request_text(&mut resumed).await.to_ascii_lowercase();
            assert!(request.contains("range: bytes=5-"));
            assert!(request.contains("if-range: \"v1\""));
            let _ = range_seen.send(());
            resumed.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 5-9/10\r\nContent-Length: 5\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nworld").await.unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("stale-worker.bin");
        let request = DownloadRequest {
            url: format!("http://{address}/file"),
            destination: destination.clone(),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(30),
        };
        let queue = DownloadQueue::new(1).unwrap();
        let id = queue.enqueue(request, None).await.unwrap();
        let (session, old_run_id) = {
            let jobs = queue.inner.jobs.lock().await;
            let job = jobs.get(&id).unwrap();
            (job.download_session.clone(), job.run_id)
        };
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_bytes(5))
            .await
            .unwrap();
        let original_deadline = session.absolute_deadline().unwrap();

        // Model a body failure already in flight at the pause boundary. Keep the
        // old token live so its failure is delivered after resume schedules a new run.
        {
            let mut jobs = queue.inner.jobs.lock().await;
            jobs.get_mut(&id).unwrap().state = JobState::Paused;
        }
        queue.resume(id).await.unwrap();
        let new_run_id = queue.inner.jobs.lock().await.get(&id).unwrap().run_id;
        assert_ne!(old_run_id, new_run_id);
        close_old_response.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), range_received)
            .await
            .unwrap()
            .unwrap();

        let completed = wait_for_completion(&queue, id).await;
        assert!(completed.resumed);
        assert_eq!(session.absolute_deadline(), Some(original_deadline));
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"helloworld");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retry_cleanup_transitions_only_after_alias_removal_for_each_pending_state() {
        for expected_terminal in ["completed", "failed", "cancelled"] {
            let directory = tempfile::tempdir().unwrap();
            let partial_path = directory.path().join("owned-partial.bin");
            let backing_path = directory.path().join("open-partial-handle.bin");
            let file = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&backing_path)
                .await
                .unwrap();
            let mut file = file;
            file.write_all(b"partial").await.unwrap();
            file.sync_all().await.unwrap();
            tokio::fs::create_dir(&partial_path).await.unwrap();
            let blocker = partial_path.join("blocker");
            tokio::fs::write(&blocker, b"keep directory non-empty")
                .await
                .unwrap();

            let session = DownloadSession::default();
            // Keep the owned handle open separately while the alias is replaced
            // with a directory, making remove_file failure deterministic.
            install_test_partial(&session, partial_path.clone(), file, 7).await;
            let original_deadline = session.deadline_for_test().await.unwrap();
            let destination = directory.path().join("completed.bin");
            if expected_terminal == "completed" {
                tokio::fs::write(&destination, b"output!").await.unwrap();
            }
            let initial_state = match expected_terminal {
                "completed" => JobState::Completed(DownloadResult {
                    destination: destination.clone(),
                    bytes: 7,
                    sha256: "fixture".to_owned(),
                    resumed: false,
                    cleanup_pending: Some("old cleanup diagnostic".to_owned()),
                }),
                "failed" => JobState::FailedCleanupPending {
                    operation: "fixture transfer failure".to_owned(),
                    cleanup_error: "old cleanup diagnostic".to_owned(),
                },
                "cancelled" => {
                    JobState::CancelledCleanupPending("old cleanup diagnostic".to_owned())
                }
                _ => unreachable!(),
            };
            let (queue, id) =
                queue_with_entry(destination.clone(), initial_state.clone(), session.clone()).await;
            let initial_label = initial_state.label();

            assert!(queue.retry_cleanup(id).await.is_err());
            let after_failure = queue
                .snapshots()
                .await
                .into_iter()
                .find(|job| job.id == id)
                .unwrap();
            assert_eq!(after_failure.state.label(), initial_label);
            let diagnostic = cleanup_diagnostic_for_test(&after_failure.state).unwrap();
            assert!(!diagnostic.is_empty());
            assert_ne!(diagnostic, "old cleanup diagnostic");
            assert!(tokio::fs::metadata(&partial_path).await.unwrap().is_dir());
            assert_eq!(session.deadline_for_test().await, Some(original_deadline));

            tokio::fs::remove_file(&blocker).await.unwrap();
            tokio::fs::remove_dir(&partial_path).await.unwrap();
            tokio::fs::write(&partial_path, b"temporary alias")
                .await
                .unwrap();
            queue.retry_cleanup(id).await.unwrap();

            let after_success = queue
                .snapshots()
                .await
                .into_iter()
                .find(|job| job.id == id)
                .unwrap();
            match (expected_terminal, after_success.state) {
                ("completed", JobState::Completed(result)) => {
                    assert!(result.cleanup_pending.is_none());
                    assert_eq!(result.destination, destination);
                    assert_eq!(result.bytes, 7);
                    assert_eq!(result.sha256, "fixture");
                    assert!(!result.resumed);
                }
                ("failed", JobState::Failed(operation)) => {
                    assert_eq!(operation, "fixture transfer failure")
                }
                ("cancelled", JobState::Cancelled) => {}
                (unexpected, state) => {
                    panic!("unexpected cleanup transition for {unexpected}: {state:?}")
                }
            }
            assert!(!tokio::fs::try_exists(&partial_path).await.unwrap());
            assert_eq!(session.deadline_for_test().await, None);
            tokio::fs::remove_file(&backing_path).await.unwrap();
        }
    }

    #[cfg(windows)]
    #[test]
    fn destination_identity_is_case_insensitive_on_windows() {
        let directory = tempfile::tempdir().unwrap();
        let upper = destination_key(&directory.path().join("Archive.zip"));
        let lower = destination_key(&directory.path().join("archive.zip"));
        assert!(same_destination(&upper, &lower));
    }

    async fn wait_for_completion(queue: &DownloadQueue, id: Uuid) -> DownloadResult {
        wait_for_completion_within(queue, id, Duration::from_secs(3)).await
    }

    /// Waits for a terminal completion and reports a failed job with its
    /// operation instead of a bare timeout.
    async fn wait_for_completion_within(
        queue: &DownloadQueue,
        id: Uuid,
        timeout: Duration,
    ) -> DownloadResult {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(snapshot) = queue
                    .snapshots()
                    .await
                    .into_iter()
                    .find(|snapshot| snapshot.id == id)
                {
                    match snapshot.state {
                        JobState::Completed(result) => return result,
                        JobState::Failed(operation) => {
                            panic!("the queued download failed: {operation}")
                        }
                        _ => {}
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    async fn read_request_text(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1024];
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }

    async fn queue_with_entry(
        destination: PathBuf,
        state: JobState,
        session: DownloadSession,
    ) -> (DownloadQueue, Uuid) {
        let queue = DownloadQueue::new(1).unwrap();
        let id = Uuid::new_v4();
        let request = DownloadRequest {
            url: "https://example.test/file".to_owned(),
            destination: destination.clone(),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes: 1024,
            max_duration: Duration::from_secs(60),
        };
        queue.inner.jobs.lock().await.insert(
            id,
            JobEntry {
                request,
                destination_key: destination_key(&destination),
                history_generation: Uuid::from_u128(0x1597).to_string(),
                window: None,
                state,
                priority: Arc::new(std::sync::atomic::AtomicU8::new(Priority::Normal as u8)),
                cancel: CancellationToken::new(),
                run_id: Uuid::new_v4(),
                transfer_lock: Arc::new(Mutex::new(())),
                download_session: session,
            },
        );
        (queue, id)
    }

    fn cleanup_diagnostic_for_test(state: &JobState) -> Option<&str> {
        match state {
            JobState::Completed(result) => result.cleanup_pending.as_deref(),
            JobState::FailedCleanupPending { cleanup_error, .. } => Some(cleanup_error),
            JobState::CancelledCleanupPending(error) => Some(error),
            _ => None,
        }
    }
}
