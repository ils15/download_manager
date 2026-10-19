use crate::{integrity, url_policy::validate_http_url};
use futures_util::{stream::FuturesUnordered, Stream, StreamExt};
use reqwest::{header, Client, StatusCode};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const MAX_DOWNLOAD_DURATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const NANOS_PER_SECOND: u128 = 1_000_000_000;
pub(crate) const MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND: u64 = 1_000_000_000;
const GLOBAL_BANDWIDTH_QUANTUM_BYTES: u64 = 16 * 1024;
const GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES: u64 = GLOBAL_BANDWIDTH_QUANTUM_BYTES;
pub const MAX_SEGMENT_COUNT: u8 = 4;
const SEGMENTED_MIN_BYTES: u64 = 8 * 1024 * 1024;

// False protects a live partial alias; true makes the staging path eligible for cleanup.
type SegmentTempRegistry = Arc<StdMutex<HashMap<PathBuf, bool>>>;

#[derive(Clone)]
pub(crate) struct SharedBandwidthLimiter {
    inner: Arc<SharedBandwidthInner>,
}

struct SharedBandwidthInner {
    rate_bytes_per_second: AtomicU64,
    state: StdMutex<SharedBandwidthState>,
    changed: Notify,
}

struct SharedBandwidthState {
    next_ticket: u64,
    waiters: VecDeque<u64>,
    rate_bytes_per_second: u64,
    tokens: u128,
    fractional_tokens: u128,
    last_refill: Instant,
}

struct BandwidthTicket {
    inner: Arc<SharedBandwidthInner>,
    id: u64,
    completed: bool,
}

struct BandwidthReservation {
    inner: Arc<SharedBandwidthInner>,
    unused_bytes: u64,
    limited: bool,
}

impl BandwidthReservation {
    fn limited(inner: Arc<SharedBandwidthInner>, reserved_bytes: u64) -> Self {
        Self {
            inner,
            unused_bytes: reserved_bytes,
            limited: true,
        }
    }

    fn unlimited(inner: Arc<SharedBandwidthInner>) -> Self {
        Self {
            inner,
            unused_bytes: 0,
            limited: false,
        }
    }

    fn charge(&mut self, bytes: u64) {
        if self.limited {
            assert!(
                bytes <= self.unused_bytes,
                "bandwidth reservation overcharged"
            );
            self.unused_bytes -= bytes;
        }
    }
}

impl Drop for BandwidthReservation {
    fn drop(&mut self) {
        if !self.limited || self.unused_bytes == 0 {
            return;
        }
        let rate = self.inner.rate_bytes_per_second.load(Ordering::Acquire);
        if rate == 0 {
            return;
        }

        let mut state = lock_bandwidth_state(&self.inner.state);
        let now = Instant::now();
        if state.rate_bytes_per_second != rate {
            state.rate_bytes_per_second = rate;
            state.tokens = 0;
            state.fractional_tokens = 0;
            state.last_refill = now;
        }
        let capacity = u128::from(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES);
        refill_bandwidth_tokens(&mut state, rate, capacity, now);
        state.tokens = state
            .tokens
            .saturating_add(u128::from(self.unused_bytes))
            .min(capacity);
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

impl SharedBandwidthLimiter {
    pub(crate) fn new(rate_bytes_per_second: u64) -> Result<Self, DownloadError> {
        if rate_bytes_per_second > MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND {
            return Err(DownloadError::InvalidGlobalBandwidthLimit);
        }
        Ok(Self {
            inner: Arc::new(SharedBandwidthInner {
                rate_bytes_per_second: AtomicU64::new(rate_bytes_per_second),
                state: StdMutex::new(SharedBandwidthState {
                    next_ticket: 0,
                    waiters: VecDeque::new(),
                    rate_bytes_per_second,
                    tokens: GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES as u128,
                    fractional_tokens: 0,
                    last_refill: Instant::now(),
                }),
                changed: Notify::new(),
            }),
        })
    }

    pub(crate) fn set_limit(&self, rate_bytes_per_second: u64) -> Result<(), DownloadError> {
        if rate_bytes_per_second > MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND {
            return Err(DownloadError::InvalidGlobalBandwidthLimit);
        }
        self.inner
            .rate_bytes_per_second
            .store(rate_bytes_per_second, Ordering::Release);
        self.inner.changed.notify_waiters();
        Ok(())
    }

    async fn reserve(
        &self,
        bytes: u64,
        cancel: &CancellationToken,
    ) -> Result<BandwidthReservation, DownloadError> {
        if bytes > GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES {
            return Err(DownloadError::InvalidBandwidthReservation);
        }
        if bytes == 0 || self.inner.rate_bytes_per_second.load(Ordering::Acquire) == 0 {
            return Ok(BandwidthReservation::unlimited(self.inner.clone()));
        }

        let ticket_id = {
            let mut state = lock_bandwidth_state(&self.inner.state);
            let ticket_id = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiters.push_back(ticket_id);
            ticket_id
        };
        let mut ticket = BandwidthTicket {
            inner: self.inner.clone(),
            id: ticket_id,
            completed: false,
        };

        enum NextStep {
            Unlimited,
            Granted,
            WaitForTurn,
            WaitForTokens(Duration),
        }

        loop {
            if cancel.is_cancelled() {
                return Err(DownloadError::Cancelled);
            }

            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            let step = {
                let mut state = lock_bandwidth_state(&self.inner.state);
                let now = Instant::now();
                let rate = self.inner.rate_bytes_per_second.load(Ordering::Acquire);
                if rate == 0 {
                    NextStep::Unlimited
                } else {
                    if state.rate_bytes_per_second != rate {
                        state.rate_bytes_per_second = rate;
                        state.tokens = 0;
                        state.fractional_tokens = 0;
                        state.last_refill = now;
                    }
                    if state.waiters.front() != Some(&ticket.id) {
                        NextStep::WaitForTurn
                    } else {
                        let capacity = u128::from(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES);
                        refill_bandwidth_tokens(&mut state, rate, capacity, now);
                        if state.tokens >= u128::from(bytes) {
                            state.tokens -= u128::from(bytes);
                            let _ = state.waiters.pop_front();
                            ticket.completed = true;
                            NextStep::Granted
                        } else {
                            let deficit = u128::from(bytes) - state.tokens;
                            let required_units = deficit
                                .saturating_mul(NANOS_PER_SECOND)
                                .saturating_sub(state.fractional_tokens);
                            let rate = u128::from(rate);
                            let nanos = (required_units.saturating_add(rate - 1) / rate)
                                .clamp(1, u128::from(u64::MAX))
                                as u64;
                            NextStep::WaitForTokens(Duration::from_nanos(nanos))
                        }
                    }
                }
            };

            match step {
                NextStep::Unlimited => {
                    return Ok(BandwidthReservation::unlimited(self.inner.clone()));
                }
                NextStep::Granted => {
                    self.inner.changed.notify_waiters();
                    return Ok(BandwidthReservation::limited(self.inner.clone(), bytes));
                }
                NextStep::WaitForTurn => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                        _ = changed => {}
                    }
                }
                NextStep::WaitForTokens(delay) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                        _ = changed => {},
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    #[cfg(test)]
    async fn acquire(&self, bytes: u64, cancel: &CancellationToken) -> Result<(), DownloadError> {
        let mut reservation = self.reserve(bytes, cancel).await?;
        reservation.charge(bytes);
        Ok(())
    }

    #[cfg(test)]
    fn waiting_count(&self) -> usize {
        lock_bandwidth_state(&self.inner.state).waiters.len()
    }

    #[cfg(test)]
    fn state_rate_for_test(&self) -> u64 {
        lock_bandwidth_state(&self.inner.state).rate_bytes_per_second
    }

    #[cfg(test)]
    pub(crate) fn configured_rate_for_test(&self) -> u64 {
        self.inner.rate_bytes_per_second.load(Ordering::Acquire)
    }
}

impl Drop for BandwidthTicket {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = lock_bandwidth_state(&self.inner.state);
        if let Some(position) = state.waiters.iter().position(|ticket| *ticket == self.id) {
            let _ = state.waiters.remove(position);
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

fn lock_bandwidth_state(
    state: &StdMutex<SharedBandwidthState>,
) -> std::sync::MutexGuard<'_, SharedBandwidthState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn refill_bandwidth_tokens(
    state: &mut SharedBandwidthState,
    rate: u64,
    capacity: u128,
    now: Instant,
) {
    let elapsed = now.saturating_duration_since(state.last_refill).as_nanos();
    let accrued = elapsed
        .saturating_mul(u128::from(rate))
        .saturating_add(state.fractional_tokens);
    let new_tokens = accrued / NANOS_PER_SECOND;
    let total = state.tokens.saturating_add(new_tokens);
    if total >= capacity {
        state.tokens = capacity;
        state.fractional_tokens = 0;
    } else {
        state.tokens = total;
        state.fractional_tokens = accrued % NANOS_PER_SECOND;
    }
    state.last_refill = now;
}

#[derive(Clone, Debug)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
    pub expected_sha256: Option<String>,
    pub max_retries: u8,
    /// Optional per-download cap in bytes per second. Zero is invalid.
    pub bytes_per_second: Option<u64>,
    /// Maximum aggregate response bytes written for this download.
    pub max_bytes: u64,
    /// Absolute wall-clock limit for the complete transfer, including retries.
    pub max_duration: Duration,
}

impl DownloadRequest {
    pub fn validate(&self) -> Result<(), DownloadError> {
        validate_http_url(&self.url)?;
        if self.destination.as_os_str().is_empty() || self.destination.file_name().is_none() {
            return Err(DownloadError::InvalidDestination);
        }
        if self.bytes_per_second == Some(0)
            || self.max_bytes == 0
            || self.max_duration.is_zero()
            || self.max_duration > MAX_DOWNLOAD_DURATION
        {
            return Err(DownloadError::InvalidLimit);
        }
        if self
            .expected_sha256
            .as_ref()
            .is_some_and(|hash| integrity::normalize_sha256(hash).is_none())
        {
            return Err(DownloadError::InvalidExpectedHash);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadResult {
    pub destination: PathBuf,
    pub bytes: u64,
    /// Digest of staging-handle bytes before the pathname-based hard-link commit.
    /// This does not guarantee the destination against concurrent local writers.
    pub sha256: String,
    pub resumed: bool,
    /// Temporary alias or segmented staging cleanup remains pending.
    /// The queue retains the session so callers can inspect and retry cleanup.
    pub cleanup_pending: Option<String>,
}

enum OperationCompletion {
    Finished(Result<DownloadResult, DownloadError>),
    TimedOut,
}

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error(transparent)]
    Url(#[from] crate::url_policy::UrlPolicyError),
    #[error("destination must include a filename")]
    InvalidDestination,
    #[error("transfer limits must be positive and duration cannot exceed seven days")]
    InvalidLimit,
    #[error("global bandwidth limit must be at most 1,000,000,000 bytes per second")]
    InvalidGlobalBandwidthLimit,
    #[error("global bandwidth reservation exceeds the fixed bucket capacity")]
    InvalidBandwidthReservation,
    #[error("expected SHA-256 must be exactly 64 hexadecimal characters")]
    InvalidExpectedHash,
    #[error("destination already exists")]
    DestinationExists,
    #[error("server returned an unsuccessful HTTP status: {0}")]
    HttpStatus(StatusCode),
    #[error("server response did not match the requested byte range")]
    InvalidRange,
    #[error("server resource changed since the partial transfer was created")]
    ResourceChanged,
    #[error("download exceeded its configured byte limit")]
    ByteLimitExceeded,
    #[error("download exceeded its absolute time limit")]
    DurationLimitExceeded,
    #[error("segmented staging cleanup failed: {0}")]
    SegmentCleanup(String),
    #[error("{operation}; temporary staging cleanup is pending and failed: {cleanup_error}")]
    CleanupPending {
        operation: String,
        cleanup_error: String,
    },
    #[error("download was cancelled; an in-memory, handle-backed partial may be resumed")]
    Cancelled,
    #[error("download SHA-256 did not match the expected digest")]
    HashMismatch,
    #[error("HTTP transport failed; URL details omitted")]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Validator {
    value: String,
}

impl Validator {
    fn from_headers(headers: &header::HeaderMap) -> Option<Self> {
        let value = headers.get(header::ETAG)?.to_str().ok()?.trim();
        let bytes = value.as_bytes();
        let is_strong = bytes.len() >= 2
            && bytes.first() == Some(&b'"')
            && bytes.last() == Some(&b'"')
            && bytes[1..bytes.len() - 1]
                .iter()
                .all(|byte| *byte >= 0x21 && *byte != b'"' && *byte != 0x7f);
        is_strong.then(|| Self {
            value: value.to_owned(),
        })
    }
}

struct PartialDownload {
    // This open handle is kept for the full lifetime of an in-process pause/resume session.
    // It is never reopened by path, and no persistent partial is resumed after process exit.
    file: fs::File,
    path: PathBuf,
    bytes: u64,
    url_fingerprint: String,
    validator: Option<Validator>,
    segmented: bool,
    _staging: Option<Arc<SegmentStagingDirectory>>,
}

#[derive(Default)]
struct DownloadSessionState {
    // Set once for the original transfer; pause retains it and only successful
    // completion or successful discard clears it.
    deadline: Option<Instant>,
    partial: Option<PartialDownload>,
    committed_result: Option<DownloadResult>,
}

const ETA_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
// Require a baseline plus repeated progress over at least two seconds; drop an
// active estimate after four seconds without a fresh sample. Paused jobs freeze it.
const ETA_MIN_SAMPLE_SPAN: Duration = Duration::from_secs(2);
const ETA_SAMPLE_WINDOW: Duration = Duration::from_secs(10);
const ETA_STALE_AFTER: Duration = Duration::from_secs(4);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgressSnapshot {
    pub bytes_received: u64,
    pub total_bytes: Option<u64>,
    pub speed_bytes_per_second: Option<u64>,
    pub eta_seconds: Option<u64>,
}

#[derive(Clone, Copy)]
struct ProgressSample {
    active_time: Duration,
    bytes_received: u64,
}

#[derive(Default)]
struct EtaEstimator {
    bytes_received: u64,
    total_bytes: Option<u64>,
    samples: VecDeque<ProgressSample>,
    rate_sample: Option<(u64, u128)>,
    speed_bytes_per_second: Option<u64>,
    eta_seconds: Option<u64>,
    paused: bool,
}

impl EtaEstimator {
    // Times are injected active-transfer durations, not persisted wall-clock timestamps.
    fn begin_segment(
        &mut self,
        total_bytes: Option<u64>,
        bytes_received: u64,
        active_time: Duration,
    ) {
        self.bytes_received = bytes_received;
        self.total_bytes = total_bytes;
        self.samples.clear();
        if total_bytes.is_some() {
            self.samples.push_back(ProgressSample {
                active_time,
                bytes_received,
            });
        }
        self.clear_rate();
        self.paused = false;
    }

    fn reset(&mut self, bytes_received: u64) {
        self.begin_segment(None, bytes_received, Duration::ZERO);
    }

    fn resume(&mut self, bytes_received: u64) {
        let total_bytes = self.total_bytes;
        self.begin_segment(total_bytes, bytes_received, Duration::ZERO);
    }

    fn record(&mut self, bytes_received: u64, active_time: Duration) {
        if self.paused {
            return;
        }
        self.bytes_received = bytes_received;
        let Some(total_bytes) = self.total_bytes else {
            self.samples.clear();
            self.clear_rate();
            return;
        };
        if bytes_received > total_bytes {
            self.clear_rate();
            return;
        }

        let Some(last) = self.samples.back().copied() else {
            self.samples.push_back(ProgressSample {
                active_time,
                bytes_received,
            });
            return;
        };
        if active_time < last.active_time || bytes_received < last.bytes_received {
            self.begin_segment(Some(total_bytes), bytes_received, active_time);
            return;
        }
        if active_time.saturating_sub(last.active_time) >= ETA_STALE_AFTER {
            self.begin_segment(Some(total_bytes), bytes_received, active_time);
            return;
        }
        if bytes_received == last.bytes_received
            || active_time.saturating_sub(last.active_time) < ETA_SAMPLE_INTERVAL
        {
            return;
        }

        self.samples.push_back(ProgressSample {
            active_time,
            bytes_received,
        });
        while self.samples.len() > 1
            && active_time.saturating_sub(self.samples.front().unwrap().active_time)
                > ETA_SAMPLE_WINDOW
        {
            self.samples.pop_front();
        }
        self.recalculate_from_samples();
    }

    fn pause(&mut self, active_time: Duration) {
        self.expire_if_stale(active_time);
        self.paused = true;
    }

    fn snapshot(&mut self, active_time: Duration) -> ProgressSnapshot {
        self.expire_if_stale(active_time);
        ProgressSnapshot {
            bytes_received: self.bytes_received,
            total_bytes: self.total_bytes,
            speed_bytes_per_second: self.speed_bytes_per_second,
            eta_seconds: self.eta_seconds,
        }
    }

    fn recalculate_from_samples(&mut self) {
        if self.samples.len() < 3 {
            self.clear_rate();
            return;
        }
        let first = self.samples.front().expect("samples were checked");
        let last = self.samples.back().expect("samples were checked");
        let span = last.active_time.saturating_sub(first.active_time);
        let rate_bytes = last.bytes_received.saturating_sub(first.bytes_received);
        let span_nanos = span.as_nanos();
        if span < ETA_MIN_SAMPLE_SPAN || rate_bytes == 0 || span_nanos == 0 {
            self.clear_rate();
            return;
        }
        let speed = (u128::from(rate_bytes) * NANOS_PER_SECOND / span_nanos)
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        self.rate_sample = Some((rate_bytes, span_nanos));
        self.speed_bytes_per_second = Some(speed);
        self.recalculate_eta();
    }

    fn recalculate_eta(&mut self) {
        let Some(total_bytes) = self.total_bytes else {
            self.eta_seconds = None;
            return;
        };
        let Some((rate_bytes, span_nanos)) = self.rate_sample else {
            self.eta_seconds = None;
            return;
        };
        let remaining = total_bytes.saturating_sub(self.bytes_received);
        if remaining == 0 || rate_bytes == 0 || span_nanos == 0 {
            self.eta_seconds = None;
            return;
        }
        let eta_nanos = u128::from(remaining)
            .saturating_mul(span_nanos)
            .saturating_add(u128::from(rate_bytes) - 1)
            / u128::from(rate_bytes);
        let eta_seconds = eta_nanos.saturating_add(NANOS_PER_SECOND - 1) / NANOS_PER_SECOND;
        self.eta_seconds = Some(eta_seconds.min(u128::from(u64::MAX)) as u64);
    }

    fn expire_if_stale(&mut self, active_time: Duration) {
        if self.paused {
            return;
        }
        if self
            .samples
            .back()
            .is_some_and(|sample| active_time.saturating_sub(sample.active_time) >= ETA_STALE_AFTER)
        {
            self.clear_rate();
        }
    }

    fn clear_rate(&mut self) {
        self.rate_sample = None;
        self.speed_bytes_per_second = None;
        self.eta_seconds = None;
    }
}

#[derive(Default)]
struct TransferProgressState {
    estimator: EtaEstimator,
    clock_origin: Option<Instant>,
    bytes_received: u64,
}

#[derive(Default)]
struct TransferProgress {
    state: StdMutex<TransferProgressState>,
    changed: Notify,
}

impl TransferProgress {
    fn set(&self, bytes_received: u64) {
        let mut state = lock_transfer_progress(&self.state);
        let active_time = state.clock_origin.map_or(Duration::ZERO, |origin| {
            Instant::now().saturating_duration_since(origin)
        });
        // `record` ignores ETA samples while paused under this same lock, but keep
        // the independently reported byte count accurate for the paused job row.
        state.estimator.record(bytes_received, active_time);
        state.bytes_received = bytes_received;
        drop(state);
        self.changed.notify_one();
    }

    fn reset(&self, bytes_received: u64) {
        let mut state = lock_transfer_progress(&self.state);
        state.clock_origin = None;
        state.bytes_received = bytes_received;
        state.estimator.reset(bytes_received);
        drop(state);
        self.changed.notify_one();
    }

    fn begin_response(&self, total_bytes: Option<u64>, bytes_received: u64) {
        let mut state = lock_transfer_progress(&self.state);
        state.clock_origin = Some(Instant::now());
        state.bytes_received = bytes_received;
        state
            .estimator
            .begin_segment(total_bytes, bytes_received, Duration::ZERO);
        drop(state);
        self.changed.notify_one();
    }

    fn pause(&self) {
        let mut state = lock_transfer_progress(&self.state);
        let active_time = state.clock_origin.map_or(Duration::ZERO, |origin| {
            Instant::now().saturating_duration_since(origin)
        });
        state.estimator.pause(active_time);
        state.clock_origin = None;
        drop(state);
        self.changed.notify_one();
    }

    fn resume(&self) {
        let mut state = lock_transfer_progress(&self.state);
        state.clock_origin = None;
        let bytes_received = state.bytes_received;
        state.estimator.resume(bytes_received);
        drop(state);
        self.changed.notify_one();
    }

    fn snapshot(&self) -> ProgressSnapshot {
        let mut state = lock_transfer_progress(&self.state);
        let active_time = state.clock_origin.map_or(Duration::ZERO, |origin| {
            Instant::now().saturating_duration_since(origin)
        });
        let mut snapshot = state.estimator.snapshot(active_time);
        snapshot.bytes_received = state.bytes_received;
        snapshot
    }
}

fn lock_transfer_progress(
    state: &StdMutex<TransferProgressState>,
) -> std::sync::MutexGuard<'_, TransferProgressState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone)]
pub struct DownloadSession {
    state: Arc<Mutex<DownloadSessionState>>,
    progress: Arc<TransferProgress>,
    commit_state: Arc<Mutex<bool>>,
    deadline_cache: Arc<std::sync::Mutex<Option<Instant>>>,
    segment_temp_paths: SegmentTempRegistry,
}

impl Default for DownloadSession {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(DownloadSessionState::default())),
            progress: Arc::new(TransferProgress::default()),
            commit_state: Arc::new(Mutex::new(false)),
            deadline_cache: Arc::new(std::sync::Mutex::new(None)),
            segment_temp_paths: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

impl DownloadSession {
    pub(crate) fn progress_snapshot(&self) -> ProgressSnapshot {
        self.progress.snapshot()
    }

    pub(crate) fn pause_progress(&self) {
        self.progress.pause();
    }

    pub(crate) fn resume_progress(&self) {
        self.progress.resume();
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_bytes(&self, expected: u64) {
        while self.progress.snapshot().bytes_received < expected {
            self.progress.changed.notified().await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn deadline_for_test(&self) -> Option<Instant> {
        self.state.lock().await.deadline
    }

    pub(crate) async fn lock_commit_state(&self) -> tokio::sync::MutexGuard<'_, bool> {
        self.commit_state.lock().await
    }

    pub(crate) fn absolute_deadline(&self) -> Option<Instant> {
        *self
            .deadline_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn cache_deadline(&self, deadline: Option<Instant>) {
        *self
            .deadline_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = deadline;
    }
}

pub(crate) async fn committed_result(session: &DownloadSession) -> Option<DownloadResult> {
    session.state.lock().await.committed_result.clone()
}

async fn wait_for_operation_until_deadline<F>(
    deadline: Instant,
    operation: Pin<&mut F>,
) -> OperationCompletion
where
    F: Future<Output = Result<DownloadResult, DownloadError>>,
{
    // The operation handles cancellation at network/IO-safe checkpoints. Do not
    // race cancellation here: dropping at an arbitrary point could desynchronize
    // file length from the recorded resumable offset. Only the absolute deadline
    // may forcibly drop this future.
    match tokio::time::timeout_at(deadline, operation).await {
        Ok(result) => OperationCompletion::Finished(result),
        Err(_) => OperationCompletion::TimedOut,
    }
}

#[cfg(test)]
pub(crate) async fn install_test_partial(
    session: &DownloadSession,
    path: PathBuf,
    file: fs::File,
    bytes: u64,
) {
    let mut state = session.state.lock().await;
    state.deadline = Some(Instant::now() + Duration::from_secs(60));
    state.partial = Some(PartialDownload {
        file,
        path,
        bytes,
        url_fingerprint: url_fingerprint("test-fixture"),
        validator: None,
        segmented: false,
        _staging: None,
    });
    session.progress.reset(bytes);
    session.cache_deadline(state.deadline);
}

#[cfg(test)]
pub(crate) async fn install_test_committed_result(
    session: &DownloadSession,
    result: DownloadResult,
) {
    session.state.lock().await.committed_result = Some(result);
    *session.commit_state.lock().await = true;
}

/// Removes any in-memory partial file owned by this queue entry.
/// On filesystem failure it returns the I/O error without taking the session
/// handle/path, so the same call can be retried later.
pub async fn discard_download_session(session: &DownloadSession) -> Result<(), DownloadError> {
    let mut state = session.state.lock().await;
    // Close/remove a protected segmented partial alias before retrying its parent
    // directory. This keeps a resumable handle from being detached from its path.
    remove_partial_alias(&mut state.partial).await?;
    cleanup_segment_temp_paths(session)
        .await
        .map_err(DownloadError::SegmentCleanup)?;
    state.deadline = None;
    state.committed_result = None;
    session.progress.reset(0);
    *session.commit_state.lock().await = false;
    session.cache_deadline(None);
    Ok(())
}

pub(crate) async fn discard_download_session_until(
    session: &DownloadSession,
    deadline: Instant,
) -> Result<(), String> {
    if deadline <= Instant::now() {
        let has_staging = !lock_temp_registry(&session.segment_temp_paths).is_empty();
        let has_partial = session
            .state
            .try_lock()
            .map(|state| state.partial.is_some())
            .unwrap_or(true);
        if !has_staging && !has_partial {
            return Ok(());
        }
        return Err("absolute download deadline elapsed before cleanup completed".to_owned());
    }
    match tokio::time::timeout_at(deadline, discard_download_session(session)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("absolute download deadline elapsed before cleanup completed".to_owned()),
    }
}

pub async fn download_with_session(
    request: DownloadRequest,
    cancel: CancellationToken,
    session: &DownloadSession,
) -> Result<DownloadResult, DownloadError> {
    download_with_session_inner(request, cancel, session, true, None, 1, None).await
}

pub(crate) async fn download_with_session_managed_with_limiter(
    request: DownloadRequest,
    cancel: CancellationToken,
    session: &DownloadSession,
    bandwidth_limiter: &SharedBandwidthLimiter,
) -> Result<DownloadResult, DownloadError> {
    download_with_session_inner(
        request,
        cancel,
        session,
        false,
        Some(bandwidth_limiter),
        1,
        None,
    )
    .await
}

pub(crate) async fn download_with_session_managed_with_limiter_and_segments(
    request: DownloadRequest,
    cancel: CancellationToken,
    session: &DownloadSession,
    bandwidth_limiter: &SharedBandwidthLimiter,
    max_segments: u8,
) -> Result<DownloadResult, DownloadError> {
    if !(1..=MAX_SEGMENT_COUNT).contains(&max_segments) {
        return Err(DownloadError::InvalidLimit);
    }
    if max_segments == 1 {
        return download_with_session_managed_with_limiter(
            request,
            cancel,
            session,
            bandwidth_limiter,
        )
        .await;
    }
    download_with_session_inner(
        request,
        cancel,
        session,
        false,
        Some(bandwidth_limiter),
        max_segments,
        None,
    )
    .await
}

pub(crate) async fn download_with_session_managed_with_limiter_segments_and_slots(
    request: DownloadRequest,
    cancel: CancellationToken,
    session: &DownloadSession,
    bandwidth_limiter: &SharedBandwidthLimiter,
    max_segments: u8,
    connection_slots: Arc<Semaphore>,
) -> Result<DownloadResult, DownloadError> {
    if !(1..=MAX_SEGMENT_COUNT).contains(&max_segments) {
        return Err(DownloadError::InvalidLimit);
    }
    download_with_session_inner(
        request,
        cancel,
        session,
        false,
        Some(bandwidth_limiter),
        max_segments,
        Some(connection_slots),
    )
    .await
}

async fn download_with_session_inner(
    request: DownloadRequest,
    cancel: CancellationToken,
    session: &DownloadSession,
    cleanup_on_error: bool,
    bandwidth_limiter: Option<&SharedBandwidthLimiter>,
    max_segments: u8,
    connection_slots: Option<Arc<Semaphore>>,
) -> Result<DownloadResult, DownloadError> {
    if !(1..=MAX_SEGMENT_COUNT).contains(&max_segments) {
        return Err(DownloadError::InvalidLimit);
    }
    if let Err(error) = request.validate() {
        return Err(handle_failure_cleanup_for_session(session, error, cleanup_on_error).await);
    }
    match fs::try_exists(&request.destination).await {
        Ok(true) => {
            return Err(handle_failure_cleanup_for_session(
                session,
                DownloadError::DestinationExists,
                cleanup_on_error,
            )
            .await)
        }
        Ok(false) => {}
        Err(error) => {
            return Err(
                handle_failure_cleanup_for_session(session, error.into(), cleanup_on_error).await,
            )
        }
    }
    if let Some(parent) = request
        .destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        if let Err(error) = fs::create_dir_all(parent).await {
            return Err(handle_failure_cleanup_for_session(
                session,
                error.into(),
                cleanup_on_error,
            )
            .await);
        }
    }
    let client = match Client::builder()
        // Redirects are intentionally disabled so the approved URL's origin is not changed silently.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(Duration::from_secs(60))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return Err(
                handle_failure_cleanup_for_session(session, error.into(), cleanup_on_error).await,
            )
        }
    };
    let deadline = {
        let mut state = session.state.lock().await;
        let deadline = session_deadline(&mut state, Instant::now(), request.max_duration);
        session.cache_deadline(Some(deadline));
        deadline
    };
    if deadline <= Instant::now() {
        let error = DownloadError::DurationLimitExceeded;
        return Err(handle_failure_cleanup_until(session, error, cleanup_on_error, deadline).await);
    }
    let mut operation = Box::pin(async {
        let mut state = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
            state = session.state.lock() => state,
        };
        let execution = DownloadExecution {
            client: &client,
            request: &request,
            cancel: &cancel,
            progress: &session.progress,
            commit_state: &session.commit_state,
            bandwidth_limiter,
            max_segments,
            connection_slots: connection_slots.clone(),
            segment_temp_paths: session.segment_temp_paths.clone(),
        };
        run_download(&execution, &mut state).await
    });
    // A pause/cancel returns at a safe transfer checkpoint after completed writes
    // have updated the in-memory offset; the open handle remains resumable without
    // a flush. Only natural deadline expiry drops the outer operation before cleanup.
    let completion = wait_for_operation_until_deadline(deadline, operation.as_mut()).await;
    // In particular, discard the timed-out future before cleanup attempts to
    // acquire the session mutex held by the transfer operation.
    drop(operation);
    let segment_cleanup_error = cleanup_segment_temp_paths_until(session, deadline)
        .await
        .err();

    match completion {
        OperationCompletion::TimedOut => {
            if let Some(cleanup_error) = segment_cleanup_error {
                return Err(DownloadError::CleanupPending {
                    operation: DownloadError::DurationLimitExceeded.to_string(),
                    cleanup_error,
                });
            }
            if let Some(result) = finish_committed_after_timeout(session).await {
                Ok(result)
            } else {
                Err(handle_failure_cleanup_until(
                    session,
                    DownloadError::DurationLimitExceeded,
                    cleanup_on_error,
                    deadline,
                )
                .await)
            }
        }
        OperationCompletion::Finished(Err(error)) if !matches!(error, DownloadError::Cancelled) => {
            if let Some(cleanup_error) = segment_cleanup_error {
                Err(DownloadError::CleanupPending {
                    operation: error.to_string(),
                    cleanup_error,
                })
            } else {
                Err(handle_failure_cleanup_until(session, error, cleanup_on_error, deadline).await)
            }
        }
        OperationCompletion::Finished(Ok(mut result)) => {
            if let Some(cleanup_error) = segment_cleanup_error {
                result.cleanup_pending = Some(cleanup_error);
                session.state.lock().await.committed_result = Some(result.clone());
                return Ok(result);
            }
            if cleanup_on_error && result.cleanup_pending.is_none() {
                let mut state = session.state.lock().await;
                state.committed_result = None;
                *session.commit_state.lock().await = false;
                session.cache_deadline(None);
            }
            Ok(result)
        }
        OperationCompletion::Finished(Err(error)) => {
            let staging_cleanup_error = if matches!(&error, DownloadError::Cancelled) {
                match cleanup_segmented_staging_on_cancel(session, deadline).await {
                    Ok(()) => cleanup_segment_temp_paths_until(session, deadline)
                        .await
                        .err(),
                    Err(error) => Some(error),
                }
            } else {
                None
            };
            match (segment_cleanup_error, staging_cleanup_error) {
                (Some(segment_error), Some(staging_error)) => Err(DownloadError::CleanupPending {
                    operation: error.to_string(),
                    cleanup_error: format!("{segment_error}; {staging_error}"),
                }),
                (Some(cleanup_error), None) | (None, Some(cleanup_error)) => {
                    Err(DownloadError::CleanupPending {
                        operation: error.to_string(),
                        cleanup_error,
                    })
                }
                (None, None) => Err(error),
            }
        }
    }
}

async fn handle_failure_cleanup(
    session: &DownloadSession,
    operation: DownloadError,
    clean: bool,
) -> DownloadError {
    if !clean {
        return operation;
    }
    match discard_download_session(session).await {
        Ok(()) => operation,
        Err(cleanup_error) => DownloadError::CleanupPending {
            operation: operation.to_string(),
            cleanup_error: cleanup_error.to_string(),
        },
    }
}

async fn handle_failure_cleanup_for_session(
    session: &DownloadSession,
    operation: DownloadError,
    clean: bool,
) -> DownloadError {
    match session.absolute_deadline() {
        Some(deadline) => handle_failure_cleanup_until(session, operation, clean, deadline).await,
        None => handle_failure_cleanup(session, operation, clean).await,
    }
}

async fn handle_failure_cleanup_until(
    session: &DownloadSession,
    operation: DownloadError,
    clean: bool,
    deadline: Instant,
) -> DownloadError {
    if !clean {
        return operation;
    }
    match discard_download_session_until(session, deadline).await {
        Ok(()) => operation,
        Err(cleanup_error) => DownloadError::CleanupPending {
            operation: operation.to_string(),
            cleanup_error,
        },
    }
}

async fn finish_committed_after_timeout(session: &DownloadSession) -> Option<DownloadResult> {
    let mut state = session.state.lock().await;
    let mut result = state.committed_result.clone()?;
    if state.partial.is_some() && result.cleanup_pending.is_none() {
        result.cleanup_pending = Some(
            "temporary alias cleanup deferred because the absolute deadline elapsed".to_owned(),
        );
    }
    state.committed_result = Some(result.clone());
    Some(result)
}

struct OwnedTempFile {
    path: PathBuf,
    file: Option<fs::File>,
}

impl OwnedTempFile {
    async fn create(path: PathBuf) -> Result<Self, DownloadError> {
        let cancel = CancellationToken::new();
        Self::create_until(path, &cancel, None).await
    }

    async fn create_until(
        path: PathBuf,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
    ) -> Result<Self, DownloadError> {
        Self::create_until_with(path, cancel, deadline, |path| async move {
            create_temp_file(&path).await
        })
        .await
    }

    async fn create_until_with<F, Fut>(
        path: PathBuf,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
        open: F,
    ) -> Result<Self, DownloadError>
    where
        F: FnOnce(PathBuf) -> Fut + Send,
        Fut: Future<Output = Result<fs::File, std::io::Error>> + Send,
    {
        ensure_transfer_active(cancel, deadline)?;
        let opening = open(path.clone());
        let file = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(DownloadError::DurationLimitExceeded);
                },
                result = opening => result?,
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                result = opening => result?,
            }
        };
        ensure_transfer_active(cancel, deadline)?;
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    async fn remove(&mut self) -> Result<(), std::io::Error> {
        drop(self.file.take());
        match fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn into_partial(
        mut self,
        url_fingerprint: String,
        bytes: u64,
        staging: Arc<SegmentStagingDirectory>,
    ) -> PartialDownload {
        staging.protect_for_partial();
        PartialDownload {
            file: self.file.take().expect("owned staging handle is present"),
            path: self.path.clone(),
            bytes,
            url_fingerprint,
            // Segmented state is deliberately not resumable. A later pause/resume
            // starts over through the existing single-stream path.
            validator: None,
            segmented: true,
            _staging: Some(staging),
        }
    }
}

struct SegmentStagingDirectory {
    path: PathBuf,
    registry: SegmentTempRegistry,
}

impl SegmentStagingDirectory {
    fn create(
        destination: &Path,
        registry: SegmentTempRegistry,
    ) -> Result<Arc<Self>, DownloadError> {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let filename = destination
            .file_name()
            .ok_or(DownloadError::InvalidDestination)?;
        for _ in 0..8 {
            let mut staging_name = filename.to_os_string();
            staging_name.push(format!(".dm-{}.staging", uuid::Uuid::new_v4()));
            let path = parent.join(staging_name);
            match std::fs::create_dir(&path) {
                Ok(()) => {
                    // Register before starting any asynchronous file open. Drop does
                    // no filesystem work; this entry survives until async cleanup
                    // confirms removal or a later local retry succeeds.
                    lock_temp_registry(&registry).insert(path.clone(), true);
                    return Ok(Arc::new(Self {
                        path,
                        registry: registry.clone(),
                    }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique segmented staging directory",
        )
        .into())
    }

    fn protect_for_partial(&self) {
        lock_temp_registry(&self.registry).insert(self.path.clone(), false);
    }

    fn mark_cleanup_allowed(&self) {
        lock_temp_registry(&self.registry).insert(self.path.clone(), true);
    }
}

fn lock_temp_registry(
    registry: &StdMutex<HashMap<PathBuf, bool>>,
) -> std::sync::MutexGuard<'_, HashMap<PathBuf, bool>> {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn cleanup_segment_temp_paths(session: &DownloadSession) -> Result<(), String> {
    cleanup_segment_temp_paths_with_deadline(session, None, false).await
}

async fn cleanup_segment_temp_paths_until(
    session: &DownloadSession,
    deadline: Instant,
) -> Result<(), String> {
    cleanup_segment_temp_paths_with_deadline(session, Some(deadline), true).await
}

async fn cleanup_segment_temp_paths_with_deadline(
    session: &DownloadSession,
    deadline: Option<Instant>,
    pending_only: bool,
) -> Result<(), String> {
    let paths = lock_temp_registry(&session.segment_temp_paths)
        .iter()
        .filter(|(_, cleanup_pending)| !pending_only || **cleanup_pending)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    cleanup_registered_segment_paths(
        session.segment_temp_paths.clone(),
        paths,
        deadline,
        |path| fs::remove_dir_all(path),
    )
    .await
}

async fn cleanup_registered_segment_paths<F, Fut>(
    registry: SegmentTempRegistry,
    paths: Vec<PathBuf>,
    deadline: Option<Instant>,
    mut remove: F,
) -> Result<(), String>
where
    F: FnMut(PathBuf) -> Fut + Send,
    Fut: Future<Output = Result<(), std::io::Error>> + Send,
{
    let mut failures = Vec::new();
    for path in paths {
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            failures.push(format!(
                "{}: cleanup pending after the absolute deadline",
                path.display()
            ));
            continue;
        }
        let remove_future = remove(path.clone());
        let outcome = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, remove_future).await {
                Ok(result) => result,
                Err(_) => {
                    failures.push(format!(
                        "{}: cleanup pending after the absolute deadline",
                        path.display()
                    ));
                    continue;
                }
            },
            None => remove_future.await,
        };
        match outcome {
            Ok(()) => {
                lock_temp_registry(&registry).remove(&path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                lock_temp_registry(&registry).remove(&path);
            }
            Err(error) => failures.push(format!("{}: {error}", path.display())),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

async fn cleanup_segmented_staging_on_cancel(
    session: &DownloadSession,
    deadline: Instant,
) -> Result<(), String> {
    if deadline <= Instant::now() {
        return Err("absolute download deadline elapsed before cleanup completed".to_owned());
    }
    match tokio::time::timeout_at(deadline, async {
        let mut state = session.state.lock().await;
        let segmented = state
            .partial
            .as_ref()
            .is_some_and(|partial| partial.segmented);
        if segmented {
            remove_partial_alias(&mut state.partial)
                .await
                .map_err(|error| error.to_string())?;
            session.progress.reset(0);
        }
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err("absolute download deadline elapsed before cleanup completed".to_owned()),
    }
}

struct SegmentPart {
    index: usize,
    temp: OwnedTempFile,
    bytes: u64,
}

struct DownloadExecution<'a> {
    client: &'a Client,
    request: &'a DownloadRequest,
    cancel: &'a CancellationToken,
    progress: &'a TransferProgress,
    commit_state: &'a Mutex<bool>,
    bandwidth_limiter: Option<&'a SharedBandwidthLimiter>,
    max_segments: u8,
    connection_slots: Option<Arc<Semaphore>>,
    segment_temp_paths: SegmentTempRegistry,
}

struct SegmentExecution<'a, 'b> {
    download: &'a DownloadExecution<'b>,
    staging: &'a Arc<SegmentStagingDirectory>,
    deadline: Option<Instant>,
    total: u64,
    validator: &'a Validator,
    progress_bytes: &'a AtomicU64,
    pacer: &'a SegmentPacer,
}

struct SegmentPacer {
    bytes_per_second: Option<u64>,
    started: Instant,
    transferred: AtomicU64,
}

impl SegmentPacer {
    fn new(bytes_per_second: Option<u64>) -> Self {
        Self {
            bytes_per_second,
            started: Instant::now(),
            transferred: AtomicU64::new(0),
        }
    }

    async fn wait(
        &self,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
    ) -> Result<(), DownloadError> {
        let Some(limit) = self.bytes_per_second else {
            return Ok(());
        };
        let transferred = self.transferred.load(Ordering::Acquire);
        let seconds = transferred / limit;
        let nanos = ((transferred % limit) as u128 * NANOS_PER_SECOND / u128::from(limit)) as u64;
        let expected = Duration::from_secs(seconds).saturating_add(Duration::from_nanos(nanos));
        let Some(delay) = expected.checked_sub(self.started.elapsed()) else {
            return Ok(());
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DownloadError::Cancelled),
            _ = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => Err(DownloadError::DurationLimitExceeded),
            _ = tokio::time::sleep(delay) => Ok(()),
        }
    }
}

async fn send_before_deadline(
    builder: reqwest::RequestBuilder,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<reqwest::Response, DownloadError> {
    ensure_transfer_active(cancel, deadline)?;
    if let Some(deadline) = deadline {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DownloadError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => Err(DownloadError::DurationLimitExceeded),
            response = builder.send() => response.map_err(DownloadError::from),
        }
    } else {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DownloadError::Cancelled),
            response = builder.send() => response.map_err(DownloadError::from),
        }
    }
}

fn exact_content_length(headers: &header::HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

async fn probe_segment_support(
    client: &Client,
    request: &DownloadRequest,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    bandwidth_limiter: Option<&SharedBandwidthLimiter>,
) -> Result<Option<(u64, Validator)>, DownloadError> {
    let response = send_before_deadline(
        client.get(&request.url).header(header::RANGE, "bytes=0-0"),
        cancel,
        deadline,
    )
    .await?;
    if response.status() != StatusCode::PARTIAL_CONTENT
        || exact_content_length(response.headers()) != Some(1)
    {
        return Ok(None);
    }
    let Some(range) = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
    else {
        return Ok(None);
    };
    let Some(validator) = Validator::from_headers(response.headers()) else {
        return Ok(None);
    };
    if range.start != 0
        || range.end != 0
        || range.total < SEGMENTED_MIN_BYTES
        || range.total > request.max_bytes
    {
        return Ok(None);
    }

    let mut stream = response.bytes_stream();
    let first_chunk = next_response_chunk(&mut stream, bandwidth_limiter, cancel, deadline).await;
    let first_chunk = match first_chunk {
        Ok(chunk) => chunk,
        // A probe body that cannot be read to the end means the origin does not
        // serve ranges reliably. Treat that as "ranges unsupported" instead of
        // failing the transfer, so the single-stream path decides the outcome.
        // Timeouts and connection failures stay hard errors: they say nothing
        // about range support and must not be retried as a fresh full request.
        Err(DownloadError::Http(error)) if !error.is_timeout() && !error.is_connect() => {
            return Ok(None)
        }
        Err(error) => return Err(error),
    };
    let Some((chunk, mut reservation)) = first_chunk else {
        return Ok(None);
    };
    if chunk.len() != 1 {
        return Ok(None);
    }
    if let Some(reservation) = reservation.as_mut() {
        reservation.charge(1);
    }
    Ok(Some((range.total, validator)))
}

async fn download_segment(
    context: &SegmentExecution<'_, '_>,
    index: usize,
    start: u64,
    end: u64,
) -> Result<Option<SegmentPart>, DownloadError> {
    let download = context.download;
    let request = download.request;
    let response = send_before_deadline(
        download
            .client
            .get(&request.url)
            .header(header::RANGE, format!("bytes={start}-{end}"))
            .header(header::IF_RANGE, context.validator.value.as_str()),
        download.cancel,
        context.deadline,
    )
    .await?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Ok(None);
    }
    let expected_bytes = end - start + 1;
    let Some(range) = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
    else {
        return Ok(None);
    };
    if range.start != start
        || range.end != end
        || range.total != context.total
        || exact_content_length(response.headers()) != Some(expected_bytes)
        || Validator::from_headers(response.headers()).as_ref() != Some(context.validator)
    {
        return Ok(None);
    }

    let mut temp = OwnedTempFile::create_until(
        context.staging.path.join(format!("segment-{index}.part")),
        download.cancel,
        context.deadline,
    )
    .await?;
    let mut stream = response.bytes_stream();
    let mut received = 0u64;
    loop {
        let next_chunk = next_response_chunk(
            &mut stream,
            download.bandwidth_limiter,
            download.cancel,
            context.deadline,
        )
        .await;
        let next_chunk = match next_chunk {
            Ok(chunk) => chunk,
            // A segment body that ends before its advertised length, or any other
            // non-timeout transport failure, means the range response is
            // inconsistent. Abandon the segmented attempt and let the
            // single-stream fallback decide the outcome; the final destination is
            // never touched by the segmented attempt.
            Err(DownloadError::Http(error)) if !error.is_timeout() && !error.is_connect() => {
                return Ok(None)
            }
            Err(error) => return Err(error),
        };
        let Some((chunk, poll_reservation)) = next_chunk else {
            break;
        };
        let chunk_bytes = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        if received
            .checked_add(chunk_bytes)
            .is_none_or(|next| next > expected_bytes)
        {
            return Ok(None);
        }
        let temp_file = temp.file.as_mut().expect("segment handle is present");
        let mut offset = 0usize;
        let mut reservation = poll_reservation;
        while offset < chunk.len() {
            ensure_transfer_active(download.cancel, context.deadline)?;
            let part_len = (chunk.len() - offset).min(GLOBAL_BANDWIDTH_QUANTUM_BYTES as usize);
            let mut part_reservation = if offset == 0 {
                reservation.take()
            } else if let Some(limiter) = download.bandwidth_limiter {
                Some(limiter.reserve(part_len as u64, download.cancel).await?)
            } else {
                None
            };
            ensure_transfer_active(download.cancel, context.deadline)?;
            if let Some(reservation) = part_reservation.as_mut() {
                reservation.charge(part_len as u64);
            }
            temp_file
                .write_all(&chunk[offset..offset + part_len])
                .await?;
            let part_bytes = part_len as u64;
            received += part_bytes;
            let aggregate = context
                .progress_bytes
                .fetch_add(part_bytes, Ordering::AcqRel)
                + part_bytes;
            download.progress.set(aggregate);
            context
                .pacer
                .transferred
                .fetch_add(part_bytes, Ordering::AcqRel);
            offset += part_len;
            context
                .pacer
                .wait(download.cancel, context.deadline)
                .await?;
        }
    }
    if received != expected_bytes {
        return Ok(None);
    }
    temp.file
        .as_mut()
        .expect("segment handle is present")
        .flush()
        .await?;
    Ok(Some(SegmentPart {
        index,
        temp,
        bytes: received,
    }))
}

async fn cleanup_segment_parts(
    parts: &mut [SegmentPart],
    deadline: Option<Instant>,
) -> Result<(), DownloadError> {
    let mut failures = Vec::new();
    for part in parts {
        let path = part.temp.path.clone();
        let remove = part.temp.remove();
        let outcome = match deadline {
            Some(deadline) if deadline <= Instant::now() => {
                failures.push(format!(
                    "{}: cleanup pending after the absolute deadline",
                    path.display()
                ));
                continue;
            }
            Some(deadline) => match tokio::time::timeout_at(deadline, remove).await {
                Ok(result) => result,
                Err(_) => {
                    failures.push(format!(
                        "{}: cleanup pending after the absolute deadline",
                        path.display()
                    ));
                    continue;
                }
            },
            None => remove.await,
        };
        if let Err(error) = outcome {
            failures.push(format!("{}: {error}", path.display()));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(DownloadError::SegmentCleanup(failures.join("; ")))
    }
}

async fn download_segmented(
    execution: &DownloadExecution<'_>,
    state: &mut DownloadSessionState,
    probe_permit: Option<OwnedSemaphorePermit>,
) -> Result<SegmentedAttempt, DownloadError> {
    let request = execution.request;
    let cancel = execution.cancel;
    let deadline = state.deadline;
    let staging = SegmentStagingDirectory::create(
        &request.destination,
        execution.segment_temp_paths.clone(),
    )?;
    let Some((total, validator)) = probe_segment_support(
        execution.client,
        request,
        cancel,
        deadline,
        execution.bandwidth_limiter,
    )
    .await?
    else {
        drop(staging);
        return Ok(SegmentedAttempt::Fallback(probe_permit));
    };
    ensure_transfer_active(cancel, deadline)?;

    let segment_count = usize::from(execution.max_segments).min(total as usize);
    if segment_count < 2 {
        drop(staging);
        return Ok(SegmentedAttempt::Fallback(probe_permit));
    }
    // Never hold the probe slot while waiting for the complete segment reservation.
    // acquire_many is atomic, so competing segmented jobs cannot deadlock by holding
    // partial reservations.
    drop(probe_permit);
    let _segment_permit =
        acquire_connection_slots(execution, segment_count as u32, deadline).await?;
    execution.progress.begin_response(Some(total), 0);
    let progress_bytes = AtomicU64::new(0);
    let pacer = SegmentPacer::new(request.bytes_per_second);
    let segment_execution = SegmentExecution {
        download: execution,
        staging: &staging,
        deadline,
        total,
        validator: &validator,
        progress_bytes: &progress_bytes,
        pacer: &pacer,
    };
    let mut tasks = FuturesUnordered::new();
    let base = total / segment_count as u64;
    let remainder = total % segment_count as u64;
    let mut start = 0u64;
    for index in 0..segment_count {
        let length = if (index as u64) < remainder {
            base + 1
        } else {
            base
        };
        let end = start + length - 1;
        tasks.push(download_segment(&segment_execution, index, start, end));
        start = end + 1;
    }

    let mut parts = Vec::with_capacity(segment_count);
    while let Some(result) = tasks.next().await {
        match result {
            Ok(Some(part)) => parts.push(part),
            Ok(None) => {
                drop(tasks);
                cleanup_segment_parts(&mut parts, deadline).await?;
                execution.progress.reset(0);
                return Ok(SegmentedAttempt::Fallback(None));
            }
            Err(error) => {
                drop(tasks);
                cleanup_segment_parts(&mut parts, deadline).await?;
                execution.progress.reset(0);
                return Err(error);
            }
        }
    }
    parts.sort_by_key(|part| part.index);
    if parts.iter().map(|part| part.bytes).sum::<u64>() != total {
        cleanup_segment_parts(&mut parts, deadline).await?;
        execution.progress.reset(0);
        return Ok(SegmentedAttempt::Fallback(None));
    }

    let mut assembled =
        OwnedTempFile::create_until(staging.path.join("assembled.part"), cancel, deadline).await?;
    let mut staged = 0u64;
    for part in &mut parts {
        let source = part.temp.file.as_mut().expect("segment handle is present");
        source.seek(SeekFrom::Start(0)).await?;
        let mut buffer = [0u8; 64 * 1024];
        let mut copied_from_part = 0u64;
        loop {
            ensure_transfer_active(cancel, deadline)?;
            let read = source.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            let next = staged
                .checked_add(read as u64)
                .ok_or(DownloadError::ByteLimitExceeded)?;
            if next > total || next > request.max_bytes {
                execution.progress.reset(0);
                return Ok(SegmentedAttempt::Fallback(None));
            }
            assembled
                .file
                .as_mut()
                .expect("staging handle is present")
                .write_all(&buffer[..read])
                .await?;
            staged = next;
            copied_from_part += read as u64;
        }
        if part.bytes != copied_from_part {
            execution.progress.reset(0);
            return Ok(SegmentedAttempt::Fallback(None));
        }
    }
    if staged != total {
        cleanup_segment_parts(&mut parts, deadline).await?;
        execution.progress.reset(0);
        return Ok(SegmentedAttempt::Fallback(None));
    }
    assembled
        .file
        .as_mut()
        .expect("staging handle is present")
        .flush()
        .await?;
    cleanup_segment_parts(&mut parts, deadline).await?;

    let fingerprint = url_fingerprint(&request.url);
    state.partial = Some(assembled.into_partial(fingerprint, total, staging.clone()));
    execution.progress.set(total);
    let result = finalize_download(
        state,
        request,
        cancel,
        execution.commit_state,
        false,
        total,
        || {},
    )
    .await?;
    Ok(SegmentedAttempt::Completed(result))
}

enum SegmentedAttempt {
    Completed(DownloadResult),
    Fallback(Option<OwnedSemaphorePermit>),
}

async fn acquire_connection_slots(
    execution: &DownloadExecution<'_>,
    count: u32,
    deadline: Option<Instant>,
) -> Result<Option<OwnedSemaphorePermit>, DownloadError> {
    let Some(slots) = execution.connection_slots.as_ref() else {
        return Ok(None);
    };
    ensure_transfer_active(execution.cancel, deadline)?;
    let acquire = slots.clone().acquire_many_owned(count);
    let permit = if let Some(deadline) = deadline {
        tokio::select! {
            biased;
            _ = execution.cancel.cancelled() => return Err(DownloadError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => {
                return Err(DownloadError::DurationLimitExceeded);
            },
            result = acquire => result,
        }
    } else {
        tokio::select! {
            biased;
            _ = execution.cancel.cancelled() => return Err(DownloadError::Cancelled),
            result = acquire => result,
        }
    }
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "connection slot pool closed",
        )
    })?;
    Ok(Some(permit))
}

async fn run_download(
    execution: &DownloadExecution<'_>,
    state: &mut DownloadSessionState,
) -> Result<DownloadResult, DownloadError> {
    for attempt in 0..=execution.request.max_retries {
        if execution.cancel.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        let result = match acquire_connection_slots(execution, 1, state.deadline).await {
            Err(error) => Err(error),
            Ok(probe_or_stream_permit)
                if attempt == 0 && execution.max_segments > 1 && state.partial.is_none() =>
            {
                match download_segmented(execution, state, probe_or_stream_permit).await {
                    Ok(SegmentedAttempt::Completed(result)) => return Ok(result),
                    Ok(SegmentedAttempt::Fallback(held_permit)) => {
                        let permit = match held_permit {
                            Some(permit) => Ok(Some(permit)),
                            None => acquire_connection_slots(execution, 1, state.deadline).await,
                        };
                        match permit {
                            Ok(_permit) => {
                                download_once(
                                    execution.client,
                                    execution.request,
                                    execution.cancel,
                                    state,
                                    execution.progress,
                                    execution.commit_state,
                                    execution.bandwidth_limiter,
                                )
                                .await
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            Ok(_stream_permit) => {
                download_once(
                    execution.client,
                    execution.request,
                    execution.cancel,
                    state,
                    execution.progress,
                    execution.commit_state,
                    execution.bandwidth_limiter,
                )
                .await
            }
        };
        match result {
            Ok(result) => return Ok(result),
            Err(error) => {
                let retryable = is_retryable(&error);
                if attempt < execution.request.max_retries && retryable {
                    tokio::select! {
                        biased;
                        _ = execution.cancel.cancelled() => return Err(DownloadError::Cancelled),
                        _ = tokio::time::sleep(Duration::from_secs(1u64 << attempt.min(5))) => {}
                    }
                } else {
                    return Err(error);
                }
            }
        }
    }
    unreachable!("retry loop always returns")
}

async fn next_response_chunk<S, T, E>(
    stream: &mut S,
    bandwidth_limiter: Option<&SharedBandwidthLimiter>,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<Option<(T, Option<BandwidthReservation>)>, DownloadError>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    E: Into<DownloadError>,
{
    let reservation = if let Some(limiter) = bandwidth_limiter {
        ensure_transfer_active(cancel, deadline)?;
        let reservation = if let Some(deadline) = deadline {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(DownloadError::DurationLimitExceeded);
                },
                reservation = limiter.reserve(GLOBAL_BANDWIDTH_QUANTUM_BYTES, cancel) => {
                    reservation?
                },
            }
        } else {
            limiter
                .reserve(GLOBAL_BANDWIDTH_QUANTUM_BYTES, cancel)
                .await?
        };
        ensure_transfer_active(cancel, deadline)?;
        Some(reservation)
    } else {
        None
    };

    ensure_transfer_active(cancel, deadline)?;
    let next_chunk = if let Some(deadline) = deadline {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => {
                return Err(DownloadError::DurationLimitExceeded);
            },
            chunk = stream.next() => chunk,
        }
    } else {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
            chunk = stream.next() => chunk,
        }
    };

    let Some(chunk) = next_chunk else {
        drop(reservation);
        return Ok(None);
    };
    let chunk = match chunk {
        Ok(chunk) => chunk,
        Err(error) => return Err(error.into()),
    };
    ensure_transfer_active(cancel, deadline)?;
    Ok(Some((chunk, reservation)))
}

async fn write_chunk_parts<W, F>(
    writer: &mut W,
    chunk: &[u8],
    initial_reservation: Option<BandwidthReservation>,
    bandwidth_limiter: Option<&SharedBandwidthLimiter>,
    cancel: &CancellationToken,
    deadline: Option<Instant>,
    mut on_part_written: F,
) -> Result<(), DownloadError>
where
    W: tokio::io::AsyncWrite + Unpin,
    F: FnMut(u64) -> Result<(), DownloadError>,
{
    let mut initial_reservation = initial_reservation;
    let mut offset = 0usize;
    while offset < chunk.len() {
        ensure_transfer_active(cancel, deadline)?;
        let part_len = (chunk.len() - offset).min(GLOBAL_BANDWIDTH_QUANTUM_BYTES as usize);
        let mut reservation = if offset == 0 {
            initial_reservation.take()
        } else if let Some(limiter) = bandwidth_limiter {
            let reservation = limiter.reserve(part_len as u64, cancel).await?;
            ensure_transfer_active(cancel, deadline)?;
            Some(reservation)
        } else {
            None
        };
        ensure_transfer_active(cancel, deadline)?;
        if let Some(reservation) = reservation.as_mut() {
            reservation.charge(part_len as u64);
        }
        writer.write_all(&chunk[offset..offset + part_len]).await?;
        on_part_written(part_len as u64)?;
        offset += part_len;
    }
    Ok(())
}

async fn download_once(
    client: &Client,
    request: &DownloadRequest,
    cancel: &CancellationToken,
    state: &mut DownloadSessionState,
    progress: &TransferProgress,
    commit_state: &Mutex<bool>,
    bandwidth_limiter: Option<&SharedBandwidthLimiter>,
) -> Result<DownloadResult, DownloadError> {
    let fingerprint = url_fingerprint(&request.url);
    if state
        .partial
        .as_ref()
        .is_some_and(|saved| saved.url_fingerprint != fingerprint)
    {
        reset_partial(&mut state.partial, &fingerprint, progress).await?;
    }
    let resume_offset = state
        .partial
        .as_ref()
        .and_then(|saved| (saved.bytes > 0 && saved.validator.is_some()).then_some(saved.bytes));

    let mut builder = client.get(&request.url);
    if let Some(offset) = resume_offset {
        let validator = state
            .partial
            .as_ref()
            .and_then(|saved| saved.validator.as_ref())
            .expect("resume has a validator");
        builder = builder
            .header(header::RANGE, format!("bytes={offset}-"))
            .header(header::IF_RANGE, validator.value.as_str());
    }
    let mut response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
        response = builder.send() => response?,
    };
    let mut resumed = resume_offset.is_some();

    if resumed && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        reset_partial(&mut state.partial, &fingerprint, progress).await?;
        response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
            response = client.get(&request.url).send() => response?,
        };
        resumed = false;
    } else if resumed && response.status() == StatusCode::OK {
        // If-Range permits a full response when the object changed; truncate only the
        // already-open, exclusively-created handle, never a path selected by the caller.
        reset_partial(&mut state.partial, &fingerprint, progress).await?;
        resumed = false;
    }

    if !response.status().is_success() {
        return Err(DownloadError::HttpStatus(response.status()));
    }
    let response_content_length = response.content_length();
    let (offset, expected_total, expected_body_bytes) = if resumed {
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::InvalidRange);
        }
        let saved = state
            .partial
            .as_ref()
            .expect("resumed request has a partial handle");
        let range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range)
            .ok_or(DownloadError::InvalidRange)?;
        let body_bytes = validate_range_response(
            range,
            saved.bytes,
            response_content_length,
            request.max_bytes,
        )?;
        let response_validator =
            Validator::from_headers(response.headers()).ok_or(DownloadError::ResourceChanged)?;
        if saved.validator.as_ref() != Some(&response_validator) {
            return Err(DownloadError::ResourceChanged);
        }
        (saved.bytes, Some(range.total), Some(body_bytes))
    } else {
        if response.status() == StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::InvalidRange);
        }
        if response_content_length.is_some_and(|length| length > request.max_bytes) {
            return Err(DownloadError::ByteLimitExceeded);
        }
        (0, response_content_length, response_content_length)
    };

    let deadline = state.deadline;
    if state.partial.is_none() {
        let (path, file) = create_unique_partial(&request.destination).await?;
        state.partial = Some(PartialDownload {
            file,
            path,
            bytes: 0,
            url_fingerprint: fingerprint.clone(),
            validator: None,
            segmented: false,
            _staging: None,
        });
    }
    let saved = state.partial.as_mut().expect("partial file was created");
    if !resumed {
        saved.bytes = 0;
        saved.file.set_len(0).await?;
        saved.file.seek(SeekFrom::Start(0)).await?;
        saved.url_fingerprint = fingerprint;
        saved.segmented = false;
        progress.reset(0);
    } else {
        saved.file.seek(SeekFrom::End(0)).await?;
    }
    saved.validator = Validator::from_headers(response.headers());
    progress.begin_response(expected_total, offset);

    let mut stream = response.bytes_stream();
    let PartialDownload {
        file: saved_file,
        bytes: saved_bytes,
        ..
    } = saved;
    let mut received = offset;
    let mut received_this_response = 0u64;
    let mut transferred = 0u64;
    let throttle_start = Instant::now();
    loop {
        let Some((chunk, poll_reservation)) =
            next_response_chunk(&mut stream, bandwidth_limiter, cancel, deadline).await?
        else {
            break;
        };
        let chunk_bytes = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let next_received = received
            .checked_add(chunk_bytes)
            .ok_or(DownloadError::ByteLimitExceeded)?;
        let next_response_bytes = received_this_response
            .checked_add(chunk_bytes)
            .ok_or(DownloadError::InvalidRange)?;
        // Enforce the aggregate cap before touching the file, including chunked bodies.
        if next_received > request.max_bytes {
            return Err(DownloadError::ByteLimitExceeded);
        }
        if expected_body_bytes.is_some_and(|expected| next_response_bytes > expected) {
            return Err(DownloadError::InvalidRange);
        }

        write_chunk_parts(
            saved_file,
            &chunk,
            poll_reservation,
            bandwidth_limiter,
            cancel,
            deadline,
            |part_bytes| {
                received = received
                    .checked_add(part_bytes)
                    .ok_or(DownloadError::ByteLimitExceeded)?;
                received_this_response = received_this_response
                    .checked_add(part_bytes)
                    .ok_or(DownloadError::InvalidRange)?;
                transferred = transferred.saturating_add(part_bytes);
                *saved_bytes = received;
                progress.set(received);
                Ok(())
            },
        )
        .await?;
        debug_assert_eq!(received, next_received);
        debug_assert_eq!(received_this_response, next_response_bytes);
        if let Some(limit) = request.bytes_per_second {
            let seconds = transferred / limit;
            let nanos = ((transferred % limit) as u128 * 1_000_000_000 / limit as u128) as u64;
            let expected = Duration::from_secs(seconds).saturating_add(Duration::from_nanos(nanos));
            if let Some(delay) = expected.checked_sub(throttle_start.elapsed()) {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
    if expected_body_bytes
        .is_some_and(|expected| validate_received_range(expected, received_this_response).is_err())
        || expected_total
            .is_some_and(|expected| validate_received_range(expected, received).is_err())
    {
        return Err(DownloadError::InvalidRange);
    }
    finalize_download(
        state,
        request,
        cancel,
        commit_state,
        resumed,
        received,
        || {},
    )
    .await
}

async fn finalize_download<F>(
    state: &mut DownloadSessionState,
    request: &DownloadRequest,
    cancel: &CancellationToken,
    commit_state: &Mutex<bool>,
    resumed: bool,
    received: u64,
    after_hash: F,
) -> Result<DownloadResult, DownloadError>
where
    F: FnOnce(),
{
    let deadline = state.deadline;
    let saved = state.partial.as_mut().expect("download has a partial file");
    tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
        result = saved.file.sync_all() => result?,
    }
    ensure_transfer_active(cancel, deadline)?;

    let digest = sha256_open_file(&mut saved.file, cancel).await?;
    after_hash();
    ensure_transfer_active(cancel, deadline)?;
    if let Some(expected) = &request.expected_sha256 {
        let expected =
            integrity::normalize_sha256(expected).ok_or(DownloadError::InvalidExpectedHash)?;
        if expected != digest {
            return Err(DownloadError::HashMismatch);
        }
    }
    ensure_transfer_active(cancel, deadline)?;

    let destination = request.destination.clone();
    let bytes = received;
    let path = saved.path.clone();
    // Queue pause/cancel and commit use this same short gate. The final check and
    // hard-link therefore have a single ordering point relative to queue actions.
    let mut committed = commit_state.lock().await;
    if *committed {
        return state
            .committed_result
            .clone()
            .ok_or(DownloadError::DestinationExists);
    }
    ensure_transfer_active(cancel, deadline)?;
    // No await separates the final cancellation/deadline check from this atomic
    // filesystem commit.
    if let Err(error) = std::fs::hard_link(&path, &destination) {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(DownloadError::DestinationExists);
        }
        return Err(error.into());
    }
    let mut result = DownloadResult {
        destination,
        bytes,
        sha256: digest,
        resumed,
        cleanup_pending: None,
    };
    state.committed_result = Some(result.clone());
    *committed = true;
    drop(committed);
    let cleanup_pending = match deadline {
        Some(deadline) => {
            match tokio::time::timeout_at(deadline, cleanup_committed_alias(state)).await {
                Ok(pending) => pending,
                Err(_) => Some(
                    "temporary alias cleanup deferred because the absolute deadline elapsed"
                        .to_owned(),
                ),
            }
        }
        None => cleanup_committed_alias(state).await,
    };
    result.cleanup_pending = cleanup_pending;
    state.committed_result = Some(result.clone());
    Ok(result)
}

fn ensure_transfer_active(
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<(), DownloadError> {
    if cancel.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(DownloadError::DurationLimitExceeded);
    }
    Ok(())
}

fn parse_content_range(value: &str) -> Option<ContentRange> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    if !is_ascii_digits(start) || !is_ascii_digits(end) || !is_ascii_digits(total) {
        return None;
    }
    let parsed = ContentRange {
        start: start.parse().ok()?,
        end: end.parse().ok()?,
        total: total.parse().ok()?,
    };
    (parsed.total > 0 && parsed.start <= parsed.end && parsed.end < parsed.total).then_some(parsed)
}

#[cfg(test)]
#[path = "downloader_engine_segmented_tests.rs"]
mod segmented_tests;

fn is_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn session_deadline(state: &mut DownloadSessionState, now: Instant, duration: Duration) -> Instant {
    *state.deadline.get_or_insert(now + duration)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

fn validate_range_response(
    range: ContentRange,
    requested_start: u64,
    content_length: Option<u64>,
    max_bytes: u64,
) -> Result<u64, DownloadError> {
    if range.start != requested_start
        || range.start > range.end
        || range.end >= range.total
        || range.end.checked_add(1) != Some(range.total)
        || range.total > max_bytes
    {
        return Err(if range.total > max_bytes {
            DownloadError::ByteLimitExceeded
        } else {
            DownloadError::InvalidRange
        });
    }
    let body_bytes = range
        .end
        .checked_sub(range.start)
        .and_then(|length| length.checked_add(1))
        .ok_or(DownloadError::InvalidRange)?;
    if content_length.is_some_and(|length| length != body_bytes) {
        return Err(DownloadError::InvalidRange);
    }
    Ok(body_bytes)
}

fn validate_received_range(expected: u64, received: u64) -> Result<(), DownloadError> {
    if expected == received {
        Ok(())
    } else {
        Err(DownloadError::InvalidRange)
    }
}

async fn create_unique_partial(destination: &Path) -> Result<(PathBuf, fs::File), DownloadError> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = destination
        .file_name()
        .ok_or(DownloadError::InvalidDestination)?;
    for _ in 0..8 {
        let mut temporary_name = filename.to_os_string();
        temporary_name.push(format!(".dm-{}.part", uuid::Uuid::new_v4()));
        let path = parent.join(temporary_name);
        match create_temp_file(&path).await {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique download temp file",
    )
    .into())
}

async fn create_temp_file(path: &Path) -> Result<fs::File, std::io::Error> {
    tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .await
}

async fn reset_partial(
    partial: &mut Option<PartialDownload>,
    fingerprint: &str,
    progress: &TransferProgress,
) -> Result<(), DownloadError> {
    if let Some(saved) = partial.as_mut() {
        saved.file.set_len(0).await?;
        saved.file.seek(SeekFrom::Start(0)).await?;
        saved.bytes = 0;
        saved.url_fingerprint = fingerprint.to_owned();
        saved.validator = None;
    }
    progress.reset(0);
    Ok(())
}

async fn remove_partial_alias(partial: &mut Option<PartialDownload>) -> Result<(), std::io::Error> {
    let Some(path) = partial.as_ref().map(|saved| saved.path.clone()) else {
        return Ok(());
    };
    remove_if_exists(&path).await?;
    // Keep the handle and session entry intact unless alias removal succeeds.
    if let Some(staging) = partial.as_ref().and_then(|saved| saved._staging.as_ref()) {
        staging.mark_cleanup_allowed();
    }
    partial.take();
    Ok(())
}

async fn cleanup_committed_alias(state: &mut DownloadSessionState) -> Option<String> {
    match remove_partial_alias(&mut state.partial).await {
        Ok(()) => {
            state.deadline = None;
            None
        }
        Err(error) => Some(error.to_string()),
    }
}

async fn remove_if_exists(path: &Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn sha256_open_file(
    file: &mut fs::File,
    cancel: &CancellationToken,
) -> Result<String, DownloadError> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        if cancel.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if cancel.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn url_fingerprint(url: &str) -> String {
    Sha256::digest(url.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn is_retryable(error: &DownloadError) -> bool {
    match error {
        DownloadError::HttpStatus(status) => {
            *status == StatusCode::REQUEST_TIMEOUT
                || *status == StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
        }
        DownloadError::Http(error) => error.is_timeout() || error.is_connect() || error.is_body(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot},
    };

    #[test]
    fn eta_requires_known_total_and_stable_positive_active_samples() {
        let mut unknown_total = EtaEstimator::default();
        unknown_total.begin_segment(None, 0, Duration::ZERO);
        unknown_total.record(500, Duration::from_millis(500));
        unknown_total.record(1_000, Duration::from_secs(1));
        unknown_total.record(2_000, Duration::from_secs(2));
        let unknown = unknown_total.snapshot(Duration::from_secs(2));
        assert_eq!(unknown.bytes_received, 2_000);
        assert_eq!(unknown.total_bytes, None);
        assert_eq!(unknown.speed_bytes_per_second, None);
        assert_eq!(unknown.eta_seconds, None);

        let mut zero_speed = EtaEstimator::default();
        zero_speed.begin_segment(Some(10_000), 0, Duration::ZERO);
        zero_speed.record(0, Duration::from_millis(500));
        zero_speed.record(0, Duration::from_secs(1));
        zero_speed.record(0, Duration::from_secs(2));
        let zero = zero_speed.snapshot(Duration::from_secs(2));
        assert_eq!(zero.speed_bytes_per_second, None);
        assert_eq!(zero.eta_seconds, None);

        let mut known_total = EtaEstimator::default();
        known_total.begin_segment(Some(10_000), 0, Duration::ZERO);
        known_total.record(500, Duration::from_millis(500));
        assert_eq!(
            known_total.snapshot(Duration::from_millis(500)).eta_seconds,
            None
        );
        known_total.record(1_000, Duration::from_secs(1));
        assert_eq!(
            known_total.snapshot(Duration::from_secs(1)).eta_seconds,
            None
        );
        known_total.record(2_000, Duration::from_secs(2));
        let estimate = known_total.snapshot(Duration::from_secs(2));
        assert_eq!(estimate.speed_bytes_per_second, Some(1_000));
        assert_eq!(estimate.eta_seconds, Some(8));
    }

    #[test]
    fn eta_expires_at_the_four_second_active_staleness_boundary() {
        let mut estimator = EtaEstimator::default();
        estimator.begin_segment(Some(10_000), 0, Duration::ZERO);
        estimator.record(500, Duration::from_millis(500));
        estimator.record(1_000, Duration::from_secs(1));
        estimator.record(2_000, Duration::from_secs(2));
        assert_eq!(
            estimator.snapshot(Duration::from_millis(5_999)).eta_seconds,
            Some(8)
        );

        let expired = estimator.snapshot(Duration::from_secs(6));
        assert_eq!(expired.speed_bytes_per_second, None);
        assert_eq!(expired.eta_seconds, None);
    }

    #[test]
    fn paused_eta_does_not_age_and_resume_resets_the_sampling_baseline() {
        let mut estimator = EtaEstimator::default();
        estimator.begin_segment(Some(10_000), 0, Duration::ZERO);
        estimator.record(500, Duration::from_millis(500));
        estimator.record(1_000, Duration::from_secs(1));
        estimator.record(2_000, Duration::from_secs(2));
        estimator.pause(Duration::from_secs(3));

        let paused = estimator.snapshot(Duration::from_secs(3_600));
        assert_eq!(paused.speed_bytes_per_second, Some(1_000));
        assert_eq!(paused.eta_seconds, Some(8));

        estimator.resume(2_000);
        let resumed = estimator.snapshot(Duration::ZERO);
        assert_eq!(resumed.bytes_received, 2_000);
        assert_eq!(resumed.total_bytes, Some(10_000));
        assert_eq!(resumed.speed_bytes_per_second, None);
        assert_eq!(resumed.eta_seconds, None);

        estimator.record(2_500, Duration::from_millis(500));
        estimator.record(3_000, Duration::from_secs(1));
        assert_eq!(estimator.snapshot(Duration::from_secs(1)).eta_seconds, None);
        estimator.record(4_000, Duration::from_secs(2));
        let fresh_estimate = estimator.snapshot(Duration::from_secs(2));
        assert_eq!(fresh_estimate.speed_bytes_per_second, Some(1_000));
        assert_eq!(fresh_estimate.eta_seconds, Some(6));
    }

    #[tokio::test]
    async fn in_flight_write_after_pause_keeps_eta_frozen_until_resume() {
        let progress = Arc::new(TransferProgress::default());
        {
            let mut state = lock_transfer_progress(&progress.state);
            state.bytes_received = 2_000;
            state
                .estimator
                .begin_segment(Some(10_000), 0, Duration::ZERO);
            state.estimator.record(500, Duration::from_millis(500));
            state.estimator.record(1_000, Duration::from_secs(1));
            state.estimator.record(2_000, Duration::from_secs(2));
        }

        let before_pause = progress.snapshot();
        assert_eq!(before_pause.eta_seconds, Some(8));

        // A one-byte in-memory pipe keeps write_all pending after its first byte.
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let write_progress = progress.clone();
        let in_flight_write = tokio::spawn(async move {
            writer.write_all(b"abc").await.unwrap();
            write_progress.set(3_000);
        });
        let mut first_byte = [0; 1];
        reader.read_exact(&mut first_byte).await.unwrap();
        progress.pause();
        let frozen = progress.snapshot();
        let mut remaining_bytes = [0; 2];
        reader.read_exact(&mut remaining_bytes).await.unwrap();
        in_flight_write.await.unwrap();
        let after_late_write = progress.snapshot();

        assert_eq!(after_late_write.bytes_received, 3_000);
        assert_eq!(
            after_late_write.speed_bytes_per_second,
            frozen.speed_bytes_per_second
        );
        assert_eq!(after_late_write.eta_seconds, frozen.eta_seconds);
        assert_eq!(after_late_write.eta_seconds, before_pause.eta_seconds);

        progress.resume();
        let paused_baseline = progress.snapshot();
        assert_eq!(paused_baseline.bytes_received, 3_000);
        assert_eq!(paused_baseline.speed_bytes_per_second, None);
        assert_eq!(paused_baseline.eta_seconds, None);

        let resumed = progress.snapshot();
        assert_eq!(resumed.bytes_received, 3_000);
        assert_eq!(resumed.speed_bytes_per_second, None);
        assert_eq!(resumed.eta_seconds, None);
    }

    #[test]
    fn shared_limiter_rejects_over_max_without_changing_its_configured_rate() {
        let maximum = MAX_GLOBAL_BANDWIDTH_BYTES_PER_SECOND;
        let limiter = SharedBandwidthLimiter::new(maximum).unwrap();
        assert_eq!(limiter.configured_rate_for_test(), maximum);
        assert!(matches!(
            SharedBandwidthLimiter::new(maximum + 1),
            Err(DownloadError::InvalidGlobalBandwidthLimit)
        ));

        limiter.set_limit(17).unwrap();
        assert!(matches!(
            limiter.set_limit(maximum + 1),
            Err(DownloadError::InvalidGlobalBandwidthLimit)
        ));
        assert_eq!(limiter.configured_rate_for_test(), 17);
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_reservation_refunds_only_unused_bytes_and_has_fixed_capacity() {
        let limiter = SharedBandwidthLimiter::new(100).unwrap();
        let cancel = CancellationToken::new();
        limiter
            .acquire(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES, &cancel)
            .await
            .unwrap();
        let mut reservation = limiter.reserve(100, &cancel).await.unwrap();
        reservation.charge(40);
        drop(reservation);

        let refund_check_started = Instant::now();
        limiter.acquire(60, &cancel).await.unwrap();
        assert_eq!(Instant::now(), refund_check_started);

        let charged_bytes_check_started = Instant::now();
        limiter.acquire(41, &cancel).await.unwrap();
        assert_eq!(
            Instant::now().duration_since(charged_bytes_check_started),
            Duration::from_millis(410)
        );

        assert!(matches!(
            limiter
                .reserve(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES + 1, &cancel)
                .await,
            Err(DownloadError::InvalidBandwidthReservation)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn response_stream_is_not_polled_until_its_shared_quantum_is_reserved() {
        let quantum = GLOBAL_BANDWIDTH_QUANTUM_BYTES;
        let limiter = SharedBandwidthLimiter::new(quantum).unwrap();
        let cancel = CancellationToken::new();
        limiter.acquire(quantum, &cancel).await.unwrap();

        let polls = Arc::new(AtomicU64::new(0));
        let stream_polls = Arc::clone(&polls);
        let mut stream = futures_util::stream::poll_fn(move |_context| {
            stream_polls.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(Some(Ok::<Vec<u8>, DownloadError>(vec![0x2a])))
        });
        let worker_limiter = limiter.clone();
        let worker_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            next_response_chunk(&mut stream, Some(&worker_limiter), &worker_cancel, None).await
        });

        while limiter.waiting_count() != 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(polls.load(Ordering::Acquire), 0);
        tokio::time::advance(Duration::from_secs(1)).await;

        let (chunk, reservation) = worker.await.unwrap().unwrap().unwrap();
        assert_eq!(chunk, vec![0x2a]);
        assert_eq!(polls.load(Ordering::Acquire), 1);
        drop(reservation);
    }

    #[tokio::test(start_paused = true)]
    async fn large_response_chunk_is_split_and_charged_by_processed_bytes() {
        let quantum = GLOBAL_BANDWIDTH_QUANTUM_BYTES;
        let limiter = SharedBandwidthLimiter::new(quantum).unwrap();
        let cancel = CancellationToken::new();
        limiter.acquire(quantum, &cancel).await.unwrap();
        let payload = vec![0x5a; quantum as usize * 2 + 37];
        let mut stream =
            futures_util::stream::iter([Ok::<Vec<u8>, DownloadError>(payload.clone())]);
        let started = Instant::now();
        let (chunk, reservation) = next_response_chunk(&mut stream, Some(&limiter), &cancel, None)
            .await
            .unwrap()
            .unwrap();
        let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
        let mut charged_bytes = 0u64;
        let mut part_sizes = Vec::new();

        write_chunk_parts(
            &mut writer,
            &chunk,
            reservation,
            Some(&limiter),
            &cancel,
            None,
            |part_bytes| {
                charged_bytes += part_bytes;
                part_sizes.push(part_bytes);
                Ok(())
            },
        )
        .await
        .unwrap();

        let final_wait_nanos =
            ((37u128 * NANOS_PER_SECOND + u128::from(quantum) - 1) / u128::from(quantum)) as u64;
        let expected_minimum = Duration::from_secs(2) + Duration::from_nanos(final_wait_nanos);
        let elapsed = Instant::now().duration_since(started);
        // Tokio's virtual timer wakeups can round slightly late; allow 5 ms of
        // scheduler/timer jitter while still detecting materially early pacing.
        assert!(
            elapsed >= expected_minimum.saturating_sub(Duration::from_millis(5)),
            "elapsed {elapsed:?} was shorter than pacing minimum {expected_minimum:?}"
        );
        assert_eq!(charged_bytes, payload.len() as u64);
        assert_eq!(part_sizes, vec![quantum, quantum, 37]);

        drop(writer);
        let mut written = Vec::new();
        reader.read_to_end(&mut written).await.unwrap();
        assert_eq!(written, payload);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_stream_cancel_or_deadline_refunds_reservation_to_another_consumer() {
        for expire_by_deadline in [false, true] {
            let quantum = GLOBAL_BANDWIDTH_QUANTUM_BYTES;
            let limiter = SharedBandwidthLimiter::new(quantum).unwrap();
            let cancel = CancellationToken::new();
            let polls = Arc::new(AtomicU64::new(0));
            let stream_polls = Arc::clone(&polls);
            let mut stream = futures_util::stream::poll_fn(move |_context| {
                stream_polls.fetch_add(1, Ordering::AcqRel);
                Poll::<Option<Result<Vec<u8>, DownloadError>>>::Pending
            });
            let deadline =
                expire_by_deadline.then_some(Instant::now() + Duration::from_millis(100));
            let worker_limiter = limiter.clone();
            let worker_cancel = cancel.clone();
            let worker = tokio::spawn(async move {
                next_response_chunk(&mut stream, Some(&worker_limiter), &worker_cancel, deadline)
                    .await
            });
            while polls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(limiter.waiting_count(), 0);

            let other_limiter = limiter.clone();
            let other_consumer = tokio::spawn(async move {
                let other_cancel = CancellationToken::new();
                let mut reservation = other_limiter.reserve(quantum, &other_cancel).await?;
                reservation.charge(quantum);
                Ok::<(), DownloadError>(())
            });
            while limiter.waiting_count() != 1 {
                tokio::task::yield_now().await;
            }

            let started = Instant::now();
            if expire_by_deadline {
                tokio::time::advance(Duration::from_millis(100)).await;
            } else {
                cancel.cancel();
            }
            let result = worker.await.unwrap();
            if expire_by_deadline {
                assert!(matches!(result, Err(DownloadError::DurationLimitExceeded)));
                assert_eq!(
                    Instant::now().duration_since(started),
                    Duration::from_millis(100)
                );
            } else {
                assert!(matches!(result, Err(DownloadError::Cancelled)));
                assert_eq!(Instant::now(), started);
            }
            let reservation_released_at = Instant::now();
            other_consumer.await.unwrap().unwrap();
            assert_eq!(Instant::now(), reservation_released_at);
            assert_eq!(limiter.waiting_count(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_limiter_caps_bytes_across_two_concurrent_downloads() {
        let quantum = GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES;
        let limiter = SharedBandwidthLimiter::new(quantum).unwrap();
        let cancel = CancellationToken::new();
        let total_bytes = Arc::new(AtomicU64::new(0));
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();

        for download_id in 0..2 {
            let limiter = limiter.clone();
            let cancel = cancel.clone();
            let total_bytes = total_bytes.clone();
            let completed_tx = completed_tx.clone();
            tokio::spawn(async move {
                limiter.acquire(quantum, &cancel).await.unwrap();
                total_bytes.fetch_add(quantum, Ordering::AcqRel);
                completed_tx.send(download_id).unwrap();
            });
        }
        drop(completed_tx);

        while total_bytes.load(Ordering::Acquire) != quantum || limiter.waiting_count() != 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(total_bytes.load(Ordering::Acquire), quantum);
        assert!(completed_rx.recv().await.is_some());
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(completed_rx.recv().await.is_some());
        assert_eq!(total_bytes.load(Ordering::Acquire), quantum * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn shared_limiter_grants_waiting_tickets_in_fifo_order() {
        let quantum = GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES;
        let limiter = std::sync::Arc::new(SharedBandwidthLimiter::new(quantum).unwrap());
        let cancel = CancellationToken::new();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        limiter.acquire(quantum, &cancel).await.unwrap();

        for ticket_id in 0..3 {
            let worker_limiter = std::sync::Arc::clone(&limiter);
            let cancel = cancel.clone();
            let completed_tx = completed_tx.clone();
            tokio::spawn(async move {
                worker_limiter.acquire(quantum, &cancel).await.unwrap();
                completed_tx.send(ticket_id).unwrap();
            });
            while limiter.waiting_count() != ticket_id as usize + 1 {
                tokio::task::yield_now().await;
            }
        }
        drop(completed_tx);

        let started = Instant::now();
        for expected_ticket in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(completed_rx.recv().await, Some(expected_ticket));
            assert_eq!(
                Instant::now().duration_since(started),
                Duration::from_secs(expected_ticket + 1)
            );
            let no_extra_completion = completed_rx.try_recv();
            if expected_ticket < 2 {
                assert!(matches!(
                    no_extra_completion,
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            } else {
                assert!(matches!(
                    no_extra_completion,
                    Err(mpsc::error::TryRecvError::Disconnected)
                ));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_middle_limiter_waiter_preserves_tokens_and_fifo_progress() {
        let quantum = GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES;
        let limiter = Arc::new(SharedBandwidthLimiter::new(quantum).unwrap());
        let drain_cancel = CancellationToken::new();
        limiter.acquire(quantum, &drain_cancel).await.unwrap();
        tokio::time::advance(Duration::from_millis(500)).await;

        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let head_cancel = CancellationToken::new();
        let head_limiter = Arc::clone(&limiter);
        let head_completed = completed_tx.clone();
        tokio::spawn(async move {
            head_limiter.acquire(quantum, &head_cancel).await.unwrap();
            head_completed.send(0).unwrap();
        });
        while limiter.waiting_count() != 1 {
            tokio::task::yield_now().await;
        }

        let cancelled_token = CancellationToken::new();
        let waiter_cancel = cancelled_token.clone();
        let waiter_limiter = Arc::clone(&limiter);
        let cancelled_waiter =
            tokio::spawn(async move { waiter_limiter.acquire(quantum, &waiter_cancel).await });
        while limiter.waiting_count() != 2 {
            tokio::task::yield_now().await;
        }

        let follower_cancel = CancellationToken::new();
        let follower_limiter = Arc::clone(&limiter);
        let follower_completed = completed_tx.clone();
        tokio::spawn(async move {
            follower_limiter
                .acquire(quantum, &follower_cancel)
                .await
                .unwrap();
            follower_completed.send(2).unwrap();
        });
        while limiter.waiting_count() != 3 {
            tokio::task::yield_now().await;
        }
        drop(completed_tx);

        let tokens_before_cancel = lock_bandwidth_state(&limiter.inner.state).tokens;
        assert_eq!(tokens_before_cancel, u128::from(quantum / 2));
        let cancelled_at = Instant::now();
        cancelled_token.cancel();
        assert!(matches!(
            cancelled_waiter.await.unwrap(),
            Err(DownloadError::Cancelled)
        ));
        assert_eq!(limiter.waiting_count(), 2);
        assert_eq!(Instant::now(), cancelled_at);
        assert_eq!(
            lock_bandwidth_state(&limiter.inner.state).tokens,
            tokens_before_cancel
        );

        let started = Instant::now();
        tokio::time::advance(Duration::from_millis(500)).await;
        assert_eq!(completed_rx.recv().await, Some(0));
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_millis(500)
        );
        while limiter.waiting_count() != 1 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(completed_rx.recv().await, Some(2));
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_millis(1500)
        );
        assert_eq!(limiter.waiting_count(), 0);
        assert_eq!(lock_bandwidth_state(&limiter.inner.state).tokens, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn changing_shared_limit_recalculates_waiters_and_zero_unblocks_them() {
        let limiter = std::sync::Arc::new(SharedBandwidthLimiter::new(1).unwrap());
        let cancel = CancellationToken::new();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        limiter
            .acquire(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES, &cancel)
            .await
            .unwrap();

        for ticket_id in 0..2 {
            let worker_limiter = std::sync::Arc::clone(&limiter);
            let cancel = cancel.clone();
            let completed_tx = completed_tx.clone();
            tokio::spawn(async move {
                worker_limiter.acquire(1, &cancel).await.unwrap();
                completed_tx.send(ticket_id).unwrap();
            });
            while limiter.waiting_count() != ticket_id as usize + 1 {
                tokio::task::yield_now().await;
            }
        }
        drop(completed_tx);

        let started = Instant::now();
        limiter.set_limit(2).unwrap();
        while limiter.state_rate_for_test() != 2 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(Duration::from_millis(500)).await;
        assert_eq!(completed_rx.recv().await, Some(0));
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_millis(500)
        );
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        limiter.set_limit(0).unwrap();
        assert_eq!(completed_rx.recv().await, Some(1));
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_millis(500)
        );
        assert_eq!(limiter.waiting_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_shared_limit_allows_concurrent_download_bytes_without_waiting() {
        let limiter = SharedBandwidthLimiter::new(0).unwrap();
        let cancel = CancellationToken::new();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();

        for download_id in 0..2 {
            let limiter = limiter.clone();
            let cancel = cancel.clone();
            let completed_tx = completed_tx.clone();
            tokio::spawn(async move {
                limiter.acquire(10_000, &cancel).await.unwrap();
                completed_tx.send(download_id).unwrap();
            });
        }
        drop(completed_tx);

        let first = completed_rx.recv().await.unwrap();
        let second = completed_rx.recv().await.unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test(start_paused = true)]
    async fn shared_limiter_wait_is_cancelled_and_deadline_drop_releases_its_ticket() {
        let limiter = SharedBandwidthLimiter::new(1).unwrap();
        let drain_cancel = CancellationToken::new();
        limiter
            .acquire(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES, &drain_cancel)
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let cancellation_limiter = limiter.clone();
        let cancellation =
            tokio::spawn(async move { cancellation_limiter.acquire(1, &worker_cancel).await });
        while limiter.waiting_count() != 1 {
            tokio::task::yield_now().await;
        }
        cancel.cancel();
        assert!(matches!(
            cancellation.await.unwrap(),
            Err(DownloadError::Cancelled)
        ));
        assert_eq!(limiter.waiting_count(), 0);

        let deadline_cancel = CancellationToken::new();
        let timeout_result = tokio::time::timeout(
            Duration::from_millis(100),
            limiter.acquire(1, &deadline_cancel),
        )
        .await;
        assert!(timeout_result.is_err());
        assert_eq!(limiter.waiting_count(), 0);
    }

    #[test]
    fn content_range_must_match_start_end_total_and_declared_body_length() {
        let valid = parse_content_range("bytes 5-9/10").unwrap();
        assert!(matches!(
            validate_range_response(valid, 5, Some(5), 10),
            Ok(5)
        ));
        assert!(parse_content_range("bytes 10-9/20").is_none());
        assert!(parse_content_range("bytes 5-10/10").is_none());
        assert!(matches!(
            validate_range_response(valid, 4, Some(5), 10),
            Err(DownloadError::InvalidRange)
        ));
        let truncated_range = parse_content_range("bytes 5-8/10").unwrap();
        assert!(matches!(
            validate_range_response(truncated_range, 5, Some(4), 10),
            Err(DownloadError::InvalidRange)
        ));
        assert!(matches!(
            validate_range_response(valid, 5, Some(4), 10),
            Err(DownloadError::InvalidRange)
        ));
        assert!(matches!(
            validate_range_response(valid, 5, Some(5), 9),
            Err(DownloadError::ByteLimitExceeded)
        ));
        assert!(matches!(
            validate_received_range(5, 4),
            Err(DownloadError::InvalidRange)
        ));
    }

    #[test]
    fn session_deadline_is_not_renewed_when_resuming_after_a_pause() {
        let started = Instant::now();
        let duration = Duration::from_secs(10);
        let mut state = DownloadSessionState::default();
        let first_deadline = session_deadline(&mut state, started, duration);
        let resumed_at = started + Duration::from_secs(8);
        let resumed_deadline = session_deadline(&mut state, resumed_at, duration);
        assert_eq!(resumed_deadline, first_deadline);
        assert!(resumed_deadline < resumed_at + duration);
    }

    #[tokio::test]
    async fn expired_deadline_drops_a_stalled_operation_without_drain() {
        let state_lock = Arc::new(Mutex::new(()));
        let operation_lock = state_lock.clone();
        let mut operation = Box::pin(async move {
            let _guard = operation_lock.lock().await;
            std::future::pending::<Result<DownloadResult, DownloadError>>().await
        });
        std::future::poll_fn(|context| {
            assert!(operation.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;

        let completion = wait_for_operation_until_deadline(
            Instant::now() - Duration::from_millis(1),
            operation.as_mut(),
        )
        .await;
        assert!(matches!(completion, OperationCompletion::TimedOut));
        drop(operation);
        assert!(state_lock.try_lock().is_ok());
    }

    #[tokio::test]
    async fn cancellation_at_hash_finalization_boundary_prevents_commit() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("final.bin");
        let partial_path = directory.path().join("final.bin.dm-part");
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .await
            .unwrap();
        file.write_all(b"verified bytes").await.unwrap();
        file.sync_all().await.unwrap();

        let bytes = b"verified bytes".len() as u64;
        let mut state = DownloadSessionState {
            deadline: Some(Instant::now() + Duration::from_secs(10)),
            partial: Some(PartialDownload {
                file,
                path: partial_path,
                bytes,
                url_fingerprint: url_fingerprint("https://example.com/file"),
                validator: None,
                segmented: false,
                _staging: None,
            }),
            ..DownloadSessionState::default()
        };
        let request = request_for(
            "https://example.com/file",
            destination.clone(),
            1024,
            Duration::from_secs(10),
        );
        let cancel = CancellationToken::new();
        let commit_state = Mutex::new(false);
        let result = finalize_download(
            &mut state,
            &request,
            &cancel,
            &commit_state,
            false,
            bytes,
            || cancel.cancel(),
        )
        .await;

        assert!(matches!(result, Err(DownloadError::Cancelled)));
        assert!(!tokio::fs::try_exists(&destination).await.unwrap());
        assert!(state.committed_result.is_none());
        assert_eq!(
            state.partial.as_ref().map(|partial| partial.bytes),
            Some(bytes)
        );
        assert!(!*commit_state.lock().await);
    }

    #[tokio::test]
    async fn finalization_matches_expected_sha256_and_commits_matching_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let (mut state, request, bytes) =
            finalization_fixture(directory.path(), Some(expected.to_owned())).await;
        request.validate().unwrap();
        let destination = request.destination.clone();
        let cancel = CancellationToken::new();
        let commit_state = Mutex::new(false);

        let result = finalize_download(
            &mut state,
            &request,
            &cancel,
            &commit_state,
            false,
            bytes,
            || {},
        )
        .await
        .unwrap();

        assert_eq!(result.sha256, expected);
        assert_eq!(tokio::fs::read(destination).await.unwrap(), b"abc");
        assert!(*commit_state.lock().await);
    }

    #[test]
    fn api_rejects_outer_whitespace_in_expected_sha256() {
        let request = DownloadRequest {
            expected_sha256: Some(format!(
                " {} ",
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            )),
            ..request_for(
                "https://example.com/file",
                PathBuf::from("final.bin"),
                1024,
                Duration::from_secs(60),
            )
        };
        assert!(matches!(
            request.validate(),
            Err(DownloadError::InvalidExpectedHash)
        ));
    }

    #[tokio::test]
    async fn finalization_rejects_a_mismatched_expected_sha256_before_commit() {
        let directory = tempfile::tempdir().unwrap();
        let (mut state, request, bytes) =
            finalization_fixture(directory.path(), Some("0".repeat(64))).await;
        request.validate().unwrap();
        let destination = request.destination.clone();
        let cancel = CancellationToken::new();
        let commit_state = Mutex::new(false);

        let result = finalize_download(
            &mut state,
            &request,
            &cancel,
            &commit_state,
            false,
            bytes,
            || {},
        )
        .await;

        assert!(matches!(result, Err(DownloadError::HashMismatch)));
        assert!(!tokio::fs::try_exists(destination).await.unwrap());
        assert!(!*commit_state.lock().await);
    }

    #[test]
    fn retries_only_transient_http_failures() {
        assert!(is_retryable(&DownloadError::HttpStatus(
            StatusCode::SERVICE_UNAVAILABLE
        )));
        assert!(is_retryable(&DownloadError::HttpStatus(
            StatusCode::TOO_MANY_REQUESTS
        )));
        assert!(!is_retryable(&DownloadError::HttpStatus(
            StatusCode::NOT_FOUND
        )));
        assert!(!is_retryable(&DownloadError::InvalidRange));
    }

    #[test]
    fn rejects_zero_limits_and_malformed_expected_hashes() {
        let base = DownloadRequest {
            url: "https://example.com/a".into(),
            destination: PathBuf::from("a.bin"),
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: Some(0),
            max_bytes: 1,
            max_duration: Duration::from_secs(1),
        };
        assert!(matches!(base.validate(), Err(DownloadError::InvalidLimit)));
        let invalid_hash = DownloadRequest {
            bytes_per_second: None,
            expected_sha256: Some("xyz".into()),
            ..base
        };
        assert!(matches!(
            invalid_hash.validate(),
            Err(DownloadError::InvalidExpectedHash)
        ));
        let zero_duration = DownloadRequest {
            max_duration: Duration::ZERO,
            expected_sha256: None,
            ..invalid_hash
        };
        assert!(matches!(
            zero_duration.validate(),
            Err(DownloadError::InvalidLimit)
        ));
        let too_long = DownloadRequest {
            max_duration: MAX_DOWNLOAD_DURATION + Duration::from_secs(1),
            ..zero_duration
        };
        assert!(matches!(
            too_long.validate(),
            Err(DownloadError::InvalidLimit)
        ));
    }

    #[tokio::test]
    async fn precreated_partial_paths_are_never_opened_or_truncated() {
        let directory = tempfile::tempdir().unwrap();
        let external = directory.path().join("external.bin");
        let candidate = directory.path().join("candidate.dm-part");
        let destination = directory.path().join("file.bin");
        let legacy_path = directory.path().join("file.bin.dm-part");
        tokio::fs::write(&external, b"external contents")
            .await
            .unwrap();
        tokio::fs::hard_link(&external, &candidate).await.unwrap();
        let result = create_temp_file(&candidate).await;
        assert!(matches!(result, Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists));
        tokio::fs::hard_link(&external, &legacy_path).await.unwrap();
        let (generated_path, generated_file) = create_unique_partial(&destination).await.unwrap();
        assert_ne!(generated_path, legacy_path);
        drop(generated_file);
        tokio::fs::remove_file(generated_path).await.unwrap();
        assert_eq!(
            tokio::fs::read(&external).await.unwrap(),
            b"external contents"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_post_commit_alias_cleanup_is_reported_and_keeps_session_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let (mut state, path, moved_file, blocker) = cleanup_blocked_state(directory.path()).await;
        let destination = directory.path().join("committed.bin");
        tokio::fs::write(&destination, b"committed output")
            .await
            .unwrap();
        state.committed_result = Some(DownloadResult {
            destination: destination.clone(),
            bytes: 16,
            sha256: "digest".to_owned(),
            resumed: false,
            cleanup_pending: None,
        });
        let session = DownloadSession {
            state: Arc::new(Mutex::new(state)),
            ..DownloadSession::default()
        };
        let result = finish_committed_after_timeout(&session).await.unwrap();
        assert_eq!(result.destination, destination);
        assert!(result.cleanup_pending.is_some());
        assert!(tokio::fs::try_exists(&result.destination).await.unwrap());
        {
            let state = session.state.lock().await;
            assert!(state.partial.is_some());
            assert!(state.deadline.is_some());
            assert!(state.committed_result.is_some());
        }

        tokio::fs::remove_file(blocker).await.unwrap();
        tokio::fs::remove_dir(&path).await.unwrap();
        tokio::fs::rename(&moved_file, &path).await.unwrap();
        discard_download_session(&session).await.unwrap();
        let state = session.state.lock().await;
        assert!(state.partial.is_none());
        assert!(state.deadline.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_discard_retains_handle_and_can_be_retried() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path, moved_file, blocker) = cleanup_blocked_state(directory.path()).await;
        let session = DownloadSession {
            state: Arc::new(Mutex::new(state)),
            ..DownloadSession::default()
        };
        assert!(discard_download_session(&session).await.is_err());
        {
            let state = session.state.lock().await;
            assert!(state.partial.is_some());
            assert!(state.deadline.is_some());
        }

        tokio::fs::remove_file(blocker).await.unwrap();
        tokio::fs::remove_dir(&path).await.unwrap();
        tokio::fs::rename(&moved_file, &path).await.unwrap();
        discard_download_session(&session).await.unwrap();
        let state = session.state.lock().await;
        assert!(state.partial.is_none());
        assert!(state.deadline.is_none());
    }

    #[tokio::test]
    async fn chunked_body_stops_before_writing_past_the_aggregate_byte_cap() {
        let (url, server) = start_server(|mut stream| async move {
            read_request(&mut stream).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await;
            let _ = stream
                .write_all(b"4\r\nabcd\r\n4\r\nefgh\r\n0\r\n\r\n")
                .await;
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let request = request_for(
            &url,
            directory.path().join("output.bin"),
            5,
            Duration::from_secs(3),
        );
        let session = DownloadSession::default();
        let result = download_with_session(request, CancellationToken::new(), &session).await;
        assert!(matches!(result, Err(DownloadError::ByteLimitExceeded)));
        let mut entries = tokio::fs::read_dir(directory.path()).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn absolute_duration_bounds_cleanup_and_preserves_partial_for_retry() {
        let (first_chunk_sent, first_chunk_received) = oneshot::channel();
        let (url, server) = start_server(|mut stream| async move {
            let mut first_chunk_sent = Some(first_chunk_sent);
            read_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            for _ in 0..20 {
                if stream.write_all(b"1\r\nx\r\n").await.is_err() {
                    break;
                }
                if let Some(sender) = first_chunk_sent.take() {
                    let _ = sender.send(());
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            let _ = stream.write_all(b"0\r\n\r\n").await;
        })
        .await;
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("output.bin");
        let request = request_for(&url, destination.clone(), 1024, Duration::from_millis(140));
        let session = DownloadSession::default();
        let worker_session = session.clone();
        let worker = tokio::spawn(async move {
            download_with_session(request, CancellationToken::new(), &worker_session).await
        });
        tokio::time::timeout(Duration::from_secs(2), first_chunk_received)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_bytes(1))
            .await
            .unwrap();
        let result = worker.await.unwrap();
        assert!(matches!(
            result,
            Err(DownloadError::CleanupPending { operation, .. })
                if operation == DownloadError::DurationLimitExceeded.to_string()
        ));
        assert!(!tokio::fs::try_exists(&destination).await.unwrap());
        assert!(session.state.lock().await.partial.is_some());
        discard_download_session(&session).await.unwrap();
        let mut entries = tokio::fs::read_dir(directory.path()).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pause_then_resume_uses_range_and_commits_the_exact_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (first_body_sent, first_body_received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let first_request = read_request_text(&mut first).await;
            assert!(!first_request.to_ascii_lowercase().contains("range: bytes="));
            first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nhello").await.unwrap();
            let _ = first_body_sent.send(());

            let (mut resumed, _) = listener.accept().await.unwrap();
            let resumed_request = read_request_text(&mut resumed).await.to_ascii_lowercase();
            assert!(resumed_request.contains("range: bytes=5-"));
            assert!(resumed_request.contains("if-range: \"v1\""));
            resumed.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 5-9/10\r\nContent-Length: 5\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nworld").await.unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("output.bin");
        let url = format!("http://{address}/file");
        let request = request_for(&url, destination.clone(), 1024, Duration::from_secs(5));
        let session = DownloadSession::default();
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let first_session = session.clone();
        let first_request = request.clone();
        let first_worker = tokio::spawn(async move {
            download_with_session(first_request, worker_cancel, &first_session).await
        });
        first_body_received.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_bytes(5))
            .await
            .unwrap();
        cancel.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), first_worker)
                .await
                .unwrap()
                .unwrap(),
            Err(DownloadError::Cancelled)
        ));
        {
            let state = session.state.lock().await;
            assert_eq!(state.partial.as_ref().map(|partial| partial.bytes), Some(5));
        }

        let result = download_with_session(request, CancellationToken::new(), &session)
            .await
            .unwrap();
        assert!(result.resumed);
        assert_eq!(result.bytes, 10);
        assert_eq!(result.cleanup_pending, None);
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"helloworld");
        let mut entries = tokio::fs::read_dir(directory.path()).await.unwrap();
        assert_eq!(
            entries
                .next_entry()
                .await
                .unwrap()
                .unwrap()
                .file_name()
                .to_str()
                .unwrap(),
            "output.bin"
        );
        assert!(entries.next_entry().await.unwrap().is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn resume_does_not_renew_an_expired_original_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (body_sent, body_received) = oneshot::channel();
        let (stop_server, stop_waiting) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let _ = read_request_text(&mut first).await;
            first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nhello").await.unwrap();
            let _ = body_sent.send(());
            tokio::select! {
                accepted = listener.accept() => {
                    if let Ok((mut resumed, _)) = accepted {
                        let _ = read_request_text(&mut resumed).await;
                        let _ = resumed.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 5-9/10\r\nContent-Length: 5\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nworld").await;
                    }
                }
                _ = stop_waiting => {}
            }
            drop(first);
        });

        let directory = tempfile::tempdir().unwrap();
        let request = request_for(
            &format!("http://{address}/file"),
            directory.path().join("output.bin"),
            1024,
            Duration::from_secs(5),
        );
        let session = DownloadSession::default();
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let worker_session = session.clone();
        let worker_request = request.clone();
        let worker = tokio::spawn(async move {
            download_with_session(worker_request, worker_cancel, &worker_session).await
        });
        body_received.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_bytes(5))
            .await
            .unwrap();
        let original_deadline = session.absolute_deadline().unwrap();
        cancel.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap(),
            Err(DownloadError::Cancelled)
        ));
        assert_eq!(session.deadline_for_test().await, Some(original_deadline));
        {
            let mut state = session.state.lock().await;
            // Simulate elapsed pause time without wall-clock sleeps.
            state.deadline = Some(Instant::now() - Duration::from_millis(1));
        }

        let result = download_with_session(request, CancellationToken::new(), &session).await;
        let _ = stop_server.send(());
        assert!(matches!(
            result,
            Err(DownloadError::CleanupPending {
                operation,
                cleanup_error: _
            }) if operation == DownloadError::DurationLimitExceeded.to_string()
        ));
        let state = session.state.lock().await;
        assert_eq!(state.partial.as_ref().map(|partial| partial.bytes), Some(5));
        drop(state);
        assert!(!tokio::fs::try_exists(directory.path().join("output.bin"))
            .await
            .unwrap());
        discard_download_session(&session).await.unwrap();
        let state = session.state.lock().await;
        assert!(state.partial.is_none());
        assert!(state.deadline.is_none());
        drop(state);
        server.await.unwrap();
    }

    fn request_for(
        url: &str,
        destination: PathBuf,
        max_bytes: u64,
        max_duration: Duration,
    ) -> DownloadRequest {
        DownloadRequest {
            url: url.to_owned(),
            destination,
            expected_sha256: None,
            max_retries: 0,
            bytes_per_second: None,
            max_bytes,
            max_duration,
        }
    }

    async fn finalization_fixture(
        directory: &Path,
        expected_sha256: Option<String>,
    ) -> (DownloadSessionState, DownloadRequest, u64) {
        let destination = directory.join("final.bin");
        let partial_path = directory.join("staging.bin");
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&partial_path)
            .await
            .unwrap();
        file.write_all(b"abc").await.unwrap();
        let bytes = 3;
        let state = DownloadSessionState {
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            partial: Some(PartialDownload {
                file,
                path: partial_path,
                bytes,
                url_fingerprint: url_fingerprint("https://example.com/file"),
                validator: None,
                segmented: false,
                _staging: None,
            }),
            ..DownloadSessionState::default()
        };
        let mut request = request_for(
            "https://example.com/file",
            destination,
            1024,
            Duration::from_secs(60),
        );
        request.expected_sha256 = expected_sha256;
        (state, request, bytes)
    }

    #[cfg(unix)]
    async fn cleanup_blocked_state(
        directory: &Path,
    ) -> (DownloadSessionState, PathBuf, PathBuf, PathBuf) {
        let path = directory.join("partial.bin");
        let moved_file = directory.join("held-open.bin");
        tokio::fs::write(&path, b"partial").await.unwrap();
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        tokio::fs::rename(&path, &moved_file).await.unwrap();
        tokio::fs::create_dir(&path).await.unwrap();
        let blocker = path.join("child");
        tokio::fs::write(&blocker, b"block removal").await.unwrap();
        let state = DownloadSessionState {
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            partial: Some(PartialDownload {
                file,
                path: path.clone(),
                bytes: 7,
                url_fingerprint: "test-fingerprint".to_owned(),
                validator: None,
                segmented: false,
                _staging: None,
            }),
            committed_result: None,
        };
        (state, path, moved_file, blocker)
    }

    async fn start_server<F, Fut>(handler: F) -> (String, tokio::task::JoinHandle<()>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handler(stream).await;
        });
        (format!("http://{address}/file"), server)
    }

    async fn read_request(stream: &mut TcpStream) {
        let _ = read_request_text(stream).await;
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
}
