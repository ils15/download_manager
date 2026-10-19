use crate::url_policy::validate_http_url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const HOST_ID: &str = "org.independent.download_manager";

/// Age after which a claim marker is treated as crash residue.
///
/// A consumer that dies between creating the marker and releasing it leaves
/// the marker behind, and the item would stay unclaimable. Ten minutes is
/// generous enough to cover a suspended or stalled consumer and short enough
/// that a crashed consumer blocks one item only briefly. The recovery is
/// attempted once per item and per poll cycle, so two consumers cannot loop
/// against each other.
const CLAIM_MARKER_STALE_AFTER: Duration = Duration::from_secs(600);

/// Longest inbox item name that is mirrored verbatim in a claim-marker name.
///
/// Longer names are replaced by a digest so that the marker path stays
/// inside the fixed directory-entry budget of Windows. The digest form never
/// ends in `.json.claim`, so it cannot collide with the verbatim form.
const CLAIM_MARKER_NAME_LIMIT: usize = 64;

/// How many times the removal of an inbox path is retried while the operating
/// system still holds that path.
///
/// Windows closes the handle that blocks a removal outside the process, so a
/// bounded number of short pauses is enough. The retries never continue
/// forever: a failure that does not clear is reported after the last attempt.
const INBOX_CLEANUP_RETRIES: usize = 6;

/// Pause between two removal attempts of the same inbox path.
const INBOX_CLEANUP_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Upper bound for the whole removal of one inbox path.
///
/// The bound keeps a stuck filesystem from holding the caller for longer than
/// one inbox poll cycle.
const INBOX_CLEANUP_DEADLINE: Duration = Duration::from_secs(4);

#[derive(Debug, Error)]
pub enum NativeHostError {
    #[error("native message exceeds the 1 MiB limit")]
    MessageTooLarge,
    #[error("message must request one HTTP(S) URL")]
    InvalidMessage,
    #[error("application data directory is unavailable")]
    DataDirectory,
    #[error(transparent)]
    Url(#[from] crate::url_policy::UrlPolicyError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Native Messaging inbox claim {claim} failed: {cause}; {recovery}")]
    InboxClaim {
        claim: String,
        cause: String,
        recovery: String,
    },
    #[error("Native Messaging item was quarantined at {quarantine}: {reason}")]
    RejectedMessage {
        quarantine: String,
        reason: &'static str,
    },
    #[error("Native Messaging publication failed; temporary {temporary} is retained: {cause}")]
    PublishTemporary { temporary: String, cause: String },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IncomingMessage {
    action: String,
    url: String,
}

/// One validated inbox item held in a processing folder until acknowledged or restored.
/// Dropping the handle intentionally leaves the file recoverable in that folder.
#[derive(Debug)]
pub struct PendingUrlClaim {
    url: String,
    claim_dir: PathBuf,
    claimed_path: PathBuf,
    original_path: PathBuf,
}

impl PendingUrlClaim {
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Acknowledge only after the URL has been accepted by the download queue.
    pub fn acknowledge(self) -> Result<(), NativeHostError> {
        if let Err(error) = remove_inbox_file(&self.claimed_path) {
            return Err(NativeHostError::InboxClaim {
                claim: self.claim_dir.display().to_string(),
                cause: error.to_string(),
                recovery: format!(
                    "the queue accepted the URL, but the item remains claimed at {}; do not restore it while the queue is active",
                    self.claimed_path.display()
                ),
            });
        }
        if let Err(error) = remove_inbox_dir(&self.claim_dir) {
            return Err(NativeHostError::InboxClaim {
                claim: self.claim_dir.display().to_string(),
                cause: error.to_string(),
                recovery: format!(
                    "the queue accepted the URL, but the empty claim folder remains at {}; it can be removed once every instance is closed",
                    self.claim_dir.display()
                ),
            });
        }
        Ok(())
    }

    /// Return a rejected item to the inbox. A failed restore retains its claim file.
    pub fn restore(self, cause: impl Into<String>) -> Result<(), NativeHostError> {
        restore_claim_paths(&self.claim_dir, &self.claimed_path, &self.original_path).map_err(
            |recovery| NativeHostError::InboxClaim {
                claim: self.claim_dir.display().to_string(),
                cause: cause.into(),
                recovery,
            },
        )
    }
}

/// Reports whether a filesystem failure is a transient Windows condition.
///
/// Windows reports a path that another handle still holds as
/// `ERROR_ACCESS_DENIED` (5), `ERROR_SHARING_VIOLATION` (32) or
/// `ERROR_LOCK_VIOLATION` (33), and a directory whose child entry is still
/// delete-pending as `ERROR_DIR_NOT_EMPTY` (145). Every one of them clears on
/// its own once the blocking handle is closed.
///
/// The raw codes are Windows specific, so a Unix failure never matches them:
/// the Unix behaviour stays exactly as it was and a genuine Unix failure is
/// reported without a retry.
fn is_transient_windows_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(5) | Some(32) | Some(33) | Some(145)
    )
}

/// Reports whether a removed path is really gone.
///
/// A Windows path whose last handle is still closing answers with a
/// permission error instead of "not found", so that answer counts as gone:
/// the operating system already accepted the removal.
fn removal_is_confirmed(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) if is_transient_windows_error(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

/// Removes one inbox path, retrying while the operating system still holds it.
///
/// The removal is idempotent: a path that is already gone is reported as
/// `Ok(false)` instead of an error, so a repeated cleanup never fails.
/// `Ok(true)` means that this call removed the path.
///
/// A removal is only reported as complete once the path is confirmed gone. A
/// failure that does not clear is reported after a bounded number of attempts
/// and after the total time bound, so the caller is never blocked forever.
///
/// `remove` and `pause` are injected so that the retry can be exercised
/// deterministically, without depending on the operating system and without a
/// real pause.
fn remove_inbox_path_with(
    path: &Path,
    mut remove: impl FnMut(&Path) -> io::Result<()>,
    mut pause: impl FnMut(Duration),
) -> io::Result<bool> {
    let deadline = Instant::now() + INBOX_CLEANUP_DEADLINE;
    let mut attempts = 0usize;
    let mut last_error = None;
    loop {
        match remove(path) {
            // A removal only counts as complete once the path is gone.
            Ok(()) => {
                if removal_is_confirmed(path)? {
                    return Ok(true);
                }
            }
            // The path is already gone, so the removal is complete.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            // A genuine failure, such as a real permission problem or an
            // invalid path, is reported without a retry.
            Err(error) if !is_transient_windows_error(&error) => return Err(error),
            Err(error) => last_error = Some(error),
        }
        attempts += 1;
        if attempts > INBOX_CLEANUP_RETRIES || Instant::now() >= deadline {
            let error = last_error.unwrap_or_else(|| {
                io::Error::other(format!("the path was not removed: {}", path.display()))
            });
            return Err(error);
        }
        pause(INBOX_CLEANUP_RETRY_DELAY);
    }
}

/// Removes one inbox file, retrying while the operating system still holds it.
fn remove_inbox_file(path: &Path) -> io::Result<bool> {
    remove_inbox_path_with(path, remove_file_entry, std::thread::sleep)
}

/// Removes one empty inbox directory, retrying while the operating system
/// still holds it.
fn remove_inbox_dir(path: &Path) -> io::Result<bool> {
    remove_inbox_path_with(path, remove_dir_entry, std::thread::sleep)
}

/// `fs::remove_file` with the argument type that the retry helper expects.
fn remove_file_entry(path: &Path) -> io::Result<()> {
    fs::remove_file(path)
}

/// `fs::remove_dir` with the argument type that the retry helper expects.
fn remove_dir_entry(path: &Path) -> io::Result<()> {
    fs::remove_dir(path)
}

#[derive(Debug, Serialize)]
struct HostReply {
    accepted: bool,
}

/// Process one Chrome Native Messaging frame. The extension sends only a URL
/// after the user clicks its explicit "Send this tab" button.
pub fn run_stdio() -> Result<(), NativeHostError> {
    let mut stdin = io::stdin().lock();
    let Some(message) = read_frame(&mut stdin)? else {
        return Ok(());
    };
    let data_dir = application_data_dir().ok_or(NativeHostError::DataDirectory)?;
    let url = accept_message(&data_dir, &message)?;
    write_frame(
        &mut io::stdout().lock(),
        &HostReply {
            accepted: !url.is_empty(),
        },
    )?;
    Ok(())
}

pub fn accept_message(data_dir: &Path, bytes: &[u8]) -> Result<String, NativeHostError> {
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(NativeHostError::MessageTooLarge);
    }
    let mut message: IncomingMessage =
        serde_json::from_slice(bytes).map_err(|_| NativeHostError::InvalidMessage)?;
    if message.action != "download" {
        return Err(NativeHostError::InvalidMessage);
    }
    let url = validate_http_url(&message.url)?.to_string();
    message.url = url.clone();
    let inbox = data_dir.join("incoming");
    fs::create_dir_all(&inbox)?;
    publish_inbox_message(&inbox, &message)?;
    Ok(url)
}

fn publish_inbox_message(inbox: &Path, message: &IncomingMessage) -> Result<(), NativeHostError> {
    for _ in 0..16 {
        let id = Uuid::new_v4();
        let temporary = inbox.join(format!("{id}.tmp"));
        let published = inbox.join(format!("{id}.json"));
        if published.exists() {
            continue;
        }
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        };
        let write_result = (|| -> Result<(), NativeHostError> {
            serde_json::to_writer(&mut file, message)?;
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = write_result {
            return Err(NativeHostError::PublishTemporary {
                temporary: temporary.display().to_string(),
                cause: error.to_string(),
            });
        }
        if let Err(error) = fs::rename(&temporary, &published) {
            return Err(NativeHostError::PublishTemporary {
                temporary: temporary.display().to_string(),
                cause: error.to_string(),
            });
        }
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique Native Messaging inbox temporary",
    )
    .into())
}

pub fn claim_next_pending_url(data_dir: &Path) -> Result<Option<PendingUrlClaim>, NativeHostError> {
    claim_next_pending_url_with(data_dir, |from, to| fs::rename(from, to), |_| {})
}

/// Claim the next inbox item with an exclusive claim marker.
///
/// The claim marker decides which consumer owns an inbox item. It is created
/// atomically, so exactly one consumer can hold it even when several
/// consumers race. A same-volume rename cannot decide that on Windows, where
/// `MoveFileExW` lets two concurrent consumers move the same source, which
/// queues the same URL twice.
///
/// `rename` is injected so the losing side of the race can be reproduced
/// deterministically in tests; production always uses the standard library
/// rename.
///
/// `before_release` runs once the item is in the claim folder and before the
/// claim marker is released. It is injected so that a test can let another
/// consumer take the claim over, which is the only way to reach the path
/// where this consumer does not own the item any more; production does
/// nothing.
fn claim_next_pending_url_with(
    data_dir: &Path,
    rename: impl Fn(&Path, &Path) -> io::Result<()>,
    before_release: impl Fn(&Path),
) -> Result<Option<PendingUrlClaim>, NativeHostError> {
    let inbox = data_dir.join("incoming");
    let entries = match fs::read_dir(&inbox) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    for path in paths {
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        // Only the consumer that creates the claim marker may move the item.
        // A marker that another consumer holds is skipped without an error.
        let Some(marker) = acquire_claim_marker(&inbox, &path)? else {
            continue;
        };

        // Creating a unique directory reserves a no-clobber claim name.
        let claim_dir = match create_claim_directory(&inbox) {
            Ok(claim_dir) => claim_dir,
            Err(error) => {
                let _ = release_claim_marker(&marker);
                return Err(error.into());
            }
        };
        let claimed_path = claim_dir.join("message.json");
        if let Err(rename_error) = rename(&path, &claimed_path) {
            // The item was not moved. The claim marker is released so that
            // the item stays claimable, and the source is inspected exactly
            // once: a source that is still there means the failure was
            // genuine (a real permission problem or a persistent sharing
            // violation) and is propagated with the item left untouched in
            // the inbox. A source that can no longer be inspected proves
            // that another consumer won the race, so the item is skipped.
            let mut recovery = Vec::new();
            if let Err(cleanup_error) = remove_inbox_dir(&claim_dir) {
                let detail = format!("claim folder cleanup failed: {cleanup_error}");
                recovery.push(detail);
            }
            if let Err(release_error) = release_claim_marker(&marker) {
                let detail = format!("claim marker release failed: {release_error}");
                recovery.push(detail);
            }
            let taken_by_other_consumer = source_is_no_longer_claimable(&path);
            if taken_by_other_consumer && recovery.is_empty() {
                continue;
            }
            let cause = rename_error.to_string();
            if !taken_by_other_consumer && recovery.is_empty() {
                return Err(rename_error.into());
            }
            let details = recovery.join("; ");
            let claim_note = "the item was claimed by another consumer";
            let recovery = if taken_by_other_consumer {
                if details.is_empty() {
                    claim_note.to_owned()
                } else {
                    format!("{claim_note}; {details}")
                }
            } else {
                details
            };
            return Err(NativeHostError::InboxClaim {
                claim: claim_dir.display().to_string(),
                cause,
                recovery,
            });
        }

        // The item is in the claim folder. Releasing the marker also proves
        // that this consumer still owns the claim: a consumer that recovered
        // a stale marker may have taken the item in the meantime, and this
        // copy must not be queued a second time.
        before_release(&marker.path);
        match release_claim_marker(&marker) {
            Ok(true) => {}
            Ok(false) => {
                // Another consumer owns the claim now. The copy in the claim
                // folder is the only copy of this URL, because this consumer
                // moved the item out of the inbox, so the copy is returned to
                // the inbox instead of being left behind: a later poll claims
                // it again and the URL is not lost.
                let recovery = match restore_unqueued_claim(&claim_dir, &claimed_path, &path) {
                    Ok(()) => format!(
                        "the copy was returned to the inbox at {} and a later poll claims it again",
                        path.display()
                    ),
                    Err(recovery) => recovery,
                };
                let cause = "the inbox item was claimed by another consumer";
                return Err(NativeHostError::InboxClaim {
                    claim: claim_dir.display().to_string(),
                    cause: cause.to_owned(),
                    recovery,
                });
            }
            Err(error) => {
                // The marker could not be read or removed, so this consumer
                // cannot prove that it owns the claim. The copy is the only
                // copy of this URL, so it is returned to the inbox for the
                // same reason. The marker may still be held, and a later
                // poll claims the item once the marker is released or has
                // become stale.
                let recovery = match restore_unqueued_claim(&claim_dir, &claimed_path, &path) {
                    Ok(()) => format!(
                        "the copy was returned to the inbox at {} and a later poll claims it again",
                        path.display()
                    ),
                    Err(recovery) => recovery,
                };
                return Err(NativeHostError::InboxClaim {
                    claim: claim_dir.display().to_string(),
                    cause: error.to_string(),
                    recovery,
                });
            }
        }

        let metadata = match fs::symlink_metadata(&claimed_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                return Err(claim_processing_failure(
                    &claim_dir,
                    &claimed_path,
                    &path,
                    error,
                ));
            }
        };
        let invalid_reason = if !metadata.file_type().is_file() {
            Some("inbox item is not a regular file")
        } else if metadata.len() > MAX_MESSAGE_BYTES as u64 {
            Some("inbox item exceeds the message size limit")
        } else {
            None
        };
        if let Some(reason) = invalid_reason {
            let quarantine = quarantine_claim(data_dir, &claim_dir, &claimed_path)?;
            return Err(NativeHostError::RejectedMessage {
                quarantine: quarantine.display().to_string(),
                reason,
            });
        }

        let bytes = match fs::read(&claimed_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(claim_processing_failure(
                    &claim_dir,
                    &claimed_path,
                    &path,
                    error,
                ));
            }
        };
        let message = match serde_json::from_slice::<IncomingMessage>(&bytes) {
            Ok(message) => message,
            Err(_) => {
                let quarantine = quarantine_claim(data_dir, &claim_dir, &claimed_path)?;
                return Err(NativeHostError::RejectedMessage {
                    quarantine: quarantine.display().to_string(),
                    reason: "inbox item has malformed JSON or schema",
                });
            }
        };
        if message.action != "download" {
            let quarantine = quarantine_claim(data_dir, &claim_dir, &claimed_path)?;
            return Err(NativeHostError::RejectedMessage {
                quarantine: quarantine.display().to_string(),
                reason: "inbox item requested an unsupported action",
            });
        }
        let url = match validate_http_url(&message.url) {
            Ok(url) => url.to_string(),
            Err(_) => {
                let quarantine = quarantine_claim(data_dir, &claim_dir, &claimed_path)?;
                return Err(NativeHostError::RejectedMessage {
                    quarantine: quarantine.display().to_string(),
                    reason: "inbox URL failed policy validation",
                });
            }
        };
        return Ok(Some(PendingUrlClaim {
            url,
            claim_dir,
            claimed_path,
            original_path: path,
        }));
    }
    Ok(None)
}

/// Report whether the source item is no longer a claimable inbox file.
///
/// The rename already failed, so the source is inspected exactly once. A source
/// that can no longer be inspected is not claimable any more: POSIX reports
/// NotFound once the winning consumer moved the file away, and Windows reports
/// ERROR_ACCESS_DENIED while the winning consumer holds that file open for the
/// same rename (STATUS_DELETE_PENDING). A source that can still be inspected is
/// a live inbox item, so the failure was genuine and has to be propagated with
/// the item left untouched in the inbox.
fn source_is_no_longer_claimable(source: &Path) -> bool {
    match fs::symlink_metadata(source) {
        Ok(_) => false,
        Err(error) => matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
        ),
    }
}

/// Exclusive claim marker of one inbox item.
///
/// The marker name is derived from the item name, so every consumer computes
/// the same path without extra coordination. Its content is a per-claim
/// token: the marker is removed only while it still carries that token, so
/// a consumer never releases a marker that another consumer has taken.
struct ClaimMarker {
    path: PathBuf,
    token: String,
}

/// Acquire the exclusive claim marker of one inbox item.
///
/// `Ok(None)` means that another consumer holds the item, so the caller skips
/// the item without reporting an error.
fn acquire_claim_marker(inbox: &Path, item: &Path) -> io::Result<Option<ClaimMarker>> {
    let marker_path = claim_marker_path(inbox, item);
    match create_claim_marker(&marker_path) {
        Ok(marker) => return Ok(Some(marker)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) if is_transient_windows_error(&error) => {
            // Windows reports a marker that another consumer is creating or
            // releasing right now as ERROR_ACCESS_DENIED instead of
            // ERROR_FILE_EXISTS. The item belongs to that consumer, so it is
            // skipped and claimed by a later poll cycle instead of being
            // reported as a failure.
            return Ok(None);
        }
        Err(error) => return Err(error),
    }
    // Crash residue: the previous owner died without releasing the
    // marker. The recovery runs once per item and per poll cycle, so
    // two consumers cannot loop against each other.
    if !claim_marker_is_stale(&marker_path)? {
        return Ok(None);
    }
    match remove_inbox_file(&marker_path) {
        Ok(true) => {}
        // The marker is already gone, so the item is left to the consumer
        // that claims it next.
        Ok(false) => return Ok(None),
        Err(error) => return Err(error),
    }
    match create_claim_marker(&marker_path) {
        Ok(marker) => Ok(Some(marker)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(None),
        Err(error) if is_transient_windows_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Create the claim marker of one inbox item.
///
/// `create_new` maps to `O_CREAT|O_EXCL` on Unix and to `CREATE_NEW` on
/// Windows, so exactly one consumer can create the marker.
fn create_claim_marker(marker_path: &Path) -> io::Result<ClaimMarker> {
    let token = Uuid::new_v4().simple().to_string();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(marker_path)?;
    let write_result = file.write_all(token.as_bytes());
    drop(file);
    if let Err(error) = write_result {
        let _ = remove_inbox_file(marker_path);
        return Err(error);
    }
    Ok(ClaimMarker {
        path: marker_path.to_path_buf(),
        token,
    })
}

/// Report whether a claim marker is old enough to be treated as crash residue.
///
/// A marker whose modification time is unknown or in the future is never
/// treated as stale: breaking the claim of a live consumer would queue the
/// same URL twice.
fn claim_marker_is_stale(marker_path: &Path) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) if is_transient_windows_error(&error) => {
            // Windows reports a marker that another consumer is releasing
            // right now as ERROR_ACCESS_DENIED. The marker is not crash
            // residue, so the item still belongs to that consumer.
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let Ok(modified) = metadata.modified() else {
        return Ok(false);
    };
    let Ok(age) = SystemTime::now().duration_since(modified) else {
        return Ok(false);
    };
    Ok(age > CLAIM_MARKER_STALE_AFTER)
}

/// Release the claim marker, but only while it still carries this claim's
/// token.
///
/// `Ok(false)` means that the marker is gone or belongs to another claim, so
/// this consumer does not own the item any more.
///
/// A transient Windows condition, such as a handle that another process still
/// holds, is retried within a bounded window: a claim that cannot be
/// released because of a busy file would leave the item unqueued, and the
/// retry only covers conditions that clear on their own.
fn release_claim_marker(marker: &ClaimMarker) -> io::Result<bool> {
    let deadline = Instant::now() + INBOX_CLEANUP_DEADLINE;
    let mut attempts = 0usize;
    loop {
        match release_claim_marker_once(marker) {
            Ok(released) => return Ok(released),
            Err(error) if !is_transient_windows_error(&error) => return Err(error),
            Err(error) => {
                attempts += 1;
                if attempts > INBOX_CLEANUP_RETRIES || Instant::now() >= deadline {
                    return Err(error);
                }
                std::thread::sleep(INBOX_CLEANUP_RETRY_DELAY);
            }
        }
    }
}

/// Release the claim marker once, without retrying a transient condition.
fn release_claim_marker_once(marker: &ClaimMarker) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(&marker.path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() {
        return Ok(false);
    }
    let content = match fs::read(&marker.path) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if content != marker.token.as_bytes() {
        return Ok(false);
    }
    // A marker that is already gone is not owned by this consumer any more.
    remove_inbox_file(&marker.path)
}

/// Derive the deterministic claim-marker path of one inbox item.
fn claim_marker_path(inbox: &Path, item: &Path) -> PathBuf {
    let Some(name) = item.file_name() else {
        return inbox.join(".claim-unnamed.claim");
    };
    if name.len() <= CLAIM_MARKER_NAME_LIMIT {
        return inbox.join(format!(".{}.claim", name.to_string_lossy()));
    }
    inbox.join(format!(".claim-{}.claim", claim_marker_digest(name)))
}

/// Short digest of an item name, used to bound the marker path length.
fn claim_marker_digest(name: &OsStr) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(os_string_bytes(name));
    let mut encoded = String::with_capacity(32);
    for byte in digest.iter().take(16) {
        encoded.push(HEX[usize::from(*byte >> 4)] as char);
        encoded.push(HEX[usize::from(*byte & 0x0f)] as char);
    }
    encoded
}

/// Raw bytes of an OS string, so that a digest covers the exact name.
#[cfg(unix)]
fn os_string_bytes(value: &OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn os_string_bytes(value: &OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    let mut bytes = Vec::new();
    for unit in value.encode_wide() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

#[cfg(not(any(unix, windows)))]
fn os_string_bytes(value: &OsStr) -> Vec<u8> {
    value.to_string_lossy().into_owned().into_bytes()
}

fn create_claim_directory(inbox: &Path) -> io::Result<PathBuf> {
    create_unique_directory(inbox, ".processing")
}

fn create_unique_directory(parent: &Path, prefix: &str) -> io::Result<PathBuf> {
    for _ in 0..16 {
        let claim = parent.join(format!("{prefix}-{}", Uuid::new_v4()));
        match fs::create_dir(&claim) {
            Ok(()) => return Ok(claim),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique Native Messaging inbox claim",
    ))
}

fn quarantine_claim(
    data_dir: &Path,
    claim_dir: &Path,
    claimed_path: &Path,
) -> Result<PathBuf, NativeHostError> {
    let rejected_dir = data_dir.join("rejected");
    if let Err(error) = fs::create_dir_all(&rejected_dir) {
        return Err(NativeHostError::InboxClaim {
            claim: claim_dir.display().to_string(),
            cause: error.to_string(),
            recovery: format!(
                "rejected item remains recoverable at {}; retry quarantine or inspect it manually",
                claimed_path.display()
            ),
        });
    }
    let quarantine_dir = create_unique_directory(&rejected_dir, "rejected").map_err(|error| {
        NativeHostError::InboxClaim {
            claim: claim_dir.display().to_string(),
            cause: error.to_string(),
            recovery: format!(
                "rejected item remains recoverable at {}; retry quarantine or inspect it manually",
                claimed_path.display()
            ),
        }
    })?;
    let quarantine_path = quarantine_dir.join("message.json");
    if let Err(error) = fs::rename(claimed_path, &quarantine_path) {
        let _ = remove_inbox_dir(&quarantine_dir);
        return Err(NativeHostError::InboxClaim {
            claim: claim_dir.display().to_string(),
            cause: error.to_string(),
            recovery: format!(
                "rejected item remains recoverable at {}; retry quarantine or inspect it manually",
                claimed_path.display()
            ),
        });
    }
    if let Err(error) = remove_inbox_dir(claim_dir) {
        eprintln!(
            "Native Messaging quarantined an invalid item at {} but could not remove empty claim folder {}: {error}",
            quarantine_path.display(),
            claim_dir.display()
        );
    }
    Ok(quarantine_path)
}

fn claim_processing_failure(
    claim_dir: &Path,
    claimed_path: &Path,
    original_path: &Path,
    cause: io::Error,
) -> NativeHostError {
    let recovery = match restore_claim_paths(claim_dir, claimed_path, original_path) {
        Ok(()) => "the inbox item was restored for a later retry".to_owned(),
        Err(recovery) => recovery,
    };
    NativeHostError::InboxClaim {
        claim: claim_dir.display().to_string(),
        cause: cause.to_string(),
        recovery,
    }
}

fn restore_claim_paths(
    claim_dir: &Path,
    claimed_path: &Path,
    original_path: &Path,
) -> Result<(), String> {
    fs::rename(claimed_path, original_path).map_err(|error| {
        format!(
            "the item remains claimed at {}; after stopping all app instances, move it to a new *.json name under incoming to retry (do not copy it): {error}",
            claimed_path.display()
        )
    })?;
    match remove_inbox_dir(claim_dir) {
        Ok(_) => Ok(()),
        Err(error) => Err(format!(
            "the item was restored to incoming, but the empty claim folder remains at {}: {error}",
            claim_dir.display()
        )),
    }
}

/// Return a copy that this consumer did not queue to the inbox.
///
/// The item was moved out of the inbox by this consumer, so the copy in the
/// claim folder is the only copy of that URL: a consumer that took the claim
/// marker over cannot queue the item, because the item is no longer in the
/// inbox. Leaving the copy behind would lose the URL for good, so it is
/// returned to the inbox under its original name and a later poll claims it
/// again.
///
/// The return never overwrites an inbox item: the name is taken with an
/// atomic link, and a name that another consumer took first leaves this copy
/// recoverable in the claim folder. The item therefore exists exactly once,
/// so no URL is queued twice.
fn restore_unqueued_claim(
    claim_dir: &Path,
    claimed_path: &Path,
    original_path: &Path,
) -> Result<(), String> {
    match fs::hard_link(claimed_path, original_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(format!(
                "the item is already back in the inbox at {}, so this copy stays recoverable at {}: {error}",
                original_path.display(),
                claimed_path.display()
            ));
        }
        Err(error) => {
            // A filesystem without hard links cannot take the name
            // atomically, so the copy is renamed back instead. The
            // destination is checked first, so a copy that another consumer
            // returned first is not overwritten.
            if original_path.exists() {
                return Err(format!(
                    "the item is already back in the inbox at {}, so this copy stays recoverable at {}: {error}",
                    original_path.display(),
                    claimed_path.display()
                ));
            }
            if let Err(rename_error) = fs::rename(claimed_path, original_path) {
                return Err(format!(
                    "the item remains claimed at {}; after stopping all app instances, move it to a new *.json name under incoming to retry (do not copy it): {rename_error}",
                    claimed_path.display()
                ));
            }
            return remove_empty_claim_dir(claim_dir);
        }
    }
    // The inbox holds the item again. The copy in the claim folder is
    // removed, so that the item exists exactly once.
    if let Err(error) = remove_inbox_file(claimed_path) {
        return Err(format!(
            "the item was returned to the inbox at {}, but this copy also remains at {}: {error}",
            original_path.display(),
            claimed_path.display()
        ));
    }
    remove_empty_claim_dir(claim_dir)
}

/// Remove the claim folder of a copy that was returned to the inbox.
fn remove_empty_claim_dir(claim_dir: &Path) -> Result<(), String> {
    match remove_inbox_dir(claim_dir) {
        Ok(_) => Ok(()),
        Err(error) => Err(format!(
            "the item was returned to the inbox, but the empty claim folder remains at {}: {error}",
            claim_dir.display()
        )),
    }
}

pub fn application_data_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(|root| PathBuf::from(root).join("IndependentDownloadManager"))
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
            })
            .map(|root| root.join("independent-download-manager"))
    }
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, NativeHostError> {
    let mut first = [0u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    let mut rest = [0u8; 3];
    reader.read_exact(&mut rest)?;
    let length = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]) as usize;
    if length > MAX_MESSAGE_BYTES {
        return Err(NativeHostError::MessageTooLarge);
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

fn write_frame(writer: &mut impl Write, message: &impl Serialize) -> Result<(), NativeHostError> {
    let payload = serde_json::to_vec(message)?;
    if payload.len() > MAX_MESSAGE_BYTES {
        return Err(NativeHostError::MessageTooLarge);
    }
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;
    Ok(())
}

pub fn host_id() -> &'static str {
    HOST_ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn accepts_only_explicit_download_messages_allowed_by_url_policy() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            accept_message(
                directory.path(),
                br#"{"action":"download","url":"https://example.com/file#fragment"}"#
            )
            .unwrap(),
            "https://example.com/file"
        );
        assert!(accept_message(
            directory.path(),
            br#"{"action":"download","url":"file:///secret"}"#
        )
        .is_err());
        assert!(accept_message(
            directory.path(),
            br#"{"action":"download","url":"https://u:p@example.com/"}"#
        )
        .is_err());
        assert!(accept_message(
            directory.path(),
            br#"{"action":"download","url":"https://example.com/","cookies":"x"}"#
        )
        .is_err());
        let pending = claim_next_pending_url(directory.path()).unwrap().unwrap();
        assert_eq!(pending.url(), "https://example.com/file");
        pending.acknowledge().unwrap();
        assert!(claim_next_pending_url(directory.path()).unwrap().is_none());
    }

    #[test]
    fn incomplete_temporary_is_ignored_and_published_message_is_claimed() {
        let directory = tempfile::tempdir().unwrap();
        let inbox = directory.path().join("incoming");
        fs::create_dir_all(&inbox).unwrap();
        let interrupted = inbox.join("interrupted.tmp");
        fs::write(&interrupted, br#"{"action":"download","url":"https://exa""#).unwrap();

        assert!(claim_next_pending_url(directory.path()).unwrap().is_none());
        assert!(interrupted.exists());

        accept_message(
            directory.path(),
            br#"{"action":"download","url":"https://example.com/published"}"#,
        )
        .unwrap();
        let published_count = fs::read_dir(&inbox)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .count();
        assert_eq!(published_count, 1);

        let claim = claim_next_pending_url(directory.path()).unwrap().unwrap();
        assert_eq!(claim.url(), "https://example.com/published");
        claim.acknowledge().unwrap();
        assert!(interrupted.exists());
    }

    #[test]
    fn malformed_and_policy_rejected_items_are_quarantined_without_logging_contents() {
        let directory = tempfile::tempdir().unwrap();
        let inbox = directory.path().join("incoming");
        fs::create_dir_all(&inbox).unwrap();
        let secret = "private-token-value";
        let malformed = b"{not valid JSON; private-token-value";
        let rejected_url =
            format!(r#"{{"action":"download","url":"https://example.com/file?token={secret}"}}"#);
        fs::write(inbox.join("001.json"), malformed).unwrap();
        fs::write(
            inbox.join("002-private-token-value.json"),
            rejected_url.as_bytes(),
        )
        .unwrap();

        for expected_reason in [
            "inbox item has malformed JSON or schema",
            "inbox URL failed policy validation",
        ] {
            let error = claim_next_pending_url(directory.path()).unwrap_err();
            let diagnostic = error.to_string();
            assert!(!diagnostic.contains(secret));
            assert!(!diagnostic.contains("002-private-token-value"));
            let NativeHostError::RejectedMessage { quarantine, reason } = error else {
                panic!("invalid inbox item should be quarantined with a diagnostic");
            };
            assert_eq!(reason, expected_reason);
            let expected_bytes = if expected_reason.starts_with("inbox item") {
                &malformed[..]
            } else {
                rejected_url.as_bytes()
            };
            assert_eq!(fs::read(quarantine).unwrap().as_slice(), expected_bytes);
        }

        let tampered = br#"{
            "action":"download",
            "url":"https://example.com/file",
            "cookies":"private-token-value"
        }"#;
        fs::write(inbox.join("003-private-token-value.json"), tampered).unwrap();
        let error = claim_next_pending_url(directory.path()).unwrap_err();
        let diagnostic = error.to_string();
        assert!(!diagnostic.contains(secret));
        assert!(!diagnostic.contains("003-private-token-value"));
        let NativeHostError::RejectedMessage { quarantine, reason } = error else {
            panic!("tampered inbox schema should be quarantined with a diagnostic");
        };
        assert_eq!(reason, "inbox item has malformed JSON or schema");
        assert_eq!(fs::read(quarantine).unwrap().as_slice(), tampered);
        assert!(claim_next_pending_url(directory.path()).unwrap().is_none());
        assert_eq!(fs::read_dir(&inbox).unwrap().count(), 0);
    }

    #[test]
    fn failed_claim_processing_restores_item_and_reports_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let inbox = directory.path().join("incoming");
        fs::create_dir_all(&inbox).unwrap();
        let original = inbox.join("request.json");
        let claim_dir = inbox.join(".processing-test");
        fs::create_dir(&claim_dir).unwrap();
        let claimed = claim_dir.join("message.json");
        let contents = br#"{"action":"download","url":"https://example.com/file"}"#;
        fs::write(&claimed, contents).unwrap();

        let error = claim_processing_failure(
            &claim_dir,
            &claimed,
            &original,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated processing failure",
            ),
        );

        assert!(matches!(&error, NativeHostError::InboxClaim { .. }));
        assert!(error.to_string().contains("restored for a later retry"));
        assert_eq!(fs::read(original).unwrap(), contents);
        assert!(!claim_dir.exists());
    }

    #[test]
    fn a_consumer_that_lost_the_rename_race_reports_nothing_and_keeps_the_item_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/raced"}"#,
        )
        .unwrap();

        // Another consumer takes the item first, so the rename fails the way
        // Windows reports it (ERROR_ACCESS_DENIED) instead of the POSIX
        // NotFound. The exclusive claim marker prevents a concurrent second
        // rename; this reproduces the remaining case, where the item is gone
        // by the time the rename runs.
        let winner_claim = directory.path().join("winner-claim");
        fs::create_dir_all(&winner_claim).unwrap();
        let winner_target = winner_claim.join("message.json");
        let rename_target = winner_target.clone();
        let rename = move |from: &Path, _to: &Path| -> io::Result<()> {
            fs::rename(from, &rename_target)?;
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated ERROR_ACCESS_DENIED",
            ))
        };

        let claim = claim_next_pending_url_with(&data_dir, rename, |_| {}).unwrap();
        assert!(claim.is_none());
        // The item stays recoverable in the winning consumer's claim folder,
        // and the losing consumer left no empty claim folder behind.
        assert!(winner_target.exists());
        assert_eq!(processing_claim_folder_count(&data_dir), 0);
    }

    /// A consumer that lost the claim returns its copy to the inbox.
    ///
    /// The claim marker decides which consumer owns an item. When another
    /// consumer takes the marker over, the copy that this consumer holds is
    /// the only copy of that URL, because this consumer moved the item out
    /// of the inbox. A stranded copy would lose the URL for good, so the copy
    /// goes back to the inbox and a later poll claims it again, exactly once.
    #[test]
    fn a_consumer_that_lost_the_claim_returns_the_item_to_the_inbox() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/lost"}"#,
        )
        .unwrap();
        let inbox = data_dir.join("incoming");
        let original = inbox_item_path(&data_dir);
        let marker = claim_marker_path(&inbox, &original);

        // Another consumer takes the claim over while this consumer holds
        // the copy: the marker is replaced by one that carries another
        // token, which is what the recovery of a stale marker does when it
        // removes a marker that a live consumer owns.
        let steal_marker = |marker_path: &Path| {
            fs::remove_file(marker_path).unwrap();
            fs::write(marker_path, b"another-claim-token").unwrap();
        };

        let error =
            claim_next_pending_url_with(&data_dir, |from, to| fs::rename(from, to), steal_marker)
                .unwrap_err();
        assert!(matches!(&error, NativeHostError::InboxClaim { .. }));
        assert!(
            error.to_string().contains("returned to the inbox"),
            "the failure does not report the restore: {error}"
        );

        // The copy is back in the inbox under its original name and no claim
        // folder is left behind, so the URL is not stranded.
        assert!(original.exists());
        assert_eq!(inbox_item_count(&data_dir), 1);
        assert_eq!(processing_claim_folder_count(&data_dir), 0);

        // The marker is still held by the consumer that took the claim over,
        // so the item waits for that consumer.
        assert!(claim_next_pending_url(&data_dir).unwrap().is_none());

        // The consumer that took the claim over releases the marker, and the
        // item is claimed exactly once.
        fs::remove_file(&marker).unwrap();
        let claim = claim_next_pending_url(&data_dir).unwrap().unwrap();
        assert_eq!(claim.url(), "https://example.com/lost");
        claim.acknowledge().unwrap();
        assert!(claim_next_pending_url(&data_dir).unwrap().is_none());
        assert_eq!(inbox_item_count(&data_dir), 0);
    }

    /// A copy that is returned to the inbox never overwrites another copy.
    ///
    /// The item exists exactly once: a copy that another consumer returned
    /// first keeps the name, and the copy of this consumer stays recoverable
    /// instead of replacing it.
    #[test]
    fn a_returned_copy_never_overwrites_an_item_that_is_already_back() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        let inbox = data_dir.join("incoming");
        fs::create_dir_all(&inbox).unwrap();
        let original = inbox.join("item.json");
        let contents = br#"{"action":"download","url":"https://example.com/kept"}"#;
        fs::write(&original, contents).unwrap();
        let claim_dir = inbox.join(".processing-test");
        fs::create_dir(&claim_dir).unwrap();
        let claimed = claim_dir.join("message.json");
        fs::write(&claimed, contents).unwrap();

        // The item is already back in the inbox, so the copy must not
        // replace it.
        let error = restore_unqueued_claim(&claim_dir, &claimed, &original).unwrap_err();
        assert!(error.contains("already back in the inbox"));
        assert_eq!(fs::read(&original).unwrap(), contents);
        assert!(claimed.exists());
    }

    #[test]
    fn a_claim_failure_with_the_item_still_in_the_inbox_propagates_the_error() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/kept"}"#,
        )
        .unwrap();

        // A real permission problem or a persistent sharing violation leaves
        // the item in the inbox, so it has to be reported instead of being
        // mistaken for a lost race.
        let rename = |_from: &Path, _to: &Path| -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated sharing violation",
            ))
        };

        let error = claim_next_pending_url_with(&data_dir, rename, |_| {}).unwrap_err();
        assert!(matches!(&error, NativeHostError::Io(_)));
        assert!(error.to_string().contains("simulated sharing violation"));
        // The item is untouched and remains recoverable in the inbox.
        assert_eq!(inbox_item_count(&data_dir), 1);
        assert_eq!(processing_claim_folder_count(&data_dir), 0);
    }

    #[cfg(unix)]
    #[test]
    fn an_inaccessible_inbox_reports_the_permission_error_instead_of_reporting_nothing() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        let inbox = data_dir.join("incoming");
        fs::create_dir_all(&inbox).unwrap();
        let message = br#"{"action":"download","url":"https://example.com/kept"}"#;
        fs::write(inbox.join("001.json"), message).unwrap();
        if fs::metadata(&inbox).unwrap().uid() == 0 {
            // Root bypasses directory permissions, so the failure cannot be
            // provoked here.
            return;
        }
        fs::set_permissions(&inbox, fs::Permissions::from_mode(0o500)).unwrap();

        let outcome = claim_next_pending_url(&data_dir);

        fs::set_permissions(&inbox, fs::Permissions::from_mode(0o700)).unwrap();
        let error = outcome.expect_err("an inaccessible inbox must not look empty");
        assert!(matches!(&error, NativeHostError::Io(_)));
        assert!(fs::read(inbox.join("001.json")).is_ok());
    }

    fn processing_claim_folder_count(data_dir: &Path) -> usize {
        let mut count = 0;
        let entries = fs::read_dir(data_dir.join("incoming")).unwrap();
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(".processing-") {
                count += 1;
            }
        }
        count
    }

    fn inbox_item_count(data_dir: &Path) -> usize {
        let mut count = 0;
        let entries = fs::read_dir(data_dir.join("incoming")).unwrap();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
                count += 1;
            }
        }
        count
    }

    #[test]
    fn unacknowledged_claim_remains_recoverable_until_explicit_restore() {
        let directory = tempfile::tempdir().unwrap();
        accept_message(
            directory.path(),
            br#"{"action":"download","url":"https://example.com/file"}"#,
        )
        .unwrap();

        let claim = claim_next_pending_url(directory.path()).unwrap().unwrap();
        assert_eq!(claim.url(), "https://example.com/file");
        assert!(claim.claimed_path.exists());
        assert!(claim_next_pending_url(directory.path()).unwrap().is_none());

        claim.restore("simulated queue rejection").unwrap();
        let retry = claim_next_pending_url(directory.path()).unwrap().unwrap();
        assert_eq!(retry.url(), "https://example.com/file");
        retry.acknowledge().unwrap();
    }

    #[test]
    fn native_frame_is_little_endian_and_enforces_the_size_cap() {
        let mut input = &b"\x02\x00\x00\x00{}"[..];
        assert_eq!(read_frame(&mut input).unwrap(), Some(b"{}".to_vec()));
        let oversized = (MAX_MESSAGE_BYTES as u32 + 1).to_le_bytes().to_vec();
        let mut oversized_input = oversized.as_slice();
        assert!(matches!(
            read_frame(&mut oversized_input),
            Err(NativeHostError::MessageTooLarge)
        ));
    }

    /// What one consumer achieved while draining the inbox.
    #[derive(Debug, Default)]
    struct ConsumerOutcome {
        /// The URLs the consumer queued, in the order they were claimed.
        claimed: Vec<String>,
        /// The diagnostics of the races this consumer lost.
        lost: Vec<String>,
    }

    /// Claim inbox items until the inbox offers nothing more.
    ///
    /// Losing a race is not a failure. The exclusive claim marker guarantees
    /// that only one consumer owns an item, so the consumer that loses the
    /// race must neither queue the URL nor stop polling: the other items are
    /// still claimable. A copy that could not be queued is returned to the
    /// inbox, and the caller accounts for it.
    fn drain_inbox(data_dir: &Path) -> ConsumerOutcome {
        let mut outcome = ConsumerOutcome::default();
        loop {
            match claim_next_pending_url(data_dir) {
                Ok(Some(claim)) => {
                    let url = claim.url().to_owned();
                    if let Err(error) = claim.acknowledge() {
                        panic!("the queue accepted {url}, the item stayed claimed: {error}");
                    }
                    outcome.claimed.push(url);
                }
                Ok(None) => break,
                Err(NativeHostError::InboxClaim {
                    claim,
                    cause,
                    recovery,
                }) => {
                    outcome.lost.push(format!("{claim}: {cause}; {recovery}"));
                }
                Err(error) => panic!("a consumer failed to claim an inbox item: {error}"),
            }
        }
        outcome
    }

    /// The entries that are left in the inbox, classified by kind.
    #[derive(Debug, Default)]
    struct InboxState {
        /// The items that were never claimed.
        items: Vec<String>,
        /// The claim markers that were never released.
        markers: Vec<String>,
        /// The claim folders of the consumers.
        claim_folders: Vec<String>,
        /// Every other entry.
        other: Vec<String>,
    }

    /// Classify the entries that are left in the inbox.
    fn inspect_inbox(data_dir: &Path) -> InboxState {
        let mut state = InboxState::default();
        for entry in fs::read_dir(data_dir.join("incoming")).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            if name.starts_with(".processing-") {
                state.claim_folders.push(name);
            } else if name.ends_with(".claim") {
                state.markers.push(name);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
                state.items.push(name);
            } else {
                state.other.push(name);
            }
        }
        state.items.sort();
        state.markers.sort();
        state.claim_folders.sort();
        state.other.sort();
        state
    }

    /// Read the URLs of the copies that the claim folders still hold.
    ///
    /// A copy that the operating system already removed cannot be read, and a
    /// copy that NTFS still reports as delete-pending answers with a
    /// permission error. Both mean that the folder holds no recoverable item.
    fn recoverable_copies(data_dir: &Path) -> Vec<String> {
        let inbox = data_dir.join("incoming");
        let mut urls = Vec::new();
        for folder in inspect_inbox(data_dir).claim_folders {
            let copy = inbox.join(folder.as_str()).join("message.json");
            let Ok(bytes) = fs::read(&copy) else {
                continue;
            };
            let Ok(message) = serde_json::from_slice::<IncomingMessage>(&bytes) else {
                continue;
            };
            urls.push(message.url);
        }
        urls
    }

    /// Read the URLs of the items that wait in the inbox.
    ///
    /// An item that a consumer could not queue is back in the inbox, where a
    /// later poll claims it again.
    fn inbox_item_urls(data_dir: &Path) -> Vec<String> {
        let mut urls = Vec::new();
        for item in inspect_inbox(data_dir).items {
            let Ok(bytes) = fs::read(data_dir.join("incoming").join(item.as_str())) else {
                continue;
            };
            let Ok(message) = serde_json::from_slice::<IncomingMessage>(&bytes) else {
                continue;
            };
            urls.push(message.url);
        }
        urls
    }

    /// The aggregate property of concurrent claiming.
    ///
    /// Every published item is accounted for exactly once: it is queued by
    /// exactly one consumer, or it waits in the inbox for a later poll. No
    /// item is ever queued twice, no item is lost, and no copy is left in a
    /// claim folder, where no consumer would own it any more.
    ///
    /// The race is not provoked on purpose: the property has to hold whether
    /// every consumer wins a distinct item or a consumer loses one.
    #[test]
    fn concurrent_consumers_claim_each_inbox_item_exactly_once() {
        const CONSUMERS: usize = 8;
        const ITEMS: usize = 12;

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        let mut published = Vec::new();
        for index in 0..ITEMS {
            let url = format!("https://example.com/i{index}");
            let message = format!(r#"{{"action":"download","url":"{url}"}}"#);
            accept_message(&data_dir, message.as_bytes()).unwrap();
            published.push(url);
        }

        let barrier = Arc::new(std::sync::Barrier::new(CONSUMERS));
        let consumers = (0..CONSUMERS)
            .map(|_| {
                let data_dir = data_dir.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    drain_inbox(&data_dir)
                })
            })
            .collect::<Vec<_>>();

        let mut queued = Vec::new();
        let mut lost = Vec::new();
        for consumer in consumers {
            let outcome = consumer.join().unwrap();
            queued.extend(outcome.claimed);
            lost.extend(outcome.lost);
        }
        // Only the first lost race is reported: every lost race has the same
        // shape, and the failure message stays readable.
        lost.truncate(1);

        let state = inspect_inbox(&data_dir);
        let recoverable = recoverable_copies(&data_dir);
        let waiting = inbox_item_urls(&data_dir);

        // No URL is queued twice: the queue never receives the same item from
        // two consumers.
        let mut distinct = queued.clone();
        distinct.sort();
        distinct.dedup();
        assert!(
            distinct.len() == queued.len(),
            "a URL was queued more than once: {queued:?}; lost race: {lost:?}"
        );

        // Every published item is accounted for exactly once: queued by one
        // consumer, or waiting in the inbox for a later poll.
        for url in &published {
            let claimed = queued.iter().filter(|other| *other == url).count();
            let waiting_here = waiting.iter().filter(|other| *other == url).count();
            assert!(
                claimed + waiting_here == 1,
                "the item {url} was queued {claimed} times and waits {waiting_here} times in the inbox"
            );
        }

        // No copy is stranded: a copy in a claim folder belongs to no
        // consumer, so its URL would be lost.
        assert!(
            recoverable.is_empty(),
            "the inbox kept copies that no consumer owns: {recoverable:?}"
        );

        // The inbox holds no unknown item: every item in the inbox is one of
        // the published items.
        for url in &waiting {
            assert!(
                published.contains(url),
                "the inbox kept an item that was never published: {url}"
            );
        }

        // The inbox keeps no claim marker: every marker was released.
        let markers = &state.markers;
        assert!(
            markers.is_empty(),
            "the inbox kept claim markers that were never released: {markers:?}"
        );
        let other = &state.other;
        assert!(
            other.is_empty(),
            "the inbox kept entries that are neither items nor markers: {other:?}"
        );
    }

    /// Replay the Windows double-claim race many times.
    ///
    /// `MoveFileExW` lets two concurrent consumers rename the same inbox item,
    /// so a single round exposes the race only by luck. The rounds make the
    /// double claim deterministic enough to catch a regression of the
    /// exclusive claim marker.
    ///
    /// The round asserts the property that matters and nothing else: the item
    /// is claimed at most once, so it is never queued twice. A consumer that
    /// loses the race for the item is an expected outcome: the copy is back
    /// in the inbox, where a later poll claims it again, and its URL is not
    /// queued twice. A failure does not depend on the order in which the
    /// consumers happened to run.
    #[cfg(windows)]
    #[test]
    fn concurrent_consumers_never_claim_one_item_twice_on_windows() {
        const ROUNDS: usize = 200;
        const CONSUMERS: usize = 8;

        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        let inbox = data_dir.join("incoming");
        fs::create_dir_all(&inbox).unwrap();

        let mut published = Vec::new();
        let mut queued = Vec::new();

        for round in 0..ROUNDS {
            let url = format!("https://example.com/raced-{round}");
            let message = format!(r#"{{"action":"download","url":"{url}"}}"#);
            let path = inbox.join(format!("round-{round}.json"));
            fs::write(&path, message.as_bytes()).unwrap();
            published.push(url.clone());

            let barrier = Arc::new(std::sync::Barrier::new(CONSUMERS));
            let consumers = (0..CONSUMERS)
                .map(|_| {
                    let data_dir = data_dir.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        drain_inbox(&data_dir)
                    })
                })
                .collect::<Vec<_>>();

            let mut claimed = Vec::new();
            for consumer in consumers {
                claimed.extend(consumer.join().unwrap().claimed);
            }

            // The item is claimed at most once, by at most one consumer. A
            // consumer that lost the race claims nothing: the copy is back
            // in the inbox, where a later poll claims it again.
            let claimed_here = claimed.iter().filter(|other| *other == &url).count();
            assert!(
                claimed_here <= 1,
                "round {round} claimed the item more than once: {claimed:?}"
            );
            queued.extend(claimed);

            // No copy is stranded in a claim folder: a copy that no consumer
            // owns would lose its URL.
            let recoverable = recoverable_copies(&data_dir);
            assert!(
                recoverable.is_empty(),
                "round {round} left a copy that no consumer owns: {recoverable:?}"
            );

            // A marker that survives belongs to an item that waits in the
            // inbox: the item is not lost, it waits for the marker to be
            // released or to become stale.
            let state = inspect_inbox(&data_dir);
            for marker in &state.markers {
                let item = marker
                    .strip_prefix('.')
                    .and_then(|name| name.strip_suffix(".claim"))
                    .unwrap_or_default();
                assert!(
                    state.items.iter().any(|waiting| waiting == item),
                    "round {round} kept the marker of an item that does not wait: {marker}"
                );
            }
            let other = &state.other;
            assert!(
                other.is_empty(),
                "round {round} kept entries that are neither items nor markers: {other:?}"
            );
        }

        // Every published item is accounted for exactly once: queued by one
        // consumer, or waiting in the inbox for a later poll. A copy of an
        // item that was queued would be a duplicate waiting to be queued
        // again.
        let waiting = inbox_item_urls(&data_dir);
        for url in &published {
            let claimed = queued.iter().filter(|other| *other == url).count();
            let waiting_here = waiting.iter().filter(|other| *other == url).count();
            assert!(
                claimed + waiting_here == 1,
                "the item {url} was queued {claimed} times and waits {waiting_here} times in the inbox"
            );
        }
        let recoverable = recoverable_copies(&data_dir);
        assert!(
            recoverable.is_empty(),
            "the inbox kept copies that no consumer owns: {recoverable:?}"
        );
    }

    #[test]
    fn a_stale_claim_marker_is_recovered_and_the_item_is_claimed() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/stale"}"#,
        )
        .unwrap();
        let inbox = data_dir.join("incoming");
        let item = inbox_item_path(&data_dir);
        let marker = claim_marker_path(&inbox, &item);
        // A consumer that died without releasing its claim marker.
        fs::write(&marker, b"crashed-consumer").unwrap();
        set_modified_time(
            &marker,
            SystemTime::now() - Duration::from_secs(CLAIM_MARKER_STALE_AFTER.as_secs() + 60),
        );

        let claim = claim_next_pending_url(&data_dir).unwrap().unwrap();
        assert_eq!(claim.url(), "https://example.com/stale");
        claim.acknowledge().unwrap();
        assert!(!marker.exists());
    }

    #[test]
    fn a_fresh_claim_marker_holds_the_item_for_its_owner() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/held"}"#,
        )
        .unwrap();
        let inbox = data_dir.join("incoming");
        let item = inbox_item_path(&data_dir);
        let marker = claim_marker_path(&inbox, &item);
        fs::write(&marker, b"live-consumer").unwrap();

        // The marker is fresh, so the item belongs to the consumer that holds
        // it and no other consumer may take it.
        assert!(claim_next_pending_url(&data_dir).unwrap().is_none());
        assert_eq!(inbox_item_count(&data_dir), 1);
        assert_eq!(fs::read(&marker).unwrap(), b"live-consumer".to_vec());
    }

    #[test]
    fn a_failed_rename_after_winning_the_claim_releases_the_marker() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        accept_message(
            &data_dir,
            br#"{"action":"download","url":"https://example.com/kept"}"#,
        )
        .unwrap();
        let inbox = data_dir.join("incoming");
        let item = inbox_item_path(&data_dir);
        let marker = claim_marker_path(&inbox, &item);

        // The item stays in the inbox, so the failure is genuine.
        let rename = |_from: &Path, _to: &Path| -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "simulated sharing violation",
            ))
        };
        let error = claim_next_pending_url_with(&data_dir, rename, |_| {}).unwrap_err();
        assert!(matches!(&error, NativeHostError::Io(_)));

        // The marker was released, so the item stays claimable and no orphan
        // marker is left behind.
        assert!(!marker.exists());
        assert_eq!(inbox_item_count(&data_dir), 1);
        let claim = claim_next_pending_url(&data_dir).unwrap().unwrap();
        assert_eq!(claim.url(), "https://example.com/kept");
        claim.acknowledge().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_claim_marker_is_private_to_the_user() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let marker_path = directory.path().join("item.json.claim");
        let marker = create_claim_marker(&marker_path).unwrap();
        assert_eq!(
            fs::metadata(&marker_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(release_claim_marker(&marker).unwrap());
        assert!(!marker_path.exists());
    }

    #[test]
    fn a_claim_marker_name_is_deterministic_and_bounded() {
        let inbox = Path::new("inbox");
        assert_eq!(
            claim_marker_path(inbox, &inbox.join("item.json")),
            inbox.join(".item.json.claim")
        );

        let long_item = inbox.join(format!("{}.json", "n".repeat(200)));
        let marker = claim_marker_path(inbox, &long_item);
        assert_eq!(marker.parent(), Some(inbox));
        assert!(marker.file_name().unwrap().to_str().unwrap().len() < 64);
        // The same item always maps to the same marker, and a different item
        // maps to a different one.
        assert_eq!(marker, claim_marker_path(inbox, &long_item));
        assert_ne!(marker, claim_marker_path(inbox, &inbox.join("other.json")));
    }

    /// The retry of a removal is exercised without the operating system and
    /// without a real pause: the removal is injected, so the first attempt can
    /// fail with a transient Windows code while the second one succeeds.
    #[test]
    fn a_transient_removal_failure_is_retried_until_the_path_is_gone() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.json");
        let mut attempts = 0usize;
        let mut pauses = Vec::new();
        let remove = |_entry: &Path| -> io::Result<()> {
            attempts += 1;
            if attempts == 1 {
                // Windows reports a directory whose child entry is still
                // delete-pending as ERROR_DIR_NOT_EMPTY.
                Err(io::Error::from_raw_os_error(145))
            } else {
                Ok(())
            }
        };
        let pause = |delay: Duration| pauses.push(delay);

        let outcome = remove_inbox_path_with(&path, remove, pause);

        assert!(outcome.unwrap(), "the removal must be reported as complete");
        assert_eq!(attempts, 2, "the transient failure must be retried once");
        assert_eq!(pauses, vec![INBOX_CLEANUP_RETRY_DELAY]);
    }

    #[test]
    fn a_genuine_removal_failure_is_reported_without_a_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.json");
        let mut attempts = 0usize;
        let mut pauses = 0usize;
        let remove = |_entry: &Path| -> io::Result<()> {
            attempts += 1;
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a removable entry",
            ))
        };
        let pause = |_delay: Duration| pauses += 1;

        let error = remove_inbox_path_with(&path, remove, pause).unwrap_err();

        assert!(error.to_string().contains("not a removable entry"));
        assert_eq!(attempts, 1, "a genuine failure must not be retried");
        assert_eq!(pauses, 0, "a genuine failure must not pause");
    }

    #[test]
    fn a_removal_that_never_clears_stops_after_a_bounded_number_of_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.json");
        let mut attempts = 0usize;
        let remove = |_entry: &Path| -> io::Result<()> {
            attempts += 1;
            // Windows reports a path that another handle still holds as
            // ERROR_ACCESS_DENIED.
            Err(io::Error::from_raw_os_error(5))
        };
        let pause = |_delay: Duration| {};

        let error = remove_inbox_path_with(&path, remove, pause).unwrap_err();

        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(
            attempts,
            INBOX_CLEANUP_RETRIES + 1,
            "the retry must be bounded"
        );
    }

    #[test]
    fn removing_an_absent_inbox_path_is_not_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.json");
        let mut attempts = 0usize;
        let remove = |_entry: &Path| -> io::Result<()> {
            attempts += 1;
            Err(io::Error::new(io::ErrorKind::NotFound, "already removed"))
        };
        let pause = |_delay: Duration| {};

        let outcome = remove_inbox_path_with(&path, remove, pause);

        assert!(
            !outcome.unwrap(),
            "an absent path was not removed by this call"
        );
        assert_eq!(attempts, 1, "an absent path must not be retried");
    }

    #[test]
    fn a_removal_that_reports_success_without_removing_the_path_is_retried() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.json");
        fs::write(&path, b"still there").unwrap();
        let mut attempts = 0usize;
        let remove = |entry: &Path| -> io::Result<()> {
            attempts += 1;
            if attempts == 1 {
                // The operating system accepted the removal, but the entry is
                // still visible, so the removal is not complete yet.
                Ok(())
            } else {
                fs::remove_file(entry)
            }
        };
        let pause = |_delay: Duration| {};

        let outcome = remove_inbox_path_with(&path, remove, pause);

        assert!(outcome.unwrap(), "the removal must be reported as complete");
        assert_eq!(attempts, 2, "an unconfirmed removal must be retried");
        assert!(!path.exists());
    }

    #[test]
    fn only_the_windows_busy_codes_are_treated_as_transient() {
        for code in [5, 32, 33, 145] {
            let busy = io::Error::from_raw_os_error(code);
            assert!(
                is_transient_windows_error(&busy),
                "the code {code} must be retried"
            );
        }
        let genuine = io::Error::new(io::ErrorKind::InvalidInput, "genuine failure");
        assert!(!is_transient_windows_error(&genuine));
        // A Unix failure carries a Unix code, so it is never mistaken for a
        // busy Windows path and is reported without a retry.
        let denied = io::Error::new(io::ErrorKind::PermissionDenied, "genuine denial");
        assert!(!is_transient_windows_error(&denied));
    }

    fn inbox_item_path(data_dir: &Path) -> PathBuf {
        fs::read_dir(data_dir.join("incoming"))
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .expect("the inbox should hold exactly one item")
    }

    fn set_modified_time(path: &Path, modified: SystemTime) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        let times = fs::FileTimes::new().set_modified(modified);
        file.set_times(times).unwrap();
    }
}
