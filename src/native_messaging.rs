use crate::url_policy::validate_http_url;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const HOST_ID: &str = "org.independent.download_manager";

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
        if let Err(error) = fs::remove_file(&self.claimed_path) {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(NativeHostError::InboxClaim {
                    claim: self.claim_dir.display().to_string(),
                    cause: error.to_string(),
                    recovery: format!(
                        "the queue accepted the URL, but the item remains claimed at {}; do not restore it while the queue is active",
                        self.claimed_path.display()
                    ),
                });
            }
        }
        if let Err(error) = fs::remove_dir(&self.claim_dir) {
            if error.kind() != io::ErrorKind::NotFound {
                eprintln!(
                    "Native Messaging acknowledged an inbox item but could not remove empty claim folder {}: {error}",
                    self.claim_dir.display()
                );
            }
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

        // Creating a unique directory reserves a no-clobber claim name. The
        // subsequent same-volume rename gives exactly one consumer the item.
        let claim_dir = create_claim_directory(&inbox)?;
        let claimed_path = claim_dir.join("message.json");
        if let Err(error) = fs::rename(&path, &claimed_path) {
            match fs::remove_dir(&claim_dir) {
                Ok(()) if error.kind() == io::ErrorKind::NotFound => continue,
                Ok(()) => return Err(error.into()),
                Err(cleanup_error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(NativeHostError::InboxClaim {
                        claim: claim_dir.display().to_string(),
                        cause: error.to_string(),
                        recovery: format!(
                            "the source was already claimed, but the empty claim directory could not be removed: {cleanup_error}"
                        ),
                    });
                }
                Err(cleanup_error) => {
                    return Err(NativeHostError::InboxClaim {
                        claim: claim_dir.display().to_string(),
                        cause: error.to_string(),
                        recovery: format!(
                            "the source was not claimed and the empty claim directory could not be removed: {cleanup_error}"
                        ),
                    });
                }
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
        let _ = fs::remove_dir(&quarantine_dir);
        return Err(NativeHostError::InboxClaim {
            claim: claim_dir.display().to_string(),
            cause: error.to_string(),
            recovery: format!(
                "rejected item remains recoverable at {}; retry quarantine or inspect it manually",
                claimed_path.display()
            ),
        });
    }
    if let Err(error) = fs::remove_dir(claim_dir) {
        if error.kind() != io::ErrorKind::NotFound {
            eprintln!(
                "Native Messaging quarantined an invalid item at {} but could not remove empty claim folder {}: {error}",
                quarantine_path.display(),
                claim_dir.display()
            );
        }
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
    match fs::remove_dir(claim_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "the item was restored to incoming, but the empty claim folder remains at {}: {error}",
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
}
