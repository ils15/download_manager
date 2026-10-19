use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Semaphore},
    task::JoinHandle,
};

#[derive(Clone, Copy, Debug)]
enum ServerBehavior {
    SupportsRanges,
    IgnoresRanges,
    StallFull,
    ChangedEtag,
    WrongContentRange,
    ChangedTotal,
    WrongContentLength,
    ShortSegment,
    StallSegment,
    /// The ranged responses are inconsistent, so the segmented attempt is
    /// abandoned, and the single-stream fallback then receives a body that ends
    /// one byte before the advertised `Content-Length`.
    TruncatedFullBody,
}

struct LocalRangeServer {
    url: String,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    range_requests: Arc<AtomicUsize>,
    full_requests: Arc<AtomicUsize>,
}

impl LocalRangeServer {
    async fn start(payload: Arc<Vec<u8>>, behavior: ServerBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let range_requests = Arc::new(AtomicUsize::new(0));
        let full_requests = Arc::new(AtomicUsize::new(0));
        let server_range_requests = range_requests.clone();
        let server_full_requests = full_requests.clone();
        let server_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = server_shutdown.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let body = payload.clone();
                        let stop = server_shutdown.clone();
                        let ranges = server_range_requests.clone();
                        let full = server_full_requests.clone();
                        tokio::spawn(async move {
                            serve_one(stream, body, behavior, stop, ranges, full).await;
                        });
                    }
                }
            }
        });
        Self {
            url: format!("http://{address}/payload.bin"),
            shutdown,
            task,
            range_requests,
            full_requests,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.task.await;
    }
}

async fn serve_one(
    mut stream: TcpStream,
    payload: Arc<Vec<u8>>,
    behavior: ServerBehavior,
    shutdown: CancellationToken,
    range_requests: Arc<AtomicUsize>,
    full_requests: Arc<AtomicUsize>,
) {
    let range_header = read_request(&mut stream).await;
    let Some(range_header) = range_header else {
        full_requests.fetch_add(1, Ordering::AcqRel);
        if matches!(behavior, ServerBehavior::StallFull) {
            stall_full_response(&mut stream, &payload, &shutdown).await;
            return;
        }
        if matches!(behavior, ServerBehavior::TruncatedFullBody) {
            // End the single-stream body one byte before the advertised length so
            // the fallback observes an incomplete body.
            let _ = write_truncated_body(&mut stream, &payload).await;
            return;
        }
        let _ = write_response(&mut stream, 200, "\"fixture-v1\"", &payload).await;
        return;
    };
    range_requests.fetch_add(1, Ordering::AcqRel);
    let Some((start, end)) = range_header
        .as_str()
        .strip_prefix("bytes=")
        .and_then(|value| value.split_once('-'))
    else {
        return;
    };
    let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) else {
        return;
    };
    if matches!(
        behavior,
        ServerBehavior::IgnoresRanges | ServerBehavior::StallFull
    ) {
        if matches!(behavior, ServerBehavior::StallFull) {
            stall_full_response(&mut stream, &payload, &shutdown).await;
            return;
        }
        let _ = write_response(&mut stream, 200, "\"fixture-v1\"", &payload).await;
        return;
    }
    if start >= payload.len() || end >= payload.len() || start > end {
        return;
    }
    let is_probe = start == 0 && end == 0;
    let advertised_total = if !is_probe && matches!(behavior, ServerBehavior::ChangedTotal) {
        payload.len() - 1
    } else {
        payload.len()
    };
    let range_value = format!("bytes {start}-{end}/{advertised_total}");
    let advertised_range = if !is_probe
        && matches!(
            behavior,
            ServerBehavior::WrongContentRange | ServerBehavior::TruncatedFullBody
        ) {
        format!("bytes {}-{end}/{}", start + 1, payload.len())
    } else {
        range_value
    };
    let etag = if !is_probe && matches!(behavior, ServerBehavior::ChangedEtag) {
        "\"fixture-v2\""
    } else {
        "\"fixture-v1\""
    };
    let body = &payload[start..=end];
    let head = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: {}\r\nETag: {}\r\nConnection: close\r\n\r\n",
        if !is_probe && matches!(behavior, ServerBehavior::WrongContentLength) {
            body.len() - 1
        } else {
            body.len()
        },
        advertised_range,
        etag
    );
    if stream.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    if !is_probe && matches!(behavior, ServerBehavior::StallSegment) {
        let _ = stream.write_all(&body[..1]).await;
        let _ = stream.flush().await;
        shutdown.cancelled().await;
        return;
    }
    let body_to_send = if !is_probe && matches!(behavior, ServerBehavior::ShortSegment) {
        &body[..body.len().saturating_sub(1)]
    } else {
        body
    };
    let _ = stream.write_all(body_to_send).await;
}

async fn stall_full_response(stream: &mut TcpStream, payload: &[u8], shutdown: &CancellationToken) {
    let header = format!(
        "HTTP/1.1 200 Test\r\nContent-Length: {}\r\nETag: \"fixture-v1\"\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    if stream.write_all(header.as_bytes()).await.is_ok() {
        let _ = shutdown.cancelled().await;
    }
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    etag: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nETag: {etag}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await
}

/// Advertises the full `Content-Length` but sends one byte less, then closes the
/// connection. The client observes an incomplete body.
async fn write_truncated_body(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let header = format!(
        "HTTP/1.1 200 Test\r\nContent-Length: {}\r\nETag: \"fixture-v1\"\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream
        .write_all(&body[..body.len().saturating_sub(1)])
        .await
}

async fn read_request(stream: &mut TcpStream) -> Option<String> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await.ok()?;
        if read == 0 {
            return None;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&request);
    let range = text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("range")
            .then_some(value.trim().to_owned())
    });
    range
}

fn payload() -> Arc<Vec<u8>> {
    Arc::new(
        (0..SEGMENTED_MIN_BYTES)
            .map(|index| (index % 251) as u8)
            .collect(),
    )
}

fn request(url: String, destination: PathBuf, max_duration: Duration) -> DownloadRequest {
    DownloadRequest {
        url,
        destination,
        expected_sha256: None,
        max_retries: 0,
        bytes_per_second: None,
        max_bytes: SEGMENTED_MIN_BYTES,
        max_duration,
    }
}

/// Lists the directory entries without unwrapping an I/O result: a directory that
/// cannot be read is a test failure with a diagnostic, not a panic inside an
/// `unwrap`.
fn directory_entry_names(directory: &Path) -> Vec<String> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => panic!("the download directory must be readable: {error}"),
    };
    let mut names = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => names.push(entry.file_name().to_string_lossy().into_owned()),
            Err(error) => panic!("a download directory entry must be readable: {error}"),
        }
    }
    names
}

fn assert_no_download_temporaries(directory: &Path) {
    for name in directory_entry_names(directory) {
        assert!(
            !name.contains(".dm-") && !name.ends_with(".part"),
            "temporary file remains: {name}"
        );
    }
}

/// The segmented attempt owns a staging directory next to the destination. It must
/// be discarded on every fallback, including a fallback that ends in failure.
fn assert_no_segment_staging_remainders(directory: &Path) {
    for name in directory_entry_names(directory) {
        assert!(
            !name.contains(".staging"),
            "segment staging directory remains: {name}"
        );
    }
}

#[tokio::test]
async fn segmented_download_concatenates_ranges_in_order_and_hashes_exact_content() {
    let body = payload();
    let expected_hash = Sha256::digest(body.as_slice())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let server = LocalRangeServer::start(body.clone(), ServerBehavior::SupportsRanges).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("ordered.bin");
    let mut request = request(
        server.url.clone(),
        destination.clone(),
        Duration::from_secs(60),
    );
    request.expected_sha256 = Some(expected_hash.clone());
    let limiter = SharedBandwidthLimiter::new(0).unwrap();

    let result = download_with_session_managed_with_limiter_segments_and_slots(
        request,
        CancellationToken::new(),
        &DownloadSession::default(),
        &limiter,
        4,
        Arc::new(Semaphore::new(4)),
    )
    .await
    .unwrap();

    assert_eq!(result.bytes, body.len() as u64);
    assert_eq!(result.sha256, expected_hash);
    assert_eq!(std::fs::read(&destination).unwrap(), body.as_slice());
    assert_eq!(server.range_requests.load(Ordering::Acquire), 5);
    assert_eq!(server.full_requests.load(Ordering::Acquire), 0);
    assert_no_download_temporaries(directory.path());
    server.stop().await;
}

#[tokio::test]
async fn segmented_ranges_hold_the_atomic_slot_reservation_until_cancelled() {
    let body = payload();
    let server = LocalRangeServer::start(body, ServerBehavior::StallSegment).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("quota-ranged.bin");
    let limiter = SharedBandwidthLimiter::new(0).unwrap();
    let slots = Arc::new(Semaphore::new(2));
    let session = DownloadSession::default();
    let cancel = CancellationToken::new();
    let worker_session = session.clone();
    let worker_cancel = cancel.clone();
    let worker_url = server.url.clone();
    let worker_destination = destination.clone();
    let worker_limiter = limiter.clone();
    let worker_slots = slots.clone();

    let worker = tokio::spawn(async move {
        download_with_session_managed_with_limiter_segments_and_slots(
            request(worker_url, worker_destination, Duration::from_secs(30)),
            worker_cancel,
            &worker_session,
            &worker_limiter,
            2,
            worker_slots,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(5), session.wait_for_bytes(2))
        .await
        .expect("both range streams should write while holding the reserved slots");
    assert_eq!(server.range_requests.load(Ordering::Acquire), 3);
    assert_eq!(slots.available_permits(), 0);
    cancel.cancel();
    assert!(matches!(
        worker.await.unwrap(),
        Err(DownloadError::Cancelled)
    ));
    assert_eq!(slots.available_permits(), 2);
    assert!(!destination.exists());
    server.stop().await;
}

#[tokio::test]
async fn range_ignoring_fallbacks_share_single_slots_not_the_segment_limit() {
    let body = payload();
    let server = LocalRangeServer::start(body, ServerBehavior::StallFull).await;
    let directory = tempfile::tempdir().unwrap();
    let limiter = SharedBandwidthLimiter::new(0).unwrap();
    let slots = Arc::new(Semaphore::new(2));
    let mut cancels = Vec::new();
    let mut workers = Vec::new();

    for index in 0..3 {
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let session = DownloadSession::default();
        let worker_session = session.clone();
        let worker_url = server.url.clone();
        let destination = directory.path().join(format!("fallback-{index}.bin"));
        let worker_limiter = limiter.clone();
        let worker_slots = slots.clone();
        cancels.push(cancel);
        workers.push(tokio::spawn(async move {
            download_with_session_managed_with_limiter_segments_and_slots(
                request(worker_url, destination, Duration::from_secs(30)),
                worker_cancel,
                &worker_session,
                &worker_limiter,
                4,
                worker_slots,
            )
            .await
        }));
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        while server.full_requests.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two single-stream fallbacks should occupy two slots");
    assert_eq!(server.range_requests.load(Ordering::Acquire), 2);
    assert_eq!(server.full_requests.load(Ordering::Acquire), 2);
    assert_eq!(slots.available_permits(), 0);
    assert!(workers.iter().all(|worker| !worker.is_finished()));

    for cancel in cancels {
        cancel.cancel();
    }
    for worker in workers {
        assert!(matches!(
            worker.await.unwrap(),
            Err(DownloadError::Cancelled)
        ));
    }
    server.stop().await;
}

#[tokio::test]
async fn unsupported_or_inconsistent_ranges_cleanly_fall_back_to_single_stream() {
    for behavior in [
        ServerBehavior::IgnoresRanges,
        ServerBehavior::ChangedEtag,
        ServerBehavior::WrongContentRange,
        ServerBehavior::ChangedTotal,
        ServerBehavior::WrongContentLength,
        ServerBehavior::ShortSegment,
    ] {
        let body = payload();
        let server = LocalRangeServer::start(body.clone(), behavior).await;
        let directory = tempfile::tempdir().expect("a temporary directory must be created");
        let destination = directory.path().join("fallback.bin");
        let limiter = SharedBandwidthLimiter::new(0).expect("unlimited limiter");

        let result = download_with_session_managed_with_limiter_segments_and_slots(
            request(
                server.url.clone(),
                destination.clone(),
                Duration::from_secs(60),
            ),
            CancellationToken::new(),
            &DownloadSession::default(),
            &limiter,
            4,
            Arc::new(Semaphore::new(4)),
        )
        .await;

        let result = match result {
            Ok(result) => result,
            Err(error) => panic!("the fallback must complete for {behavior:?}: {error}"),
        };
        assert_eq!(result.bytes, body.len() as u64);
        assert!(
            destination.exists(),
            "the single-stream fallback must publish the destination"
        );
        let published = std::fs::read(&destination).expect("unreadable file");
        assert_eq!(published, body.as_slice());
        assert_eq!(server.full_requests.load(Ordering::Acquire), 1);
        assert_no_segment_staging_remainders(directory.path());
        assert_no_download_temporaries(directory.path());
        server.stop().await;
    }
}

#[tokio::test]
async fn truncated_single_stream_fallback_fails_cleanly_and_publishes_no_destination() {
    let body = payload();
    let behavior = ServerBehavior::TruncatedFullBody;
    let server = LocalRangeServer::start(body.clone(), behavior).await;
    let directory = tempfile::tempdir().expect("a temporary directory must be created");
    let destination = directory.path().join("truncated.bin");
    let limiter = SharedBandwidthLimiter::new(0).expect("unlimited limiter");
    let session = DownloadSession::default();

    let result = download_with_session_managed_with_limiter_segments_and_slots(
        request(
            server.url.clone(),
            destination.clone(),
            Duration::from_secs(60),
        ),
        CancellationToken::new(),
        &session,
        &limiter,
        4,
        Arc::new(Semaphore::new(4)),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("an incomplete body must not be a finished download"),
        Err(error) => error,
    };
    let transport = matches!(&error, DownloadError::Http(_));
    let range = matches!(&error, DownloadError::InvalidRange);
    assert!(
        transport || range,
        "an incomplete body must be a clean transfer error: {error}"
    );
    assert!(
        !destination.exists(),
        "a failed single-stream fallback must not publish a destination"
    );
    assert_eq!(server.full_requests.load(Ordering::Acquire), 1);
    // The segmented staging is discarded even though the fallback itself failed.
    assert_no_segment_staging_remainders(directory.path());
    // The single-stream partial stays owned by the session for an in-process
    // resume; the explicit local cleanup releases it.
    discard_download_session(&session)
        .await
        .expect("discardable session");
    assert_no_download_temporaries(directory.path());
    server.stop().await;
}

#[tokio::test]
async fn cancellation_removes_all_segment_temporaries() {
    let body = payload();
    let server = LocalRangeServer::start(body, ServerBehavior::StallSegment).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("cancelled.bin");
    let cancel = CancellationToken::new();
    let worker_cancel = cancel.clone();
    let session = DownloadSession::default();
    let worker_session = session.clone();
    let limiter = SharedBandwidthLimiter::new(0).unwrap();
    let worker_limiter = limiter.clone();
    let worker_url = server.url.clone();
    let worker_destination = destination.clone();
    let worker = tokio::spawn(async move {
        download_with_session_managed_with_limiter_segments_and_slots(
            request(worker_url, worker_destination, Duration::from_secs(60)),
            worker_cancel,
            &worker_session,
            &worker_limiter,
            4,
            Arc::new(Semaphore::new(4)),
        )
        .await
    });

    session.wait_for_bytes(1).await;
    cancel.cancel();
    assert!(matches!(
        worker.await.unwrap(),
        Err(DownloadError::Cancelled)
    ));
    assert!(!destination.exists());
    assert_no_download_temporaries(directory.path());
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn absolute_timeout_keeps_cleanup_pending_for_manual_retry() {
    let body = payload();
    let server = LocalRangeServer::start(body, ServerBehavior::StallSegment).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("timed-out.bin");
    let session = DownloadSession::default();
    let worker_session = session.clone();
    let limiter = SharedBandwidthLimiter::new(0).unwrap();
    let worker_url = server.url.clone();
    let worker_destination = destination.clone();
    let worker = tokio::spawn(async move {
        download_with_session_managed_with_limiter_segments_and_slots(
            request(worker_url, worker_destination, Duration::from_secs(1)),
            CancellationToken::new(),
            &worker_session,
            &limiter,
            4,
            Arc::new(Semaphore::new(4)),
        )
        .await
    });

    session.wait_for_bytes(1).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        worker.await.unwrap(),
        Err(DownloadError::CleanupPending { .. })
    ));
    assert!(!destination.exists());
    assert!(!session.segment_temp_paths.lock().unwrap().is_empty());
    discard_download_session(&session).await.unwrap();
    assert_no_download_temporaries(directory.path());
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn deadline_bounds_cleanup_and_leaves_the_registry_entry_pending() {
    let registry = Arc::new(StdMutex::new(HashMap::new()));
    let path = PathBuf::from("pending-staging-cleanup");
    registry.lock().unwrap().insert(path.clone(), true);
    let deadline = Instant::now() + Duration::from_secs(1);
    let cleanup_registry = registry.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_registered_segment_paths(
            cleanup_registry,
            vec![path.clone()],
            Some(deadline),
            move |_| async { std::future::pending::<Result<(), std::io::Error>>().await },
        )
        .await
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let result = cleanup.await.unwrap();
    assert!(result.is_err());
    assert!(registry
        .lock()
        .unwrap()
        .contains_key(&PathBuf::from("pending-staging-cleanup")));
}

#[tokio::test(start_paused = true)]
async fn deadline_during_an_open_in_flight_keeps_cleanup_pending_until_handle_drop() {
    let directory = tempfile::tempdir().unwrap();
    let session = DownloadSession::default();
    let destination = directory.path().join("late-open.bin");
    let staging =
        SegmentStagingDirectory::create(&destination, session.segment_temp_paths.clone()).unwrap();
    let staging_path = staging.path.clone();
    let late_path = staging.path.join("segment-0.part");
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(session
        .segment_temp_paths
        .lock()
        .unwrap()
        .contains_key(&staging_path));
    let (open_started_tx, open_started_rx) = oneshot::channel();
    let (continue_open_tx, continue_open_rx) = std::sync::mpsc::channel();
    let (open_finished_tx, open_finished_rx) = oneshot::channel();
    let handle_is_open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let opener_handle_state = handle_is_open.clone();
    let open_cancel = CancellationToken::new();
    let task_path = late_path.clone();
    let open = tokio::spawn(async move {
        OwnedTempFile::create_until_with(
            task_path,
            &open_cancel,
            Some(deadline),
            move |path| async move {
                tokio::task::spawn_blocking(move || {
                    let file = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(path);
                    let opened = file.is_ok();
                    if opened {
                        opener_handle_state.store(true, Ordering::Release);
                    }
                    open_started_tx.send(opened).unwrap();
                    let file = file?;
                    let _ = continue_open_rx.recv();
                    drop(file);
                    opener_handle_state.store(false, Ordering::Release);
                    let _ = open_finished_tx.send(());
                    Err::<fs::File, _>(std::io::Error::other(
                        "test opener released after its caller timed out",
                    ))
                })
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?
            },
        )
        .await
    });
    assert!(open_started_rx.await.unwrap());
    assert!(late_path.exists());
    assert!(handle_is_open.load(Ordering::Acquire));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        open.await.unwrap(),
        Err(DownloadError::DurationLimitExceeded)
    ));

    let cleanup = cleanup_registered_segment_paths(
        session.segment_temp_paths.clone(),
        vec![staging_path.clone()],
        Some(deadline),
        fs::remove_dir_all,
    )
    .await;
    assert!(cleanup
        .unwrap_err()
        .contains("cleanup pending after the absolute deadline"));
    assert!(staging_path.exists());
    assert!(session
        .segment_temp_paths
        .lock()
        .unwrap()
        .contains_key(&staging_path));

    // Simulate Windows' sharing violation deterministically while the open
    // handle is held; the failed attempt must leave its registry entry retryable.
    let cleanup_handle_state = handle_is_open.clone();
    let cleanup_error = cleanup_registered_segment_paths(
        session.segment_temp_paths.clone(),
        vec![staging_path.clone()],
        None,
        move |path| {
            let cleanup_handle_state = cleanup_handle_state.clone();
            async move {
                if cleanup_handle_state.load(Ordering::Acquire) {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "simulated Windows sharing violation",
                    ))
                } else {
                    fs::remove_dir_all(path).await
                }
            }
        },
    )
    .await
    .unwrap_err();
    assert!(cleanup_error.contains("simulated Windows sharing violation"));
    assert!(session
        .segment_temp_paths
        .lock()
        .unwrap()
        .contains_key(&staging_path));

    drop(staging);
    assert!(staging_path.exists());
    assert!(session
        .segment_temp_paths
        .lock()
        .unwrap()
        .contains_key(&staging_path));

    continue_open_tx.send(()).unwrap();
    open_finished_rx.await.unwrap();
    assert!(!handle_is_open.load(Ordering::Acquire));
    assert!(staging_path.exists());
    assert!(late_path.exists());
    assert_eq!(
        session
            .segment_temp_paths
            .lock()
            .unwrap()
            .get(&staging_path),
        Some(&true)
    );

    // Manual local cleanup is intentionally allowed after the transfer deadline.
    cleanup_segment_temp_paths(&session).await.unwrap();
    assert!(!late_path.exists());
    assert!(!staging_path.exists());
    assert!(session.segment_temp_paths.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn concurrent_segment_reservations_share_the_global_byte_budget() {
    let limiter = SharedBandwidthLimiter::new(100).unwrap();
    let cancel = CancellationToken::new();
    limiter
        .acquire(GLOBAL_BANDWIDTH_BUCKET_CAPACITY_BYTES, &cancel)
        .await
        .unwrap();

    let first_limiter = limiter.clone();
    let first_cancel = cancel.clone();
    let first = tokio::spawn(async move { first_limiter.acquire(100, &first_cancel).await });
    let second_limiter = limiter.clone();
    let second_cancel = cancel.clone();
    let second = tokio::spawn(async move { second_limiter.acquire(100, &second_cancel).await });
    while limiter.waiting_count() != 2 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(Duration::from_secs(1)).await;
    while !first.is_finished() {
        tokio::task::yield_now().await;
    }
    assert!(!second.is_finished());
    first.await.unwrap().unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    second.await.unwrap().unwrap();
}
