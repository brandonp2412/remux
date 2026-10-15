use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Session, SessionOptions,
    SessionPersistenceConfig, TorrentStatsState,
    api::{Api, TorrentIdOrHash},
    dht::PersistentDhtConfig,
    http_api::HttpApi,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
struct TorrentFile {
    name: String,
    length: u64,
}

#[derive(Clone, Debug)]
struct TorrentStreamSnapshot {
    selected_file_bytes: u64,
    downloaded_and_checked_bytes: u64,
    live_peers: usize,
    connecting_peers: usize,
    seen_peers: usize,
    torrent_download_mib_s: f64,
}

#[derive(Clone, Debug, Default)]
struct TorrentObservedProgress {
    file_name: String,
    selected_file_total_bytes: u64,
    selected_file_growth_mib_s: f64,
    positive_growth_samples: u32,
    range_requests: u64,
    range_ready: u64,
    range_bytes_served: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct TorrentProbeProgress {
    pub info_hash: String,
    pub file_idx: usize,
    pub file_name: String,
    pub selected_file_bytes: u64,
    pub selected_file_total_bytes: u64,
    pub live_peers: usize,
    pub connecting_peers: usize,
    pub seen_peers: usize,
    pub torrent_download_mib_s: f64,
    pub selected_file_growth_mib_s: f64,
    pub positive_growth_samples: u32,
    pub range_requests: u64,
    pub range_ready: u64,
    pub range_bytes_served: u64,
}

fn normalized_info_hash(hash: &str) -> String {
    librqbit::Magnet::parse(&format!("magnet:?xt=urn:btih:{hash}"))
        .ok()
        .and_then(|magnet| magnet.as_id20())
        .map(|id| id.as_string())
        .unwrap_or_else(|| hash.to_ascii_lowercase())
}

fn torrent_stream_snapshot(
    session: &Arc<Session>,
    torrent_id: usize,
    file_idx: usize,
) -> Option<TorrentStreamSnapshot> {
    let api = Api::new(session.clone(), None, None);
    let stats = api
        .api_stats_v1(TorrentIdOrHash::Id(torrent_id))
        .ok()?;
    let live = stats
        .live
        .as_ref();
    Some(TorrentStreamSnapshot {
        selected_file_bytes: stats
            .file_progress
            .get(file_idx)
            .copied()
            .unwrap_or_default(),
        downloaded_and_checked_bytes: live
            .map(|live| {
                live.snapshot
                    .downloaded_and_checked_bytes
            })
            .unwrap_or_default(),
        live_peers: live
            .map(|live| {
                live.snapshot
                    .peer_stats
                    .live
            })
            .unwrap_or_default(),
        connecting_peers: live
            .map(|live| {
                live.snapshot
                    .peer_stats
                    .connecting
            })
            .unwrap_or_default(),
        seen_peers: live
            .map(|live| {
                live.snapshot
                    .peer_stats
                    .seen
            })
            .unwrap_or_default(),
        torrent_download_mib_s: live
            .map(|live| {
                live.download_speed
                    .mbps
            })
            .unwrap_or_default(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SidecarSubtitleFile {
    file_idx: usize,
    path: String,
    language: Option<String>,
    is_forced: bool,
    is_hearing_impaired: bool,
}

pub struct TorrentManager {
    session: Arc<Session>,
    torrent_data_path: PathBuf,
    http_port: u16,
    leases: tokio::sync::Mutex<
        std::collections::HashMap<String, std::sync::Weak<TorrentLease>>,
    >,
    progress_observers:
        Arc<tokio::sync::Mutex<std::collections::HashSet<(usize, usize)>>>,
    probe_progress: Arc<
        std::sync::Mutex<
            std::collections::HashMap<(String, usize), TorrentObservedProgress>,
        >,
    >,
    background_downloads:
        tokio::sync::Mutex<std::collections::HashMap<String, BackgroundDownload>>,
    background_downloads_path: PathBuf,
    background_limits: BackgroundLimits,
}

/// Shared by playback sessions and response bodies using this exact torrent.
pub struct TorrentLease {
    manager: Arc<TorrentManager>,
    hash: String,
}

impl Drop for TorrentLease {
    fn drop(&mut self) {
        let manager = self
            .manager
            .clone();
        let hash = self
            .hash
            .clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                // Allow a seek/reconnect or the next episode to acquire the
                // same torrent before releasing its peers and downloaded data.
                tokio::time::sleep(Duration::from_secs(30)).await;
                if let Err(error) = manager
                    .delete_if_unused(&hash)
                    .await
                {
                    warn!(%hash, "failed to release torrent: {error:#}");
                }
            });
        }
    }
}

impl TorrentManager {
    pub async fn new(
        data_dir: PathBuf,
        cache_dir: PathBuf,
        http_port: Option<u16>,
        disable_dht: bool,
        peer_port: Option<u16>,
    ) -> Result<Self> {
        let session = Session::new_with_opts(
            data_dir.clone(),
            SessionOptions {
                disable_dht,
                disable_dht_persistence: disable_dht,
                listen_port_range: peer_port.map(|p| p..p + 10),
                persistence: Some(SessionPersistenceConfig::Json {
                    folder: Some(cache_dir.join("rqbit")),
                }),
                dht_config: Some(PersistentDhtConfig {
                    config_filename: Some(cache_dir.join("dht.json")),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;

        // None → let the OS pick a free ephemeral port.
        let bind_port = http_port.unwrap_or(0);
        let listener =
            tokio::net::TcpListener::bind(format!("127.0.0.1:{}", bind_port)).await?;

        let bound_port = listener
            .local_addr()?
            .port();

        let api = Api::new(session.clone(), None, None);
        let http_api = HttpApi::new(api, None);
        tokio::spawn(http_api.make_http_api_and_run(listener, None));

        debug!(port = bound_port, "torrent HTTP server listening");
        let background_downloads_path = cache_dir.join("background-downloads.json");
        let background_downloads =
            match tokio::fs::read(&background_downloads_path).await {
                Ok(data) => serde_json::from_slice::<Vec<BackgroundDownload>>(&data)
                    .context("invalid background download state; not overwriting it")?
                    .into_iter()
                    .map(|job| {
                        (
                            job.info_hash
                                .clone(),
                            job,
                        )
                    })
                    .collect(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Default::default()
                }
                Err(error) => {
                    return Err(error).context("reading background download state");
                }
            };
        Ok(Self {
            session,
            torrent_data_path: data_dir,
            http_port: bound_port,
            leases: Default::default(),
            progress_observers: Default::default(),
            probe_progress: Default::default(),
            background_downloads: tokio::sync::Mutex::new(background_downloads),
            background_downloads_path,
            background_limits: BackgroundLimits::default(),
        })
    }

    pub(crate) fn note_range_requested(&self, info_hash: &str, file_idx: usize) {
        let key = (normalized_info_hash(info_hash), file_idx);
        if let Ok(mut progress) = self
            .probe_progress
            .lock()
        {
            progress
                .entry(key)
                .or_default()
                .range_requests += 1;
        }
    }

    pub(crate) fn note_range_ready(&self, info_hash: &str, file_idx: usize) {
        let key = (normalized_info_hash(info_hash), file_idx);
        if let Ok(mut progress) = self
            .probe_progress
            .lock()
        {
            progress
                .entry(key)
                .or_default()
                .range_ready += 1;
        }
    }

    pub(crate) fn note_range_bytes(
        &self,
        info_hash: &str,
        file_idx: usize,
        bytes: usize,
    ) {
        let key = (normalized_info_hash(info_hash), file_idx);
        if let Ok(mut progress) = self
            .probe_progress
            .lock()
        {
            let entry = progress
                .entry(key)
                .or_default();
            entry.range_bytes_served = entry
                .range_bytes_served
                .saturating_add(bytes as u64);
        }
    }

    pub(crate) fn probe_progress(
        &self,
        info_hash: &str,
        file_idx: Option<usize>,
        file_hint: Option<&str>,
    ) -> Option<TorrentProbeProgress> {
        let normalized = normalized_info_hash(info_hash);
        let hash_id =
            librqbit::Magnet::parse(&format!("magnet:?xt=urn:btih:{normalized}"))
                .ok()?
                .as_id20()?;
        let handle = self
            .session
            .get(TorrentIdOrHash::Hash(hash_id))?;
        let files = handle
            .with_metadata(|metadata| {
                metadata
                    .file_infos
                    .iter()
                    .map(|file| TorrentFile {
                        name: file
                            .relative_filename
                            .to_string_lossy()
                            .into_owned(),
                        length: file.len,
                    })
                    .collect::<Vec<_>>()
            })
            .ok()?;
        let selected_idx = select_file_index(&files, file_idx, file_hint).ok()?;
        let selected_file = files
            .get(selected_idx)?
            .clone();
        let snapshot =
            torrent_stream_snapshot(&self.session, handle.id(), selected_idx)?;

        let observed = self
            .probe_progress
            .lock()
            .ok()
            .and_then(|progress| {
                progress
                    .get(&(normalized.clone(), selected_idx))
                    .cloned()
            })
            .unwrap_or_default();

        Some(TorrentProbeProgress {
            info_hash: normalized,
            file_idx: selected_idx,
            file_name: if observed
                .file_name
                .is_empty()
            {
                selected_file.name
            } else {
                observed.file_name
            },
            selected_file_bytes: snapshot.selected_file_bytes,
            selected_file_total_bytes: if observed.selected_file_total_bytes == 0 {
                selected_file.length
            } else {
                observed.selected_file_total_bytes
            },
            live_peers: snapshot.live_peers,
            connecting_peers: snapshot.connecting_peers,
            seen_peers: snapshot.seen_peers,
            torrent_download_mib_s: snapshot.torrent_download_mib_s,
            selected_file_growth_mib_s: observed.selected_file_growth_mib_s,
            positive_growth_samples: observed.positive_growth_samples,
            range_requests: observed.range_requests,
            range_ready: observed.range_ready,
            range_bytes_served: observed.range_bytes_served,
        })
    }

    pub async fn acquire(self: &Arc<Self>, hash: &str) -> Arc<TorrentLease> {
        // Magnet hashes may be hex or base32; librqbit lists them as hex.
        let hash = librqbit::Magnet::parse(&format!("magnet:?xt=urn:btih:{hash}"))
            .ok()
            .and_then(|magnet| magnet.as_id20())
            .map(|id| id.as_string())
            .unwrap_or_else(|| hash.to_ascii_lowercase());
        let mut leases = self
            .leases
            .lock()
            .await;
        if let Some(lease) = leases
            .get(&hash)
            .and_then(std::sync::Weak::upgrade)
        {
            return lease;
        }
        let lease = Arc::new(TorrentLease {
            manager: self.clone(),
            hash: hash.clone(),
        });
        leases.insert(hash, Arc::downgrade(&lease));
        lease
    }

    async fn delete_if_unused(&self, hash: &str) -> Result<()> {
        if self
            .background_downloads
            .lock()
            .await
            .contains_key(hash)
        {
            return Ok(());
        }
        // Acquisition and deletion share the lock, so a new reader cannot
        // acquire a torrent between the last-user check and deletion.
        let mut leases = self
            .leases
            .lock()
            .await;
        if leases
            .get(hash)
            .is_none_or(|lease| lease.strong_count() != 0)
        {
            return Ok(());
        }
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        if let Some(id) = api
            .api_torrent_list()
            .torrents
            .into_iter()
            .find(|torrent| {
                torrent
                    .info_hash
                    .eq_ignore_ascii_case(hash)
            })
            .and_then(|torrent| torrent.id)
        {
            api.api_torrent_action_delete(TorrentIdOrHash::Id(id))
                .await?;
        }
        leases.remove(hash);
        if let Ok(mut progress) = self
            .probe_progress
            .lock()
        {
            progress.retain(|(progress_hash, _), _| progress_hash != hash);
        }
        Ok(())
    }

    pub async fn from_config(config: &crate::Config) -> Result<Self> {
        let data_dir = config
            .torrent_data_dir
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Config::resolve() must be called before TorrentManager::from_config"))?;
        let mut manager = Self::new(
            std::path::PathBuf::from(data_dir),
            config
                .data_dir
                .join("cache"),
            config.torrent_http_port,
            config.disable_dht,
            config.torrent_peer_port,
        )
        .await?;
        manager.background_limits = BackgroundLimits {
            max_active: config.background_download_max_active,
            max_bytes: config
                .background_download_max_gib
                .saturating_mul(1024 * 1024 * 1024),
            max_age_hours: config.background_download_max_age_hours,
        };
        manager
            .restore_background_downloads()
            .await?;
        Ok(manager)
    }

    fn managed_torrent_files(&self, info_hash: &str) -> Option<Vec<TorrentFile>> {
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let torrent_id = api
            .api_torrent_list()
            .torrents
            .into_iter()
            .find(|torrent| {
                torrent
                    .info_hash
                    .eq_ignore_ascii_case(info_hash)
            })?
            .id?;
        api.api_torrent_details(TorrentIdOrHash::Id(torrent_id))
            .ok()?
            .files
            .map(|files| {
                files
                    .into_iter()
                    .map(|file| TorrentFile {
                        name: file.name,
                        length: file.length,
                    })
                    .collect()
            })
    }

    /// Gracefully shut down the librqbit session, releasing all sockets
    /// (including the DHT UDP socket). Call this before dropping the manager
    /// to avoid "address already in use" errors on restart.
    pub async fn shutdown(&self) {
        self.session
            .stop()
            .await;
    }

    /// Resolve a magnet URI (possibly with `&tr=`, `&file_idx=`, `&file=` params
    /// we encode) to a local `http://127.0.0.1:<port>/torrents/<id>/stream/<file_idx>` URL
    pub async fn resolve_url(&self, magnet: &str) -> Result<(String, usize)> {
        let file_idx_override = parse_file_idx_param(magnet);
        let wanted_file = parse_file_param(magnet);
        debug!(
            magnet,
            ?wanted_file,
            ?file_idx_override,
            "resolving torrent"
        );

        let response = self
            .session
            .add_torrent(AddTorrent::from_url(magnet), Some(stream_only_options()))
            .await
            .context("failed to add torrent")?;

        let (torrent_id, handle) = match response {
            AddTorrentResponse::Added(id, h) => (id, h),
            AddTorrentResponse::AlreadyManaged(id, h) => (id, h),
            AddTorrentResponse::ListOnly(_) => {
                anyhow::bail!("unexpected ListOnly response")
            }
        };

        tokio::time::timeout(Duration::from_secs(30), handle.wait_until_initialized())
            .await
            .context("timed out waiting for torrent metadata")?
            .context("torrent initialization failed")?;

        let files = handle.with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .map(|file| TorrentFile {
                    name: file
                        .relative_filename
                        .to_string_lossy()
                        .into_owned(),
                    length: file.len,
                })
                .collect::<Vec<_>>()
        })?;
        let file_idx =
            select_file_index(&files, file_idx_override, wanted_file.as_deref())?;

        // Existing persisted torrents may have been created with every file
        // selected. Clear that natural queue as well; active FileStreams keep
        // requesting their own pieces independently.
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let selected_background_files: std::collections::HashSet<usize> = self
            .background_downloads
            .lock()
            .await
            .get(&normalized_info_hash(
                &librqbit::Magnet::parse(magnet)
                    .ok()
                    .and_then(|parsed| parsed.as_id20())
                    .map(|id| id.as_string())
                    .unwrap_or_default(),
            ))
            .filter(|job| !job.paused)
            .map(|job| std::collections::HashSet::from([job.file_idx]))
            .unwrap_or_default();
        api.api_torrent_action_update_only_files(
            TorrentIdOrHash::Id(torrent_id),
            &selected_background_files,
        )
        .await
        .context("failed to clear torrent file selection")?;
        if !matches!(
            handle
                .stats()
                .state,
            TorrentStatsState::Live
        ) {
            if let Err(error) = api
                .api_torrent_action_start(TorrentIdOrHash::Id(torrent_id))
                .await
            {
                // Another request may have started the torrent between the
                // state check and this action. Only suppress that race.
                if matches!(
                    handle
                        .stats()
                        .state,
                    TorrentStatsState::Live
                ) {
                    debug!(torrent_id, "torrent was started concurrently");
                } else {
                    return Err(error).context("failed to start torrent");
                }
            }
        }

        debug!(
            torrent_id,
            file_idx,
            file = %files[file_idx].name,
            file_count = files.len(),
            "selected torrent stream file"
        );

        let info_hash = librqbit::Magnet::parse(magnet)
            .ok()
            .and_then(|magnet| magnet.as_id20())
            .map(|hash| hash.as_string())
            .unwrap_or_else(|| "<unknown>".to_string());
        let selected_file = files[file_idx].clone();
        let progress_key = (info_hash.clone(), file_idx);
        if let Ok(mut progress) = self
            .probe_progress
            .lock()
        {
            let entry = progress
                .entry(progress_key.clone())
                .or_default();
            entry.file_name = selected_file
                .name
                .clone();
            entry.selected_file_total_bytes = selected_file.length;
        }
        let initial = torrent_stream_snapshot(&self.session, torrent_id, file_idx);
        if let Some(snapshot) = &initial {
            info!(
                torrent_id,
                info_hash = %info_hash,
                file_idx,
                file = %selected_file.name,
                selected_file_bytes = snapshot.selected_file_bytes,
                selected_file_total_bytes = selected_file.length,
                downloaded_and_checked_bytes = snapshot.downloaded_and_checked_bytes,
                live_peers = snapshot.live_peers,
                connecting_peers = snapshot.connecting_peers,
                seen_peers = snapshot.seen_peers,
                torrent_download_mib_s = snapshot.torrent_download_mib_s,
                "torrent selected-file progress"
            );
        }

        let initial_bytes = initial
            .as_ref()
            .map(|snapshot| snapshot.selected_file_bytes)
            .unwrap_or_default();
        let observe_progress = if initial_bytes < selected_file.length {
            self.progress_observers
                .lock()
                .await
                .insert((torrent_id, file_idx))
        } else {
            false
        };
        if observe_progress {
            let session = self
                .session
                .clone();
            let progress_observers = self
                .progress_observers
                .clone();
            let probe_progress = self
                .probe_progress
                .clone();
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let mut previous_elapsed = Duration::ZERO;
                let mut previous_bytes = initial_bytes;
                // Keep the final progress observation comfortably ahead of the
                // 15-second P2P ffprobe budget. Sampling exactly at 15 seconds
                // races the probe timeout, so genuinely growing torrents can
                // be discarded before their second positive-growth sample is
                // published to the probe fallback policy.
                for target_secs in [2_u64, 5, 9, 13] {
                    let target = Duration::from_secs(target_secs);
                    tokio::time::sleep(target.saturating_sub(started.elapsed())).await;
                    let elapsed = started.elapsed();
                    let Some(snapshot) =
                        torrent_stream_snapshot(&session, torrent_id, file_idx)
                    else {
                        break;
                    };
                    let delta_bytes = snapshot
                        .selected_file_bytes
                        .saturating_sub(previous_bytes);
                    let delta_seconds = elapsed
                        .saturating_sub(previous_elapsed)
                        .as_secs_f64();
                    let selected_file_growth_mib_s = if delta_seconds > 0.0 {
                        delta_bytes as f64 / 1024.0 / 1024.0 / delta_seconds
                    } else {
                        0.0
                    };
                    if let Ok(mut progress) = probe_progress.lock() {
                        let entry = progress
                            .entry(progress_key.clone())
                            .or_default();
                        entry.file_name = selected_file
                            .name
                            .clone();
                        entry.selected_file_total_bytes = selected_file.length;
                        entry.selected_file_growth_mib_s = selected_file_growth_mib_s;
                        if delta_bytes > 0 {
                            entry.positive_growth_samples = entry
                                .positive_growth_samples
                                .saturating_add(1);
                        }
                    }
                    info!(
                        torrent_id,
                        info_hash = %info_hash,
                        file_idx,
                        file = %selected_file.name,
                        elapsed_ms = elapsed.as_millis() as u64,
                        selected_file_bytes = snapshot.selected_file_bytes,
                        selected_file_total_bytes = selected_file.length,
                        selected_file_growth_mib_s,
                        downloaded_and_checked_bytes = snapshot.downloaded_and_checked_bytes,
                        live_peers = snapshot.live_peers,
                        connecting_peers = snapshot.connecting_peers,
                        seen_peers = snapshot.seen_peers,
                        torrent_download_mib_s = snapshot.torrent_download_mib_s,
                        "torrent selected-file progress"
                    );
                    if snapshot.selected_file_bytes >= selected_file.length {
                        break;
                    }
                    previous_elapsed = elapsed;
                    previous_bytes = snapshot.selected_file_bytes;
                }
                progress_observers
                    .lock()
                    .await
                    .remove(&(torrent_id, file_idx));
            });
        }

        Ok((
            format!(
                "http://127.0.0.1:{}/torrents/{}/stream/{}",
                self.http_port, torrent_id, file_idx
            ),
            file_idx,
        ))
    }

    /// Delete managed torrents and their files, skipping any whose ID is in `active`.
    pub async fn delete_unused_with_files(
        &self,
        active: &std::collections::HashSet<usize>,
    ) -> Result<usize> {
        // Match the lock order used by pause/delete to avoid deadlocking cleanup.
        let background = self
            .background_downloads
            .lock()
            .await;
        let leases = self
            .leases
            .lock()
            .await;
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let ids: Vec<_> = api
            .api_torrent_list()
            .torrents
            .into_iter()
            .filter(|torrent| {
                !leases
                    .get(
                        &torrent
                            .info_hash
                            .to_ascii_lowercase(),
                    )
                    .is_some_and(|lease| lease.strong_count() != 0)
            })
            .filter(|torrent| {
                !background.contains_key(
                    &torrent
                        .info_hash
                        .to_ascii_lowercase(),
                )
            })
            .filter_map(|t| t.id)
            .filter(|id| !active.contains(id))
            .collect();
        let count = ids.len();
        for id in ids {
            if let Err(e) = api
                .api_torrent_action_delete(TorrentIdOrHash::Id(id))
                .await
            {
                warn!(id, "failed to delete torrent: {e:#}");
            }
        }
        Ok(count)
    }

    /// Parse the torrent ID out of a librqbit stream URL.
    /// Format: `http://127.0.0.1:{port}/torrents/{id}/stream/{file_idx}`
    pub fn torrent_id_from_url(url: &str) -> Option<usize> {
        let after_host = url
            .split_once("//")?
            .1
            .split_once('/')?
            .1;
        let mut parts = after_host.splitn(3, '/');
        if parts.next()? != "torrents" {
            return None;
        }
        parts
            .next()?
            .parse()
            .ok()
    }

    /// Apply upload/download speed limits.  0 = no limit (for download) or
    /// effectively-disabled (for upload — 1 bps is used since the API requires
    /// `NonZeroU32`).
    pub fn update_limits(&self, upload_kbps: i64, download_kbps: i64) {
        use std::num::NonZeroU32;
        // upload: 0 means "don't seed" — clamp to 1 bps (librqbit requires NonZero)
        let upload = NonZeroU32::new(if upload_kbps <= 0 {
            1
        } else {
            (upload_kbps as u32).saturating_mul(1024)
        });
        // download: 0 means unlimited → None
        let download = if download_kbps <= 0 {
            None
        } else {
            NonZeroU32::new((download_kbps as u32).saturating_mul(1024))
        };
        self.session
            .ratelimits
            .set_upload_bps(upload);
        self.session
            .ratelimits
            .set_download_bps(download);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackgroundDownload {
    owner_id: String,
    info_hash: String,
    magnet: String,
    file_idx: usize,
    file_name: String,
    total_bytes: u64,
    paused: bool,
    created_at: u64,
}

#[derive(Clone, Copy, Debug)]
struct BackgroundLimits {
    max_active: usize,
    max_bytes: u64,
    max_age_hours: u64,
}

impl Default for BackgroundLimits {
    fn default() -> Self {
        Self {
            max_active: 2,
            max_bytes: 32 * 1024 * 1024 * 1024,
            max_age_hours: 168,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BackgroundDownloadStatus {
    pub info_hash: String,
    pub file_idx: usize,
    pub file_name: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub peers: usize,
    pub download_mib_s: f64,
    pub paused: bool,
    pub created_at: u64,
}

#[cfg(unix)]
fn filesystem_free_bytes(path: &std::path::Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(
        path.as_os_str()
            .as_bytes(),
    )?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("reading torrent filesystem free space");
    }
    let stats = unsafe { stats.assume_init() };
    Ok(stats
        .f_bavail
        .saturating_mul(stats.f_frsize as u64))
}

#[cfg(not(unix))]
fn filesystem_free_bytes(_path: &std::path::Path) -> Result<u64> {
    Ok(u64::MAX)
}

fn unix_time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl TorrentManager {
    async fn save_background_downloads(
        &self,
        jobs: &std::collections::HashMap<String, BackgroundDownload>,
    ) -> Result<()> {
        let mut entries: Vec<_> = jobs
            .values()
            .cloned()
            .collect();
        entries.sort_by(|a, b| {
            a.info_hash
                .cmp(&b.info_hash)
        });
        let contents = serde_json::to_vec_pretty(&entries)?;
        let parent = self
            .background_downloads_path
            .parent()
            .context("background download path has no parent")?;
        tokio::fs::create_dir_all(parent).await?;
        let temporary = self
            .background_downloads_path
            .with_extension("json.tmp");
        tokio::fs::write(&temporary, contents).await?;
        tokio::fs::rename(&temporary, &self.background_downloads_path).await?;
        Ok(())
    }

    fn background_status(&self, job: &BackgroundDownload) -> BackgroundDownloadStatus {
        let snapshot = librqbit::Magnet::parse(&job.magnet)
            .ok()
            .and_then(|m| m.as_id20())
            .and_then(|hash| {
                self.session
                    .get(TorrentIdOrHash::Hash(hash))
            })
            .and_then(|handle| {
                torrent_stream_snapshot(&self.session, handle.id(), job.file_idx)
            });
        BackgroundDownloadStatus {
            info_hash: job
                .info_hash
                .clone(),
            file_idx: job.file_idx,
            file_name: job
                .file_name
                .clone(),
            total_bytes: job.total_bytes,
            downloaded_bytes: snapshot
                .as_ref()
                .map_or(0, |s| s.selected_file_bytes),
            peers: snapshot
                .as_ref()
                .map_or(0, |s| s.live_peers),
            download_mib_s: snapshot
                .as_ref()
                .map_or(0.0, |s| s.torrent_download_mib_s),
            paused: job.paused,
            created_at: job.created_at,
        }
    }

    pub async fn list_background_downloads(
        &self,
        owner_id: &str,
    ) -> Vec<BackgroundDownloadStatus> {
        let jobs = self
            .background_downloads
            .lock()
            .await;
        let mut items: Vec<_> = jobs
            .values()
            .filter(|job| job.owner_id == owner_id)
            .map(|job| self.background_status(job))
            .collect();
        items.sort_by_key(|item| item.created_at);
        items
    }

    pub async fn create_background_download(
        &self,
        owner_id: &str,
        magnet: &str,
        advertised_height: u32,
    ) -> Result<BackgroundDownloadStatus> {
        anyhow::ensure!(
            advertised_height >= 720,
            "background downloads require >=720p source"
        );
        let hash = librqbit::Magnet::parse(magnet)
            .context("invalid torrent magnet")?
            .as_id20()
            .context("torrent magnet has no info hash")?
            .as_string();
        {
            let jobs = self
                .background_downloads
                .lock()
                .await;
            if let Some(existing) = jobs.get(&hash) {
                anyhow::ensure!(
                    existing.owner_id == owner_id,
                    "torrent owned by another user"
                );
                return Ok(self.background_status(existing));
            }
            anyhow::ensure!(
                jobs.values()
                    .filter(|job| !job.paused)
                    .count()
                    < self
                        .background_limits
                        .max_active,
                "background download concurrency limit reached"
            );
        }
        let (_, file_idx) = self
            .resolve_url(magnet)
            .await?;
        let files = self
            .managed_torrent_files(&hash)
            .context("torrent metadata unavailable")?;
        let chosen = files
            .get(file_idx)
            .context("torrent file not found")?;
        anyhow::ensure!(
            is_video_file(&chosen.name),
            "selected torrent file is not video"
        );
        anyhow::ensure!(chosen.length > 0, "selected torrent file is empty");
        let mut jobs = self
            .background_downloads
            .lock()
            .await;
        if let Some(existing) = jobs.get(&hash) {
            anyhow::ensure!(
                existing.owner_id == owner_id,
                "torrent owned by another user"
            );
            return Ok(self.background_status(existing));
        }
        anyhow::ensure!(
            jobs.values()
                .filter(|job| !job.paused)
                .count()
                < self
                    .background_limits
                    .max_active,
            "background download concurrency limit reached"
        );
        let reserved_bytes: u64 = jobs
            .values()
            .map(|job| job.total_bytes)
            .sum();
        anyhow::ensure!(
            chosen.length
                <= self
                    .background_limits
                    .max_bytes
                    .saturating_sub(reserved_bytes),
            "background download disk budget exceeded"
        );
        // Reserve enough room for the selected file on the *torrent* filesystem,
        // not just in the configured aggregate reservation counter.
        const FREE_SPACE_RESERVE: u64 = 2 * 1024 * 1024 * 1024;
        let free_bytes = filesystem_free_bytes(&self.torrent_data_path)?;
        anyhow::ensure!(
            chosen.length <= free_bytes.saturating_sub(FREE_SPACE_RESERVE),
            "insufficient free disk space for background download"
        );
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let torrent_id = self
            .session
            .get(TorrentIdOrHash::Hash(
                librqbit::Magnet::parse(magnet)?
                    .as_id20()
                    .context("missing hash")?,
            ))
            .context("torrent disappeared")?
            .id();
        api.api_torrent_action_update_only_files(
            TorrentIdOrHash::Id(torrent_id),
            &std::collections::HashSet::from([file_idx]),
        )
        .await?;
        let job = BackgroundDownload {
            owner_id: owner_id.to_owned(),
            info_hash: hash.clone(),
            magnet: magnet.to_owned(),
            file_idx,
            file_name: chosen
                .name
                .clone(),
            total_bytes: chosen.length,
            paused: false,
            created_at: unix_time(),
        };
        jobs.insert(hash.clone(), job.clone());
        if let Err(error) = self
            .save_background_downloads(&jobs)
            .await
        {
            jobs.remove(&hash);
            return Err(error);
        }
        Ok(self.background_status(&job))
    }

    pub async fn set_background_download_paused(
        &self,
        owner_id: &str,
        hash: &str,
        paused: bool,
    ) -> Result<BackgroundDownloadStatus> {
        let hash = normalized_info_hash(hash);
        let mut jobs = self
            .background_downloads
            .lock()
            .await;
        let mut job = jobs
            .get(&hash)
            .context("background download not found")?
            .clone();
        anyhow::ensure!(
            job.owner_id == owner_id,
            "background download not owned by user"
        );
        anyhow::ensure!(
            unix_time().saturating_sub(job.created_at)
                <= self
                    .background_limits
                    .max_age_hours
                    .saturating_mul(3600),
            "background download expired"
        );
        if !paused {
            anyhow::ensure!(
                jobs.values()
                    .filter(|other| !other.paused && other.info_hash != hash)
                    .count()
                    < self
                        .background_limits
                        .max_active,
                "background download concurrency limit reached"
            );
        }
        if job.paused == paused {
            return Ok(self.background_status(&job));
        }
        let leases = self
            .leases
            .lock()
            .await;
        anyhow::ensure!(
            !leases
                .get(&hash)
                .is_some_and(|lease| lease.strong_count() > 0),
            "cannot pause a torrent currently in use by playback"
        );
        let handle = self
            .session
            .get(TorrentIdOrHash::Hash(
                librqbit::Magnet::parse(&job.magnet)?
                    .as_id20()
                    .context("invalid stored hash")?,
            ))
            .context("torrent not currently managed")?;
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        let id = TorrentIdOrHash::Id(handle.id());
        if paused {
            api.api_torrent_action_update_only_files(id, &Default::default())
                .await?;
            api.api_torrent_action_pause(id)
                .await?;
        } else {
            api.api_torrent_action_update_only_files(
                id,
                &std::collections::HashSet::from([job.file_idx]),
            )
            .await?;
            api.api_torrent_action_start(id)
                .await?;
        }
        job.paused = paused;
        jobs.insert(hash.clone(), job.clone());
        self.save_background_downloads(&jobs)
            .await?;
        Ok(self.background_status(&job))
    }

    pub async fn delete_background_download(
        &self,
        owner_id: &str,
        hash: &str,
    ) -> Result<()> {
        let hash = normalized_info_hash(hash);
        let mut jobs = self
            .background_downloads
            .lock()
            .await;
        let existing = jobs
            .get(&hash)
            .context("background download not found")?;
        anyhow::ensure!(
            existing.owner_id == owner_id,
            "background download not owned by user"
        );
        let leases = self
            .leases
            .lock()
            .await;
        anyhow::ensure!(
            !leases
                .get(&hash)
                .is_some_and(|lease| lease.strong_count() > 0),
            "cannot delete a torrent currently in use by playback"
        );
        if let Some(torrent) = Api::new(
            self.session
                .clone(),
            None,
            None,
        )
        .api_torrent_list()
        .torrents
        .into_iter()
        .find(|torrent| {
            torrent
                .info_hash
                .eq_ignore_ascii_case(&hash)
        }) {
            if let Some(id) = torrent.id {
                Api::new(
                    self.session
                        .clone(),
                    None,
                    None,
                )
                .api_torrent_action_delete(TorrentIdOrHash::Id(id))
                .await?;
            }
        }
        jobs.remove(&hash);
        self.save_background_downloads(&jobs)
            .await?;
        Ok(())
    }

    async fn restore_background_downloads(&self) -> Result<()> {
        let mut jobs = self
            .background_downloads
            .lock()
            .await;
        let expiration = self
            .background_limits
            .max_age_hours
            .saturating_mul(3600);
        let expired: Vec<String> = jobs
            .values()
            .filter(|job| unix_time().saturating_sub(job.created_at) > expiration)
            .map(|job| {
                job.info_hash
                    .clone()
            })
            .collect();
        let api = Api::new(
            self.session
                .clone(),
            None,
            None,
        );
        for hash in expired {
            let managed = api
                .api_torrent_list()
                .torrents
                .into_iter()
                .find(|torrent| {
                    torrent
                        .info_hash
                        .eq_ignore_ascii_case(&hash)
                })
                .and_then(|torrent| torrent.id);
            if let Some(id) = managed {
                if let Err(error) = api
                    .api_torrent_action_delete(TorrentIdOrHash::Id(id))
                    .await
                {
                    warn!(%hash, "failed to purge expired background torrent: {error:#}");
                    continue;
                }
            }
            jobs.remove(&hash);
        }
        let mut active = 0;
        let mut oldest_first: Vec<_> = jobs
            .keys()
            .cloned()
            .collect();
        oldest_first.sort_by_key(|hash| {
            jobs.get(hash)
                .map_or(u64::MAX, |job| job.created_at)
        });
        for hash in &oldest_first {
            let job = jobs
                .get_mut(hash)
                .expect("saved key");
            if !job.paused {
                if active
                    >= self
                        .background_limits
                        .max_active
                {
                    job.paused = true;
                } else {
                    active += 1;
                }
            }
        }
        for hash in oldest_first {
            let job = jobs
                .get(&hash)
                .expect("saved key");
            let options = AddTorrentOptions {
                paused: job.paused,
                only_files: Some(if job.paused {
                    vec![]
                } else {
                    vec![job.file_idx]
                }),
                ..Default::default()
            };
            let added = self
                .session
                .add_torrent(AddTorrent::from_url(&job.magnet), Some(options))
                .await;
            let handle = match added {
                Ok(
                    AddTorrentResponse::Added(_, handle)
                    | AddTorrentResponse::AlreadyManaged(_, handle),
                ) => handle,
                Ok(AddTorrentResponse::ListOnly(_)) => continue,
                Err(error) => {
                    warn!(hash = %job.info_hash, "background download restore failed: {error:#}");
                    continue;
                }
            };
            match tokio::time::timeout(
                Duration::from_secs(30),
                handle.wait_until_initialized(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(hash = %job.info_hash, "background download metadata failed: {error:#}");
                    continue;
                }
                Err(error) => {
                    warn!(hash = %job.info_hash, "background download metadata timed out: {error}");
                    continue;
                }
            }
            let api = Api::new(
                self.session
                    .clone(),
                None,
                None,
            );
            let files = std::collections::HashSet::from_iter(if job.paused {
                None
            } else {
                Some(job.file_idx)
            });
            if let Err(error) = api
                .api_torrent_action_update_only_files(
                    TorrentIdOrHash::Id(handle.id()),
                    &files,
                )
                .await
            {
                warn!(hash = %job.info_hash, "background download selection restore failed: {error:#}");
            }
            if job.paused {
                if let Err(error) = api
                    .api_torrent_action_pause(TorrentIdOrHash::Id(handle.id()))
                    .await
                {
                    warn!(hash = %job.info_hash, "background download pause restore failed: {error:#}");
                }
            } else if let Err(error) = api
                .api_torrent_action_start(TorrentIdOrHash::Id(handle.id()))
                .await
            {
                warn!(hash = %job.info_hash, "background download resume failed: {error:#}");
            }
        }
        self.save_background_downloads(&jobs)
            .await?;
        Ok(())
    }
}

impl crate::stream::StreamInfo {
    /// Return supported subtitle files associated with this stream when its
    /// torrent metadata has already been initialized. This never starts a
    /// download; subtitle bytes are requested only if a client selects a track.
    pub(crate) fn subtitle_sidecars(
        &self,
        torrent: &TorrentManager,
    ) -> Vec<crate::addons::SubtitleInfo> {
        let crate::stream::StreamDescriptor::Torrent {
            info_hash,
            file_hint,
            file_idx,
            trackers,
        } = &self.descriptor
        else {
            return Vec::new();
        };
        let Some(files) = torrent.managed_torrent_files(info_hash) else {
            return Vec::new();
        };
        let Ok(selected_idx) =
            select_file_index(&files, *file_idx, file_hint.as_deref())
        else {
            return Vec::new();
        };
        select_sidecar_subtitles(&files, selected_idx)
            .into_iter()
            .map(|sidecar| crate::addons::SubtitleInfo {
                id: format!("torrent:{info_hash}:{}", sidecar.file_idx),
                filename: Some(
                    sidecar
                        .path
                        .clone(),
                ),
                url: Some(crate::stream::StreamDescriptor::Torrent {
                    info_hash: info_hash.clone(),
                    file_hint: Some(sidecar.path),
                    file_idx: Some(sidecar.file_idx),
                    trackers: trackers.clone(),
                }),
                lang: sidecar.language,
                is_forced: sidecar.is_forced,
                is_hi: sidecar.is_hearing_impaired,
                from_trusted: None,
                ai_translated: None,
            })
            .collect()
    }
}

fn stream_only_options() -> AddTorrentOptions {
    AddTorrentOptions {
        // An empty selection leaves piece ownership to librqbit's HTTP
        // FileStream. Metadata lookup therefore cannot start downloading or
        // allocating every file in a bundle.
        only_files: Some(Vec::new()),
        ..Default::default()
    }
}

fn is_video_file(name: &str) -> bool {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    remux_sdks::remux::VideoContainer::parse_known(&ext).is_some()
}

fn is_supported_sidecar_subtitle(name: &str) -> bool {
    std::path::Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("srt"))
}

fn subtitle_language_from_name(name: &str) -> Option<String> {
    let stem = std::path::Path::new(name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default();
    stem.split(|character: char| !character.is_ascii_alphabetic())
        .rev()
        .find_map(|token| {
            let lowercase = token.to_ascii_lowercase();
            if matches!(
                lowercase.as_str(),
                "cc" | "default"
                    | "forced"
                    | "foreign"
                    | "sdh"
                    | "signs"
                    | "hearing"
                    | "impaired"
                    | "hearingimpaired"
            ) || token == "HI"
            {
                return None;
            }

            isolang::Language::from_639_1(&lowercase)
                .or_else(|| isolang::Language::from_639_3(&lowercase))
                .or_else(|| {
                    remux_sdks::remux::common_audio_languages()
                        .iter()
                        .find(|(code, _)| code.eq_ignore_ascii_case(&lowercase))
                        .and_then(|(_, name)| isolang::Language::from_name(name))
                })
                .or_else(|| {
                    isolang::languages().find(|language| {
                        language
                            .to_name()
                            .eq_ignore_ascii_case(&lowercase)
                    })
                })
                .and_then(|language| language.to_639_1())
                .map(str::to_string)
        })
}

fn subtitle_stem_matches_video(selected_stem: &str, subtitle_stem: &str) -> bool {
    !selected_stem.is_empty()
        && subtitle_stem
            .strip_prefix(selected_stem)
            .is_some_and(|suffix| {
                suffix.is_empty()
                    || suffix
                        .chars()
                        .next()
                        .is_some_and(|character| !character.is_ascii_alphanumeric())
            })
}

fn select_sidecar_subtitles(
    files: &[TorrentFile],
    selected_idx: usize,
) -> Vec<SidecarSubtitleFile> {
    let Some(selected) = files.get(selected_idx) else {
        return Vec::new();
    };
    let selected_name = selected
        .name
        .replace('\\', "/");
    let selected_path = std::path::Path::new(&selected_name);
    let selected_parent = selected_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new(""));
    let selected_stem = selected_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let videos_in_parent = files
        .iter()
        .filter(|file| {
            if !is_video_file(&file.name) {
                return false;
            }
            let normalized = file
                .name
                .replace('\\', "/");
            std::path::Path::new(&normalized)
                .parent()
                .unwrap_or_else(|| std::path::Path::new(""))
                == selected_parent
        })
        .count();

    files
        .iter()
        .enumerate()
        .filter_map(|(file_idx, file)| {
            if file.length == 0
                || file.length > 20 * 1024 * 1024
                || !is_supported_sidecar_subtitle(&file.name)
            {
                return None;
            }
            let normalized = file
                .name
                .replace('\\', "/");
            let subtitle_path = std::path::Path::new(&normalized);
            let subtitle_parent = subtitle_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new(""));
            let subtitle_stem = subtitle_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            let basename_matches =
                subtitle_stem_matches_video(&selected_stem, &subtitle_stem);
            let same_directory = subtitle_parent == selected_parent;
            let in_subtitle_directory = subtitle_parent
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.eq_ignore_ascii_case("subs")
                        || name.eq_ignore_ascii_case("subtitles")
                })
                && subtitle_parent
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new(""))
                    == selected_parent;
            // Generic names such as `Subs/2_English.srt` are safe only when
            // the selected directory contains a single video. Filename-matched
            // subtitles remain safe for episode packs and movie collections.
            if !basename_matches
                && !(videos_in_parent == 1 && (same_directory || in_subtitle_directory))
            {
                return None;
            }
            let tokens: Vec<&str> = subtitle_stem
                .split(|character: char| !character.is_ascii_alphanumeric())
                .filter(|token| !token.is_empty())
                .collect();
            Some(SidecarSubtitleFile {
                file_idx,
                path: normalized.clone(),
                language: subtitle_language_from_name(&normalized),
                is_forced: tokens
                    .iter()
                    .any(|token| matches!(*token, "forced" | "foreign" | "signs")),
                is_hearing_impaired: tokens
                    .iter()
                    .any(|token| {
                        matches!(*token, "sdh" | "cc" | "hi" | "hearingimpaired")
                    }),
            })
        })
        .collect()
}

fn select_file_index(
    files: &[TorrentFile],
    requested_idx: Option<usize>,
    wanted_file: Option<&str>,
) -> Result<usize> {
    if files.is_empty() {
        anyhow::bail!("torrent contains no files");
    }

    if let Some(wanted) = wanted_file {
        let wanted_is_sidecar = is_supported_sidecar_subtitle(wanted);
        if let Some((index, _)) = files
            .iter()
            .enumerate()
            .find(|(_, file)| {
                (is_video_file(&file.name)
                    || (wanted_is_sidecar && is_supported_sidecar_subtitle(&file.name)))
                    && (file
                        .name
                        .eq_ignore_ascii_case(wanted)
                        || std::path::Path::new(&file.name)
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.eq_ignore_ascii_case(wanted)))
            })
        {
            return Ok(index);
        }
    }

    if let Some(index) = requested_idx.filter(|index| {
        files
            .get(*index)
            .is_some_and(|file| is_video_file(&file.name))
    }) {
        return Ok(index);
    }

    let mut videos: Vec<(usize, &TorrentFile)> = files
        .iter()
        .enumerate()
        .filter(|(_, file)| is_video_file(&file.name))
        .collect();
    if videos.len() == 1 {
        return Ok(videos[0].0);
    }

    videos.sort_by_key(|(_, file)| std::cmp::Reverse(file.length));
    if let [largest, second, ..] = videos.as_slice() {
        // Samples and extras are common, but similarly sized videos indicate
        // a real bundle and require an exact provider hint.
        if largest
            .1
            .length
            >= second
                .1
                .length
                .saturating_mul(2)
        {
            return Ok(largest.0);
        }
    }

    match requested_idx {
        Some(index) => anyhow::bail!(
            "torrent file index {index} does not identify a video and no unique video could be selected"
        ),
        None => anyhow::bail!(
            "torrent contains {} video files; a valid file index or filename is required",
            videos.len()
        ),
    }
}

/// Extract the `file=` query parameter we encode into our magnet URIs.
fn parse_file_param(magnet: &str) -> Option<String> {
    let query = magnet
        .split_once('?')?
        .1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "file")
        .map(|(_, v)| v.into_owned())
}

/// Extract the `file_idx=` query parameter we encode into our magnet URIs.
fn parse_file_idx_param(magnet: &str) -> Option<usize> {
    let query = magnet
        .split_once('?')?
        .1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "file_idx")
        .and_then(|(_, v)| {
            v.parse()
                .ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn background_download_state_survives_restart_and_protects_cache() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir
            .path()
            .join("data");
        let cache = dir
            .path()
            .join("cache");
        let manager =
            TorrentManager::new(data.clone(), cache.clone(), None, true, None)
                .await
                .unwrap();
        let bytes = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:....................ee";
        manager
            .session
            .add_torrent(
                AddTorrent::from_bytes(&bytes[..]),
                Some(AddTorrentOptions {
                    paused: true,
                    disable_trackers: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let api = Api::new(
            manager
                .session
                .clone(),
            None,
            None,
        );
        let hash = api
            .api_torrent_list()
            .torrents[0]
            .info_hash
            .clone();
        let magnet = format!("magnet:?xt=urn:btih:{hash}");
        let job = BackgroundDownload {
            owner_id: "test".into(),
            info_hash: hash.clone(),
            magnet,
            file_idx: 0,
            file_name: "fixture.mkv".into(),
            total_bytes: 1,
            paused: true,
            created_at: unix_time(),
        };
        {
            let mut jobs = manager
                .background_downloads
                .lock()
                .await;
            jobs.insert(hash.clone(), job);
            manager
                .save_background_downloads(&jobs)
                .await
                .unwrap();
        }
        assert_eq!(
            manager
                .delete_unused_with_files(&Default::default())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            manager
                .list_background_downloads("test")
                .await
                .len(),
            1
        );
        let running = manager
            .set_background_download_paused("test", &hash, false)
            .await
            .unwrap();
        assert!(!running.paused);
        let paused = manager
            .set_background_download_paused("test", &hash, true)
            .await
            .unwrap();
        assert!(paused.paused);
        let running = manager
            .set_background_download_paused("test", &hash, false)
            .await
            .unwrap();
        assert!(!running.paused);
        let paused = manager
            .set_background_download_paused("test", &hash, true)
            .await
            .unwrap();
        assert!(paused.paused);
        let running = manager
            .set_background_download_paused("test", &hash, false)
            .await
            .unwrap();
        assert!(!running.paused);
        let paused = manager
            .set_background_download_paused("test", &hash, true)
            .await
            .unwrap();
        assert!(paused.paused);
        let running = manager
            .set_background_download_paused("test", &hash, false)
            .await
            .unwrap();
        assert!(!running.paused);
        let paused = manager
            .set_background_download_paused("test", &hash, true)
            .await
            .unwrap();
        assert!(paused.paused);
        manager
            .shutdown()
            .await;
        drop(manager);
        let reopened = TorrentManager::new(data, cache, None, true, None)
            .await
            .unwrap();
        let restored = reopened
            .list_background_downloads("test")
            .await;
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].info_hash, hash);
        assert!(restored[0].paused);
        reopened
            .shutdown()
            .await;
    }

    #[tokio::test]
    async fn cleanup_waits_for_sessions_and_readers_and_respects_reacquisition() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(
            TorrentManager::new(
                dir.path()
                    .join("data"),
                dir.path()
                    .join("cache"),
                None,
                true,
                None,
            )
            .await
            .unwrap(),
        );
        let (server, guard, token) =
            crate::integration_test::authenticated_server().await;
        let sessions = &guard
            .0
            .sessions;
        // A tiny paused torrent exercises real librqbit deletion without
        // requiring DHT, trackers, peers, or a downloaded media fixture.
        manager.session.add_torrent(
            AddTorrent::from_bytes(&b"d4:infod6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:....................ee"[..]),
            Some(AddTorrentOptions { paused: true, disable_trackers: true, ..Default::default() }),
        ).await.unwrap();
        let api = Api::new(
            manager
                .session
                .clone(),
            None,
            None,
        );
        let hash = api
            .api_torrent_list()
            .torrents[0]
            .info_hash
            .clone();
        let hash = hash.as_str();
        let other = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";

        // Requests may precede playback reports; these references must still
        // be released by stop. Two viewers share the very same torrent.
        sessions
            .retain_torrent("viewer-a", &manager, hash)
            .await;
        sessions
            .retain_torrent("viewer-b", &manager, hash)
            .await;
        let reader = manager
            .acquire(hash)
            .await;
        let weak = Arc::downgrade(&reader);
        sessions
            .stop("viewer-a")
            .await;
        manager
            .delete_if_unused(hash)
            .await
            .unwrap();
        assert!(
            weak.upgrade()
                .is_some()
        );
        assert_eq!(
            api.api_torrent_list()
                .torrents
                .len(),
            1
        );
        assert_eq!(
            manager
                .delete_unused_with_files(&Default::default())
                .await
                .unwrap(),
            0
        );
        drop(reader);
        assert!(
            weak.upgrade()
                .is_some(),
            "viewer-b still owns the torrent"
        );

        // Changing a selected source doesn't change the recorded reference
        // for an existing session; both actual sources are retained.
        sessions
            .retain_torrent("viewer-b", &manager, other)
            .await;
        sessions
            .stop("viewer-b")
            .await;
        assert!(
            weak.upgrade()
                .is_none()
        );
        let next_episode = manager
            .acquire(hash)
            .await;
        manager
            .delete_if_unused(hash)
            .await
            .unwrap();
        assert!(
            manager
                .leases
                .lock()
                .await
                .contains_key(hash)
        );
        assert_eq!(
            api.api_torrent_list()
                .torrents
                .len(),
            1
        );
        drop(next_episode);
        manager
            .delete_if_unused(hash)
            .await
            .unwrap();
        manager
            .delete_if_unused(other)
            .await
            .unwrap();
        assert!(
            manager
                .leases
                .lock()
                .await
                .is_empty()
        );
        assert!(
            api.api_torrent_list()
                .torrents
                .is_empty()
        );

        // Exercise real playback reports too: a start replaces the previous
        // session on this device, and a stop releases the replacement.
        let media = crate::integration_test::insert_test_source(&guard.0).await;
        let auth = crate::integration_test::auth_header_with_token(&token);
        for id in ["old-session", "new-session"] {
            sessions
                .retain_torrent(id, &manager, hash)
                .await;
            server
                .post("/sessions/playing")
                .add_header(
                    http::header::AUTHORIZATION,
                    http::HeaderValue::from_str(&auth).unwrap(),
                )
                .json(&serde_json::json!({ "ItemId": media.id, "PlaySessionId": id }))
                .await
                .assert_status(http::StatusCode::NO_CONTENT);
        }
        assert!(
            sessions
                .get("old-session")
                .is_none()
        );
        let lease = manager
            .acquire(hash)
            .await;
        let weak = Arc::downgrade(&lease);
        drop(lease);
        server.post("/sessions/playing/stopped")
            .add_header(http::header::AUTHORIZATION, http::HeaderValue::from_str(&auth).unwrap())
            .json(&serde_json::json!({ "ItemId": media.id, "PlaySessionId": "new-session" }))
            .await.assert_status(http::StatusCode::NO_CONTENT);
        assert!(
            weak.upgrade()
                .is_none(),
            "both removed sessions must release their references"
        );
        sessions
            .retain_torrent("orphan-request", &manager, hash)
            .await;
        let lease = manager
            .acquire(hash)
            .await;
        let weak = Arc::downgrade(&lease);
        drop(lease);
        let cleanup = sessions
            .clone()
            .spawn_cleanup_task(Duration::from_millis(1), Duration::ZERO);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak
                .upgrade()
                .is_some()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("unclaimed requests must expire");
        cleanup.abort();
        manager
            .shutdown()
            .await;
    }

    fn file(name: &str, length: u64) -> TorrentFile {
        TorrentFile {
            name: name.to_string(),
            length,
        }
    }

    #[test]
    fn bundle_uses_exact_requested_file() {
        let files = vec![
            file("Bundle/Movie.One.mkv", 2_000),
            file("Bundle/Movie.Two.mkv", 2_100),
            file("Bundle/Movie.Three.mkv", 1_900),
        ];

        assert_eq!(
            select_file_index(&files, Some(0), Some("Movie.Two.mkv")).unwrap(),
            1
        );
        assert_eq!(select_file_index(&files, Some(2), None).unwrap(), 2);
    }

    #[test]
    fn bundle_rejects_ambiguous_or_non_video_indexes() {
        let files = vec![
            file("Bundle/Movie.One.mkv", 2_000),
            file("Bundle/release.nfo", 1),
            file("Bundle/Movie.Two.mkv", 2_100),
        ];

        assert!(select_file_index(&files, Some(1), None).is_err());
        assert!(select_file_index(&files, Some(99), None).is_err());
        assert!(select_file_index(&files, None, None).is_err());
    }

    #[test]
    fn single_feature_release_ignores_samples() {
        let files = vec![
            file("Release/sample.mkv", 100),
            file("Release/Movie.mkv", 2_000),
            file("Release/subtitles.srt", 2),
        ];

        assert_eq!(select_file_index(&files, None, None).unwrap(), 1);
    }

    #[test]
    fn metadata_lookup_selects_no_files_for_download() {
        assert_eq!(stream_only_options().only_files, Some(Vec::new()));
    }

    #[test]
    fn exact_sidecar_hint_selects_the_subtitle_file() {
        let files = vec![
            file("Movie.mkv", 2_000),
            file("Subs/English.srt", 20),
            file("release.nfo", 1),
        ];

        assert_eq!(
            select_file_index(&files, Some(1), Some("Subs/English.srt")).unwrap(),
            1
        );
    }

    #[test]
    fn single_movie_release_exposes_supported_subtitle_directory() {
        let files = vec![
            file("Movie.mkv", 2_000),
            file("Subs/2_English.srt", 20),
            file("Subs/3_English.srt", 25),
            file("Subs/4_English.ass", 30),
        ];

        let subtitles = select_sidecar_subtitles(&files, 0);
        assert_eq!(subtitles.len(), 2);
        assert_eq!(subtitles[0].file_idx, 1);
        assert_eq!(
            subtitles[0]
                .language
                .as_deref(),
            Some("en")
        );
        assert_eq!(subtitles[1].file_idx, 2);
    }

    #[test]
    fn movie_bundle_rejects_ambiguous_generic_subtitles() {
        let files = vec![
            file("Movie.One.mkv", 2_000),
            file("Movie.Two.mkv", 2_100),
            file("Subs/2_English.srt", 20),
        ];

        assert!(select_sidecar_subtitles(&files, 0).is_empty());
        assert!(select_sidecar_subtitles(&files, 1).is_empty());
    }

    #[test]
    fn episode_pack_uses_only_filename_matched_subtitles() {
        let files = vec![
            file("Show.S01E01.mkv", 1_000),
            file("Show.S01E01.en.HI.forced.srt", 10),
            file("Show.S01E02.mkv", 1_000),
            file("Show.S01E02.en.srt", 10),
            file("Show.S01E010.en.srt", 10),
        ];

        let subtitles = select_sidecar_subtitles(&files, 0);
        assert_eq!(subtitles.len(), 1);
        assert_eq!(subtitles[0].file_idx, 1);
        assert_eq!(
            subtitles[0]
                .language
                .as_deref(),
            Some("en")
        );
        assert!(subtitles[0].is_forced);
        assert!(subtitles[0].is_hearing_impaired);
    }
}
