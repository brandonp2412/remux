use crate::ResultExt;
use async_trait::async_trait;
use axum::{
    body::{Body, Bytes},
    http::HeaderMap,
    response::Response,
};
use axum_anyhow::ApiResult as Result;
use futures_util::{StreamExt, TryStreamExt};
use nutype::nutype;
use std::sync::Arc;
use std::{io, path::PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::io::{ReaderStream, StreamReader};
use uuid::Uuid;

use crate::AppState;

/// A BitTorrent tracker announce URL with a supported scheme and host. UDP
/// trackers may omit a path, while HTTP trackers must name an announce path.
/// Unrelated addon `sources` entries are rejected.
#[nutype(
    validate(predicate = is_tracker_url),
    derive(Clone, Debug, PartialEq, Eq, Hash, AsRef, Serialize, Deserialize)
)]
pub struct TrackerUrl(String);

/// Whether `s` is a usable HTTP(S) or UDP tracker URL.
pub fn is_tracker_url(s: &str) -> bool {
    let Ok(url) = url::Url::parse(s.trim()) else {
        return false;
    };
    if url
        .host_str()
        .is_none()
    {
        return false;
    }
    match url.scheme() {
        "udp" => url
            .port()
            .is_some(),
        "http" | "https" => {
            !url.path()
                .is_empty()
                && url.path() != "/"
        }
        _ => false,
    }
}

/// Whether `host` names an address only reachable from the network remux
/// itself runs on: a loopback/private/link-local/CGNAT IP, a bare
/// single-label hostname (Docker Compose/Kubernetes service names), or a
/// well-known internal-only suffix (`.local`, `.internal`, `.lan`, …).
/// Used to catch stream URLs an addon resolved from inside its own container
/// that a client elsewhere on the network could never reach.
pub fn is_internal_host(host: &str) -> bool {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return is_internal_ip(ip);
    }
    let host = host.trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    if !host.contains('.') {
        return true;
    }
    matches!(
        host.rsplit('.')
            .next()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some(
            "local"
                | "internal"
                | "lan"
                | "home"
                | "docker"
                | "test"
                | "invalid"
                | "localdomain"
                | "localhost"
        )
    )
}

fn is_internal_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || is_cgnat_v4(v4)
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || is_unique_local_v6(v6)
                || is_link_local_v6(v6)
        }
    }
}

/// 100.64.0.0/10, the carrier-grade NAT range Docker/Tailscale sometimes use.
fn is_cgnat_v4(v4: std::net::Ipv4Addr) -> bool {
    let o = v4.octets();
    o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000
}

fn is_unique_local_v6(v6: std::net::Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xfe00) == 0xfc00
}

fn is_link_local_v6(v6: std::net::Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod internal_host_tests {
    use super::is_internal_host;

    #[test]
    fn flags_private_and_loopback_ips() {
        assert!(is_internal_host("127.0.0.1"));
        assert!(is_internal_host("192.168.1.50"));
        assert!(is_internal_host("10.0.10.66"));
        assert!(is_internal_host("172.17.0.2"));
        assert!(is_internal_host("169.254.1.1"));
        assert!(is_internal_host("100.64.0.5"));
        assert!(is_internal_host("::1"));
        assert!(is_internal_host("fc00::1"));
        assert!(is_internal_host("fe80::1"));
    }

    #[test]
    fn flags_bare_and_internal_suffixed_hostnames() {
        assert!(is_internal_host("aiostreams"));
        assert!(is_internal_host("localhost"));
        assert!(is_internal_host("myaddon.local"));
        assert!(is_internal_host("service.internal"));
        assert!(is_internal_host("box.lan"));
        assert!(is_internal_host("service.localhost"));
    }

    #[test]
    fn allows_public_ips_and_domains() {
        assert!(!is_internal_host("8.8.8.8"));
        assert!(!is_internal_host("example.com"));
        assert!(!is_internal_host("stream.example.co.uk"));
        assert!(!is_internal_host("aiostreams.example.com"));
    }
}

fn deserialize_tracker_urls<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<TrackerUrl>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = <Vec<String> as serde::Deserialize>::deserialize(deserializer)?;
    Ok(values
        .into_iter()
        .filter_map(|value| TrackerUrl::try_new(value).ok())
        .collect())
}

/// Typed representation of how a stream is accessed (transport mechanism).
///
/// Each variant maps to a [`StreamSource`] implementation via [`into_source`],
/// or for addon-owned streams, to the addon's [`AddonKind::serve_stream`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StreamDescriptor {
    Http {
        url: String,
        /// HTTP request headers to send when fetching this stream.
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        request_headers: std::collections::HashMap<String, String>,
        /// HTTP response headers to forward to the client.
        #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
        response_headers: std::collections::HashMap<String, String>,
    },
    Local(PathBuf),
    Rtsp {
        url: String,
    },
    Torrent {
        info_hash: String,
        /// Filename hint for multi-file torrents (matched by name).
        file_hint: Option<String>,
        /// Direct file index within the torrent (takes precedence over file_hint).
        file_idx: Option<usize>,
        /// Tracker announce URLs (populated from the stream's `sources`).
        #[serde(default, deserialize_with = "deserialize_tracker_urls")]
        trackers: Vec<TrackerUrl>,
    },
    Opendal {
        addon_id: Uuid,
        path: String,
    },
}

impl Default for StreamDescriptor {
    fn default() -> Self {
        Self::Http {
            url: String::new(),
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
    }
}

impl StreamDescriptor {
    /// True only for a genuine on-disk file — reading it costs nothing beyond
    /// local disk I/O. Every other variant (`Http`/`Rtsp`/`Torrent`/`Opendal`)
    /// means network I/O of some form, even when proxied through our own
    /// stream endpoint. Used to gate operations that are cheap for local files
    /// but expensive (a full remote read) for anything else — e.g. on-demand
    /// subtitle extraction.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub fn http(url: impl Into<String>) -> Self {
        Self::Http {
            url: url.into(),
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
    }

    pub fn rtsp(url: impl Into<String>) -> Self {
        Self::Rtsp { url: url.into() }
    }

    /// Input URL/path for ffprobe and ffmpeg (server-side tools).
    /// `Local` → raw filesystem path. `Http` → URL as-is.
    /// `Torrent`/`Opendal` → our stream proxy, which resolves them on demand.
    pub fn server_input(&self, media_id: Uuid, port: u16) -> String {
        match self {
            Self::Http { url, .. } | Self::Rtsp { url } => url.clone(),
            Self::Local(path) => path
                .to_string_lossy()
                .into_owned(),
            Self::Torrent { .. } | Self::Opendal { .. } => {
                format!("http://127.0.0.1:{}/stream/{}", port, media_id)
            }
        }
    }

    /// URL to hand to the Jellyfin client for direct play.
    /// `Http` streams play directly. Everything else routes through our stream proxy
    /// (client can't access local FS; Torrent/Opendal need server-side resolution).
    pub fn client_url(&self, media_id: Uuid, server_base: &str) -> String {
        match self {
            Self::Http { url, .. } => url.clone(),
            _ => format!("{}/stream/{}", server_base.trim_end_matches('/'), media_id),
        }
    }

    /// The raw HTTP URL for `Http` variants, or `None` for everything else.
    pub fn as_http_url(&self) -> Option<&str> {
        match self {
            Self::Http { url, .. } => Some(url),
            _ => None,
        }
    }

    /// If this descriptor is owned by an addon (needs its credentials/config to
    /// serve), return the addon's ID so the endpoint can dispatch to
    /// `AddonKind::serve_stream` instead of `into_source`.
    pub fn addon_id(&self) -> Option<Uuid> {
        match self {
            Self::Opendal { addon_id, .. } => Some(*addon_id),
            _ => None,
        }
    }

    /// Returns `false` only for HTTP streams whose ranged GET yields a 4xx/5xx
    /// status or a network/timeout error. Non-HTTP variants (local, torrent,
    /// opendal) return `true` immediately.
    pub async fn is_alive(&self) -> bool {
        let Self::Http {
            url,
            request_headers,
            ..
        } = self
        else {
            return true;
        };
        let mut request = AVAILABILITY_CLIENT.get(url);
        for (key, value) in request_headers {
            request = request.header(key.as_str(), value.as_str());
        }
        request = request.header(http::header::RANGE, "bytes=0-0");

        match request
            .send()
            .await
        {
            Ok(r) => {
                r.status()
                    .as_u16()
                    < 400
            }
            Err(_) => false,
        }
    }

    /// Instantiate the runtime service for self-contained variants.
    /// Do **not** call this for `Opendal` — those must go through the addon.
    pub fn into_source(self) -> Box<dyn StreamSource> {
        self.into_source_with_expected_bitrate(None, None)
    }

    pub fn into_source_with_expected_bitrate(
        self,
        expected_bitrate_bps: Option<i64>,
        expected_duration_secs: Option<i64>,
    ) -> Box<dyn StreamSource> {
        match self {
            Self::Http {
                url,
                request_headers,
                response_headers,
            } => Box::new(HttpSource {
                url,
                request_headers,
                response_headers,
            }),
            Self::Local(path) => Box::new(LocalSource { path }),
            Self::Torrent {
                info_hash,
                file_hint,
                file_idx,
                trackers,
            } => Box::new(TorrentSource {
                info_hash,
                file_hint,
                file_idx,
                trackers,
                expected_bitrate_bps,
                expected_duration_secs,
            }),
            Self::Rtsp { .. } => {
                panic!("Rtsp descriptors must be served through the transcode path")
            }
            Self::Opendal { .. } => {
                panic!("Opendal descriptors must be served through their addon")
            }
        }
    }
}

/// Combined stream descriptor and provider metadata stored in `db::Media.stream_info`.
///
/// Replaces the old split between `db::Media.url` (transport) and
/// `db::Media.provider_info` (Stremio metadata). All addons populate whichever
/// fields they have; the rest are `None` / empty.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    /// `#[serde(default)]` so deserializing a redacted `remux.provider_info`
    /// (see `to_public_json`, which strips this key before it reaches the
    /// API) doesn't fail — `device_profile.rs`'s `source_stream_info` round-trips
    /// that JSON back into a `StreamInfo` just to read `filename`.
    #[serde(default)]
    pub descriptor: StreamDescriptor,
    /// Filename from the provider (e.g. "Movie.2021.1080p.BluRay.mkv").
    /// Used for resolution matching during probe fallback.
    pub filename: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    /// Addon that produced this stream (stamped by the service layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// UUID of the addon that produced this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addon_id: Option<Uuid>,
    /// Lowercased service identifier from `streamData.service.id` (e.g. "real-debrid").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<String>,
    /// Whether the upstream service confirmed this stream is cached. Missing
    /// metadata is not a cache hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_cached: Option<bool>,
    pub seeders: Option<i64>,
    pub size: Option<i64>,
    pub duration: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtitles: Vec<crate::sdks::stremio::Subtitle>,
    /// Catchup URL template from M3U `catchup-source` attribute.
    /// `{utc}` / `{utcend}` placeholders are substituted at playback time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_source: Option<String>,
    /// Number of days of catchup available (`catchup-days` attribute).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_days: Option<i64>,
    /// Usenet NZB GUID (the `id` query param from the nzb_url). Used for RemuxDB matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_guid: Option<String>,
    /// Usenet indexer name (e.g. "NZBgeek"). Used for RemuxDB matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usenet_indexer: Option<String>,
    /// Raw NZB URL (from AIOStreams streamData). Used for RemuxDB matching via indexer_guid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nzb_url: Option<String>,
    /// Binge group from `behaviorHints.bingeGroup` (e.g. "real-debrid|1080p").
    /// Used as part of the stable dedup key for HTTP streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binge_group: Option<String>,
    /// Upstream addon name from `streamData.addon` (e.g. "BB", "Comet").
    /// Used alongside `service_id` for the addon-level stream filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_addon: Option<String>,
    /// Torrent info-hash for the source release (from AIOStreams streamData).
    /// Stored independently of the descriptor so debrid Http streams can match and submit by hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent_info_hash: Option<String>,
    /// File index within the torrent (from AIOStreams streamData).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent_file_idx: Option<i32>,
    /// Pre-probed codec/bitrate metadata from the addon.
    /// Extracted into `db::Media.probe_data` on conversion; not persisted here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_data: Option<crate::api::MediaSourceInfo>,
}

impl StreamInfo {
    pub fn is_p2p(&self) -> bool {
        matches!(self.descriptor, StreamDescriptor::Torrent { .. })
    }

    /// Redacted view for `remux.provider_info` in API responses. `descriptor`
    /// carries provider credentials (debrid keys in the URL, IPTV
    /// username/password, upstream request headers) that must never reach a
    /// non-admin client — no client reads it, the only consumer is
    /// `device_profile.rs` reading `filename` back out, which stays. Internal
    /// persistence (`db::Media.stream_info`) uses `StreamInfo`'s own
    /// `Serialize` impl directly and keeps `descriptor` intact.
    pub fn to_public_json(&self) -> Option<serde_json::Value> {
        let mut value = serde_json::to_value(self).ok()?;
        if let Some(obj) = value.as_object_mut() {
            obj.remove("descriptor");
        }
        Some(value)
    }

    /// Torrent identity used for RemuxDB lookup and submission.
    ///
    /// A `Torrent` descriptor is authoritative. Any other transport (notably a
    /// debrid `Http` stream) falls back to the identity preserved from
    /// AIOStreams `streamData.torrent`. `None` when the stream has no torrent
    /// identity at all.
    pub fn torrent_identity(&self) -> Option<(&str, Option<i32>)> {
        match &self.descriptor {
            StreamDescriptor::Torrent {
                info_hash,
                file_idx,
                ..
            } => Some((
                info_hash.as_str(),
                // Indices beyond i32 can't be represented in RemuxDB; treat as unknown.
                file_idx.and_then(|i| i32::try_from(i).ok()),
            )),
            _ => self
                .torrent_info_hash
                .as_deref()
                .map(|hash| (hash, self.torrent_file_idx)),
        }
    }

    pub fn resolution_tag(&self) -> Option<String> {
        let parse = |src: &str| {
            crate::db::min_screen_size(&hunch::hunch(src))
                .map(str::to_owned)
                .or_else(|| {
                    src.split(|c: char| !c.is_ascii_alphanumeric())
                        .find_map(|token| {
                            let normalized = token.to_ascii_lowercase();
                            match normalized.as_str() {
                                "4k" => Some("2160p".to_string()),
                                "2160p" | "1440p" | "1080p" | "720p" | "576p"
                                | "480p" | "360p" => Some(normalized),
                                _ => None,
                            }
                        })
                })
        };
        self.filename
            .as_deref()
            .and_then(parse)
            .or_else(|| {
                self.name
                    .as_deref()
                    .and_then(parse)
            })
    }
}

/// A runtime service that can serve stream bytes as an HTTP response.
///
/// Implemented by self-contained variants (`Http`, `Local`, `Torrent`).
/// Addon-owned variants (`Opendal`) are served through `AddonKind::serve_stream`.
#[async_trait]
pub trait StreamSource: Send + Sync {
    async fn serve(&self, state: &AppState, headers: &HeaderMap) -> Result<Response>;
}

static AVAILABILITY_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build availability client")
    });

static STREAM_PROXY_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .user_agent("remux-server/1.0")
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .build()
            .expect("failed to build stream proxy client")
    });

pub struct HttpSource {
    pub url: String,
    pub request_headers: std::collections::HashMap<String, String>,
    pub response_headers: std::collections::HashMap<String, String>,
}

pub struct LocalSource {
    pub path: PathBuf,
}

/// Public trackers used as fallback when a torrent stream provides none.
/// Sourced from https://github.com/ngosang/trackerslist (trackers_best).
const DEFAULT_TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.qu.ax:6969/announce",
    "udp://wepzone.net:6969/announce",
    "udp://tracker.srv00.com:6969/announce",
];

pub struct TorrentSource {
    pub info_hash: String,
    pub file_hint: Option<String>,
    pub file_idx: Option<usize>,
    pub trackers: Vec<TrackerUrl>,
    pub expected_bitrate_bps: Option<i64>,
    pub expected_duration_secs: Option<i64>,
}

impl TorrentSource {
    /// A per-release probe bitrate wins. Otherwise compute average required
    /// bandwidth from the *selected torrent file's* actual byte length and
    /// the title's duration. Without this, an unprobed 30 GB 4K source could
    /// incorrectly pass the generic 1 MiB/s readiness floor.
    fn expected_playback_bitrate_bps(&self, selected_bytes: u64) -> Option<i64> {
        self.expected_bitrate_bps
            .filter(|bitrate| *bitrate > 0)
            .or_else(|| {
                let seconds = u64::try_from(self.expected_duration_secs?).ok()?;
                if seconds < 60 || selected_bytes == 0 {
                    return None;
                }
                i64::try_from(u128::from(selected_bytes) * 8 / u128::from(seconds))
                    .ok()
                    .filter(|bitrate| *bitrate > 0)
            })
    }

    fn to_magnet(&self) -> String {
        let mut m = format!("magnet:?xt=urn:btih:{}", self.info_hash);
        let trackers = &self.trackers;
        if trackers.is_empty() {
            for t in DEFAULT_TRACKERS {
                m.push_str(&format!("&tr={}", urlencoding::encode(t)));
            }
        } else {
            for t in trackers {
                let t: &str = t.as_ref();
                m.push_str(&format!("&tr={}", urlencoding::encode(t)));
            }
        }
        if let Some(idx) = self.file_idx {
            m.push_str(&format!("&file_idx={}", idx));
        }
        if let Some(hint) = &self.file_hint {
            m.push_str(&format!("&file={}", urlencoding::encode(hint)));
        }
        m
    }

    async fn probe_readable_range(
        resolved_url: &str,
        start: u64,
        bytes_needed: usize,
        required_download_mibps: f64,
        wait: std::time::Duration,
    ) -> anyhow::Result<(usize, f64)> {
        let started_at = tokio::time::Instant::now();
        let received = tokio::time::timeout(wait, async {
            let mut request = STREAM_PROXY_CLIENT.get(resolved_url);
            // librqbit only accepts open-ended byte ranges. A plain GET is
            // preferable at byte zero; for seeks, preserve bytes=N- and bound
            // how many response bytes we consume below.
            if start > 0 {
                request = request.header(http::header::RANGE, format!("bytes={start}-"));
            }
            let response = request.send().await?;
            anyhow::ensure!(
                response.status() == http::StatusCode::PARTIAL_CONTENT
                    || start == 0 && response.status() == http::StatusCode::OK,
                "range at byte {start} returned HTTP {}",
                response.status()
            );
            if response.status() == http::StatusCode::PARTIAL_CONTENT {
                anyhow::ensure!(
                    upstream_range_matches_start(response.headers(), start),
                    "range at byte {start} returned an inconsistent Content-Range"
                );
            }
            let mut stream = response.bytes_stream();
            let mut received = 0usize;
            while received < bytes_needed {
                let Some(chunk) = stream.next().await else {
                    break;
                };
                received = received.saturating_add(chunk?.len());
            }
            anyhow::ensure!(
                received >= bytes_needed,
                "range at byte {start} produced only {received} of {bytes_needed} required bytes"
            );
            anyhow::Ok(received)
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "range at byte {start} timed out after {:.2}s",
                wait.as_secs_f64()
            )
        })??;
        let elapsed_secs = started_at
            .elapsed()
            .as_secs_f64()
            .max(0.001);
        let readable_mibps = received as f64 / 1024.0 / 1024.0 / elapsed_secs;
        // A tiny client seek (especially the last few bytes of an MP4) can
        // never measure a meaningful MiB/s rate: HTTP request overhead alone
        // dominates. Require correct readable bytes, not an impossible
        // throughput threshold, for ranges smaller than 64 KiB.
        if bytes_needed >= 64 * 1024 {
            anyhow::ensure!(
                readable_mibps >= required_download_mibps,
                "range at byte {start} read {received} bytes at {readable_mibps:.2} MiB/s, below required {required_download_mibps:.2} MiB/s"
            );
        }
        Ok((received, readable_mibps))
    }

    /// Size the verified contiguous startup buffer by *seconds of media*,
    /// not a universal number of megabytes. Low-bitrate 720p should not be
    /// rejected for needing 20+ seconds to read an unnecessary 4-8 MiB.
    /// A source slightly slower than real-time playback gets a longer buffer;
    /// sources substantially slower still fall back within the bounded probe.
    fn readiness_plan(
        expected_bitrate_bps: Option<i64>,
        download_mibps: f64,
    ) -> Option<(usize, f64, bool)> {
        const MIB: usize = 1024 * 1024;
        if !download_mibps.is_finite() || download_mibps < 0.0 {
            return None;
        }
        if let Some(playback_mibps) = expected_bitrate_bps
            .filter(|bps| *bps > 0)
            .map(|bps| bps as f64 / 8.0 / MIB as f64)
        {
            // Verify at least 80% of real-time throughput in the requested read.
            // Current swarm throughput can be zero when these bytes are cached.
            // The tiny floor only rejects effectively dead peers; it must
            // not exceed the required speed of low-bitrate video.
            let minimum_sustainable = (playback_mibps * 0.8).max(0.06);
            let needs_buffer = download_mibps < playback_mibps * 1.1;
            let seconds_to_buffer = if needs_buffer { 6.0 } else { 4.0 };
            // Keep a real multi-second media buffer for high-bitrate remuxes.
            // A 4-8 MiB cap is barely a second at typical Blu-ray bitrates and
            // lets the player outrun a healthy swarm immediately after startup.
            let max_bytes = 128 * MIB;
            let startup_bytes = (playback_mibps * seconds_to_buffer * MIB as f64)
                .ceil()
                .clamp(MIB as f64, max_bytes as f64)
                as usize;
            return Some((startup_bytes, minimum_sustainable, needs_buffer));
        }
        // No trustworthy duration/bitrate: retain the conservative 4-MiB
        // proof and 1 MiB/s throughput baseline until ffprobe can help.
        Some((4 * MIB, 1.0, download_mibps < 1.0))
    }

    async fn wait_until_ready(
        &self,
        torrent: &crate::torrent::TorrentManager,
        resolved_url: &str,
        requested_range: Option<&str>,
    ) -> anyhow::Result<()> {
        // The final diversified PlaybackInfo recovery probe is allowed 35s.
        // Keep this inner readiness window long enough for a healthy cold swarm
        // to make a requested sparse range readable; earlier attempts remain
        // bounded by their 18-22s outer probe timeouts.
        const READY_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
        const POLL: std::time::Duration = std::time::Duration::from_millis(250);
        // The actual requested range must be readable at a sustainable rate
        // before serving the client.
        // Keep one reader alive for the remaining readiness budget. Restarting
        // it every eight seconds drops its sparse-piece priority before a slow
        // swarm can finish a large boundary piece, despite download progress.

        let deadline = tokio::time::Instant::now() + READY_WAIT;
        let mut last_status = None;
        loop {
            if let Some(status) = torrent.prefetch_status(
                &self.info_hash,
                self.file_idx,
                self.file_hint
                    .as_deref(),
            ) {
                let fully_cached = status.current_total_bytes > 0
                    && status.current_cached_bytes >= status.current_total_bytes;
                let expected_bitrate_bps =
                    self.expected_playback_bitrate_bps(status.current_total_bytes);
                let plan =
                    Self::readiness_plan(expected_bitrate_bps, status.download_mibps);
                if fully_cached {
                    tracing::info!(
                        cached_bytes = status.current_cached_bytes,
                        total_bytes = status.current_total_bytes,
                        expected_bitrate_bps,
                        "fully cached torrent source fast-pathed via persisted piece state"
                    );
                    return Ok(());
                }
                if let Some((
                    startup_target_bytes,
                    required_download_mibps,
                    buffering,
                )) = plan
                {
                    let required = status
                        .current_total_bytes
                        .min(startup_target_bytes as u64);
                    // A cached range may be immediately readable while the
                    // swarm is idle, and a range read prioritizes missing pieces.
                    // Do not gate this proof on aggregate bytes or download rate.
                    // Aggregate torrent bytes are not a playable buffer. Verify actual
                    // consecutive bytes at the requested startup or seek offset.
                    // When bandwidth is slightly below the estimated media bitrate,
                    // demand a larger continuous read, not an arbitrarily high speed.
                    let (probe_start, probe_end) = requested_range
                        .and_then(|range| {
                            parse_range(range, status.current_total_bytes).ok()
                        })
                        .unwrap_or((
                            0,
                            status
                                .current_total_bytes
                                .saturating_sub(1),
                        ));
                    let probe_bytes_needed = usize::try_from(
                        probe_end
                            .saturating_sub(probe_start)
                            .saturating_add(1)
                            .min(startup_target_bytes as u64),
                    )
                    .unwrap_or(startup_target_bytes);
                    let range_readable: anyhow::Result<(usize, f64)> = async {
                        let remaining = deadline
                            .saturating_duration_since(tokio::time::Instant::now());
                        anyhow::ensure!(
                            !remaining.is_zero(),
                            "readiness budget exhausted before startup range check"
                        );
                        let (startup_bytes, startup_read_mibps) =
                            Self::probe_readable_range(
                                resolved_url,
                                probe_start,
                                probe_bytes_needed,
                                required_download_mibps,
                                remaining,
                            )
                            .await?;

                        // Readiness belongs to the requested range. Requiring
                        // the far end of a multi-GB sparse file here rejects
                        // healthy startup/seek bytes and spends the probe's
                        // entire deadline on optional container metadata.
                        // A later EOF request proves its own bytes below.
                        Ok((startup_bytes, startup_read_mibps))
                    }
                    .await;

                    match range_readable {
                        Ok((startup_bytes, startup_read_mibps)) => {
                            tracing::info!(
                                cached_bytes = status.current_cached_bytes,
                                total_bytes = status.current_total_bytes,
                                download_mibps = status.download_mibps,
                                live_peers = status.live_peers,
                                required_bytes = required,
                                required_download_mibps,
                                expected_bitrate_bps,
                                fully_cached,
                                buffering,
                                startup_bytes,
                                startup_read_mibps,
                                probe_start,
                                "torrent source reached probe-ready cache"
                            );
                            return Ok(());
                        }
                        Err(error) => {
                            tracing::debug!(
                                %error,
                                cached_bytes = status.current_cached_bytes,
                                download_mibps = status.download_mibps,
                                live_peers = status.live_peers,
                                required_download_mibps,
                                probe_start,
                                "torrent client ranges not sustainably readable yet"
                            );
                        }
                    }
                }
                last_status = Some(status);
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        if let Some(status) = last_status {
            anyhow::bail!(
                "torrent cached {} of {} bytes at {:.2} MiB/s in {} seconds, but could not prove the requested buffered range (media bitrate {:?} bit/s)",
                status.current_cached_bytes,
                status.current_total_bytes,
                status.download_mibps,
                READY_WAIT.as_secs(),
                self.expected_playback_bitrate_bps(status.current_total_bytes),
            );
        }
        anyhow::bail!(
            "torrent readiness status unavailable within {} seconds",
            READY_WAIT.as_secs()
        );
    }
}

impl HttpSource {
    async fn serve_inner(
        &self,
        headers: &HeaderMap,
        bound_finite_ranges: bool,
    ) -> Result<Response> {
        let requested_range = bound_finite_ranges
            .then(|| {
                headers
                    .get(http::header::RANGE)
                    .and_then(|value| {
                        value
                            .to_str()
                            .ok()
                    })
                    .and_then(parse_open_or_finite_range)
            })
            .flatten();
        let mut req = STREAM_PROXY_CLIENT
            .clone()
            .get(&self.url);
        if let Some((start, _)) = requested_range {
            // librqbit accepts only `bytes=N-`. Normalize finite ranges and
            // cap the proxied response body to the client's requested length.
            req = req.header(http::header::RANGE, format!("bytes={start}-"));
        } else if let Some(v) = headers.get(http::header::RANGE) {
            req = req.header(http::header::RANGE, v.clone());
        }
        for (k, v) in &self.request_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let upstream = req
            .send()
            .await
            .context_bad_request("upstream request failed")?;

        let upstream_status = upstream.status();
        let upstream_headers = upstream
            .headers()
            .clone();
        if let Some((start, _)) = requested_range {
            if upstream_status == http::StatusCode::PARTIAL_CONTENT
                && !upstream_range_matches_start(&upstream_headers, start)
            {
                return Err(torrent_source_buffering(anyhow::anyhow!(
                    "upstream returned incorrect Content-Range for requested byte {start}"
                )));
            }
            if start > 0 && upstream_status == http::StatusCode::OK {
                return Err(torrent_source_buffering(anyhow::anyhow!(
                    "upstream ignored requested seek offset {start}"
                )));
            }
        }
        let total_length = upstream_headers
            .get(http::header::CONTENT_RANGE)
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
            })
            .and_then(|value| value.rsplit_once('/'))
            .and_then(|(_, total)| {
                total
                    .parse::<u64>()
                    .ok()
            });
        let bounded_range = requested_range
            .filter(|(_, end)| {
                end.is_some() && upstream_status == http::StatusCode::PARTIAL_CONTENT
            })
            .map(|(start, end)| {
                let requested_end = end.expect("filtered to finite range");
                let end = total_length
                    .map(|total| requested_end.min(total.saturating_sub(1)))
                    .unwrap_or(requested_end);
                let length = end
                    .saturating_sub(start)
                    .saturating_add(1);
                (start, end, length)
            });
        let stream = upstream
            .bytes_stream()
            .map_err(io::Error::other);
        let body = if let Some((_, _, length)) = bounded_range {
            Body::from_stream(ReaderStream::new(StreamReader::new(stream).take(length)))
        } else {
            Body::from_stream(stream)
        };

        let mut resp = Response::builder()
            .status(if bounded_range.is_some() {
                http::StatusCode::PARTIAL_CONTENT
            } else {
                upstream_status
            })
            .body(body)
            .unwrap();
        let out = resp.headers_mut();
        for (k, v) in &upstream_headers {
            match k.as_str() {
                "content-length" | "content-type" | "accept-ranges"
                | "content-range" | "last-modified" => {
                    out.insert(k, v.clone());
                }
                _ => {}
            }
        }
        if let Some((start, end, length)) = bounded_range {
            out.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_str(&length.to_string())
                    .expect("range length is a valid header"),
            );
            let total = total_length
                .map(|total| total.to_string())
                .unwrap_or_else(|| "*".to_string());
            out.insert(
                http::header::CONTENT_RANGE,
                http::HeaderValue::from_str(&format!("bytes {start}-{end}/{total}"))
                    .expect("range is a valid header"),
            );
        }
        if !out.contains_key(http::header::CONTENT_TYPE) {
            out.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/octet-stream"),
            );
        }

        Ok(resp)
    }
}

/// Partial responses must start at the requested byte; accepting another
/// offset silently corrupts decoder input and makes seek checks false passes.
fn upstream_range_matches_start(headers: &HeaderMap, start: u64) -> bool {
    let Some(value) = headers
        .get(http::header::CONTENT_RANGE)
        .and_then(|value| {
            value
                .to_str()
                .ok()
        })
        .and_then(|value| value.strip_prefix("bytes "))
    else {
        return false;
    };
    let Some((span, total)) = value.split_once('/') else {
        return false;
    };
    let Some((first, last)) = span.split_once('-') else {
        return false;
    };
    let (Ok(first), Ok(last)) = (first.parse::<u64>(), last.parse::<u64>()) else {
        return false;
    };
    first == start
        && last >= first
        && (total == "*"
            || total
                .parse::<u64>()
                .is_ok_and(|n| last < n))
}

fn parse_open_or_finite_range(range: &str) -> Option<(u64, Option<u64>)> {
    let bytes = range.strip_prefix("bytes=")?;
    let (start, end) = bytes.split_once('-')?;
    if start.is_empty() || end.contains(',') {
        return None;
    }
    let start = start
        .parse()
        .ok()?;
    let end = if end.is_empty() {
        None
    } else {
        Some(
            end.parse::<u64>()
                .ok()?,
        )
    };
    end.is_none_or(|end| end >= start)
        .then_some((start, end))
}

#[async_trait]
impl StreamSource for HttpSource {
    async fn serve(&self, _state: &AppState, headers: &HeaderMap) -> Result<Response> {
        self.serve_inner(headers, false)
            .await
    }
}

#[async_trait]
impl StreamSource for LocalSource {
    async fn serve(&self, _state: &AppState, headers: &HeaderMap) -> Result<Response> {
        let file = tokio::fs::File::open(&self.path)
            .await
            .context_not_found("file not found")?;
        let metadata = file
            .metadata()
            .await
            .context_bad_request("failed to read file metadata")?;
        let file_size = metadata.len();
        let content_type = mime_from_path(&self.path);

        let range_str = headers
            .get(http::header::RANGE)
            .and_then(|v| {
                v.to_str()
                    .ok()
            })
            .map(str::to_owned);

        if let Some(range) = range_str {
            let (start, end) = parse_range(&range, file_size)
                .context_bad_request("invalid Range header")?;
            let length = end - start + 1;

            let mut file = file;
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .context_bad_request("seek failed")?;

            let body = Body::from_stream(ReaderStream::new(file.take(length)));

            Ok(Response::builder()
                .status(http::StatusCode::PARTIAL_CONTENT)
                .header(http::header::CONTENT_TYPE, content_type)
                .header(http::header::CONTENT_LENGTH, length)
                .header(http::header::ACCEPT_RANGES, "bytes")
                .header(
                    http::header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end, file_size),
                )
                .body(body)
                .unwrap())
        } else {
            let body = Body::from_stream(ReaderStream::new(file));

            Ok(Response::builder()
                .status(http::StatusCode::OK)
                .header(http::header::CONTENT_TYPE, content_type)
                .header(http::header::CONTENT_LENGTH, file_size)
                .header(http::header::ACCEPT_RANGES, "bytes")
                .body(body)
                .unwrap())
        }
    }
}

/// Pull from the torrent engine before the HTTP client asks for the next bytes.
/// librqbit only prioritizes 32 MiB from its current read position; without
/// this pump, browser backpressure prevents its read cursor moving forward.
/// Permits are held until the consumer advances, enforcing a byte limit even
/// when the upstream produces irregular chunk sizes.
fn prefetch_response(response: Response, max_bytes: usize) -> Response {
    if max_bytes == 0 {
        return response;
    }

    let (parts, body) = response.into_parts();
    let permits = Arc::new(Semaphore::new(max_bytes));
    let (sender, mut receiver) = mpsc::unbounded_channel::<
        std::result::Result<(Bytes, OwnedSemaphorePermit), axum::Error>,
    >();
    tokio::spawn(async move {
        let mut source = body.into_data_stream();
        loop {
            let next = tokio::select! {
                next = source.next() => next,
                () = sender.closed() => return,
            };
            let Some(next) = next else { break };
            match next {
                Ok(mut bytes) => {
                    while !bytes.is_empty() {
                        let chunk = bytes.split_to(
                            bytes
                                .len()
                                .min(max_bytes),
                        );
                        let count = u32::try_from(chunk.len())
                            .expect("prefetch capped at 512 MiB");
                        let permit = tokio::select! {
                            result = permits.clone().acquire_many_owned(count) => {
                                match result { Ok(permit) => permit, Err(_) => return }
                            }
                            () = sender.closed() => return,
                        };
                        if sender
                            .send(Ok((chunk, permit)))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            }
        }
    });

    let output = async_stream::stream! {
        while let Some(result) = receiver.recv().await {
            match result {
                Ok((bytes, permit)) => {
                    yield std::result::Result::<_, axum::Error>::Ok(bytes);
                    drop(permit);
                }
                Err(error) => {
                    yield Err(error);
                    break;
                }
            }
        }
    };
    Response::from_parts(parts, Body::from_stream(output))
}

#[cfg(test)]
mod torrent_prefetch_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn reads_ahead_without_client_polling_and_preserves_response() {
        let (sent, received) = tokio::sync::oneshot::channel();
        let mut sent = Some(sent);
        let stream =
            futures_util::stream::iter([b"1234".as_slice(), b"5678".as_slice()]).map(
                move |data| {
                    if let Some(sender) = sent.take() {
                        let _ = sender.send(());
                    }
                    Ok::<_, io::Error>(Bytes::copy_from_slice(data))
                },
            );
        let response = Response::builder()
            .status(http::StatusCode::PARTIAL_CONTENT)
            .header(http::header::CONTENT_RANGE, "bytes 5-12/20")
            .body(Body::from_stream(stream))
            .unwrap();
        let response = prefetch_response(response, 8);
        tokio::time::timeout(std::time::Duration::from_secs(1), received)
            .await
            .expect("prefetch should pull the source before the player reads")
            .expect("source sender disappeared");
        assert_eq!(response.status(), http::StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()[http::header::CONTENT_RANGE],
            "bytes 5-12/20"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 100)
            .await
            .unwrap();
        assert_eq!(bytes, "12345678");
    }

    #[tokio::test]
    async fn dropping_client_cancels_pending_upstream_reader() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self
                    .0
                    .take()
                {
                    let _ = sender.send(());
                }
            }
        }
        let (started, observed_start) = tokio::sync::oneshot::channel();
        let (stopped, observed_stop) = tokio::sync::oneshot::channel();
        let source = async_stream::stream! {
            let _guard = DropSignal(Some(stopped));
            let _ = started.send(());
            yield Ok::<_, io::Error>(Bytes::from_static(b"abcd"));
            std::future::pending::<()>().await;
        };
        let response = prefetch_response(Response::new(Body::from_stream(source)), 8);
        tokio::time::timeout(std::time::Duration::from_secs(1), observed_start)
            .await
            .unwrap()
            .unwrap();
        drop(response);
        tokio::time::timeout(std::time::Duration::from_secs(1), observed_stop)
            .await
            .expect("cancelled client must release the torrent reader")
            .unwrap();
    }

    #[tokio::test]
    async fn forwards_upstream_failure_after_buffered_bytes() {
        let source = futures_util::stream::iter([
            Ok(Bytes::from_static(b"abcd")),
            Err(io::Error::other("peer read failed")),
        ]);
        let response = prefetch_response(Response::new(Body::from_stream(source)), 8);
        let mut output = response
            .into_body()
            .into_data_stream();
        assert_eq!(
            output
                .next()
                .await
                .unwrap()
                .unwrap(),
            "abcd"
        );
        assert!(
            output
                .next()
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            output
                .next()
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn prefetch_is_bounded_and_drains_completely() {
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let chunks = futures_util::stream::iter(0..12).map(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok::<_, io::Error>(Bytes::from_static(b"abcd"))
        });
        let response = prefetch_response(Response::new(Body::from_stream(chunks)), 8);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while reads.load(Ordering::SeqCst) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer should fill the buffer");
        // Two chunks fit; the third is waiting on a permit. No unbounded pull.
        assert_eq!(reads.load(Ordering::SeqCst), 3);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap()
                .len(),
            48
        );
        assert_eq!(reads.load(Ordering::SeqCst), 12);
    }
}

fn torrent_source_buffering(error: anyhow::Error) -> axum_anyhow::ApiError {
    axum_anyhow::ApiError::builder()
        .status(http::StatusCode::SERVICE_UNAVAILABLE)
        .title("P2P Source Buffering")
        .detail("torrent source is not ready; retry after buffering")
        .error(error)
        .build()
}

#[async_trait]
impl StreamSource for TorrentSource {
    async fn serve(&self, state: &AppState, headers: &HeaderMap) -> Result<Response> {
        let torrent = state
            .ctx
            .torrent
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))
            .context_bad_request("P2P is disabled")?;
        const TORRENT_RESOLVE_WAIT: std::time::Duration =
            std::time::Duration::from_secs(6);

        let lease = torrent
            .acquire(&self.info_hash)
            .await;
        let (resolved, selected_file_idx) = match tokio::time::timeout(
            TORRENT_RESOLVE_WAIT,
            torrent.resolve_url(&self.to_magnet()),
        )
        .await
        {
            Ok(Ok(resolved)) => resolved,
            Ok(Err(error)) => {
                if let Err(cleanup_error) = lease
                    .discard_failed()
                    .await
                {
                    tracing::warn!(
                        error = %cleanup_error,
                        info_hash = %self.info_hash,
                        "failed to discard unresolved torrent candidate"
                    );
                }
                return Err(error).context_bad_request("failed to resolve torrent");
            }
            Err(_) => {
                if let Err(cleanup_error) = lease
                    .discard_failed()
                    .await
                {
                    tracing::warn!(
                        error = %cleanup_error,
                        info_hash = %self.info_hash,
                        "failed to discard timed-out torrent candidate"
                    );
                }
                return Err(anyhow::anyhow!(
                    "torrent metadata did not resolve within {:.1}s",
                    TORRENT_RESOLVE_WAIT.as_secs_f64()
                ))
                .context_bad_request("failed to resolve torrent");
            }
        };
        let requested_range = headers
            .get(http::header::RANGE)
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
            });
        torrent.note_range_requested(&self.info_hash, selected_file_idx);
        if let Err(error) = self
            .wait_until_ready(&torrent, &resolved, requested_range)
            .await
        {
            // Preserve verified pieces for a cold retry. The lease's normal
            // reconnect grace pauses an unused swarm without destroying data.
            drop(lease);
            return Err(torrent_source_buffering(error));
        }
        if let Err(error) = torrent
            .enable_next_episode_prefetch(&self.info_hash, selected_file_idx)
            .await
        {
            tracing::debug!(
                %error,
                info_hash = %self.info_hash,
                selected_file_idx,
                "failed to enable next-episode prefetch after startup"
            );
        }

        let response = HttpSource {
            url: resolved,
            request_headers: Default::default(),
            response_headers: Default::default(),
        }
        .serve_inner(headers, true)
        .await?;
        let (parts, body) = response.into_parts();
        let stream = async_stream::stream! {
            let _lease = lease;
            let mut stream = body.into_data_stream();
            while let Some(chunk) = stream.next().await {
                yield chunk;
            }
        };
        let response = Response::from_parts(parts, Body::from_stream(stream));
        let mib = state
            .ctx
            .config
            .torrent_prefetch_mib
            .min(512) as usize;
        Ok(prefetch_response(response, mib * 1024 * 1024))
    }
}

pub fn parse_range(range: &str, file_size: u64) -> anyhow::Result<(u64, u64)> {
    anyhow::ensure!(file_size > 0, "empty file has no satisfiable byte ranges");
    let bytes = range
        .strip_prefix("bytes=")
        .ok_or_else(|| anyhow::anyhow!("expected bytes= prefix"))?;
    let (start_str, end_str) = bytes
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("malformed range"))?;

    if start_str.is_empty() {
        let suffix: u64 = end_str.parse()?;
        anyhow::ensure!(suffix > 0, "range suffix must be positive");
        return Ok((file_size.saturating_sub(suffix), file_size - 1));
    }

    let start: u64 = start_str.parse()?;
    anyhow::ensure!(start < file_size, "range start is beyond end of file");
    let end: u64 = if end_str.is_empty() {
        file_size - 1
    } else {
        end_str
            .parse::<u64>()?
            .min(file_size - 1)
    };
    anyhow::ensure!(start <= end, "range end precedes start");
    Ok((start, end))
}

pub fn mime_from_path(path: &std::path::Path) -> &'static str {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    // Check audio-specific extensions first so that formats like .m4a (which
    // aliases to VideoContainer::Mp4 for profile matching) still get the
    // correct audio/* mime type.
    if let Some(c) = remux_sdks::remux::AudioContainer::parse_known(ext) {
        return c.mime_type();
    }
    if let Some(c) = remux_sdks::remux::VideoContainer::parse_known(ext) {
        return c.mime_type();
    }
    "application/octet-stream"
}

/// Extract the `urn:btih:` info-hash from a magnet URI.
fn extract_btih(magnet: &str) -> Option<String> {
    url::Url::parse(magnet)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == "xt")
        .and_then(|(_, v)| {
            v.strip_prefix("urn:btih:")
                .map(|h| h.to_ascii_lowercase())
        })
}

fn extract_query_param(url: &str, param: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == param)
        .map(|(_, v)| v.into_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        HttpSource, STREAM_PROXY_CLIENT, StreamDescriptor, TorrentSource, TrackerUrl,
        is_tracker_url, mime_from_path, parse_open_or_finite_range, parse_range,
        upstream_range_matches_start,
    };
    use std::path::Path;

    #[test]
    fn high_bitrate_readiness_keeps_multiple_seconds_of_media() {
        const MIB: usize = 1024 * 1024;
        let (bytes, minimum_mibps, buffering) =
            TorrentSource::readiness_plan(Some(23_347_839), 6.0).unwrap();

        assert!(!buffering);
        assert!((11 * MIB..=12 * MIB).contains(&bytes));
        assert!((2.2..2.3).contains(&minimum_mibps));
    }

    #[tokio::test]
    async fn idle_swarm_can_prove_a_cached_startup_range() {
        // Paused/idle swarms report zero current throughput even when opening
        // bytes remain cached. Only a real range read decides readiness.
        let (bytes, minimum_mibps, _) =
            TorrentSource::readiness_plan(Some(8 * 1024 * 1024), 0.0).unwrap();
        let server = httpmock::MockServer::start();
        let startup = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/cached");
            then.status(206)
                .header("Content-Range", &format!("bytes 0-{}/{}", bytes - 1, bytes))
                .body(vec![0u8; bytes]);
        });
        let result = TorrentSource::probe_readable_range(
            &format!("{}/cached", server.base_url()),
            0,
            bytes,
            minimum_mibps,
            std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert!(result.0 >= bytes);
        startup.assert();
    }

    #[tokio::test]
    async fn seek_readiness_uses_librqbit_open_ended_range() {
        let bytes = 64 * 1024;
        let server = httpmock::MockServer::start();
        let seek = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/seek")
                .header("Range", "bytes=3306-");
            then.status(206)
                .header("Content-Range", "bytes 3306-68841/68842")
                .body(vec![0u8; bytes]);
        });
        TorrentSource::probe_readable_range(
            &format!("{}/seek", server.base_url()),
            3306,
            bytes,
            0.01,
            std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
        seek.assert();
    }

    #[tokio::test]
    async fn range_readiness_rejects_unavailable_requested_bytes() {
        let (bytes, minimum_mibps, _) =
            TorrentSource::readiness_plan(None, 0.0).unwrap();
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/short");
            then.status(206)
                .body(vec![0u8; 1024]);
        });
        assert!(
            TorrentSource::probe_readable_range(
                &format!("{}/short", server.base_url()),
                0,
                bytes,
                minimum_mibps,
                std::time::Duration::from_secs(3),
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn resolution_tag_falls_back_to_stream_name() {
        let mut info = super::StreamInfo {
            filename: Some("Inception (2010).mkv".to_string()),
            name: Some("Torrentio\n4k DV | HDR".to_string()),
            ..Default::default()
        };
        assert_eq!(
            info.resolution_tag()
                .as_deref(),
            Some("2160p")
        );

        info.name = Some("Torrentio\n720p".to_string());
        assert_eq!(
            info.resolution_tag()
                .as_deref(),
            Some("720p")
        );
    }

    #[test]
    fn unfinished_torrent_read_is_retryable_not_invalid_client_input() {
        let error = super::torrent_source_buffering(anyhow::anyhow!(
            "requested pieces unavailable"
        ));
        assert_eq!(error.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.title(), "P2P Source Buffering");
        assert!(
            error
                .detail()
                .contains("retry")
        );
    }

    #[test]
    fn parse_range_rejects_invalid_or_unsatisfiable_requests() {
        for range in ["bytes=0-", "bytes=-1", "bytes=0-0"] {
            assert!(parse_range(range, 0).is_err(), "{range} on empty file");
        }
        for range in [
            "bytes=100-",
            "bytes=100-150",
            "bytes=7-6",
            "bytes=-0",
            "bytes=-",
            "bytes=0-1,2-3",
            "bytes=0-1-2",
            "bytes=abc-",
        ] {
            assert!(parse_range(range, 100).is_err(), "{range}");
        }
    }

    #[test]
    fn parse_range_preserves_satisfiable_open_finite_and_suffix_requests() {
        assert_eq!(parse_range("bytes=0-", 100).unwrap(), (0, 99));
        assert_eq!(parse_range("bytes=5-10", 100).unwrap(), (5, 10));
        assert_eq!(parse_range("bytes=90-150", 100).unwrap(), (90, 99));
        assert_eq!(parse_range("bytes=-25", 100).unwrap(), (75, 99));
        assert_eq!(parse_range("bytes=-150", 100).unwrap(), (0, 99));
    }

    #[test]
    fn mime_from_path_video() {
        assert_eq!(mime_from_path(Path::new("movie.mkv")), "video/x-matroska");
        assert_eq!(mime_from_path(Path::new("movie.mp4")), "video/mp4");
        assert_eq!(mime_from_path(Path::new("movie.m4v")), "video/mp4");
        assert_eq!(mime_from_path(Path::new("movie.mov")), "video/quicktime");
        assert_eq!(mime_from_path(Path::new("movie.avi")), "video/x-msvideo");
        assert_eq!(mime_from_path(Path::new("movie.webm")), "video/webm");
        assert_eq!(mime_from_path(Path::new("movie.ts")), "video/mp2t");
        assert_eq!(mime_from_path(Path::new("movie.MKV")), "video/x-matroska");
    }

    #[test]
    fn mime_from_path_audio() {
        assert_eq!(mime_from_path(Path::new("track.mp3")), "audio/mpeg");
        assert_eq!(mime_from_path(Path::new("track.flac")), "audio/flac");
        assert_eq!(mime_from_path(Path::new("track.m4a")), "audio/mp4");
        assert_eq!(mime_from_path(Path::new("track.ogg")), "audio/ogg");
        assert_eq!(mime_from_path(Path::new("track.opus")), "audio/opus");
        assert_eq!(mime_from_path(Path::new("track.wav")), "audio/wav");
        assert_eq!(mime_from_path(Path::new("track.aac")), "audio/aac");
    }

    #[test]
    fn mime_from_path_fallback() {
        assert_eq!(
            mime_from_path(Path::new("file.m3u8")),
            "application/octet-stream"
        );
        assert_eq!(
            mime_from_path(Path::new("file.txt")),
            "application/octet-stream"
        );
        assert_eq!(
            mime_from_path(Path::new("noextension")),
            "application/octet-stream"
        );
    }

    #[test]
    fn tracker_url_validates_absolute_urls() {
        assert!(is_tracker_url("udp://tracker.opentrackr.org:1337/announce"));
        assert!(is_tracker_url("udp://opentor.net:6969"));
        assert!(is_tracker_url("https://private.example/announce"));
        assert!(is_tracker_url("http://tracker.example:8080/announce"));

        // Missing announce paths for HTTP, missing UDP ports, relative paths,
        // unsupported schemes, and non-URLs are rejected.
        assert!(!is_tracker_url("https://tracker.example"));
        assert!(!is_tracker_url("udp://tracker.example"));
        assert!(!is_tracker_url("/announce"));
        assert!(!is_tracker_url("tracker:udp://x/announce"));
        assert!(!is_tracker_url("not-a-tracker"));
        assert!(
            TrackerUrl::try_new(
                "udp://tracker.opentrackr.org:1337/announce".to_string()
            )
            .is_ok()
        );
        assert!(TrackerUrl::try_new("not-a-tracker".to_string()).is_err());
    }

    #[test]
    fn torrent_descriptor_ignores_invalid_persisted_trackers() {
        let descriptor: StreamDescriptor = serde_json::from_str(
            r#"{"Torrent":{"info_hash":"abc","file_hint":null,"file_idx":0,"trackers":["udp://opentor.net:6969","not-a-tracker"]}}"#,
        )
        .unwrap();

        let StreamDescriptor::Torrent { trackers, .. } = descriptor else {
            panic!("expected torrent descriptor");
        };
        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers[0].as_ref(), "udp://opentor.net:6969");
    }

    #[test]
    fn stream_proxy_client_builds_without_panic() {
        let _ = &*STREAM_PROXY_CLIENT;
    }

    #[tokio::test]
    async fn torrent_proxy_bounds_finite_ranges() {
        let server = httpmock::MockServer::start();
        let payload = vec![b'x'; 2048];
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/file.mkv")
                .header("Range", "bytes=0-");
            then.status(206)
                .header("Content-Range", "bytes 0-2047/2048")
                .header("Accept-Ranges", "bytes")
                .body(payload);
        });

        let source = HttpSource {
            url: format!("{}/file.mkv", server.base_url()),
            request_headers: Default::default(),
            response_headers: Default::default(),
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RANGE,
            http::HeaderValue::from_static("bytes=0-1023"),
        );

        let response = source
            .serve_inner(&headers, true)
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[http::header::CONTENT_LENGTH], "1024");
        assert_eq!(
            response.headers()[http::header::CONTENT_RANGE],
            "bytes 0-1023/2048"
        );
        let body = axum::body::to_bytes(response.into_body(), 2048)
            .await
            .unwrap();
        assert_eq!(body.len(), 1024);
    }

    #[tokio::test]
    async fn torrent_proxy_does_not_rewrite_full_responses_as_partial() {
        let server = httpmock::MockServer::start();
        let payload = vec![b'x'; 2048];
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/file.mkv")
                .header("Range", "bytes=0-");
            then.status(200)
                .header("Accept-Ranges", "bytes")
                .body(payload);
        });

        let source = HttpSource {
            url: format!("{}/file.mkv", server.base_url()),
            request_headers: Default::default(),
            response_headers: Default::default(),
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RANGE,
            http::HeaderValue::from_static("bytes=0-1023"),
        );

        let response = source
            .serve_inner(&headers, true)
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 2048)
            .await
            .unwrap();
        assert_eq!(body.len(), 2048);
    }

    #[test]
    fn malformed_finite_http_range_is_not_normalized_as_open() {
        assert_eq!(
            parse_open_or_finite_range("bytes=1024-"),
            Some((1024, None))
        );
        assert_eq!(
            parse_open_or_finite_range("bytes=1024-2048"),
            Some((1024, Some(2048)))
        );
        for range in ["bytes=1024-abc", "bytes=1024-2048-4096", "bytes=1024-2,3"] {
            assert_eq!(parse_open_or_finite_range(range), None, "{range}");
        }
    }

    #[test]
    fn upstream_partial_range_must_match_exact_start_and_valid_span() {
        let mut headers = http::HeaderMap::new();
        for invalid in [
            "bytes 10-20/100",
            "bytes 8-7/100",
            "bytes 8-100/100",
            "bytes */100",
            "bytes 8-20/nope",
        ] {
            headers.insert(
                http::header::CONTENT_RANGE,
                invalid
                    .parse()
                    .unwrap(),
            );
            assert!(!upstream_range_matches_start(&headers, 8), "{invalid}");
        }
        headers.insert(
            http::header::CONTENT_RANGE,
            "bytes 8-20/100"
                .parse()
                .unwrap(),
        );
        assert!(upstream_range_matches_start(&headers, 8));
    }

    #[tokio::test]
    async fn torrent_proxy_rejects_mismatched_seek_response() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/wrong.mkv")
                .header("Range", "bytes=1024-");
            then.status(206)
                .header("Content-Range", "bytes 0-2047/2048")
                .body(vec![b'x'; 2048]);
        });
        let source = HttpSource {
            url: format!("{}/wrong.mkv", server.base_url()),
            request_headers: Default::default(),
            response_headers: Default::default(),
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RANGE,
            "bytes=1024-1535"
                .parse()
                .unwrap(),
        );
        let error = source
            .serve_inner(&headers, true)
            .await
            .unwrap_err();
        assert_eq!(error.status(), http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn torrent_proxy_preserves_valid_seek_bytes_and_headers() {
        let server = httpmock::MockServer::start();
        let seek = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/seek-valid.mkv")
                .header("Range", "bytes=1024-");
            then.status(206)
                .header("Content-Range", "bytes 1024-3071/3072")
                .body(vec![b'z'; 2048]);
        });
        let source = HttpSource {
            url: format!("{}/seek-valid.mkv", server.base_url()),
            request_headers: Default::default(),
            response_headers: Default::default(),
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RANGE,
            "bytes=1024-1279"
                .parse()
                .unwrap(),
        );
        let response = source
            .serve_inner(&headers, true)
            .await
            .unwrap();
        seek.assert();
        assert_eq!(response.status(), http::StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()[http::header::CONTENT_RANGE],
            "bytes 1024-1279/3072"
        );
        assert_eq!(response.headers()[http::header::CONTENT_LENGTH], "256");
        let body = axum::body::to_bytes(response.into_body(), 512)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &[b'z'; 256]);
    }

    #[tokio::test]
    async fn torrent_proxy_rejects_ignored_seek_response() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/ignored.mkv")
                .header("Range", "bytes=1024-");
            then.status(200)
                .body(vec![b'x'; 2048]);
        });
        let source = HttpSource {
            url: format!("{}/ignored.mkv", server.base_url()),
            request_headers: Default::default(),
            response_headers: Default::default(),
        };
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RANGE,
            "bytes=1024-1535"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            source
                .serve_inner(&headers, true)
                .await
                .unwrap_err()
                .status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn seek_readiness_rejects_wrong_partial_offset() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/wrongseek")
                .header("Range", "bytes=3306-");
            then.status(206)
                .header("Content-Range", "bytes 0-65535/65536")
                .body(vec![b'x'; 65536]);
        });
        assert!(
            TorrentSource::probe_readable_range(
                &format!("{}/wrongseek", server.base_url()),
                3306,
                65536,
                0.01,
                std::time::Duration::from_secs(3),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn stream_proxy_client_forwards_range_and_returns_206() {
        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/file.mkv")
                .header("Range", "bytes=0-1023");
            then.status(206)
                .header("Content-Range", "bytes 0-1023/1048576")
                .header("Accept-Ranges", "bytes")
                .body(b"payload".to_vec());
        });

        let resp = STREAM_PROXY_CLIENT
            .clone()
            .get(format!("{}/file.mkv", server.base_url()))
            .header("Range", "bytes=0-1023")
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(
            resp.status()
                .as_u16(),
            206
        );
        assert_eq!(
            resp.headers()
                .get("content-range")
                .unwrap(),
            "bytes 0-1023/1048576"
        );
        resp.text()
            .await
            .expect("body should drain");
    }
}
