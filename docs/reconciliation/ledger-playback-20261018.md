# Failure-ledger playback repair — 18 October 2026

The owner requested a failure-ledger sweep and explicitly revoked the earlier
remote safety gates. This run drives only `app.sloppatv.remuxtest`, authenticated
as `sloppatv-e2e`, through raw ADB on Glass. The scheduler remains paused during
the manual run to avoid another concurrent operator. Production is unchanged.

## Repair: diversify the first fallback

The Mentalist S1E1 failed a cold current-main attempt with HTTP 503 after
61.773 seconds. The selected HEVC releases were not ready, and the reserved
720p fallback advertised 233 seeders but initially had 1–2 live peers and no
selected-file bytes. A warm retry briefly decoded the 720p release, stalled at
3.942 seconds, then recovered to HEVC after another long negotiation.

The P2P retry budget is two. Reserving one slot for lower resolution leaves only
one same-resolution slot. Previously codec diversity applied only to subsequent
same-resolution retries, making it unreachable in this common path. The change
uses a seeded, encoded AVC alternative for that first slot when a non-AVC primary
fails. An AVC primary, absent/poorly-seeded AVC alternatives, and remuxes retain
the prior fallback behavior. The retry count and timeouts are unchanged.

## Fresh cold validation

A separate backend with empty torrent storage, isolated cloned database, and
zero test-user resume position selected an AVC 1080p ViSUM release instead of
another HEVC release. Fresh TV PlaybackInfo was issued at 17:59:03.599 NZDT,
selected at 17:59:28.188 (24.484 seconds), and hardware playback telemetry began
at 17:59:29.032 (25.433 seconds). Decoder crop was 1920x1080; video AVC and audio
E-AC3. AudioFlinger track 72, PID 7700, session 193 was active on HDMI at 48 kHz,
six channels and zero underruns. Positions advanced 20.896 → 62.062 → 99.558 s;
a seek worked. Two distinct film scenes were inspected, with no autonomous
buffering or further fallback. Torrent hash: be7584dc197928b8dae4fe8bee66247b119e09d5.

The source is viable and the exact episode is marked `resolved_verified`.
An unrelated Gumball cold resume on the same candidate failed after 73.561 seconds.
A warmed retry selected in 22.461 seconds and started HEVC telemetry in 23.534
seconds, but interrupted for approximately 26 seconds before recovery. Two
distinct decoded scenes, advancing position 326.993 -> 342.592 seconds and active
HDMI audio were inspected. This proves source viability, not a cold-start fix.
The cold resume/range readiness entry stays open.

## Ledger classifications so far

Fresh TV/provider checks established no streams for these exact episodes:
7de Laan S2E114; Barbara Karlich S25E69; Binnelanders S19E205; DAS! S28E41 and
S3E1; EastEnders S8E7; Goede Tijden Slechte Tijden S32E61; King of Mask Singer
S1E423; Storm of Love S2E317; Tagesschau S42E248. They are `source_external`.
Late Night with Seth Meyers S1E10 has three old zero-seeder releases; its only
qualified 720p candidate failed metadata resolution. It is marked currently
unavailable, not as an application regression. These classifications apply to
the exact episodes and current configured provider, not every episode of a show.
Gumball resumed and decoded after recovery on the original diagnostic backend;
its cold current-source validation remains separate.

## Checks and evidence

`cargo fmt --all -- --check`, `cargo check -p remux-server --tests -j 1`, and
a debug build passed. Test targets were type-checked, not executed; the prior
handoff says to skip Rust test suites. These builds finished before the owner's
new Pueue instruction arrived. Every subsequent heavy operation uses `buildq`;
the artifact strip completed as Glass queue task 4 with success.

Candidate executable SHA256:
768105ff6dfc18119285a394c74b1c1343899ad83f7ec89a2d6d136436d31120.
It runs only in Nox service `remux-ledger-diverse-20261018.service`, port 3104,
with data `/var/tmp/remux-ledger-diverse-cold-20261018` and a dedicated Glass
SSH tunnel. Previous owned baseline service is stopped and preserved for rollback.

Canonical evidence and ledger are under Nox `/home/brandon/.local/state/prompta`.
Screenshots/logcat/audio/session snapshots are on Glass in
`physical-soak/ledger-20261018T1742`; corresponding server journals are on Nox.
Screenshots and decoder/audio telemetry establish device-decoded playback only;
no independent physical-panel/speaker observation is claimed.

## Final checkpoint and scheduler handoff

17 fresh device attempts: 4 decoded passes (including recovered/warm runs) and
13 failures (11 source-unavailable, 2 startup failures). Manual isolated playback
is stopped before the scheduler resumes. The replacement job prompt is tracked
in `prompta-ledger-continuation-20261018.txt`; cadence remains 2700 seconds.
Production was not changed. Candidate services/tunnel remain for continuation.


## 2026-10-18 19:54–20:11 NZDT — repeat cold resume and buffering response

- New physical Google TV attempts (isolated `app.sloppatv.remuxtest`, `sloppatv-e2e`): **3** — two empty-torrent-store Gumball S6E1 cold resumes and one retained-cache retry. **0 decoded passes; 3 failed**. No HDMI/panel pass.
- First cold resume with `917d34cb` (`:3107`): fresh PlaybackInfo at 19:54:36, 1080p HEVC (`e58f92e...`) 54 MB/12 peers but ffprobe timed out; 1080p AVC fallback (`532d8e...`) slow; 720p AVC (`59d56e...`) startup range returned HTTP 400. HTTP 500 after ~73 seconds.
- Follow-up cold resume with new `1a9f5f2d` debug binary (`:3107`, empty torrent store): PlaybackInfo at 20:06:37; 720p source downloaded **50,331,648 / 283,614,722 bytes**, **6 live peers**, **3.17 MiB/s**, but 106502265–107550840 sparse resume range returned HTTP 400. **HTTP 503** `P2P_SOURCE_BUFFERING`, `Retry-After: 10` after 73.8 seconds; app log confirms retryable “Buffering—downloading slowly” message. Still no playback.
- Root retry-status bug: `startup_ready` in `TorrentPrefetchStatus` can mean just one already-verified range; it does not establish the requested resume range. Fix `1a9f5f2dcc416724d04c037d17fe64ddc85ae4b4` treats a still-incomplete, active download as retryable *after all playback probes fail*, even if its earlier header was ready. Fully-cached failures remain real errors. Focused Rust unit test executed and passed (1/1); `cargo fmt --all` and `cargo build -p remux-server -j 1` passed, all via Glass buildq jobs #25–27. No full regression suite executed.
- Warm retry at 20:09:20: native primary range succeeded for 158,900,917 / 200,843,957 downloaded bytes but HEVC ffprobe failed. Isolated backend stopped answering `/health` with 1 GB memory, threads in futex wait, `CLOSE-WAIT` loopback sockets, and no new request journal entries. Device timed out after **90,014 ms** (no decode). Underlying stream/probe/backend responsiveness remains open. The retry-status fix is **not** a playback-start repair.
- Evidence: `Glass:/home/brandon/.local/state/prompta/physical-soak/ledger-20261018T1947` (screenshots, app logcat, dumpsys); `Nox:/home/brandon/.local/state/prompta/physical-soak/ledger-20261018T1947` (backend journals, status, TCP/thread dump). Canonical failure ledger has all three new attempts.
- Owned test service `remux-ledger-retryfixed-20261018.service` and tunnel `remux-ledger-repeat-tunnel-20261018.service` stopped; ADB reverse restored `tcp:3000 -> tcp:3104`; standalone test app stopped. Nox production `:3000` and others untouched. **Next**: diagnose the isolated backend's unresponsive health endpoint around a warm ffprobe; make cold resume range actually readable, retest fresh-store on device, then unrelated title. Project Hail Mary midstream stall is still open. No production deploy.
