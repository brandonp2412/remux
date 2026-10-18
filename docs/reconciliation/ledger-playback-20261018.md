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
