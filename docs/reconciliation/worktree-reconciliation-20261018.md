# SloppaTV and Remux worktree reconciliation — 2026-10-18

This ledger records the cleanup of the two repositories before resuming the TV soak. All worktree-local edits were committed on their source branches before removing those worktree directories. The main branches contain only the integrations listed below; WIP and older unique branches remain preserved. No deployment or production restart was performed.

## Integrated work and validation

Remux main moved from `62bfe113` to `6042bf3f`. Integrated reviewed fixes for cached-torrent readiness, startup plus estimated resume-range proof, Stremio episode ordinals, retry-cache lease retention, and Tatami intro validation. The cached-readiness proof uses a single total deadline; byte offsets are estimates from known size/runtime and are tested with mocked HTTP Range requests. The PHM range proof is not an exact seek guarantee for variable-bitrate media.

SloppaTV main moved from `7cd9ddfc` to `94b98eb6`. The committed groups cover per-series track preferences, subtitle discovery/delivery, active queue changes, details/item-mutation feedback, the C++ warning fix, local resume preservation, and Remux fallback/restart behavior.

Checks: Remux `cargo test -p remux-server --lib --no-run` passed; targeted suites passed with exact results: probe 48, episode ordinal 3, retry-cache 1, Tatami 2. `cargo fmt --all -- --check` and `git diff --check` passed. Sloppa CI-equivalent checks before final source-fallback integration: ruff/ty/shellcheck/host tests, Play metadata and pinned mpv AAR, and `./gradlew --no-daemon test assembleDebug` passed. Changed-C++ quality check and host tests are rerunning on the final tree; their final status is added before this ledger is committed.

SloppaTV final main SHA is `94b98eb6`. Remux code changes conclude at `6042bf3f`; later Remux main commits contain only this reconciliation ledger and its final audit note.

## Removed worktree directories

Initial inventory: SloppaTV had 2 extra worktrees and 27 dirty main paths (including one new source header); Remux had 22 extra worktrees (11 dirty) and a clean main tree. Final inventory: zero extra worktrees in both repositories; all main changes committed.

The two SloppaTV extra worktrees (`sloppatv-auto-source-fallback-20261018`, `sloppatv-resume-preserve-20261018`) were removed after source commits `418018b4` and `039bf37c` existed. The 22 Remux extra worktrees were removed only after committing their dirty contents or confirming the clean tree was represented/empty.

## Preserved experimental and unique branches

These refs retain unique or intentionally unvalidated work. They were not merged into main or deleted:

Remux: `fix/remux-retry-cache-20261011`, `fix/source-subtitles`, `fix/torrent-prefetch`, `soak/gumball-probe-budget-20261018` (35-second seed-count timeout lacks selected-file progress evidence; duplicate raw-airdate alias is retained as an archive ref), `soak/phm-recovery-20261017` (expanded recovery experiment), `soak/playback-ready-header-20261017` (readiness settings experiment), `soak/probe-single-deadline-20261018` (separate fallback timing experiment), `soak/resume-offset-readiness-20261018` (broad raw-cluster scanning; approximate offsets need more proof), `soak/sparse-range-proof-20261018` (experimental sparse range probing), and `soak/tuner-late-rescue-20261018` (experimental rescue).

SloppaTV: `backup/glass-sloppatv-partial-20260922`, `feature/remux-sloppatv-seerr`, `fix/playback-subtitle-tracks`, `fix/sloppatv-remux-source-failover-20261011`, `safety/glass-prepull-trickplay-20260919`, and `sync/wip-glass-20261008` retain older unique work that was outside the reviewed integration scope.

## Archived completed branch refs

Completed branches whose patches are represented by main, plus completed candidates integrated through reviewed adaptations, are preserved under `refs/archive/worktree-reconciliation-20261018/<original-branch>` before their `refs/heads/*` names are retired. This keeps every original commit reachable while reducing unresolved branch clutter. `git cherry -v main <branch>` showed no unique patches for the patch-equivalent group. Manual adaptation mappings are shown explicitly below.

### Remux manual integration mappings

- `soak/episode-ordinal-fallback-20261017` HEAD `fb8b4070` → main `03635037` (episode ordinal fallback; targeted tests passed).
- `soak/phm-readiness-recheck-20261018` HEAD `0e8a03e4` → main `0ef3edd3` + `6042bf3f` (resume-range verification and HTTP/deadline regressions).
- `soak/tatami-intro-sanity-20261018` HEAD `2d034584` → main `50e8ede9` (actual-duration guard and single-intro regression).

### SloppaTV integration mappings

- `soak/resume-preserve-20261018` HEAD `039bf37c` → main `e73ff75f` (patch-equivalent hydration fix).
- `soak/auto-source-fallback-20261018` HEAD `418018b4` → main `94b98eb6` (preferred-source fallback and explicit restart preservation).

The following branch tips are then listed by repository. Each is archived under its exact branch-name path; source tip SHA is kept verbatim.

### Remux branches archived and retired

| Original branch | Tip SHA | Main mapping / disposition |
|---|---|---|
| `fix/buffered-playability-r92` | `b4eae0e56871583b8aa1e845ca0d64eac8bff7f4` | patch-equivalent to main (`git cherry` has no unique patches) |
| `integration/sloppa-prefetch-fork` | `76e7a19ff4ff230eb00353d691d40f1c6c7597a8` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/concurrent-fallback-warmup-20261018` | `d2e39cc42ca099480a8c73ecec48f33de13df864` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/episode-ordinal-fallback-20261017` | `fb8b4070d5a02d30fcd0c36df1d7b238ac350952` | 03635037 |
| `soak/oprah-movie-id-20261018` | `5dd5c7d403189d27723615bed0296beae3f795dd` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/phm-cold-recovery-20261018` | `778ce646b29b59e47122c8bb6a1287c7485f9529` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/phm-readiness-recheck-20261018` | `0e8a03e47d439dfca54cfd74ddfc811cc478a43a` | 0ef3edd3 + 6042bf3f |
| `soak/playability-first-ranking` | `92d21d8ef1a0bae4ad5da2ae5fb9b2863fa54747` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/raw-airdate-guard-20261018` | `bb29e8138bb0b05f16366e140a8187b2165bb1d2` | duplicate experiment; retained via Gumball branch and archive ref |
| `soak/retain-torrent-retry-cache` | `fcebf85123af493f9a7e46f63be7463e4fd5dd20` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/snl-cinemeta-numbering-20261018` | `2d8ebc9377ff04e580444e5e7a4d9c2eec91074b` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/subtitle-torrent-guard-20261018` | `61a1a4fb944070544e012d20bfef283239fde1cc` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/tatami-intro-sanity-20261018` | `2d034584500afe98ac6b312fca69a1f762d56a53` | 50e8ede9 |

### SloppaTV branches archived and retired

| Original branch | Tip SHA | Main mapping / disposition |
|---|---|---|
| `backup/overnight-glass-pre-release-20261016-0805` | `1e11f8b9ba7bc0db94dcb043775518ff307ca27e` | patch-equivalent to main (`git cherry` has no unique patches) |
| `chore/play-auto-publish` | `6c9ffff180d6c25f563411c002e32c24c43394fb` | patch-equivalent to main (`git cherry` has no unique patches) |
| `chore/sloppatv-reddit-draft` | `9b25069244821e0592dfe3d0885c64801ca3a30a` | patch-equivalent to main (`git cherry` has no unique patches) |
| `feat/github-badge-app-icon-20260930` | `67d61eecf11be34049f93010b6f8edd7812acd53` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/ci-361` | `4ca6a285b5ad79a90f3f30e0244738649ff5e893` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/continue-watching-toggle-and-home-prewarm` | `b14fbb476b48f69896206018fa2b87454804e2bb` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/github-gpl-license` | `c4f9d9944f4e268ff148bad62a767fdc694c9c9c` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/play-aab-path` | `d7455fa5eebdcaa513f77c3e44935f11829d1335` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/play-signing-key` | `a48cc51c52c28a254786f3947d7c7425d66d7b75` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/sloppatv-actions-20260919` | `27744cb280b2143074c549934d1b624bdbf4bd11` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/sloppatv-bug-audit-20260925` | `41e28d7c7ae927d44c85aed17ff6cacf4ab4bb5b` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/sloppatv-build-warnings-20260930` | `81482456b1c8d43eae740d10b164ff2783a39d12` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/sloppatv-play-screenshots-20260930` | `81482456b1c8d43eae740d10b164ff2783a39d12` | patch-equivalent to main (`git cherry` has no unique patches) |
| `fix/sloppatv-release-notes-20260930` | `824e9ccb2dc3769f6f3aab68b5250fe3b8da0773` | patch-equivalent to main (`git cherry` has no unique patches) |
| `handoff/nox-sloppatv-20260922` | `ca963e54745edaa0caecf2d4fafc00726109cd4b` | patch-equivalent to main (`git cherry` has no unique patches) |
| `integrate/sloppatv-leftovers-20260922` | `a74192eed59fbfb2e74f5edb4bedf927125cb5f6` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-0048` | `115cff835b25282fbea6894b72c05601f8d59632` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-0405` | `20c457be0819b6bc206b6f5305f62d7300f43adc` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-1419` | `c22197e214c662870a1c3e6fac0c75ff8263f8b5` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-1714` | `b6ffb09f38b8d649f11f219626d41787b813d8f5` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-2131` | `bfb91d3da9c63ef2beb9c95c6b93147911cbc415` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-aggressive-1129` | `d6efaa1d67cb760bb5f07366f75a755838f2605d` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-aggressive-1747b` | `9211ba9824f1cffb38b5a5520cabbaaf1bd739d2` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-aggressive-completions-2054` | `16095f30669d4b5ad49c4c1a441d7a534fc37824` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-main-decomp-1025` | `062f4e7087ae5de8712116ae4d2d03ef044ae303` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-next-1538` | `002bee96df63786bdb82bba64d2463e0bbfb9820` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-ready-1538` | `22e29584c482b7e572fc480d47145f317ff0ef33` | patch-equivalent to main (`git cherry` has no unique patches) |
| `prompta-rearch-settings-zoom-effect` | `d2373e9c26a5b11b7b720d9127a58321abc84f9f` | patch-equivalent to main (`git cherry` has no unique patches) |
| `soak/auto-source-fallback-20261018` | `418018b4763931ac2f3e76b6e909c41ee05d8c6d` | 94b98eb6 |
| `soak/resume-preserve-20261018` | `039bf37c2baab74ba662e60c25025c79bfdc6950` | e73ff75f |
| `verify/sloppatv-playback-20260922` | `cd160f1f002463bb6b415492c4b1309170fdbef3` | patch-equivalent to main (`git cherry` has no unique patches) |


## Explicit original-tip to integrated-tip mapping

For branches whose final original commit differs because it was adapted during integration, the integration targets are:

| Original branch tip | Integrated main commit(s) |
|---|---|
| Remux `d2e39cc4` concurrent fallback prewarm | `8e649e4f` |
| Remux `5dd5c7d4` stream ID lookup | `4185b851` |
| Remux `778ce646` cold recovery | `507c52a1` |
| Remux `92d21d8e` playability ranking | `dc6d1e11` |
| Remux `fcebf851` retry-cache lease | `791252f9` |
| Remux `2d8ebc93` season-zero episode numbering | `ea63f9d6` |
| Remux `61a1a4fb` subtitle torrent guard | `ee9bdb9b` |
| Remux `fb8b4070` episode ordinal fallback | `03635037` |
| Remux `0e8a03e4` cached resume readiness | `0ef3edd3`, `6042bf3f` |
| Remux `2d034584` Tatami intro sanity | `50e8ede9` |
| SloppaTV `039bf37c` hydration resume preservation | `e73ff75f` |
| SloppaTV `418018b4` source fallback/restart | `94b98eb6` |
| SloppaTV `b14fbb47` continue watching/prewarm | `24df7511` |
| SloppaTV `27744cb2` Actions update | `ba27bc8f` |
| SloppaTV `bfb91d3d` re-architecture checkpoint | `f7216ace` |
| SloppaTV `d6efaa1d` aggressive re-architecture checkpoint | `a3df105a` |
| SloppaTV `002bee96` next re-architecture checkpoint | `40ca55e3` |
| SloppaTV `d2373e9c` settings/zoom checkpoint | `f5032239` |

Other retired branch tips were already ancestors of main or had no unique patch according to `git cherry -v main <branch>`; each exact original tip is retained as an archive ref named in the preceding tables.

## Retained unique refs and tip SHAs

| Repository | Branch | Tip SHA | Preservation reason |
|---|---|---|---|
| Remux | `fix/remux-retry-cache-20261011` | `a31114666ed5` | Distinct HLS/source-subtitle and torrent-prefetch series; not reviewed for this integration. |
| Remux | `fix/source-subtitles` | `c1c7f8f07d58` | Distinct source-subtitle/HLS commits. |
| Remux | `fix/torrent-prefetch` | `5e198333873f` | Distinct torrent-prefetch feature series. |
| Remux | `soak/gumball-probe-budget-20261018` | `9c1eb2d25e3d` | WIP; seeder-count timeout extension lacks evidence of selected-file progress. |
| Remux | `soak/phm-recovery-20261017` | `74510fdf399a` | WIP expanded recovery experiment. |
| Remux | `soak/playback-ready-header-20261017` | `9f3bef22652c` | WIP readiness-settings experiment. |
| Remux | `soak/probe-single-deadline-20261018` | `74e570dd88af` | Unique late-fallback timing patch; preserve for follow-up. |
| Remux | `soak/resume-offset-readiness-20261018` | `8e77d10718b9` | WIP broad raw-cluster scan and approximate offsets need more proof. |
| Remux | `soak/sparse-range-proof-20261018` | `180d5974d0cf` | WIP sparse range probing. |
| Remux | `soak/tuner-late-rescue-20261018` | `d46df26bce7f` | WIP late tuner rescue. |
| SloppaTV | `backup/glass-sloppatv-partial-20260922` | `b47936efcdff` | Preserved stopped work. |
| SloppaTV | `feature/remux-sloppatv-seerr` | `94bc77b29b08` | Distinct Seerr integration commits. |
| SloppaTV | `fix/playback-subtitle-tracks` | `c5e66f91aa24` | Distinct source-track/cache behavior and cold HLS prep. |
| SloppaTV | `fix/sloppatv-remux-source-failover-20261011` | `d7cb2a215437` | Distinct source failover/audio-language patch. |
| SloppaTV | `safety/glass-prepull-trickplay-20260919` | `e19acf8665ab` | Preserved staged trickplay work. |
| SloppaTV | `sync/wip-glass-20261008` | `139b132ba43b` | Preserved unsynchronized Glass work. |

Post-cleanup `git branch --no-merged main` counts: Remux 10; SloppaTV 6 (16 total: 7 active experiments plus 9 older unique branches, down from 19 extra-worktree branches in the initial inventory). A total of 44 completed/equivalent branch names were retired only after their archive refs were verified at the exact original tip SHA. The exact original-tip archive ref SHA was checked against each source branch before retiring its branch name. Each repository now has exactly one worktree: its main checkout. Main status is clean after the ledger commit.

Final SloppaTV gates on the integrated main tree: `python3 tools/cpp_quality.py check --format-base HEAD` passed (changed-file formatter check, clang-tidy over 44 translation units, and cppcheck); `python3 tools/run_host_tests.py` passed (compiled C++ host tests and 67 Python tests); `./gradlew --no-daemon test assembleDebug` passed with child return code 0 (`BUILD SUCCESSFUL`, 22 seconds; `/tmp/sloppatv-final-gradle-20261018.json`). Earlier workflow-equivalent checks also passed ruff, ty, shellcheck, Play metadata validation, and pinned mpv AAR validation.
