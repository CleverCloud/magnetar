# ADR-0115 — Share one version-agnostic Pulsar fixture across the e2e suite

- **Status**: Accepted
- **Date**: 2026-10-12
- **Decider**: Florentin Dubois
- **Tags**: testing, e2e, ci, docker, xtask

## Context

Sixty-two `crates/magnetar/tests/e2e_*.rs` files each carried their own copy of the Pulsar fixture: the `DEFAULT_IMAGE_REPO` / `DEFAULT_IMAGE_TAG` constants, `image_repo()` / `image_tag()`, the `PULSAR_MEM_LIMIT` constant, a `start_pulsar()` builder chain and a `WaitFor::message_on_stdout("Created namespace public/default")` wait.
Three of them pinned a different tag by hand: `4.0.13` in `e2e_consumer_drop.rs`, in `e2e_reconnect_safety.rs` and in the v4-refusal test of `e2e_scalable_topic.rs`, and `5.0.0-M1` in the PIP-460 tests of that last file.
A broker version could therefore only be changed by editing every copy, and [ADR-0109](0109-pin-the-e2e-broker-image-tag.md) pinned `4.2.4` rather than do so because the log marker broke on 5.0.0.

The wait marker is the weak part.
Measured on 2026-10-12 against the real images:

- `Created namespace public/default` is printed on stdout by 4.0.14, 4.2.4 and 4.2.5, and never by 5.0.0, which prints `PulsarWebResource - Created namespace {clientAppId=null, namespace=public/default}`.
- With the functions worker enabled, the first `Created namespace` line is the one for `public/functions`, so a bare `Created namespace` substring is not a safe signal either.
- `GET :8080/admin/v2/brokers/ready` answers 200 0.5–3.5 s before `public/default` exists, and transiently 500 on 4.2.x.
- `/status.html` answers 404 on every version.
- `GET :8080/admin/v2/namespaces/public/default` answering 200 is the signal that holds on all four, about 7–9 s after the container starts.
- `Started Pulsar Proxy at` is still printed on stdout by 5.0.0 and 4.0.14, so the proxy's wait is unaffected.

ADR-0109 listed an HTTP readiness wait as an alternative and rejected it for that change: it touched 59 files and could not be validated on the author's host.
Both reasons are gone — the wait now lives in one place, and it was validated here against real brokers of four versions.

Centralising the fixture also exposed two harness bugs that the copies had hidden.
`PULSAR_PREFIX_<key>` environment variables reach `conf/standalone.conf` only through `bin/apply-config-from-env.py`; the image has no entrypoint (`Entrypoint: []`, `Cmd: [/bin/sh]`), so plain `bin/pulsar standalone` ignores them.
Two suites exported such variables to a plain `bin/pulsar standalone`:

- `e2e_shadow_topic_replicator.rs` set `authenticationEnabled`, `authorizationEnabled`, `tokenSecretKey` and the broker-client credentials, and never applied them.
  A probe on 2026-10-12 (plain `bin/pulsar standalone` with `PULSAR_PREFIX_authenticationEnabled=true`) left `authenticationEnabled=false` in `standalone.conf` and answered an anonymous `GET /admin/v2/namespaces/public/default` with 200.
  The suite had been running against an unauthenticated broker.
- `e2e_scalable_topic.rs` set `scalableTopicsEnabled` and `scalableTopicAutoScaleEnabled=false` and never applied them, so PIP-483 auto split/merge stayed on (the key exists in 5.0.0-M1's `ServiceConfiguration`) while the test comment said it was off.

`testcontainers` 0.28's `WaitFor::http` needs the `http_wait_plain` feature, which adds three packages to `Cargo.lock`.

## Decision

1. **One fixture, `crates/magnetar/tests/common/pulsar.rs`.**
   Every `e2e_*.rs` file declares `mod common;` (with `common/mod.rs` carrying the `#![allow(dead_code, unreachable_pub)]` convention of `magnetar-runtime-moonpool/tests/common/mod.rs`, because each test binary uses only part of it).
   It owns the image repository and tag, the `PULSAR_MEM` budget, the container command, the published ports (6650 and 8080, plus the embedded `ZooKeeper` port 2181 only for `.zookeeper_metadata()`), the readiness wait and the version check.
   A suite states only its own `.config(key, value)` (`PULSAR_PREFIX_<key>`), `.env(…)`, `.pre_start(…)`, `.flag(…)`, `.zookeeper_metadata()`, `.advertise_container_address()`, `.memory(…)` or `.admin_token(…)`.
   A Pulsar proxy is `Pulsar::new(broker).config(…).start_proxy()`; it keeps the `Started Pulsar Proxy at` stdout wait and has no admin REST to poll.

2. **A suite selects a `Broker`, never a tag.**
   The three tag constants of `common/pulsar.rs` are the only place an `apachepulsar/pulsar` tag appears in the Rust tests:

   | `Broker`           | Constant       | Value      | Override                             |
   | ------------------ | -------------- | ---------- | ------------------------------------ |
   | `Broker::Default`  | `DEFAULT_TAG`  | `4.2.4`    | `MAGNETAR_PULSAR_IMAGE_TAG`          |
   | `Broker::V4Lts`    | `V4_LTS_TAG`   | `4.0.13`   | none — pinned                        |
   | `Broker::Scalable` | `SCALABLE_TAG` | `5.0.0-M1` | `MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG` |

   `MAGNETAR_PULSAR_IMAGE_REPO` replaces the repository for all three; an empty override counts as unset.
   A 4.x-only or 5.x-only suite pins its `Broker`, so changing the default can neither skip it nor run it on the wrong broker.
   This is a behaviour change for `e2e_consumer_drop.rs`: its `4.0.13` default used to yield to `MAGNETAR_PULSAR_IMAGE_TAG`, and as `Broker::V4Lts` it no longer does.
   `e2e_reconnect_safety.rs` and the v4-refusal test already ignored the variable.

3. **Readiness is the namespace endpoint, polled from the host.**
   The container starts with `WaitFor::Nothing`; the fixture then polls `GET /admin/v2/namespaces/public/default` with the existing `reqwest` dev-dependency every 500 ms until it answers 200, under an explicit four-minute deadline.
   If the container exits first, the failure quotes the last probe and the tail of its stdout and stderr.
   No dependency and no `Cargo.lock` change: `WaitFor::http` and its feature are not used.
   A broker that enforces authentication on its admin REST needs `.admin_token(…)`, a super-user bearer token the probe sends; `e2e_handshake_error.rs` and `e2e_shadow_topic_replicator.rs` pass the admin token they already mint.
   This takes the "HTTP readiness wait" alternative that ADR-0109 rejected.

4. **The broker must be the version the tag names.**
   Once the namespace is served the fixture reads `GET /admin/v2/brokers/version` and requires its `MAJOR.MINOR` to equal the series of the requested tag (`5.0.0-M1` is series 5.0).
   The request is part of the readiness loop and retries under the same four-minute deadline, so a transient failure of the endpoint is not a verdict; a version that answers and disagrees fails at once.
   The check is skipped, with a notice on stderr, only for a tag with no leading `MAJOR.MINOR` (`latest`, or a mirror's own scheme).
   This replaces the self-referential `assert_eq!(DEFAULT_IMAGE_TAG, "4.2.4")` of `e2e_batch_chunk.rs` (`default_image_tag_tracks_latest_pulsar_four`), which is removed, and it fails on a mislabelled image where that assertion could not.

5. **Configuration is applied, not merely exported.**
   Any `.config(…)` makes the container run `bash -c`, export every pair, run `bin/apply-config-from-env.py conf/standalone.conf` (`conf/proxy.conf` for a proxy) and `exec` the broker.
   The script is built before the `GenericImage` chain because `ImageExt` consumes the builder, so a variable number of environment variables cannot be added to the chain.
   Consequences found by running it:
   `e2e_shadow_topic_replicator.rs` now runs against a broker that really enforces token authentication and authorization, and `e2e_scalable_topic.rs` really disables auto split/merge.
   `apply-config-from-env.py` edits the broker's `standalone.conf`, not the `client.conf` the in-container `pulsar-admin` reads, so the replicator suite's `pulsar-admin` calls carry `--auth-plugin` and `--auth-params` explicitly and must exit zero; with the grant silently refused, the first producer failed with `Client is not authorized to Get Partition Metadata`.

6. **The gate is renamed `check-e2e-fixture` and enforces three rules.**
   It replaces `check-e2e-container-memory` (job `e2e-container-memory` in `ci.yml` becomes `e2e-fixture`):
   1. A `GenericImage::new` chain with an `apachepulsar/…` repository, and any `apachepulsar/pulsar:` reference at all — in code, a string or a comment — is allowed only under `crates/magnetar/tests/common/`.
      Comments are included on purpose: a copy of the tag in prose is what drifted to `latest` in ADR-0109.
   2. Every Pulsar chain there carries `.with_env_var("PULSAR_MEM", …)` and reaches `.start()` on the same expression chain; an unresolvable image or a stashed builder stays a violation.
   3. The `*_TAG` constants of `common/pulsar.rs` equal the set of `docker pull apachepulsar/pulsar:<tag>` lines under `.github/workflows/`, are a subset of `FIXTURE_TAGS` in `scripts/performance_campaign.py`, and every `image: apachepulsar/pulsar:<tag>` of `crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml` equals `DEFAULT_TAG`.
      The Rust walk skips `.github`, so the gate reads `.github/workflows/` and `scripts/` explicitly.
      The rule fails closed: any non-comment line of a workflow, of the compose fixture or of `performance_campaign.py` that names `apachepulsar/pulsar` in another spelling — a `docker.io/` prefix, quotes, `docker image pull`, a `${VAR:-…}` default — is itself a violation, as is a second `FIXTURE_TAGS` assignment or a campaign reference to a tag `FIXTURE_TAGS` does not pre-pull.

7. **The performance harness follows.**
   `common/mod.rs` and `common/pulsar.rs` join `BASELINE_HARNESS_SOURCES` in `scripts/performance_campaign.py`: the nightly overlays the candidate's `e2e_reconnect_safety.rs` and `e2e_pulsar_proxy.rs` onto an older base checkout, which now fails to compile `mod common;` without them.
   `4.0.13` joins `FIXTURE_TAGS`.
   `scripts/test_performance_campaign.py` reads the default tag from `common/pulsar.rs` instead of `e2e_batch_chunk.rs`.
   `fixture_policy` in `scripts/performance.py` decides whether a test target is `fixture-required` by looking for `GenericImage::new` in its root source; with the constructor moved into `common/pulsar.rs` every migrated suite would have been classified `fixture-free`, and the nightly's "observed fixture creations contradict the assigned observation scope" check would have rejected the first run.
   It now also counts the shared fixture as a constructor: `Pulsar::new(..)` whose chain reaches `.start()` or `.start_proxy()` (directly or through the binding it was stored in), or a call of the shared `start_pulsar()`.
   `pulsar_fixture_contract.rs` only builds the command and stays `fixture-free`.
   Over the 80 root sources of `crates/magnetar/tests`, the required set equals the one `origin/main` has under the old rule (64, including the KDC and ZTS suites), plus one new fixture-free target.

8. **Docker-free tests for the fixture's logic.**
   `crates/magnetar/tests/pulsar_fixture_contract.rs` checks tag selection, override handling, version parsing and the generated container command, and runs the generated start script under `bash` against stand-ins for the image's `bin/` to prove that a value with spaces, quotes, `$`, backticks and separators reaches the applier and the broker unchanged; it runs in the non-e2e CI cell.
   `common/pulsar.rs` is compiled into every e2e binary, where a unit test would run once per suite.

The default tag, the three pins and every suite's assertions are unchanged by this ADR.
Moving the default to a 5.x tag, or running a 4.0.14 plus 5.0.0 matrix, is now a change to the constants, the `docker pull` lines, `FIXTURE_TAGS` and the compose pins together, which the gate checks; it still needs its own review of the suite against the new major ([ADR-0109](0109-pin-the-e2e-broker-image-tag.md) §3).

## Consequences

- A broker version is changed in one file, and the gate fails until CI's pre-pulls, the performance campaign's pre-pulls and the PIP-33 compose pins follow.
- A new `e2e_*.rs` cannot reintroduce an uncapped container, a log-line wait, a hand-written tag or a dropped `PULSAR_PREFIX_*` configuration: it either calls `Pulsar::new(Broker::…)` or fails the gate.
- Pulsar 5.0.0 starts under the fixture: `e2e_pulsar` passes all five tests with `MAGNETAR_PULSAR_IMAGE_TAG=5.0.0` (2026-10-12).
  That is evidence of readiness, not of 5.x compatibility of the other suites, which this ADR does not claim.
- Readiness is bounded by the broker serving `public/default` rather than by a line a future release may reword; the cost is one HTTP probe per 500 ms during startup and a four-minute ceiling, longer than the previous two- and three-minute log waits.
- Two suites changed behaviour because their configuration is now applied (decision 5).
  `e2e_shadow_topic_replicator.rs` exercises real authorization for the first time; `e2e_scalable_topic.rs` runs with auto split/merge off, as its comments always said.
- `MAGNETAR_PULSAR_IMAGE_TAG` no longer redirects `e2e_consumer_drop.rs`.
- Every e2e binary compiles `common/`, and the `pulsar` module's unused helpers are silenced by one `#![allow(dead_code, unreachable_pub)]` in `common/mod.rs`, the convention of the moonpool test crate.
- The permit-accounting suite keeps its existing `#[ignore]` for the broker defect it asserts (apache/pulsar#26416); on `4.2.4` it still fails at `totalAvailablePermits = -18`, which the fixture does not change.

## Alternatives considered

- **`WaitFor::http` from `testcontainers`.** Rejected: it needs the `http_wait_plain` feature and adds three packages to `Cargo.lock`, for a poll the existing `reqwest` dev-dependency already makes.
- **A version-agnostic log marker (`public/default`, or a bare `Created namespace`).** Rejected: the first match on 4.x with the functions worker enabled is `public/functions`, and a log line is the thing that already moved once.
- **`/admin/v2/brokers/ready` or `/status.html`.** Rejected: the first answers 200 before `public/default` exists, the second answers 404 on every version.
- **Keep the per-file copies and edit each one.** Rejected: that is the situation ADR-0109 documented, and it cannot be checked by a gate that sees one chain at a time.
- **A separate workspace crate for test support.** Rejected as heavier than needed: `tests/common/` is the layout Cargo already supports and the moonpool test crate already uses, and nothing outside `crates/magnetar/tests` needs the fixture.
- **Let `Broker::V4Lts` honour `MAGNETAR_PULSAR_IMAGE_TAG`.** Rejected: a suite that needs a 4.x broker would silently run on whatever the compatibility run selected.

## References

- `crates/magnetar/tests/common/pulsar.rs` — the fixture; `crates/magnetar/tests/pulsar_fixture_contract.rs` — its Docker-free contract.
- `xtask/src/main.rs` — `check-e2e-fixture` and its unit tests; `.github/workflows/ci.yml` — the `e2e-fixture` job and the pre-pull list.
- `scripts/performance_campaign.py`, `scripts/test_performance_campaign.py` — `BASELINE_HARNESS_SOURCES`, `FIXTURE_TAGS`.
- `docs/testing.md` § "The shared Pulsar fixture" and § "e2e container memory budget".
- [ADR-0109](0109-pin-the-e2e-broker-image-tag.md) — the pin and the rejected alternative this ADR takes; [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) — e2e tests carry no feature gate and no `#[ignore]`; [ADR-0098](0098-parallelize-per-pr-test-execution.md) — the shard inventory is the `e2e_*.rs` glob, which `common/` stays out of; [ADR-0113](0113-run-performance-measurement-nightly.md) — the nightly that overlays the harness files.
