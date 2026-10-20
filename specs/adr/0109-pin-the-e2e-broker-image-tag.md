# ADR-0109 — Pin the e2e broker image tag instead of tracking `latest`

- **Status**: Accepted
- **Date**: 2026-10-08
- **Decider**: Rémi Collignon-Ducret
- **Tags**: testing, e2e, ci, docker

## Context

Every `crates/magnetar/tests/e2e_*.rs` suite boots `apachepulsar/pulsar:<tag>` as a standalone container and waits for the stdout line `Created namespace public/default` before running.
56 of the 59 suites defaulted the tag to `latest`, with `MAGNETAR_PULSAR_IMAGE_TAG` as the override; the module comments described that as "Pulsar 4.0 LTS" and `docs/testing.md` as `4.0.4`, while the tag actually resolved to `4.2.4` on 2026-09-18 and the suite was green on `main` on 2026-10-04.

Apache Pulsar 5.0.0 was published on 2026-10-01 and `latest` moved to it (digest `sha256:7222f319…`).
Its standalone start-up log is structured: the line is now `PulsarWebResource - Created namespace {clientAppId=null, namespace=…}`, so the harness's wait never matches and every suite fails with `WaitContainer(StartupTimeout)` after two minutes — four red e2e shards on PR #866 with no client change involved.
Measured on 2026-10-08: `4.0.6` prints `[null] Created namespace public/default` ~100 ms after `messaging service is ready`; `5.0.0` prints the structured form ~100 ms after `Messaging service is ready`.

Two fixes were possible: make the wait marker version-agnostic (`"public/default"` is the first such substring on both versions) and keep tracking `latest`, which would also switch the whole suite to a new broker major inside an unrelated pull request; or pin the default tag to the last version the suite was green on.

## Decision

1. `DEFAULT_IMAGE_TAG` is `"4.2.4"` in every suite that defaulted to `latest`; the two suites pinned at `4.0.4` and the one at `4.2.3` are unchanged.
   The PIP-33 two-cluster fixture (`crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml`) pins the same tag in its six services: it also ran `latest`, and its bring-up step failed on CI once the tag moved.
   `MAGNETAR_PULSAR_IMAGE_TAG` / `MAGNETAR_PULSAR_IMAGE_REPO` keep overriding it, so a compatibility run against `5.0.0` or any other tag is one environment variable away.
2. CI pre-pulls `4.2.4` in place of `latest`; `5.0.0-M1` and `4.0.4` stay pre-pulled for the suites that use them.
3. Moving the default to a Pulsar 5.x tag is a separate decision: it needs the wait marker changed in every suite (the structured log no longer carries `Created namespace public/default`) and a review of the suite against the new major, and it must land on its own, not as a side effect of a tag that moved.

## Consequences

- The e2e suite is deterministic with respect to the broker it runs against; a Docker Hub tag move can no longer turn every shard red overnight.
- The pin has to be bumped on purpose, with a run of the suite, when the project wants to move the broker baseline.
- `docs/testing.md` and `CLAUDE.md` now name the tag the suite really runs.

## Alternatives considered

- **Version-agnostic wait marker, keep `latest`.** Rejected for now: it bundles a broker-major switch into whichever pull request happens to be open when the tag moves, and the suite has never been run against 5.0.0 GA.
- **HTTP readiness wait** (`testcontainers` `HttpWaitStrategy` on `/admin/v2/namespaces/public`).
  Semantically the right readiness check and independent of log wording; rejected for this change because it touches 59 files and could not be validated locally (port publishing is broken on the author's host).
  Worth doing when the baseline moves.
