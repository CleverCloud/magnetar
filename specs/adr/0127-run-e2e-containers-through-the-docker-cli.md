# ADR-0127 — Run the e2e containers through the `docker` CLI instead of `testcontainers`

- **Status**: Accepted (replaces the `testcontainers` mechanism that [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md), [ADR-0109](0109-pin-the-e2e-broker-image-tag.md) and [ADR-0115](0115-share-one-version-agnostic-pulsar-e2e-fixture.md) name; every decision those ADRs take about what the e2e suite runs, which broker it boots and how readiness is judged remains binding)
- **Date**: 2026-11-18
- **Decider**: Florentin Dubois
- **Tags**: testing, e2e, dependencies, supply-chain, cargo-deny

## Context

`cargo deny check` fails on `main` with [RUSTSEC-2026-0440](https://rustsec.org/advisories/RUSTSEC-2026-0440): Astral ended maintenance of `astral-tokio-tar` on 2026-11-03 and deprecated every version.
The crate reaches the graph only as a dev-dependency, through `testcontainers` 0.28.0, which is the latest release on 2026-11-18 and depends on it unconditionally (its `copy_to_container` support needs a tar writer).
The upstream replacement, [testcontainers/testcontainers-rs#1006](https://github.com/testcontainers/testcontainers-rs/pull/1006), is an open, breaking pull request with no release.
No version of `testcontainers` this repository can depend on clears the advisory.

The e2e suites use a small part of `testcontainers`: run an image with environment variables, a command and published ports; wait for a line on stdout; read the mapped host port and the logs; run a command inside the container and read its exit code and output; stop it; remove it on drop.
None of them copies a file into a container, which is the only feature that needs a tar writer.
`crates/magnetar/tests/e2e_reconnect.rs` already shells out to `docker restart`, and CI already runs `docker pull` and `docker compose` before the tests, so the `docker` client is a prerequisite of the e2e suite today.

Alternatives considered:

1. **Ignore RUSTSEC-2026-0440 in `deny.toml`.** Rejected: it keeps an unmaintained crate in the graph and turns the check off for it until someone remembers to turn it back on.
2. **Narrow `[advisories] unmaintained` to `workspace`, or `exclude-dev`.** Rejected: both are the same ignore, applied to every crate.
3. **Patch `testcontainers` to the #1006 branch.** Rejected: `[sources] allow-git` is empty on purpose, and pinning a third-party, unmerged branch is a supply-chain decision of its own.
4. **Use `bollard`, the Docker API client `testcontainers` wraps.** Viable: `bollard` 0.21 no longer depends on a tar crate, and `cargo deny check` passed with it in place of `testcontainers`. Rejected because the `docker` client already does everything the suites need without a Rust dependency, and honours `DOCKER_HOST` and the active Docker context exactly as an operator's shell does.
5. **Wait for a `testcontainers` release.** Rejected: it leaves `cargo deny check` red on `main` for an unknown time.

## Decision

The façade's e2e suites start every Docker container through `crates/magnetar/tests/common/container.rs`, which runs the `docker` command-line client, and `testcontainers` leaves the workspace.

- The module keeps the builder vocabulary the suites already use (`GenericImage::new`, `with_exposed_port`, `with_wait_for`, `with_startup_timeout`, `with_env_var`, `with_cmd`, `start`), so the shared Pulsar fixture of ADR-0115 and `cargo run -p xtask -- check-e2e-fixture` keep their meaning: a Pulsar container is still one `GenericImage::new(…)` chain ending in `.start()`, inside `crates/magnetar/tests/common/`.
- `Container::exec` always waits for the command to exit; every suite already asked for that (`CmdWaitFor::exit()`), and a non-zero exit is reported through `ExecResult::exit_code`, not as an error.
- Dropping a `Container` runs `docker rm --force --volumes`, including on a panic, as `testcontainers` did.
- `Container::host` resolves the host exactly as `testcontainers` did: the host of a `tcp://`/`http(s)://` `DOCKER_HOST`; inside a container (`/.dockerenv`, the performance suite runner), the gateway of Docker's default `bridge` network; `localhost` otherwise.
- Every `docker` call runs on `tokio::task::spawn_blocking`, so no async worker thread blocks, and no channel is introduced (ADR-0003).
- `crates/magnetar/tests/common/container.rs` joins `BASELINE_HARNESS_SOURCES` in `scripts/performance_campaign.py`: the performance nightly overlays the candidate's `common/mod.rs` and `common/pulsar.rs` onto an older base checkout, which fails to compile `pub mod container;` without it. The base keeps its own `testcontainers` dev-dependency, unused by the overlaid files.

## Consequences

- `cargo deny check` passes. Sixty dev-only package entries leave `Cargo.lock`, `astral-tokio-tar`, `testcontainers` and `bollard` among them; one (`itoa` at another version) enters.
- The `docker` client must be on `PATH` wherever the e2e suite runs. It already had to be for the `docker restart` suites and the CI pre-pull step.
- A test process that is killed leaves its containers behind, as it did with `testcontainers` (the workspace never enabled its watchdog).
- The module is test code the repository now owns. It is under 400 lines, and any capability a future suite needs (a network, a volume, a file copied in) is added here.

## References

- `crates/magnetar/tests/common/container.rs` — the module.
- `crates/magnetar/tests/common/pulsar.rs` — the shared Pulsar fixture built on it (ADR-0115).
- `xtask/src/main.rs` — `check-e2e-fixture`, unchanged in what it enforces.
- `scripts/performance_campaign.py`, `scripts/test_performance_campaign.py` — `BASELINE_HARNESS_SOURCES`.
- [ADR-0003](0003-no-channels-rule.md), [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md), [ADR-0109](0109-pin-the-e2e-broker-image-tag.md), [ADR-0115](0115-share-one-version-agnostic-pulsar-e2e-fixture.md).
