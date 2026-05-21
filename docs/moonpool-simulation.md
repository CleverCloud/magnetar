# Moonpool deterministic simulation

This document explains how the **moonpool** deterministic-simulation engine
fits into magnetar, how to run the simulation suite locally and in CI, and
which sans-io invariants the simulation transitively verifies.

It complements [`ARCHITECTURE.md`](../ARCHITECTURE.md) ("Sans-io design") and
the binding decision records:

- [ADR-0003](../specs/adr/0003-no-channels-rule.md) — ban channel crates.
- [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md) — sans-io
  `magnetar-proto` + swappable engines.
- [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md) — moonpool TLS via
  byte-pipe adapter.
- [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) — clock injection
  on `magnetar-proto`.

## What moonpool is

[`moonpool-sim`](https://crates.io/crates/moonpool-sim) is a deterministic
simulation engine. The application code talks to
[`moonpool_core::Providers`] (a bundle of `NetworkProvider`, `TimeProvider`,
`TaskProvider`, `RandomProvider`, `StorageProvider`); under simulation those
providers are virtualised so a given seed always replays bit-for-bit.

`magnetar-runtime-moonpool` is a second engine on top of `magnetar-proto`,
parallel to `magnetar-runtime-tokio`. Both engines drive the *same* sans-io
state machine — only the I/O and clock plumbing differs:

| Layer | tokio engine | moonpool engine |
| --- | --- | --- |
| Network | `tokio::net::TcpStream` | `moonpool_core::NetworkProvider` |
| Time | `tokio::time::sleep` + `std::time::Instant::now()` snapshots | `moonpool_core::TimeProvider` virtual clock |
| Random | host RNG | `moonpool_core::RandomProvider` |
| TLS | `tokio-rustls` | `rustls::ClientConnection` driven by hand over a moonpool byte pipe ([ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md)) |
| Spawn | `tokio::spawn` | `tokio::spawn` (the driver still selects with `tokio::select!`; determinism comes from the providers, not from replacing tokio) |

Both engines share the no-channels rule: state lives in
`Arc<parking_lot::Mutex<Connection>>`, the driver task is woken through one
`tokio::sync::Notify`, and user-facing futures register their `Waker` in slabs
keyed by `op_id` / `sequence_id` / `request_id` inside the connection state
machine.

## What is exercised under simulation

The moonpool engine carries its own test suite inside
`crates/magnetar-runtime-moonpool/src/**` (see `lib.rs`, `driver.rs`,
`client.rs`, `producer.rs`, `consumer.rs`, `tls.rs`, `transport.rs`). The
tests cover, end-to-end against the sans-io state machine:

- driver loop — wakeup ordering, timer fires, partial reads, EOF handling;
- client surface — `connect`, `lookup`, `partitions`, broker rotation;
- producer — `send`, `flush_batch`, chunked emission (PIP-37), redelivery
  backoff;
- consumer — `subscribe`, `receive`, batch index ACK (PIP-54), negative-ack,
  ack-timeout (the timer is the virtual one);
- reader + table view façades;
- TLS handshake under chaos — `rustls::ClientConnection` driven over the
  moonpool byte pipe per [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md).

Because all timing flows through the injected `now: Instant` / `wall_clock`
provider (see [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md)), the
ack-timeout and redelivery-backoff tests advance the virtual clock instead of
calling `tokio::time::sleep` — runs are reproducible bit-for-bit under a
given seed, with the two documented non-time leaks as the only exceptions
(see `ARCHITECTURE.md` §"Known non-determinism leaks (documented)").

## Running the simulation locally

The simulation suite is a normal cargo test target — no Docker, no live
broker, no network.

```bash
# The canonical invocation. --all-features wires in the moonpool feature on
# the top-level magnetar crate so the engine surface compiles; --locked
# pins the Cargo.lock so reproducers are stable across machines.
cargo test -p magnetar-runtime-moonpool --all-features --locked
```

To reproduce a flaky test under a specific simulation seed, set
`MOONPOOL_SEED` (the moonpool runtime reads it):

```bash
MOONPOOL_SEED=0xdeadbeefcafebabe \
  cargo test -p magnetar-runtime-moonpool --all-features --locked -- --nocapture
```

To sweep a range of seeds locally before pushing — useful when triaging a
schedule-sensitive bug:

```bash
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --all-features --locked -- --quiet || echo "seed $seed FAILED"
done
```

Tests do not require nightly. The `cargo-fuzz` smoke targets in
`crates/magnetar-proto/fuzz/` *do* need nightly, but they are independent of
the moonpool engine.

## What CI runs

`.github/workflows/ci.yml` carries a dedicated `moonpool-sim` job that runs
on every push to `main` and every PR targeting `main` (no `if:` gating). The
job builds the moonpool engine and runs the same `cargo test -p
magnetar-runtime-moonpool --all-features --locked` invocation that you would
run locally:

```yaml
moonpool-sim:
  name: moonpool deterministic-simulation
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@stable
    - uses: Swatinem/rust-cache@v2
    - name: Build moonpool engine
      run: cargo build -p magnetar-runtime-moonpool --all-features --locked
    - name: Run moonpool-sim test suite
      run: cargo test -p magnetar-runtime-moonpool --all-features --locked
```

The job lives alongside the other gates (`fmt`, `clippy`, `build`, `test`,
`doc`, `deny`, `no-channels`, `no-io-deps`, `no-internal-clock`, `e2e`). A PR
cannot merge while `moonpool-sim` is red.

## How the sans-io invariants are enforced

The simulation suite is *behavioural* — it asserts that operations complete
correctly under chaos. The structural invariants of the sans-io split live
behind the `cargo xtask` checks (all in `.github/workflows/ci.yml`):

| Invariant | ADR | Enforcement |
| --- | --- | --- |
| Banned channel crates not used anywhere | [ADR-0003](../specs/adr/0003-no-channels-rule.md) | `cargo xtask check-no-channels` + `clippy.toml` `disallowed-types` + `cargo deny` bans |
| `magnetar-proto` has zero I/O deps | [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md) | `cargo xtask check-no-io-deps` (scans `cargo tree -p magnetar-proto -e features`) |
| `magnetar-proto/src/**` does not read the host clock | [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) | `cargo xtask check-no-internal-clock` (greps for `Instant::now()` / `SystemTime::now()` outside `#[cfg(test)]` blocks and outside the two documented leak files) |
| Generated proto code stays in lockstep with the vendored `.proto` | [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md) | `cargo xtask codegen --check` |
| `rustls`-only TLS | [ADR-0005](../specs/adr/0005-rustls-only-tls.md) | `deny.toml` bans `native-tls`, `openssl`, `openssl-sys` |

Run them all locally before pushing:

```bash
cargo run --manifest-path xtask/Cargo.toml -- check-no-channels
cargo run --manifest-path xtask/Cargo.toml -- check-no-io-deps
cargo run --manifest-path xtask/Cargo.toml -- check-no-internal-clock
cargo run --manifest-path xtask/Cargo.toml -- codegen --check
```

Each is also a separate CI job (`no-channels`, `no-io-deps`,
`no-internal-clock`, plus the codegen check rolled into the `doc`/`build`
pipeline). They are deliberately *not* gated behind the moonpool job — they
must pass even if the moonpool test surface is offline for maintenance.

### The `check-no-internal-clock` allowlist

Two documented leak sites remain inside `magnetar-proto`; they predate the
clock-injection refactor and require dedicated `Random` / `Env` providers to
close. They are listed in `ARCHITECTURE.md` §"Known non-determinism leaks
(documented)" and exempted in `xtask/src/main.rs` (`CLOCK_LEAK_ALLOWLIST`):

- `crates/magnetar-proto/src/producer.rs` — `uuid::Uuid::new_v4()` in
  `ProducerState::emit_chunked` (PIP-37 chunked messages need a uuid for
  the chunk-set id).
- `crates/magnetar-proto/src/auth/token.rs` — `std::env::var()` for one-shot
  `TokenAuth` bootstrap.

When these are closed (by introducing the missing providers), drop them from
both `CLOCK_LEAK_ALLOWLIST` in `xtask/src/main.rs` and the leak section of
`ARCHITECTURE.md` in the same changeset that lands the fix.

## What is *not* yet exercised under simulation

Being honest about the gaps is part of the contract. Today the moonpool
engine surface mirrors the tokio engine (M1 → M4 landed: engine, client,
producer, consumer) but the chaos surface remains conservative:

- **Network partition / packet reordering scenarios** are not yet wired into
  CI. The plumbing supports it (the `moonpool_core::NetworkProvider`
  reorders by design under sim) but we do not yet have a test target that
  enumerates partition shapes.
- **Property-based seed sweeps** are not part of the CI matrix. CI runs the
  test binary with a single seed (the moonpool default). Multi-seed
  scheduling is a manual loop today; M9 plans automation.
- **Multi-broker failover / PIP-121 service-URL rotation** is open work
  (see `CLAUDE.md` §"Open"); the moonpool surface for it does not exist
  yet.
- **Transparent in-flight publish replay across reconnect** (Stage 3
  follow-up in `docs/implementation-plan.md`) — the sans-io machinery is
  there (`Connection::reset`, epoch bump, rebuild plumbing), but the sim
  test specifically asserting "every in-flight publish replays" is open.
- **TLS handshake chaos**: handshake correctness is verified, but
  per-handshake-byte chaos (corrupted handshake records) is not yet swept.

Tracking those gaps lives in
[`docs/implementation-plan.md`](implementation-plan.md) and the
"Open" section of [`../CLAUDE.md`](../CLAUDE.md).

## Adding a new simulation test

1. Locate the file in `crates/magnetar-runtime-moonpool/src/` (e.g.
   `producer.rs` for producer-level coverage, `client.rs` for connect /
   lookup paths).
2. Use the existing test harness in that module — every module has a
   `#[cfg(test)] mod tests` block with helpers that wire up the moonpool
   providers.
3. Drive timing through the injected `now: Instant` parameter — do not call
   `std::time::Instant::now()`. The `cargo xtask check-no-internal-clock`
   job guards this for `magnetar-proto`; for the engine itself, the
   convention is enforced by code review.
4. Run the suite locally (`cargo test -p magnetar-runtime-moonpool
   --all-features --locked`); add a seed sweep if the test exercises
   scheduling behaviour.
5. If you add a new sans-io invariant or close one of the documented
   leaks, update the matching ADR + this document in the **same**
   changeset.
