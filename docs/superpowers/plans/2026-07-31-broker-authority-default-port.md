# Broker Authority Default-Port Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Tokio's independent DIRECT broker-authority rule with the proto-owned rule and make Moonpool resolve a portless DIRECT broker name through its plaintext bootstrap default port.

**Architecture:** `magnetar-proto` exposes one string normalizer that validates an authority and accepts an optional default for scheme-less inputs; `probe_authority` remains its no-fallback wrapper.
Tokio converts the normalized authority back into its existing `ParsedUrl`, while Moonpool stores the current pooled bootstrap's default port and supplies it before resolver dispatch.

**Tech Stack:** Rust 2024 workspace, `magnetar-proto` sans-I/O core, Tokio and Moonpool runtimes, in-process Pulsar protocol fakes, `cargo`, xtask, Markdown/Prettier.

## Global Constraints

- Keep scheme-less, portless broker names supported.
- Add no dependency; `magnetar-proto` remains sans-I/O.
- Preserve `probe_authority`, Tokio's `ParsedUrl`, and the façade signatures.
- An explicit `pulsar://`, `pulsar+ssl://`, or explicit port wins over the bootstrap fallback.
- Reject empty, malformed bracketed, non-numeric-port, out-of-range-port, and unbracketed multi-colon authorities before dialing.
- Do not add a Moonpool supervised TLS constructor or TLS-capable pool.
- Use `ToOwned::to_owned()` rather than `Clone::clone()` when expressing ownership intent.
- Every behavior change carries proto, Tokio, Moonpool, differential, and end-to-end coverage in the same changeset.
- The Moonpool regression must be observed red before production code and green afterward.
- Do not weaken a check to make the validation chain pass.

## Context

Vault recall for `magnetar broker authority default port item 8 implementation plan` returned no matching note.
The approved design is `docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md`.
Baseline command `cargo test -p magnetar-runtime-moonpool --test lookup_direct_multi_broker` passed with 5 tests on 2026-07-31.

## File Map

- `crates/magnetar-proto/src/health_probe.rs`: own structural authority validation and default-port selection.
- `crates/magnetar-proto/src/lib.rs`: re-export the additive canonical helper.
- `crates/magnetar-runtime-tokio/src/client.rs`: adapt normalized authorities into the existing `ParsedUrl` result and correct rejection text.
- `crates/magnetar-runtime-tokio/tests/lookup_direct_multi_broker.rs`: exercise the Tokio DIRECT resolver/dial path for a portless advertised broker.
- `crates/magnetar-runtime-moonpool/src/pool.rs`: retain the pooled bootstrap's scheme-less default port.
- `crates/magnetar-runtime-moonpool/src/client.rs`: supply the pool default to the canonical normalizer before comparing, keying, or dialing.
- `crates/magnetar-runtime-moonpool/tests/lookup_direct_multi_broker.rs`: provide the primary red/green regression witness through a recording resolver and real socket.
- `crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs`: compare both client surfaces' requested `(host, port)` and successful DIRECT route.
- `specs/adr/0091-broker-authority-default-port-unification.md`: record the accepted contract and the Moonpool gap discovered while closing item 8.
- `specs/README.md`: index ADR-0091.
- `CHANGELOG.md`: record the runtime fix, validation hardening, and additive low-level helper.
- `docs/follow-ups.md`: remove item 8 after its acceptance checks exist.
- `docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md`: mark the user-approved design accepted.

---

### Task 1: Canonical Authority Normalizer

**Files:**

- Modify: `crates/magnetar-proto/src/health_probe.rs:91-208,355-489`
- Modify: `crates/magnetar-proto/src/lib.rs:189`

**Interfaces:**

- Consumes: `endpoint: &str`, `schemeless_default_port: Option<u16>`.
- Produces: `pub fn broker_authority(endpoint: &str, schemeless_default_port: Option<u16>) -> Option<String>` and the unchanged `pub fn probe_authority(endpoint: &str) -> Option<String>` wrapper.

- [ ] **Step 1: Add the failing canonical table tests**

Add tests whose literal expectations cover the new fallback and structural rejection independently of the implementation:

```rust
#[test]
fn broker_authority_applies_only_the_selected_default() {
    let cases = [
        ("broker.local", Some(6650), Some("broker.local:6650")),
        ("broker.local", Some(6651), Some("broker.local:6651")),
        ("broker.local", None, Some("broker.local")),
        ("pulsar://broker.local", Some(6651), Some("broker.local:6650")),
        ("pulsar+ssl://broker.local", Some(6650), Some("broker.local:6651")),
        ("broker.local:7000", Some(6650), Some("broker.local:7000")),
        ("[::1]", Some(6650), Some("[::1]:6650")),
    ];
    for (input, fallback, expected) in cases {
        assert_eq!(
            broker_authority(input, fallback).as_deref(),
            expected,
            "unexpected normalization for {input:?}",
        );
    }
}

#[test]
fn broker_authority_rejects_structurally_unusable_authorities() {
    for input in [
        "",
        "pulsar://",
        "broker:",
        "broker:abc",
        "broker:65536",
        "2001:db8::1",
        "[::1",
        "[not-ipv6]",
        "[::1]suffix",
        "[::1]:",
        "[::1]:abc",
        "[::1]:65536",
    ] {
        assert_eq!(
            broker_authority(input, Some(6650)),
            None,
            "{input:?} must be rejected before a dial",
        );
    }
}
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `cargo test -p magnetar-proto broker_authority_`

Expected: compile failure because `broker_authority` does not exist.

- [ ] **Step 3: Implement the minimal normalizer and wrapper**

Use one private validator that distinguishes bracketed IPv6 from DNS/IPv4 authority shapes:

```rust
#[must_use]
pub fn broker_authority(
    endpoint: &str,
    schemeless_default_port: Option<u16>,
) -> Option<String> {
    let (rest, default_port) = if let Some(rest) = endpoint.strip_prefix("pulsar+ssl://") {
        (rest, Some(6651))
    } else if let Some(rest) = endpoint.strip_prefix("pulsar://") {
        (rest, Some(6650))
    } else if endpoint.contains("://") {
        return None;
    } else {
        (endpoint, schemeless_default_port)
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let has_port = validate_authority(authority)?;
    Some(match (has_port, default_port) {
        (false, Some(port)) => format!("{authority}:{port}"),
        _ => authority.to_owned(),
    })
}

#[must_use]
pub fn probe_authority(endpoint: &str) -> Option<String> {
    broker_authority(endpoint, None)
}
```

`validate_authority` must parse bracket contents as `std::net::Ipv6Addr`, accept only an empty suffix or `:<u16>`, require a non-empty unbracketed host, accept zero or one colon outside brackets, and parse an explicit port as `u16`.

- [ ] **Step 4: Re-export and run focused proto tests GREEN**

Re-export both helpers from `crates/magnetar-proto/src/lib.rs`.

Run: `cargo test -p magnetar-proto health_probe::tests`

Expected: all `health_probe` tests pass; update prior malformed-bracket expectations to the newly approved rejection contract.

- [ ] **Step 5: Run proto policy gates**

Run: `cargo run -p xtask -- check-no-io-deps`

Run: `cargo run -p xtask -- check-no-internal-clock`

Expected: both gates pass.

- [ ] **Step 6: Commit the canonical API**

```bash
git add crates/magnetar-proto/src/health_probe.rs crates/magnetar-proto/src/lib.rs
git commit -s -S -m "feat(proto): canonicalize broker authority defaults"
```

### Task 2: Tokio Adoption and Integration Twin

**Files:**

- Modify: `crates/magnetar-runtime-tokio/src/client.rs:1994-2053,3287-3496`
- Modify: `crates/magnetar-runtime-tokio/tests/lookup_direct_multi_broker.rs`

**Interfaces:**

- Consumes: `magnetar_proto::broker_authority(broker_url, Some(bootstrap_scheme.default_port()))`.
- Produces: the unchanged `Result<ParsedUrl, ClientError>` and an accurate `ClientError::Other` rejection.

- [ ] **Step 1: Add a failing rejection-message test**

```rust
#[test]
fn parse_direct_broker_url_reports_unusable_authority() {
    for input in ["pulsar://", "pulsar://broker:abc", "pulsar://[::1"] {
        let err = parse_direct_broker_url(input, Scheme::Plain)
            .expect_err("an unusable authority must be rejected");
        assert!(
            err.to_string().contains("not a usable authority"),
            "unexpected rejection for {input:?}: {err}",
        );
    }
}
```

- [ ] **Step 2: Run the focused Tokio test and verify RED**

Run: `cargo test -p magnetar-runtime-tokio --lib parse_direct_broker_url_reports_unusable_authority`

Expected: failure because the existing scheme-bearing error incorrectly says `unrecognised scheme`.

- [ ] **Step 3: Delegate normalization and retain `ParsedUrl`**

Select the effective `Scheme` from an explicit recognised prefix or the bootstrap scheme, call `broker_authority` with the bootstrap default, rebuild exactly one Pulsar URL, and parse it:

```rust
let scheme = if broker_url.starts_with("pulsar+ssl://") {
    Scheme::Tls
} else if broker_url.starts_with("pulsar://") {
    Scheme::Plain
} else {
    bootstrap_scheme
};
let authority = magnetar_proto::broker_authority(
    broker_url,
    Some(bootstrap_scheme.default_port()),
)
.ok_or_else(|| unusable_broker_authority(broker_url))?;
let prefix = match scheme {
    Scheme::Plain => "pulsar://",
    Scheme::Tls => "pulsar+ssl://",
};
ParsedUrl::parse(&format!("{prefix}{authority}"))
    .map_err(|_| unusable_broker_authority(broker_url))
```

Keep error construction local and private; do not add a new public error variant.

- [ ] **Step 4: Update the equivalence table and run Tokio unit tests GREEN**

Replace the scheme-less `Diverge` rows with calls to `broker_authority(input, Some(6650))` and agreement expectations; retain `probe_authority` wrapper assertions for its no-fallback contract.

Run: `cargo test -p magnetar-runtime-tokio --lib parse_direct_broker_url`

Expected: every focused parser test passes.

- [ ] **Step 5: Add and run the Tokio resolver integration twin**

In `tests/lookup_direct_multi_broker.rs`, add a recording resolver that maps `broker-b.internal` to a random in-process broker while retaining the requested port.
Advertise the portless `broker-b.internal`, connect with `Client::connect_with_resolver_and_provider`, open a producer, and assert the resolver observed `("broker-b.internal", 6650)` and the producer reached broker B.

Run: `cargo test -p magnetar-runtime-tokio --test lookup_direct_multi_broker portless`

Expected: pass; this is characterization coverage for existing Tokio behavior through the now-canonical path.

- [ ] **Step 6: Commit Tokio adoption**

```bash
git add crates/magnetar-runtime-tokio/src/client.rs crates/magnetar-runtime-tokio/tests/lookup_direct_multi_broker.rs
git commit -s -S -m "refactor(runtime-tokio): use canonical broker authority"
```

### Task 3: Moonpool and Differential RED Witnesses

**Files:**

- Modify: `crates/magnetar-runtime-moonpool/tests/lookup_direct_multi_broker.rs`
- Modify: `crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs`

**Interfaces:**

- Consumes: public Tokio and Moonpool client/resolver interfaces.
- Produces: a Moonpool regression test and a client-level cross-runtime parity test that fail while Moonpool preserves a portless physical authority.

- [ ] **Step 1: Add the Moonpool recording resolver and portless test**

Implement a test-only resolver that records the requested `(String, u16)`, returns the original loopback bootstrap address for `127.0.0.1`, and maps `broker-b.internal` to broker B's random socket.

The test must advertise the literal `broker-b.internal`, call `Client::connect_plain_supervised(..., Some(resolver))`, open a producer, and assert:

```rust
assert!(
    requests.contains(&("broker-b.internal".to_owned(), 6650)),
    "the portless DIRECT broker must use the plaintext bootstrap default",
);
assert_eq!(producer_frames_on_b, 1);
```

- [ ] **Step 2: Run the Moonpool regression and verify RED**

Run: `cargo test -p magnetar-runtime-moonpool --test lookup_direct_multi_broker portless`

Expected: fail with `invalid host:port literal "broker-b.internal"` before the resolver sees broker B.

- [ ] **Step 3: Add a client-level differential portless test**

Extend `lookup_direct_multi_broker_equivalence.rs` with one shared observation:

```rust
#[derive(Debug, PartialEq, Eq)]
struct PortlessDirectObservation {
    requested_authority: (String, u16),
    producer_reached_resolved_broker: bool,
}
```

Drive each runtime against its own identical two-broker topology and runtime-specific recording resolver, then compare both observations to:

```rust
PortlessDirectObservation {
    requested_authority: ("broker-b.internal".to_owned(), 6650),
    producer_reached_resolved_broker: true,
}
```

- [ ] **Step 4: Run the differential test and verify RED**

Run: `cargo test -p magnetar-differential --test lookup_direct_multi_broker_equivalence portless`

Expected: Moonpool returns an authority/configuration failure while Tokio produces the expected observation.

### Task 4: Moonpool Default-Port Propagation

**Files:**

- Modify: `crates/magnetar-runtime-moonpool/src/pool.rs:63-100,210-260,647-658`
- Modify: `crates/magnetar-runtime-moonpool/src/client.rs:268-301,708-765,1558-1590,1856-1887`

**Interfaces:**

- Consumes: the canonical proto helper and the current plaintext supervised constructor.
- Produces: `ConnectionFactory::schemeless_default_port: u16`, `ProxyConnectionPool::schemeless_default_port() -> u16`, and `direct_broker_authority(input, default_port)`.

- [ ] **Step 1: Record the current pooled bootstrap default**

Add `schemeless_default_port: 6650` when `connect_plain_supervised` constructs `ConnectionFactory`; add the same field to test factories and include it in `Debug` output.

- [ ] **Step 2: Normalize before pool comparison/insertion**

Pass `pool.schemeless_default_port()` into `direct_broker_authority` and implement the helper as:

```rust
fn direct_broker_authority(
    input: &str,
    schemeless_default_port: u16,
) -> Result<String, ClientError> {
    magnetar_proto::broker_authority(input, Some(schemeless_default_port))
        .ok_or_else(|| unusable_broker_authority(input))
}
```

Keep proxy parsing on `probe_authority(input)` so its no-fallback contract does not change.

- [ ] **Step 3: Update Moonpool unit tests for the explicit fallback parameter**

Cover bare host `broker.local -> broker.local:6650`, explicit TLS scheme `pulsar+ssl://broker.local -> broker.local:6651`, explicit port precedence, and malformed rejection without duplicating the proto table.

- [ ] **Step 4: Run the primary regression GREEN**

Run: `cargo test -p magnetar-runtime-moonpool --test lookup_direct_multi_broker portless`

Expected: pass, with the resolver observing `broker-b.internal:6650` and broker B receiving the producer.

- [ ] **Step 5: Run the differential witness GREEN**

Run: `cargo test -p magnetar-differential --test lookup_direct_multi_broker_equivalence portless`

Expected: both runtime observations equal the expected hostname, port 6650, and successful producer route.

- [ ] **Step 6: Run all focused routing tests**

Run: `cargo test -p magnetar-runtime-moonpool --lib direct_broker_authority`

Run: `cargo test -p magnetar-runtime-tokio --test lookup_direct_multi_broker`

Run: `cargo test -p magnetar-runtime-moonpool --test lookup_direct_multi_broker`

Run: `cargo test -p magnetar-differential --test lookup_direct_multi_broker_equivalence`

Expected: all pass without ignored tests or warnings.

- [ ] **Step 7: Run the test-shaped-fix literal audit**

Run `git diff` over production files and search it for `broker-b.internal` and the test topic literals.
Expected: no production match; ports 6650/6651 are protocol defaults shared by the contract and are explicitly justified.

- [ ] **Step 8: Commit Moonpool and parity coverage**

```bash
git add crates/magnetar-runtime-moonpool/src/client.rs \
  crates/magnetar-runtime-moonpool/src/pool.rs \
  crates/magnetar-runtime-moonpool/tests/lookup_direct_multi_broker.rs \
  crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs
git commit -s -S -m "fix(runtime-moonpool): default portless direct brokers"
```

### Task 5: Decision and User-Facing Documentation

**Files:**

- Create: `specs/adr/0091-broker-authority-default-port-unification.md`
- Modify: `specs/README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/follow-ups.md`
- Modify: `docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md`

**Interfaces:**

- Consumes: verified implementation/test evidence from Tasks 1-4.
- Produces: an accepted ADR, changelog entry, closed tracker item, and approved design status.

- [ ] **Step 1: Write ADR-0091 using ADR-0090's structure**

Record status/date/decider/tags, the discovered Moonpool failure chain, the canonical helper decision, explicit/default precedence, structural rejection table, runtime-specific consequences, compatibility, alternatives, test evidence, and references.

- [ ] **Step 2: Update the ADR index and changelog**

Add ADR-0091 in numeric order to `specs/README.md`.
Under the current unreleased changelog sections, record the additive `broker_authority` API and the portless Moonpool DIRECT fix; state that façade signatures are unchanged.

- [ ] **Step 3: Close the follow-up and design status**

Remove item 8 from both the follow-up index and body only after focused tests are green.
Change the design status to `Approved on 2026-07-31`.

- [ ] **Step 4: Format and verify documentation**

Run: `python scripts/markdown-sembr.py CHANGELOG.md docs/follow-ups.md docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md docs/superpowers/plans/2026-07-31-broker-authority-default-port.md specs/README.md specs/adr/0091-broker-authority-default-port-unification.md`

Run: `npx prettier --write CHANGELOG.md docs/follow-ups.md docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md docs/superpowers/plans/2026-07-31-broker-authority-default-port.md specs/README.md specs/adr/0091-broker-authority-default-port-unification.md`

Run the corresponding `npx prettier --check` command.

Expected: all matched files use Prettier style; `rg -n "^## 8\\.|\\| 8 +\\|" docs/follow-ups.md` returns no matches.

- [ ] **Step 5: Commit the documentation**

```bash
git add CHANGELOG.md docs/follow-ups.md docs/superpowers/specs/2026-07-31-broker-authority-default-port-design.md specs/README.md specs/adr/0091-broker-authority-default-port-unification.md
git commit -s -S -m "docs(adr): accept broker authority defaulting contract"
```

### Task 6: Complete Verification and Branch Audit

**Files:**

- Verify all files changed by Tasks 1-5; no new production file is introduced here.

**Interfaces:**

- Consumes: the complete implementation branch.
- Produces: fresh verification evidence suitable for review and PR creation.

- [ ] **Step 1: Run the real-Pulsar DIRECT e2e witness**

Run: `cargo test -p magnetar --test e2e_lookup_direct_multi_broker`

Expected: the existing full-service-URL/bootstrap-reuse path passes against Pulsar 4.0.4.

- [ ] **Step 2: Run the required Rust validation chain in order**

On Linux, prefix build/test/clippy with `CC=clang CXX=clang++ ASM=clang AR=llvm-ar RANLIB=llvm-ranlib`:

```bash
cargo +nightly fmt --all
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
```

Run the 32-seed Moonpool sweep exactly as documented in `CLAUDE.md`, then:

```bash
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
cargo run -p xtask -- check-no-channels
cargo run -p xtask -- check-no-io-deps
cargo run -p xtask -- check-no-internal-clock
cargo run -p xtask -- check-log-fields
cargo run -p xtask -- check-e2e-container-memory
cargo run -p xtask -- codegen --check
cargo run -p xtask -- check-sim-coverage --enforce
cargo run -p xtask -- check-runtime-test-parity
cargo run -p xtask -- check-crypto-matrix
```

Expected: every command exits zero; sim coverage reports zero uncovered added lines.

- [ ] **Step 3: Sweep references after the API/contract change**

Run repo-wide, file-type-unfiltered searches for `probe_authority`, `broker_authority`, `parse_direct_broker_url`, `direct_broker_authority`, `schemeless_default_port`, the old rejection text, item 8, and ADR-0091.
Classify every hit as updated or unaffected with a reason.

- [ ] **Step 4: Audit scope and commits**

Run: `git diff --stat main...HEAD`

Run: `git status --short --branch`

Run: `git log --show-signature --format='%h %G? %s' main..HEAD`

Give one goal-linked justification per changed file and verify every commit is signed and signed-off.

- [ ] **Step 5: Request publication approval**

Stop before pushing.
Present the exact branch, commit range, verification evidence, and proposed PR title/body; pushing and PR creation require the user's explicit outward-action approval.
