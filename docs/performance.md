# Performance measurement

The versioned external driver starts with an executable test inventory and native suite measurements.
It is a partial harness: the façade example supplies bounded client/control scenarios and a same-image campaign now connects those scenarios to independent collectors.
The PR workflow reconciles complete functional family inventories and separately reports measured metrics; the advanced dimensions below and baseline qualification remain required work.
Doctests are inventoried and executed as functional evidence; their client performance metrics remain unmeasured.
Production optimization must wait for that qualification.

Run through the workstation's normal Cargo/Mise wrapper:

```sh
cargo run -p xtask -- performance -- inventory \
  --checkout /absolute/path/to/checkout --output /outside/checkouts/inventory
cargo run -p xtask -- performance -- plan \
  --checkout /absolute/path/to/checkout --expected-sha "$FROZEN_SHA" \
  --output /outside/checkouts/plan --shards 4 \
  --environment /outside/checkouts/verified-image/environment.json
cargo run -p xtask -- performance -- compare \
  --base /absolute/path/to/base --candidate /absolute/path/to/candidate \
  --expected-base-sha "$FROZEN_BASE_SHA" --expected-candidate-sha "$FROZEN_HEAD_SHA" \
  --output /outside/checkouts/comparison --repetitions 3
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_performance*.py'
cargo run -p xtask -- performance -- reconcile \
  --base-plan /outside/checkouts/base-plan/plan.json \
  --candidate-plan /outside/checkouts/candidate-plan/plan.json \
  --report /outside/checkouts/shard-0/report.json \
  --report /outside/checkouts/shard-1/report.json \
  --report /outside/checkouts/shard-2/report.json \
  --report /outside/checkouts/shard-3/report.json \
  --output /outside/checkouts/reconciliation
```

`--package` is an explicit partial selection for developing the driver; it cannot qualify workspace coverage.
`--shard N --shards M` partitions the discovered family identifiers by SHA-256 modulo `M`.
Cargo metadata supplies the complete expected family identifiers before compilation.
Each shard records those identifiers and its assignment, then compiles and lists only its assigned executable targets and doctest libraries.
The compiled catalogue must equal that assignment, including empty cfg-stripped families.
A complete campaign must reconcile the union across shards with the expected inventory independently for each reference and axis; missing, duplicate and unexpected families are errors.
`plan` freezes those expected identifiers from metadata without compiling test targets.
Plans intended for reconciliation run in the inspected campaign image with `--environment`, its verified launcher JSON; a diagnostic plan without known image provenance cannot qualify a global union.
`reconcile` verifies every input campaign's final artifact manifest, immutable reference and axis, exact shard indices and assignments, catalogue union and completed nonignored work.
Each manifest must match its plan's package scope, source snapshot, planning/measurement harness digest, symbolized profile, features, compiler, seed and inspected image/Dockerfile digests.
Runner, kernel, CPU, affinity and seed are checked locally for each base/candidate pair; different shards may use different physical CPUs.
It records expected, inventoried, executed, ignored, empty and per-metric covered counts independently for each reference; existing ignored cases never become covered work.
Each plan and reconciliation names its package scope and axis; `workspace_union_verified` is false for a successful partial-package union.
Missing functional execution fails reconciliation even when a family was listed successfully.
Reconciliation failures persist a machine-readable reason and distinguish known functional child failure from invalid/incomplete collection.
Repetitions are bounded to 2–20.
After both builds and doctest execution finish, the driver repeats the base binary first to record base/base variation, then alternates base/candidate order.
The SHA arguments are immutable lowercase 40-character revisions supplied by the caller; each checkout must match its expected side before building.
CI resolves both revisions immutably and retains them together with the event contract: a nightly compares `main`'s head against the `main` head its last successful nightly measured, and a manual dispatch compares the dispatched ref against `main`'s head.
A dispatched branch targeting another branch still compares performance against this frozen `main`; its target branch is not silently substituted.

## Executable inventory and functional work

Each checkout is built with its own `Cargo.lock`, `--locked --release --all-features`, thin LTO and the existing release settings.
The driver sets `CARGO_PROFILE_RELEASE_DEBUG=1` and `CARGO_PROFILE_RELEASE_STRIP=none` to retain attribution symbols.
The source SHA, complete tracked/untracked/ignored source manifest, compiled input manifest, lockfile digest, binary digest, exact toolchain, runner, kernel and CPU are recorded.
The harness digest is separate from the measured revision.
No library allocator or production behavior is changed.

The source tree must be clean unless an explicitly audited overlay is supplied with `--overlay-manifest` (inventory) or `--base-overlay`/`--candidate-overlay` (compare).
The overlay JSON has `schema_version: 1`, the exact `measured_sha`, and a `files` array containing every dirty file's `path`, two-character Git porcelain `status`, and `sha256`.
Ignored files use status `!!` and also require audit: Git ignore rules never define the source boundary.
The driver refuses missing, extra, changed or unsupported dirty paths, including untracked Rust helpers.
It records the overlay and all regular source files, then checks the source snapshot again after building and executing.
Only the checkout's root `.git` metadata, Cargo `target/` output and Python `__pycache__/` output directories are excluded from the directory snapshot.
The reserved root `target` output is excluded whether `os.walk` classifies it as a directory or an unmounted build-cache symlink; other source symlinks still fail the source boundary checks.
Compiled inputs in those output directories are still named and hashed by the compiler dep-info manifest; their hashes are verified before and after each family execution.
An overlay is preparation evidence, not a substitute for reviewing the actual files.

The workspace membership, package selection and target `test`/`doctest` metadata determine expected families, including libraries with `test = false` and `doctest = true`.
Executable builds group assigned targets by package using explicit `--lib`, `--test`, `--bin`, `--example` or `--bench` selectors; they do not build the whole workspace before sharding.
Raw metadata, assignment and each build command are retained.
Cargo's `compiler-artifact` messages locate the executable test binaries.
Each reference uses a separate campaign-owned Cargo target directory and executes from Cargo's original layout.
This preserves both absolute `CARGO_BIN_EXE_*` paths and companions resolved relative to `current_exe()`.
The scalable-topic suite explicitly builds its cross-package `magnetarctl` companion before timing.
Other companions are discovered from compiler env-dep records and matched to Cargo's non-test executable artifacts; an unresolved child is invalid.
When `cargo test` emits a profile-root companion without an adjacent `.d`, the driver requires exactly one byte-identical executable under Cargo's `deps` directory and retains that executable's dep-info.
The compiler rule must name the associated executable; missing or ambiguous matches, a foreign rule or a path outside the reference-owned target directory are invalid.
The resolution mode, original dep-info path and compiler executable digest remain in the execution closure.
The execution closure records every harness/companion's SHA-256, ELF build ID, raw compiler env-dep/dep-info and compiled sources.
Copies preserve the Cargo-relative layout under `execution/` for immutable raw artifacts; actual execution paths remain in the reference-owned target directory.
Executable, retained-copy, dep-info and compiled-source digests are checked before and after measurements.
A mutable shared target cannot supply execution children for both references.
Their `--list --format terse` and `--list --ignored --format terse` outputs supply the catalogue, rather than a lexical count of source attributes.
An empty cfg-stripped target stays visible.
Ignored cases stay visible and are excluded from completed work explicitly; an ignored case is not a covered measurement.
A catalogue containing only ignored cases is explicitly unmeasured with completed work zero and null metrics; it is not launched or credited as coverage.
Each library's doctests are inventoried with stable `cargo test --doc -- --list --format terse`, including its ignored catalogue.
Their compiled input closure comes independently from stable `cargo rustdoc --lib --target <rustc-host> --release --all-features --locked -- --emit=dep-info --cfg doctest`, which enables both rustdoc's `cfg(doc)` and the doctest configuration.
The explicit host target determines the exact crate-named dep-info path; no unit-test executable or first matching output file is required, so `[lib] test = false` libraries remain discoverable.
The command, raw dep-info and its SHA-256 are retained, including generated or ignored inputs referenced by documentation macros.
The execution entry references the retained dep-info copy and uses its own reference-specific Cargo target directory, so a later reference build cannot overwrite that proof.
Input hashes are checked across catalogue generation and before/after execution, and the dep-info digest must remain unchanged.
Selected doctest families execute once per side before suite timing, and every observed name/ignored status and all rustdoc result groups must match the frozen catalogue.
Edition 2024 merged doctests and compile-fail doctests may produce separate summaries; every group must succeed with no filtering.
Rustdoc's exact ` - compile` and ` - compile fail` execution suffixes identify compile-only `no_run` and compile-fail cases whose listed names omit those suffixes. The parser reconciles those two formats, still requiring every listed name, ignored status and completed count; compile-only cases are functional compilation evidence, not execution or performance measurements.
Doctests have functional scope `cargo-rustdoc-compilation-and-execution`, and their performance metrics remain unmeasured because stable rustdoc compiles snippets while executing them.
Their passed counts establish functional compilation/execution only, and neither ignored nor empty families count as covered performance measurements.
Native test execution retains existing assertions and deadlines.
Successful work requires one valid libtest summary matching the inventory, no failures, no filtering and at least one passed test.
Missing results and mismatched work counts produce invalid observations; a known nonzero functional child exit produces `functional-failure`.
Negative tests use their original assertions and are counted as passed test cases, never as zero delivered messages.

## Measurement scope

Native elapsed time uses Python's monotonic `perf_counter_ns()` around each fresh GNU time launcher process.
Its scope includes launcher creation, log opening, the test harness, fixture setup, Docker requests, warmup, assertions and teardown.
Nanoseconds are the storage unit; `elapsed_resolution_ns` records the clock resolution reported on that runner and does not claim nanosecond accuracy.
GNU time's `%e` elapsed value is retained as a rounded centisecond diagnostic and never converted into native nanoseconds.
GNU time's `%M` peak RSS is KiB for the test process and its waited descendants, including a child CLI where a test launches one.
Docker-managed brokers are separate daemon-owned processes and excluded from this RSS scope.
These observations have scope `test-process-tree-with-fixture-and-launcher` and denominator `passed-test-cases`.
They do not quantify native transfer latency or broker memory.
Compilation and image pulls from the build step are outside these observations, but an image pull triggered by a test fixture remains in suite elapsed time.

Each family records coverage separately for native time, RSS, syscalls, allocations and copies.
Native/RSS coverage is reconciled with the observations after execution: `direct-passed-cases`, `invalid`, or `unmeasured`, with a reason for unavailable coverage.
Ignored tests remain explicit exceptions and are never included in completed work.
Suite native/RSS values do not establish client syscall/allocation/copy coverage.
Suite instrumented metrics remain unmeasured; the separate scenario campaign does not grant equivalent coverage to unrelated test families.
The parsers support separate strace summaries and Valgrind DHAT heap/copy profiles for the subsequent qualified scenarios.
DHAT copy measures intercepted copying calls only; inlined copies are outside that observation.
DHAT heap's `gb` fields represent live bytes at the global maximum, not a sum of independent point peaks.
Total process profiles must remain available when stack-based attribution separates client and harness costs.

The PIP-33 test defaults to fixed localhost ports and accepts the isolated endpoint variables documented in [testing.md](testing.md).
The driver refuses to run it against an ambient host fixture and reports it unmeasured unless the launcher supplies inspected task-owned processes with the exact main endpoint bindings.
It never starts, reconfigures, stops or removes the shared fixture.

## Results and comparisons

`report.json` schema version 1 retains both inventories, provenance, raw samples, completed work, per-metric coverage, unavailable reasons and comparison rows.
`report.md` links the raw report and shows median, range, absolute/relative delta and informative verdict.
Per-suite stdout, stderr and GNU time output remain adjacent to the report, outside product checkouts.
The unit is named on every row; suite elapsed time is `ns/suite-launcher-lifetime`, RSS is `KiB/process-and-waited-descendants-peak`.

Executable catalogues run through Cargo with a controlled runner for both normal and ignored lists. Each target retains Cargo's actual package working directory, package/manifest variables, binary companion paths and Linux dynamic-library search path; a direct-launch guard restores and records that context for every observation. Cargo compilation/cataloguing finishes before timing. The Python context guard runs before the measurement window, then executes GNU time directly over the harness and waited descendants. Its recorded monotonic start fixes the native interval; its own RSS is excluded. The context has its own normalized comparison identity (checkout and target bindings remain explicit in raw provenance); a differing or missing context refuses comparison and reconciliation. The captured environment is an explicit allowlist of Cargo runtime variables, Linux loader/tool/home/cache paths, the replay seed and named fixture image/endpoints. Every retained value participates in the runtime identity; ambient tokens, secrets and arbitrary variables are neither serialized nor inherited by the test ELF. Cargo configuration with a nonempty `[env]` block is explicitly unsupported and fails inventory. Doctests continue to execute through Cargo/rustdoc. These contracts follow Cargo's [test working-directory rules](https://doc.rust-lang.org/cargo/commands/cargo-test.html#working-directory-of-tests) and [runtime environment rules](https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates).

A comparison refuses mixed harness, profile, features, toolchain, runner, kernel, CPU or broker digests.
Different lockfiles remain visible because dependency changes belong to the candidate.
A changed executable catalogue, test/helper source, scope, denominator or completed work makes that family incomparable even when both sides report the same number of passed cases.
The scenario digest reads the executable's rustc dep-info (`.d`) file, including `#[path]`, `include!`, and embedded-file inputs wherever they live in the checkout.
Missing dep-info is an invalid inventory; ignored compiled fixtures cannot silently disappear.
Cargo build scripts may declare a [directory input](https://doc.rust-lang.org/cargo/reference/build-scripts.html#rerun-if-changed), including the CLI's Git reference directory in a regular checkout.
Such inputs expand into every directory marker (including empty directories) and regular file leaf in both compiler and scenario maps.
Expansion and digests derive from one recursive snapshot, then a second read must match before acceptance; overlapping inputs with conflicting digests are refused.
File entries retain their content SHA-256; `directory-sha256:` digests cover sorted paths, entry types and file digests, so membership, type and content changes are detected before and after execution, even if a file contains the exact directory encoding.
Symlinks, special files and escaping directory inputs are refused; Git metadata has no exclusion from an explicit compiler rule.
The retained source archive includes the directory markers and every expanded leaf without implicit recursive traversal.
The digest additionally includes every file in that crate's `tests/` directory conservatively.
For embedded unit tests it also includes that crate's `src/` directory because production code and test bodies share those files; changing such a source may leave native/RSS coverage valid while refusing its suite comparison.
Integration suite comparisons can still compare changed production implementations when the fixed test/helper closure is identical.
The separately frozen client scenarios supply scoped costs for implementation changes affecting embedded test sources; their pipeline execution does not by itself qualify a baseline.
Added/removed families remain in the report with the absent side unmeasured.
Identical revisions, compiled sources and build configuration label the comparison as calibration, even when embedded target paths make harness ELF digests differ.
The global suite label requires known matching revision, source manifest, lockfile and execution configuration. Unmeasured family effects never establish calibration: different references with refused family comparisons are `base-candidate-partial/unmeasured`. Functional failure and collection validity remain separate report states.
An identical full execution closure also establishes calibration; an identical harness alone does not when its companion differs.
These deltas describe observed variation and do not demonstrate a product performance effect.
Missing values are null, distinct from measured zero.
A zero baseline reports an absolute delta and new cost without an infinite percentage.
Observed performance regressions are informative and do not fail the command.
Invalid collections return an error status separately from performance verdicts.
Current campaigns always report partial because scenario/instrumentation coverage and doctest performance measurements are incomplete; they cannot be presented as a qualified baseline.

No privileged PR execution or external publisher is part of this initial driver.
The complete workflow executes all families nightly and on manual dispatch with read-only repository permission, publishes raw artifacts and appends the Markdown to the Actions summary.

## Existing isolated scenario example

`crates/magnetar/examples/performance.rs` accepts one immutable JSON request file and prints one JSON observation only after successful work and close.
Build with the same symbolized profile used by the inventory:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
  cargo build -p magnetar-driver --example performance --features moonpool --release --locked
target/release/examples/performance /outside/checkouts/request.json
```

Example request for a task-owned, already prepared broker:

```json
{
  "scenario_id": "roundtrip",
  "scenario_revision": "frozen-scenario-revision",
  "runtime": "tokio",
  "service_url": "pulsar://127.0.0.1:6650",
  "topic": "persistent://public/default/unique-owned-topic",
  "messages": 16,
  "payload_bytes": 1024,
  "seed": 17,
  "batching": false,
  "control_cost_multiplier": 1
}
```

Each run requires a new topic/subscription and identical prepared broker state on the two sides.
The example does not create or remove a broker; its caller must verify fixture identity and isolation.
Unknown JSON fields or scenarios, unsupported runtime/transport and unbounded requests are rejected.
Messages are bounded to 1–100000, payloads to 8–1048576 bytes, and calibration multiplier to 1–10.

`producer` confirms sends and flushes with no consumer alive during its run window (`producer-confirmed-send-and-flush`); delivery validation follows outside that window.
`consumer` preloads the corpus with no consumer alive, then starts the run window before creating a fresh subscription and receiving/acknowledging messages (`consumer-subscribe-transfer-and-ack`).
`roundtrip` sends, receives and acknowledges (`client-send-receive-and-ack`); `idle` performs one 100 ms idle cycle with an open producer and consumer (`idle-client-open-producer-and-consumer`).
Tokio and Moonpool with real `TokioProviders` are available over plain TCP.
Moonpool uses the existing supervised constructor to support proxy lookup routing, even when reconnect configuration is absent.
The payload oracle verifies exact identities/content, rejects unexpected identities, truncation and duplicates, and requires completed work to equal requested work.
The warmup consumer is closed before preparation of the measured traffic.
The durable subscription is reopened afterward, so replaying the acknowledged warmup message fails the payload oracle.
Setup, warmup, validation and close remain visible in external lifecycle profiles and must not be attributed solely to the native run scope.

Non-idle scenarios finish with a distinct terminal marker outside the work denominator and native run window.
The drain validates the complete marker payload, validates and acknowledges every intervening corpus message, and requires all requested identities before accepting the marker.
The marker's delivered position must match its confirmed send: the only accepted representation difference is absent non-batch receipt size `-1` versus delivery size `0`, with both batch indices `-1`; full equality then preserves all other position fields, including the feature-dependent segment.
Both raw positions remain in the observation.
After the broker confirms the marker ACK, the example records the broker's last position, refuses a broker-reported message after the marker, requires an empty local queue and zero producer pending work, and records the end-of-topic flag separately.
The bounded oracle requires a new non-partitioned topic and a single producer; it does not prove absence of arbitrarily late redelivery or qualify batch-position semantics.

Payload preparation and warmup precede the native `Instant` run window; consumer/producer close and client close follow it.
The observation retains raw send/receive-and-ACK latency samples, completed operations, confirmed payload bytes, runtime/seed/payload dimensions and ordered phase timestamps.
Phase markers on stderr use UNIX epoch nanoseconds for collector alignment; this storage unit is not a guarantee of timestamp accuracy.
The denominator is confirmed or received-and-acknowledged messages, except idle's explicit single cycle.
Concurrency is currently one with sequential receipts.
When enabled, batching is configured for at most 32 messages/256 KiB and 1 ms publish delay; sequential awaited receipts do not prove that multi-message batches formed.

`control-empty`, `control-allocation`, `control-syscall` and `control-copy` run bounded loops with denominator `control-cycle`.
Their scope is `calibration-control-loop` and transport is `not-applicable`; they create no Magnetar client.
The allocation control explicitly uses `Vec<u8>`, so `payload_bytes` determines byte capacity and the exact net realloc growth per cycle.
Allocation exercises `Vec` growth; syscall writes one byte to an already opened `/dev/null`; copy verifies a safe slice copy, which may be inlined and invisible to DHAT copy.
`control-invalid` exits unsuccessfully without an observation; the multiplier increases control cost without changing completed cycle count.
These controls qualify collector scope only after actual instrumented runs on the same built binary.
They do not establish universal malloc/copy counts or a Magnetar performance gain.

The example observation is wrapped by the separate campaign below, which attaches exact build/fixture/collector provenance and executes separate native/instrumented repetitions.
TLS, grouped ACKs, concurrent traffic, explicit feature/fault cells, quiescent RSS/retention, per-family equivalent associations and complete CI coverage remain required work.
Current test/control smokes prove functionality; they are not a qualified performance baseline.

## Same-image scenario campaign

Build the local collector image before the campaign; this command publishes no registry image:

```sh
perf_dockerfile_sha=$(sha256sum scripts/performance/Dockerfile | cut -d " " -f 1)
docker build --build-arg MAGNETAR_DOCKERFILE_SHA256="$perf_dockerfile_sha" \
  --iidfile /outside/checkouts/image.id \
  -f scripts/performance/Dockerfile scripts/performance
```

The Dockerfile pins Rust 1.98.1 through an immutable base image and pins the strace 6.1, heaptrack 1.4, Valgrind 3.19 and clang/GSSAPI package versions.
The launcher requires the exact derived image ID reported by `docker image inspect`, not a mutable tag.
It verifies that the task-owned broker's exact container ID, image ID, ownership label, running state and loopback endpoint match the supplied contract before creating the campaign container.
No host credentials, home directory or Docker socket is mounted in this scenario container.
The campaign container runs as the invoking host UID/GID, so retained artifacts and dedicated Cargo/target caches remain writable by that runner. Its HOME is a private, ephemeral 16 MiB tmpfs with mode 0700; no host home is reused. An explicit suite Docker socket mount adds only the socket's freshly observed numeric GID. The launcher verifies the actual user, groups, HOME, Cargo cache and tmpfs configuration before start and retains the raw container inspection. This lets the host deduplicate and clean its own completed artifacts without changing shared ownership or permissions.
CI planning and example contracts use the same UID/GID and private HOME policy, with an ephemeral Cargo home under container `/tmp`. Planning retains and verifies its actual process identity; it cannot leave root-owned plan directories in the artifact bind mount.
The inspected image and explicit CPU affinity are checked again before execution; package/compiler drift or missing environment identity fails collection.

The same launcher accepts `--suite --shard N --shards M` to build the metadata-assigned targets and execute their native/RSS observations in that image.
The worker assigned `e2e_pulsar_proxy` inspects Docker's default `bridge` network and supplies its unique IPv4 gateway as `MAGNETAR_E2E_DOCKER_HOST_GATEWAY`. Without that prerequisite the existing test returns before exercising the proxy, which the positive fixture-use guard rejects. The worker freezes network identity, IPAM and options, rechecks them before and after execution, and retains the raw inspections. Network membership may change as test-owned containers start and stop. Missing, ambiguous, custom or changed network/gateway identities fail collection; other shards do not inherit the gateway. The launcher's `--proxy-network` contract and explicit `--binding` are tied to these inspections, the actual container environment and the captured Cargo runtime identity. The three private PIP-33 URL bindings remain independently checked against their inspected fixture. Tests, fixture-use assertions and deadlines are unchanged.
`--package magnetar-proto` can bound a local pipeline probe; such a report remains an explicit partial package selection.
The suite keeps each reference's ELF and compiler dep-info in its own artifact directory before the second build can reuse a cache.
Both builds and doctest execution finish before retained native windows, with seed 17 fixed on the launcher path.
Raw metadata and expected/assigned identifiers remain alongside source archives, per-reference catalogues and actual execution reports.
A child nonzero exit is a functional failure; malformed or missing successful-process output is an invalid collection, with metrics null in either case.
An identical suite ELF is calibration even when measured cost ranges do not overlap; it has no product-effect verdict.
The suite path does not qualify the ambient PIP-33 fixture or grant instrumented coverage to test families.

```sh
cargo run -p xtask --locked -- performance -- campaign --launch \
  --base /absolute/path/to/base --candidate /absolute/path/to/candidate \
  --expected-base-sha "$FROZEN_BASE_SHA" --expected-candidate-sha "$FROZEN_HEAD_SHA" \
  --base-overlay /outside/checkouts/base-overlay.json \
  --candidate-overlay /outside/checkouts/candidate-overlay.json \
  --image-id "$FROZEN_IMAGE_ID" --broker-container "$OWNED_BROKER_ID" \
  --broker-image-id "$FROZEN_BROKER_IMAGE_ID" --fixture-label "$OWNERSHIP_LABEL" \
  --service-url "$PRIVATE_BROKER_URL" --cargo-cache /outside/checkouts/cargo-cache \
  --build-cache /outside/checkouts/build-cache \
  --output /outside/checkouts/new-campaign --repetitions 2 \
  --scenario roundtrip --runtime tokio --messages 16 --payload-bytes 256
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_performance*.py'
```

Each checkout retains its own lockfile and measured SHA, while both must carry identical, explicitly audited example source bytes.
Both reference builds finish in the same image with the same symbolized release profile and `moonpool,scalable-topics` features before any retained native timing starts.
An optional mutable build cache saves dependency compilation; Cargo still builds each reference with `--locked`, and the executed ELF is copied into that reference's retained artifact directory.
The report records the ELF SHA/build ID, dynamic glibc symbols, compiler/Cargo versions, profile/features, source manifest and a complete source tar with its SHA.
The source/ELF/lock manifests accompany raw artifacts; an executable alone is not a reproducible source artifact.

The campaign first executes bounded base/base repetitions, then alternates base/candidate order.
Native, strace, glibc libmemusage and DHAT-copy passes run separately.
Only the native pass supplies run duration, raw latency arrays, launcher elapsed time and clean CPU/RSS measurements.
Instrumented durations remain raw evidence and are excluded from native comparisons.
Every request gets a fresh topic/subscription binding, retained separately from semantic workload identity.
The semantic identity retains runtime, scenario source/revision, messages, payload, seed, batching and cost multiplier, so changing work cannot masquerade as the same benchmark.
This pipeline does not establish that broker process state or runner noise is stable: the base/base samples and controlled broker activity still require baseline qualification.

Tool calibration checks exactly 1000/2000 `getpid` calls, malloc/calloc/realloc calls and net growth/peak, and 1000/2000 symbolized forced library copies.
The double workload is a declared calibration perturbation, not a changed product workload presented comparable.
The DHAT workload point must match exactly; runtime loader copying is retained separately instead of adding a tolerance to the whole-process total.
An empty executable and an intentionally unsuccessful child are exercised, and missing collection data is refused.
The scenario's terminal drain, completed message/payload counts, ordered phases and latency sample counts must match the immutable request.
Collector panics, denied tracing, unknown/missing formats and child failures fail the campaign; valid higher performance costs remain informative.

`scenario-report.json` and `scenario-report.md` retain both source manifests, compiler dep-info and input hashes, whole-lifecycle profiles, calibration samples, semantic identities, unique bindings, units and every unavailable dimension.
The input manifest reuses the suite driver’s conservative closure of compiler dependencies and local tests/fixtures; it is checked again after the campaign.
`artifacts.json` hashes the retained source/raw/report files outside the mutable target cache.
glibc libmemusage measures dynamic malloc-family calls from Rust System and C dependencies together; realloc bytes mean cumulative net growth, not final requested sizes.
DHAT-copy covers intercepted library copies and cannot establish inlined, kernel, DMA or complete application copying.
TLS, effective batch formation, grouped ACKs, concurrent traffic, pure/SimProviders scenarios, feature/fault cells, quiescent retention and unrelated family equivalents remain explicitly unmeasured.
Doctests remain functional compilation/execution evidence with unmeasured performance.

The inspected image must carry matching Dockerfile SHA-256 and frozen base digest build labels; stale or unlabeled images are refused and the raw inspection is retained.
The image pins Debian Go 1.19.8 (`golang-go` 2:1.19~1 and `golang-1.19-go` 1.19.8-2) and checks both installed package versions alongside the collectors.
Go is required by the existing all-features FIPS build; aws-lc-fips-sys 0.14.2 requires at least 1.17.13.
The image also copies only the Docker 29.7.2 CLI from an official OCI image pinned by digest; no daemon or plugins are installed.
Its source digest, executable SHA-256 and client version are retained separately from the Debian package inventory in planning and execution provenance.
Existing reconnect tests invoke `docker restart` and `docker stop` through this client, using the suite runner's explicitly mounted socket and numeric socket group; no Docker API version override is supplied.
An unsuccessful suite build or catalogue command persists the original command diagnostic and child exit code as `functional-failure`, with stage `build/inventory` and null metrics.
The fixture observer still verifies its ready/end barriers after that failure but credits no test or fixture-use coverage; a successful launch without a suite report remains invalid.
An internal invalid collection without a report remains fatal and preserves its original structured reason, status and stage through the outer launcher; it cannot provide executed or fixture-use coverage.
The final `campaign-artifacts.json` is generated after container exit and includes the inner `artifacts.json`, environment, raw image inspection, launch container and exit status; hashes can be checked with `verify_artifact_manifest`.
Each collection writes a structured `.collection.json`; child failures are `functional-failure`, profiler/format failures are `invalid-collection`, and campaign failures persist `campaign-failure.json` with null metrics.
An identical ELF is explicitly `calibration-identical-elf` and has no product effect verdict, even when ranges are disjoint; the report separately summarizes base/base observations.
Client scenario calibration also accepts identical conservative checkout inputs, compiler inputs, lockfile, build command/configuration, harness and semantic workload (`calibration-identical-inputs`), even across different Git revisions or ELF bytes. Revision and full-tree hashes remain provenance; ELF reproducibility is reported separately. The observed Cargo root `performance.d` from the accepted pipeline contains 104 local inputs across seven crates (magnetar, magnetar-admin, magnetar-auth-oauth2, magnetar-fakes, magnetar-proto and both runtimes), including the generated `src/pb/*.rs`. It contains no tests, build.rs, Cargo.toml or .proto inputs. The full source manifest remains provenance and a conservative identity guard; differences outside the compiler rule have unknown attribution. The input manifest retains strict dep-info `compiler_sources` separately from its unchanged conservative `scenario_sources` closure. Only changed inputs proven compiled on both references may establish a product effect; conservative test/fixture/helper additions remain comparison guards. Any changed checkout path outside that strict closure (except the independently verified lockfile), incompatible helper/configuration/workload guards, or missing input proof makes the effect unmeasured with null deltas, regardless of extension or directory. This includes documentation, CI files and unproved dependent-crate inputs; their full snapshot remains provenance, and no gain is inferred. Missing semantic identity likewise leaves the effect unmeasured unless identical ELF establishes calibration.
GNU time CPU fields have 0.01-second granularity: zero reported fields are rendered `<0.01`, and CPU deltas/percentages are unavailable because quantization cannot prove an exact difference.
This granularity is not an accuracy claim and is retained in both JSON and Markdown.

## Nightly and on-demand delivery

[performance.yml](../.github/workflows/performance.yml) runs nightly at 01:17 UTC on `main` and on manual dispatch; it no longer runs on pull requests ([ADR-0113](../specs/adr/0113-run-performance-measurement-nightly.md)). A nightly takes the head SHA of the last successful scheduled run as its base and `main`'s current head as its candidate, so consecutive nights cover `main` without a gap; without such a run, or when that head is no longer an ancestor of `main`, the base is `main` as of 24 hours earlier. When base and candidate are equal it writes "nothing to measure" to the summary and skips the workers and reconciliation. A manual dispatch resolves `refs/heads/main` independently and preserves that exact SHA together with the exact dispatched head SHA, including a branch that targets something other than main; a pull request that needs a measurement before merge dispatches the workflow on its branch. The workflow checks out the candidate head and never presents a merge ref or another target as a comparison against main. Only the prepare job holds `actions: read`, to look up the previous nightly. Nightly and manual runs use separate concurrency groups: a dispatch never cancels a running nightly, and a nightly still running when the next one fires makes it wait. The same audited performance example is overlaid onto the base reference; each reference retains its own Cargo.lock and builds with `--locked`. The image is built once with verified Dockerfile/base labels, saved into a checksummed artifact and loaded/inspected by workers. No external image registry is published.

The command entry points are also available through `cargo xtask performance -- campaign ci ...`; the driver remains external to measured binaries:

```sh
python3 scripts/performance.py campaign ci prepare \
  --candidate "$HEAD_CHECKOUT" --base "$MAIN_CHECKOUT" \
  --expected-base-sha "$MAIN_SHA" --expected-candidate-sha "$HEAD_SHA" \
  --output "$PREPARED" --shards 8 --seed-workers 4
python3 scripts/performance.py campaign ci worker \
  --candidate "$HEAD_CHECKOUT" --base "$MAIN_CHECKOUT" --prepared "$PREPARED" \
  --kind workspace --worker 0 --cpu-count 2 \
  --cargo-cache "$CARGO_SOURCE_CACHE" --build-cache "$REFERENCE_TARGETS" \
  --output "$WORKER_OUTPUT"
python3 scripts/performance.py campaign ci reconcile \
  --prepared "$PREPARED" --workers "$DOWNLOADED_WORKERS" --output "$REPORT"
```

| Worker group                | Expected functional scope                                                                                                                                         | Native/RSS observations                                                                                                                                         | Unmeasured metrics                                                                      |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Eight workspace shards      | Every executable target and doctest library discovered from each reference, all features                                                                          | Two base/base calibration launches, then two launches per reference in alternating order for every nonignored executable family                                 | Suite syscalls, allocations, copies; doctest performance                                |
| Four Moonpool replay groups | Complete runtime-moonpool package, no default features, crypto-aws-lc-rs, fixed seeds 1–32 plus the deduplicated union of open anchors from both exact references | Same six executable-family launches per seed; each group's seeds run sequentially; actual compiler features must exclude buggify                                | Other workspace packages on this axis; doctest performance; instrumented client metrics |
| One scenario worker         | Producer, consumer, roundtrip and idle on Tokio and Moonpool, 1024 messages of 1024 bytes                                                                         | Four independent passes per scenario: native, strace, glibc memusage, DHAT intercepted copies; each pass has two calibration and two observations per reference | Advanced dimensions listed below; no equivalent coverage for unrelated test families    |

Doctests execute once per reference/seed before timing; ignored and cfg-empty families remain visible and unmeasured. Every executable family with runnable cases must complete them on each launch. Source/configuration equality means calibration even when absolute Cargo paths change ELF bytes. A successful family union for a partial package or a single replay group cannot qualify workspace coverage. Reconciliation validates every worker, seed, shard, reference and family exactly once; absent, duplicate or unexpected inputs fail. The reconciliation job runs after worker failures and missing artifacts. Valid higher cost is informative and succeeds; functional failure and invalid collection are separate machine states that fail CI.

Suite targets remain reference-owned outside the checkouts for the entire worker, including CLI companions embedded through Cargo paths. Mutable targets are not restored from Actions caches. The dependency cache contains source downloads only; its key is not provenance. Build-session contracts freeze source, lockfile, axis, profile, toolchain, image and harness while allowing sequential seed reuse. Immutable execution copies are deduplicated between completed runs and the archive retains permissions/hardlinks, raw dep-info and compiled sources.

The baseline retains its exact main revision and product sources, with a closed three-file audited harness overlay: `crates/magnetar/examples/performance.rs`, `crates/magnetar/tests/e2e_reconnect_safety.rs` and `crates/magnetar/tests/e2e_pulsar_proxy.rs` are copied byte-for-byte from the candidate. The reconnect fixture waits for the old proxy relays to terminate before releasing held broker frames; notification alone allowed the held ACK response to escape before the requested cut. Its assertions, three test cases and 30-second deadline are unchanged. The Pulsar proxy fixture explicitly sets `clusterName=standalone`, matching the standalone broker; the frozen image's empty proxy default fails OpenTelemetry initialization before the proxy becomes ready. Its standalone sets `PULSAR_STANDALONE_USE_ZOOKEEPER=1`, supplying the ZooKeeper endpoint the proxy requires instead of the standalone's RocksDB default. Readiness waits for the frozen proxy binary's `Started Pulsar Proxy at` message; the old `Started ProxyService at` literal never matched that signal. The standalone advertises its unique container IP through `--advertised-address`; a shell check rejects missing or multiple addresses before `exec`. Advertising `localhost:6650` would send the proxy back to its own listener. The proxy roundtrip assertion, test count and startup/operation deadlines are unchanged. Both references therefore execute the same fixture control, configuration and oracle. Plans, source snapshots, compiler inputs and the harness manifest retain these exact overlay paths and hashes; any other dirty baseline path is rejected.

Workers inspect pinned fixture image IDs and registry digests before/after execution. A continuous Docker event observer records created child fixtures and rejects an image outside that frozen inventory or an image mutation during the suite. Its unique ready marker must be captured before campaign start, and its end marker after campaign exit before bounded observer shutdown; markers use the tools image and are created without starting a process. Every assigned target retains its source/digest and an explicit fixture declaration: an inline `GenericImage::new(...)` call in Rust code means fixture-required for runnable cases; other targets declare fixture-free. The lexical scan masks line/nested block comments and normal/raw/byte/C-string/character literals, preserves lifetimes as code, and refuses unterminated regions. Constructor text in scanner fixtures or documentation cannot declare runtime Docker usage. This changes only the declaration: every actual fixture-required observation still needs a frozen child, and any creation in a fixture-free observation is rejected. Ignored/cfg-empty catalogues require no child. Each valid observation retains epoch boundaries and must have at least one frozen child creation in that interval if fixture-required; a creation contradicting fixture-free scope or credited twice is rejected. This proves positive use per observation, not an exact static count of all fixture instances. Helper-based constructors absent from the assigned declaration require updating that contract before qualification; observed children cannot silently pass as fixture-free. PIP-33 uses its separate six-ID inspection proof. Raw events, markers, observer status and per-observation attribution are included in the final manifest. The observer is outside the measured test process. Existing E2E targets create their own fixtures: their elapsed time includes that lifecycle, and their broker process memory is outside test-process RSS. No standalone broker is required for a suite that does not use one.

Preparation explicitly preloads and freezes Pulsar `4.2.3` for the existing batching integration family, alongside `4.2.4` for the dedicated scenarios. Its image ID and registry digest enter the same immutable inventory before any observation. A pull or other image event during execution, or a child from an unknown image, still invalidates collection.

The eight-scenario union is derived from each retained `scenario-report.json`, not from the intended worker ledger. Reconciliation checks exact main/PR snapshots and ELF identities, image/compiler/harness, runtime/scenario and fixed semantic workload, every pass/repetition, completed work and terminal oracle against manifest-bound request/output/collection/launcher artifacts. A successful but misrouted invocation cannot count toward the expected union.

A PIP-33 shard creates its private fixture only when its assignment includes that family. On an ephemeral Actions runner it first proves that main's localhost ports are free, then retains six process inspections, known image IDs, ownership labels and loopback listeners, including Zookeeper/BookKeeper metrics ports. Both references receive identical endpoints. The launcher freshly inspects these exact IDs before and after the campaign; it accepts only the default ports main actually uses. A local host with occupied historical ports must fail this route; endpoint-only baseline overlays require separate audited provenance and are not silently supplied. Cleanup re-inspects ownership and removes only IDs carrying that worker's unique label.

Prepare/worker/reconciliation job limits are 90/180/30 minutes, with thirteen workers and at most four concurrently. Each measured process is bound to two CPUs; PIP-33 services are limited to two CPUs and 2 GiB each, and the standalone scenario broker to two CPUs/2 GiB. These are job/resource caps, not enlarged test deadlines. Native timing starts after both reference builds finish; no concurrent build or profiler runs during it. A GNU time CPU value 0.00 is displayed below its 0.01-second resolution.

The initial four-shard Actions execution reached the unchanged 180-minute job limit in its last workspace shard before completing the final base repetition. Its two reference builds reported 121 minutes 52 seconds in Cargo, followed by 38 minutes of completed native test-process time; these omit catalogue, provenance and launcher work. Eight workspace shards reduce the assigned targets per build and the fixture launches per worker while retaining every family and all six executable-family observations. The larger partition's duration, disk fit and complete union remain unqualified until an actual Actions execution finishes; no timeout or test deadline is increased.

The Actions summary links a complete report and raw artifacts. Full tables show main, PR, absolute delta, relative delta, units and scope for time, memory and syscall metrics; "main" and "PR" label the base and candidate, which in a nightly are both `main` revisions identified by their SHAs. JSON records requested/completed work, ignored cases, expected/executed/metric coverage and precise null reasons. Runtime parity and the 16-cell crypto build matrix are independent every-PR gates; crypto is build-only evidence. All checkout credentials are nonpersistent, permissions are contents:read (plus actions:read on the prepare job), and this workflow has no secret, privileged comment or pull_request_target path.

Local verification covers contracts, parser/guard mutations, actionlint, a real partial-package Cargo probe, and the earlier collector pipeline. The complete worker matrix, private fixed-port recipe, compressed image/raw upload size and timeout/disk fit still require the first real Actions execution. The observed tools image contains 765,290,295 uncompressed layer bytes; that is not a measured upload size. Historical local probes measured a 7m36s scenario build (37 MiB ELF, 1.2 GiB target) and a 102.1s partial proto route (12.8 MiB ELF, 394.5 MiB additional cache). These do not extrapolate into a qualified workspace budget or product gain. The new example split requires its own rebuilt ELF/scenario replay.
