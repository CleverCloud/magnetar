# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Non-durable consumers no longer skip lower unacknowledged messages after reconnect.** Reattachment now reuses only the caller's original start position instead of the highest locally submitted ack, which may be unconfirmed and non-contiguous.
  Established durable consumers continue to defer to the broker cursor.
  (issue #403; ADR-0099)

## [1.4.0] - 2026-08-10

### Added

- **PIP-460 consumer registration, namespace watch, and transaction-coordinator discovery** (behind the default-off `scalable-topics` feature).
  Resolving a scalable topic's layout says what segments exist; it does not say which of them are yours.
  `PulsarClient::scalable_topic_subscribe` registers with the controller leader and returns the initial `ConsumerAssignment` — a `layout_epoch` plus the `segment://` topics this consumer owns — after which every rebalance arrives as an `AssignmentDelta` naming exactly what to attach to (`gained`) and detach from (`lost`).
  An assignment whose `layout_epoch` does not advance is rejected rather than applied: the broker recomputes assignments per layout, so acting on an out-of-order push would attach the consumer to segments that no longer exist.
  `ScalableConsumerType` carries `Stream` and `Checkpoint` only — a `QueueConsumer` never registers, mirroring upstream.
  `PulsarClient::watch_scalable_topics` opens a namespace-level watch over the scalable topics matching a set of AND property filters, delivering a snapshot then incremental diffs; a diff applies `removed` before `added`, per upstream's own note, since the reverse order drops a topic named in both lists.
  `PulsarClient::watch_tc_assignments` opens PIP-473's metadata-driven transaction-coordinator discovery, negotiated on its **own** `supports_tc_metadata_discovery` flag — upstream advertises it independently, so a broker may serve scalable topics without it and `supports_scalable_topics` alone must not unlock the watch.
  Every one of these is gated on the same per-connection negotiation as the rest of the surface, so none reaches a Pulsar 4.x broker.
  (ADR-0093 §D5)

### Changed

- **BREAKING: PIP-460 scalable topics now speak the wire surface Apache Pulsar actually ships, negotiated per connection.** The vendored proto had carried the real PIP-460 messages since rev `7735851` (2026-05-04) and `pb/pulsar.proto.rs` had been generating `SegmentInfoProto`, `ScalableTopicDag`, `CommandScalableTopicLookup` / `…Update` / `…Close` ever since — unused.
  Every consumer instead spoke `pb/scalable_topics.rs`, a hand-encoded projection written while PIP-460 was upstream `Draft`, and nothing in it matched: `BaseCommand` types 80-85 against upstream's 70-78, a `CommandScalableTopicLookupResponse` that does not exist, a separate DAG-watch handshake keyed by a `lookup_token` that does not exist, `SplitEvent` / `MergeEvent` delta frames that do not exist, four segment states against upstream's two, and a fabricated `ProtocolVersion = 22` where upstream gates the feature on `FeatureFlags.supports_scalable_topics` and still tops at `v21`.
  `magnetar-fakes` implemented the same projection, so all four ADR-0024 test layers plus the golden trace were green against bytes no Pulsar broker at any version could parse.
  The vendored proto moves to `v5.0.0-M1` (`8dae0236`), the hand-encoded module is deleted along with the `PB_HAND_MAINTAINED_FILES` carve-out that hid it from `codegen --check`, and the state machine follows the upstream shape: the lookup **is** the watch subscribe, keyed by a client-allocated `session_id`; `CommandScalableTopicUpdate` carries both the initial layout and every pushed one; layouts are whole snapshots ordered by a monotonic `epoch`, with split and merge derived from the `parent_ids` / `child_ids` edges rather than read from event frames; and segment placement is an optional join from the parallel `SegmentBrokerAddress` list.
  **Pulsar 4.x compatibility is preserved and is now explicit**: the client advertises `supports_scalable_topics` on `CommandConnect` and refuses to write any scalable-topic command to a peer that did not answer in kind, returning `ScalableTopicError::BrokerUnsupported` with an empty outbound buffer — which also covers a 5.x broker started with `scalableTopicsEnabled=false`.
  Surface changes, all behind the default-off `scalable-topics` feature: `SegmentDescriptor::broker_url` becomes `Option<String>` and the type gains `broker_url_tls`, `parent_ids`, `child_ids`, `created_at_epoch`, `sealed_at_epoch` and `legacy_topic_name`; `SegmentState` loses `Splitting` and `Merging`; `DagDelta` gains `epoch`; `SplitEvent` / `MergeEvent` lose their `*_at_entry` fields; `DagError` swaps `UnknownSegment` for `Broker` and `Empty`; `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS` and the whole `pb::scalable_topics` module are removed; `Connection::{send_scalable_topic_lookup, open_dag_watch, close_dag_watch}` become `{open_scalable_topic_session, close_scalable_topic_session}`; and every `watch_session_id` field is now `session_id`.
  A default build's public API is unchanged — the Cargo feature now gates client logic only, since the generated wire types are always compiled.
  (ADR-0093, supersedes ADR-0031; vendor bump per ADR-0026 §D4)

### Removed

- **Five uncalled scalable-topic accessors are removed** (behind the default-off `scalable-topics` feature).
  `DagWatchSession::{session_id, epoch}`, `ScalableConsumerSession::consumer_id` and `ScalableTopicsWatch::{watch_id, is_resolved}` had no caller in either engine, the façade or the CLI — their only uses were assertions written to observe them, and publishing API whose sole consumer is a test that asserts it exists is not worth the surface on a module already marked experimental.
  `ScalableTopicsWatch`'s backing `resolved` field goes with its getter.
  `DagWatchSession::is_resolved` is kept, because `conn.rs` genuinely branches on it to tell a session's first layout from a pushed one.
  The tests assert the observable contract instead: an unresolved watch has an empty matching set, and the delta carries the epoch it moved to.
  They were also the bulk of what `check-sim-coverage` flagged on CI while reporting clean locally, since a trivial getter's coverage counter survives or vanishes with inlining decisions that differ between build environments.
  (8e2cbd9, a58a5f8)

### Fixed

- **Reconnect no longer hangs batched sends or trusts unconfirmed consumer ack state.** Every batched `SendFut` cut before its receipt now resolves with a deterministic reset error instead of being orphaned outside replay and timeout tracking, including batches already flushed to the wire.
  An individual PIP-54 ack after reset reconstructs an all-unacked bitset and clears only the requested index, so a missing tracker cannot silently acknowledge the whole producer-batched entry.
  Established durable consumers omit the local highest-ack watermark on every reattach and defer to the broker's persisted cursor; fresh and non-durable start positions are unchanged.
  (issues #395, #396, #398; ADR-0096)

- **`ClientBuilder::memory_limit` now propagates `MemoryLimitPolicy::ProducerBlock`.** The builder previously copied only the byte limit into `ConnectionConfig`, leaving the runtime policy at its `FailImmediately` default while reporting the requested policy through the façade getter.
  The public enum is now converted exhaustively into the proto configuration, so a saturated producer parks and progresses when budget is released.
  (issue #397)

- **A client waiting on a scalable-topic reply no longer parks forever when the connection dies.** `scalable_topic_lookup` and `scalable_topic_subscribe` re-check `is_closed()` only when a scalable event arrives, and a dying connection sends none — so the guard ran or did not depending on whether it won a race against EOF.
  Both drivers now wake the scalable waiters wherever they mark the connection disconnected, on either engine.
  The differential transcript covering it passed locally and timed out on CI before this, which is how the race surfaced.
  (ADR-0093)

- **A scalable-topic watch no longer dies on the broker's own duplicate layout.** `DagWatchSession::handle_update` treated a layout `epoch` that did not strictly advance as `DagError::NonMonotonic`, and `Connection` closes the session on any update error.
  Pulsar 5.0.0-M1 answers `CommandScalableTopicLookup` with the current layout and then pushes that same layout, at that same epoch, on the watch the lookup opened — so the very first thing a real broker sends after resolving tore the session down, and the client never saw another epoch, the split among them.
  A non-advancing epoch is now ignored: `handle_update` returns `Ok(None)`, the layout is untouched, no event is emitted, and the session stays open.
  Only a session mismatch, a broker-side error, or a bodyless update ends a session.
  `DagError::NonMonotonic` is removed and `handle_update` returns `Result<Option<DagDelta>, DagError>`; both are behind the default-off `scalable-topics` feature.
  Every scripted test advanced the epoch on each frame, which is why all four layers were green — only the e2e against a real broker caught it.
  All four now cover the duplicate.
  (ADR-0095, amending ADR-0093 § D2)

- **A rejected scalable-topic lookup now returns an error instead of parking the caller until the connection closes.** `scalable_topic_lookup` drained only `LookupResolved` for its own session id, so a broker that refuses the session — topic not found, authorization — ended it as `DagWatchClosed`, which the loop never matched.
  Both engines now race the two outcomes, which is the contract the sibling `scalable_topic_subscribe` always had.
  `MergeEvent::parent_segment_ids` is sorted rather than carried in the broker's wire order: nothing in the `.proto` requires `parent_ids` to be sorted, so two engines observing the same merge could build `MergeEvent`s that compare unequal.
  Both are behind the default-off `scalable-topics` feature.
  (cross-review findings on #391; be1b876)

- **`check-sim-coverage` no longer reports lines as uncovered that a passing test executed.** The gate inherited the workspace's `[profile.test] opt-level = 1`, and at opt-level ≥ 1 rustc enables MIR inlining: an inlined callee's coverage counter never fires, so the call site is attributed and the callee reads zero.
  `magnetar-proto`'s `ScalableConsumerSession::consumer_type()` is called twice from a plain synchronous `#[test]`, inside a run reporting 127/127 test binaries `ok`, and measured `DA:271,0` at `opt-level = 1` against `DA:271,2` — exactly the two call sites — at `0`.
  The verdict was not stable either, since inlining follows codegen-unit partitioning: one commit produced 63, 70 and 81 `SF:` records warm, cold and on CI, with three different uncovered sets, and CI blamed the five signature lines of the `async fn` `Client::scalable_topic_subscribe` while its coroutine body reported hits throughout — rustc lowers an `async fn` into an inlinable outer future constructor mapped to the signature plus a coroutine that cannot be inlined.
  It also failed **open**, crediting a line that never ran when the neighbour it was folded into did, which is the half that matters for a gate whose job is proving patch coverage.
  The measurement now runs at `opt-level = 0` via `CARGO_PROFILE_TEST_OPT_LEVEL` on both the execution and re-export commands — a `[profile.test]` override rather than a `RUSTFLAGS` entry, since `cargo-llvm-cov` owns `RUSTFLAGS` for `-C instrument-coverage`.
  Ordinary `cargo test` keeps `opt-level = 1`.
  Toolchain skew and stale object files were both tested and refuted first.
  (ADR-0094)

- **The PIP-33 two-cluster test fixture now actually initializes cluster metadata.** `pulsar-init`'s `command:` was a YAML folded scalar whose continuation lines sat at a deeper indent than its first content line, so their newlines survived folding and `bash -c` received a twelve-line script: `initialize-cluster-metadata` ran with no options at all and each `--flag` line ran as its own command, while a trailing `|| true` turned that into `Exited (0)` and satisfied the downstream `service_completed_successfully` conditions.
  The fixture had only ever worked because the post-up `configure_replicated_subs.sh` registers both clusters itself, so a local `docker compose up -d` alone left the replicated-subscription tests with nothing to replicate.
  Every service `command:` is now an exec-form list over a literal block scalar, which has no folding semantics; the two bookkeeper services' `A && B || true && C` precedence bug no longer swallows a failing `apply-config-from-env.py`; `pulsar-init` gains the `PULSAR_MEM` cap every other service in the fixture already carried; and both workflows assert that both clusters are registered before `configure_replicated_subs.sh` runs, with a bounded 30-attempt retry so a healthy-but-not-yet-warm broker is not read as a missing cluster.
  (issue #389; 9c231f7, cfff6cd)

## [1.3.0] - 2026-08-03

### Added

- **`magnetar_proto::broker_authority` centralizes broker authority normalization with an optional scheme-less default port:** callers now share one sans-io implementation for ASCII-case-insensitive Pulsar scheme recognition, path trimming, explicit-port precedence, bracketed IPv6 handling, and structural rejection.
  The additive `broker_endpoint_scheme` and `BrokerEndpointScheme` API lets runtime adapters preserve that same canonical scheme classification.
  `probe_authority` remains the unchanged no-fallback wrapper, while DIRECT-routing clients can supply the bootstrap protocol default.
  The API is additive; `probe_authority`, Tokio's `ParsedUrl`, and every ergonomic façade signature remain unchanged.
  (ADR-0091, amends ADR-0087)

- **`ClientBuilder::stats_interval` — the client now samples its own rolling rate windows, so `aggregate_stats()` stops summing zeros:** `ConsumerStats::msgs_per_sec` / `bytes_per_sec` and the `ProducerStats` pair are computed by `record_rate_window`, which needs two snapshots of the same slot before it can publish anything.
  Nothing in either engine ever made the second call — every caller in the tree was a test — so the fields stayed `0.0` forever unless the application ticked them itself.
  That hit the three aggregating wrappers hardest.
  `PartitionedProducer::aggregate_stats`, `MultiTopicsConsumer::aggregate_stats` (and its `PartitionedConsumer` alias) and `PatternConsumer::aggregate_stats` fold their children's rates as an f64 sum, so all three reported a structural zero; only `PartitionedProducer` even exposed its children, and only on the tokio engine, while the moonpool `Producer<P>` / `Consumer<P>` carried no `record_rate_window` method at all.
  `record_rate_window` was also the one periodic obligation in the client not expressed as a deadline — keepalive, the nack / unacked / ack-grouping trackers, chunk expiry, receiver-queue auto-adjust, batch flush, send timeout, relocated in-flight sends and `ack_response_timeout` are all armed in `Connection::poll_timeout` and swept in `Connection::handle_timeout`, magnetar's structural equivalent of Java's client-wide `HashedWheelTimer`.
  Setting `ClientBuilder::stats_interval(dur)` (new `ConnectionConfig::stats_interval: Option<Duration>`, additive field, default `None`) arms it there: every producer and consumer on the connection is re-sampled once per interval, which reaches every per-partition and per-topic child that no public API can reach.
  `Duration::ZERO` disables, spelling Java's `statsIntervalSeconds = 0`.
  No fan-out method was added to any wrapper, deliberately: Java's wrappers have none either (`PartitionedProducerImpl.getStats()` only resets and folds children), and since `fold` sums rates as bare f64 with no window metadata, a caller ticking three children of four would get an authoritative-looking total that means nothing — one clock ticking every slot is what makes the sum well-defined.
  Costs no new state, no task, no `select!` arm, and emits no frame or `ConnectionEvent`, so the golden `EventStream` traces are untouched.
  With the knob at its `None` default the moonpool wake schedule is bit-for-bit unchanged.
  A producer or consumer created mid-window is seeded at its own creation and reports `0.0` for its first full interval; Java's recorders behave identically.
  `record_rate_window` stays public for manual sampling, but the two cadences interleave — pick one.
  The default is Java's `Some(60 s)` (`ClientConfigurationData.statsIntervalSeconds`), so rates are published out of the box.
  It landed in two commits on purpose — the mechanism first with the sweep off, then the one-line default flip once a 1..32 moonpool seed sweep ran clean **with the sweep armed** — so a seed regression bisects to one line rather than to the whole mechanism.
  (docs/follow-ups.md §2; ADR-0089)

### Fixed

- **Moonpool now dials portless, scheme-less DIRECT broker targets with the plaintext bootstrap default instead of rejecting them before DNS resolution:** the Tokio runtime already converted a broker-advertised `broker.internal` target into `broker.internal:6650`, but Moonpool preserved it without a port and `Transport::connect_with_resolver` failed with `invalid host:port literal` before a configured resolver could run.
  The supervised plaintext pool now records `6650` as its scheme-less default and supplies it to the canonical authority helper before bootstrap comparison, pool insertion, or dialing.
  Explicit ports still win, explicit `pulsar://` and `pulsar+ssl://` schemes still select `6650` and `6651`, proxy forwarding keeps its no-fallback contract, and no TLS-capable Moonpool pool or façade signature was added.
  (ADR-0091, closes `docs/follow-ups.md` item 8)

- **Port-less bracketed IPv6 broker URLs now get the scheme's default port, and an empty broker-advertised authority is rejected instead of reaching the wire:** the "strip `pulsar://` / `pulsar+ssl://`, trim the path, synthesise the default port" rule existed in four hand-rolled copies — `magnetar_proto::probe_authority`, `proxy_broker_authority` / `direct_broker_authority` (`magnetar-runtime-moonpool`), and `strip_url_to_host_port` (same crate's driver).
  They agreed only because each had been written to match, which is the arrangement that produced the ADR-0085 defect in the first place.
  Two bugs followed from the duplication rather than from any one copy.
  First, default-port synthesis triggered on "the authority contains no `:`" — never true of a bracketed IPv6 literal, whose colons belong to the address — so `pulsar://[::1]` yielded the port-less `"[::1]"` and every dialer rejected it.
  It now yields `[::1]:6650` (and `pulsar+ssl://[2001:db8::1]` yields `[2001:db8::1]:6651`), so a health probe, a `ServiceUrlProvider` service URL, and a broker-advertised lookup response carrying that shape all resolve correctly.
  ADR-0085 had recorded this as an accepted limitation, deliberately inherited so the copies could not diverge; `strip_url_to_host_port` shared it independently.
  Second, `proxy_broker_authority` had no empty-authority check at all, so `""` returned `Ok("")` and `"pulsar://"` returned `Ok(":6650")` — fabricated authorities that went on to the wire in `CommandConnect.proxy_to_broker_url`.
  Both are now `Err`.
  This is the same class of defect as issue #364 and survived that fix because the reference sweep looked for scheme truncation rather than for a missing emptiness guard.
  The three duplicates now delegate to `probe_authority`; `strip_url_to_host_port` keeps its stricter contract (a scheme is mandatory, so a bare `host:port` is still `None`) by gating locally before delegating.
  `magnetar_runtime_tokio::client::parse_direct_broker_url` keeps its own `url`-crate parse — a `ParsedUrl` struct return is a different seam — and instead gained a table-driven equivalence audit pinning where it agrees with `probe_authority` and where it deliberately diverges.
  A bracketed authority with an unterminated bracket (`pulsar://[::1`) still gets no synthesised port; the unrecognised-scheme rejection, bare-`host:port` pass-through and trailing-path trimming are unchanged.
  (docs/follow-ups.md §8; ADR-0087, amends ADR-0085)

- **Latency histograms are now stamped from the injected clock, so moonpool's are reproducible per seed:** `ConsumerState::pop_message` recorded `msg.arrived_at.elapsed()` and `ProducerState::apply_receipt` recorded `op.enqueued_at.elapsed()` — host-clock reads inside the sans-io core, in violation of ADR-0011.
  Both _write_ ends were already injected (`arrived_at` from `ConsumerState::deliver`'s `now`, `enqueued_at` from `queue_send`'s), so only the read side leaked.
  The consequence under simulation was worse than noise: virtual time outruns host time in `SimProviders`, so `elapsed()` saturated to `0` on essentially every sample and the receive/send-latency percentiles were the one part of `ConsumerStats` / `ProducerStats` carrying no signal at all — three test files had grown explicit workarounds for it, including a `seed_deterministic_latency` helper in the differential suite that overwrote each histogram with a synthetic distribution before comparing.
  `ConsumerState::pop_message`, `Connection::pop_message`, and `ProducerState::apply_receipt` now take `now: Instant` and derive the sample with `saturating_duration_since` (never the `Sub` impl, which panics on underflow); the engines snapshot at the call boundary before taking the connection mutex, tokio via `Instant::now()` and moonpool via its injected `now_instant_provider`.
  `cargo run -p xtask -- check-no-internal-clock` now also rejects `.elapsed()` and no longer carries a file allowlist — `magnetar-proto/src/producer.rs` had been whole-file-skipped on a `uuid` rationale the gate never scanned for, which is why one of the two leaks was doubly invisible.
  **BREAKING CHANGE** (`magnetar-proto` only, re-exported as `magnetar::proto`): `ConsumerState::pop_message`, `Connection::pop_message`, and `ProducerState::apply_receipt` take an additional `now: Instant` parameter, passed last — any direct caller of these sans-io APIs (outside the `magnetar-runtime-tokio` / `magnetar-runtime-moonpool` engines, which are updated in this changeset) must pass the instant snapshotted at its call site.
  The ergonomic façade surface is unchanged: `Consumer::{receive, receive_batch, drain_messages}`, `Producer::send`, `ConsumerApi`, `ProducerApi`, `aggregate_stats`, `receive_latency_histogram`, and `send_latency_histogram` all keep their signatures.
  (docs/follow-ups.md §3; ADR-0086, amends ADR-0011)

- **Admin REST paths now percent-encode to RFC 3986 `pchar`, so `|` in a subscription or topic name no longer 400s:** `AdminClient::url_for` built paths with `Url::path_segments_mut().push()`, whose WHATWG encode set is laxer than RFC 3986 and leaves `[`, `]`, `^` and `|` raw.
  All four are illegal in a URI path, and Pulsar's Jetty front end rejects them at URI-parse time with `400 Illegal Path Character` — before routing, so the response carried no broker `reason` and the failure read as an unexplained empty-bodied 400.
  This hit every verb taking a subscription named `<consumer>|<app-id>` (a common convention): `magnetarctl admin subscriptions delete … 'name|app_id'` could never delete such a subscription, and `resetcursor` / `skip` / `skip_all` / `expireMessages` failed identically.
  Segments are now encoded by an explicit `pchar` encoder (`unreserved / sub-delims / ":" / "@"` pass through, everything else becomes a `%XX` triplet), matching the `%7C` that Java's `TopicsImpl#deleteSubscriptionAsync` puts on the wire via `Codec.encode`.
  The new encode set is a strict superset of the previous one, so the only paths that change on the wire are those containing those four bytes.
- **Moonpool proxy/DIRECT broker-URL parsing no longer truncates a corrupted scheme into a nonsense authority:** `Client::proxy_broker_authority` / `direct_broker_authority` (moonpool engine only) previously fell through to a naive `host_port.split('/')` on any input that didn't match `strip_prefix("pulsar://")` / `strip_prefix("pulsar+ssl://")`, silently deriving authorities like `"ptlsar:"` from a single-bit-corrupted `broker_service_url` (e.g. moonpool-sim's bit-flip chaos) instead of failing the lookup.
  Both helpers now return `Result<String, ClientError>` and reject an input containing `"://"` with an unrecognised scheme explicitly, while still accepting a genuine scheme-less bare `host:port` unchanged.
  (#362, #363, #364, #367; ADR-0055)
- **Health-probe endpoints with a corrupted URL scheme are now refused instead of dialled, and scheme-only URLs get their default port:** `TokioHealthProbe::authority` and `MoonpoolHealthProbe::authority` were byte-identical copies of one parser and shared the defect above — `strip_prefix(…).unwrap_or(endpoint)` fell through to the unstripped string on an unrecognised scheme, and the following `split('/')` truncated a bit-flipped `"ptlsar://broker:6650"` into the nonsense authority `"ptlsar:"`, which the PIP-121 auto-cluster-failover probe then handed to `tokio::net::lookup_host` / `NetworkProvider::connect`.
  The probe verdict was already correct (the doomed dial fails, and a failed dial is `verdict = false`), so the user-visible effect was one wasted DNS lookup per probe cycle rather than a routing defect — but both copies had rotted in lockstep, which no cross-engine differential test can catch.
  Both engines now delegate to the new sans-io `magnetar_proto::probe_authority`, which returns `None` for an unrecognised `"://"` scheme (reported unhealthy, zero I/O) and additionally synthesises the scheme's default port for a port-less URL, so `pulsar://broker.local` probes `broker.local:6650` instead of always reading unhealthy on a portless authority.
  A genuine scheme-less bare `host:port` is accepted unchanged.
  (ADR-0085, refining ADR-0023)
- **`send_timeout` was blind to publishes relocated by a supervised reconnect:** `Connection::reset()` moves every in-flight `OpSend` out of `ProducerState::pending` and into `Connection::in_flight_publish_snapshots` so the supervisor can transparently replay it on the new session — but `ProducerState::drain_timed_out_sends` (the `send_timeout` sweep) only ever walked `pending`, so a publish parked across a reconnect was invisible to timeout enforcement until either a successful rebuild replayed it or the supervisor exhausted its entire reconnect attempt budget.
  With a realistic `max_attempts`, a user's `send().await` could stay `Pending` for hours with `send_timeout` configured and armed.
  `Connection::poll_timeout` and `Connection::handle_timeout` now also sweep `in_flight_publish_snapshots`, using each op's ORIGINAL `enqueued_at` (not a fresh reset()-relative budget) as the deadline base, so a relocated send now surfaces the same `SendError { code: -1, message: "send timeout" }` outcome the live-queue path installs, at the correctly-measured deadline — a previously-silent hang becomes a deterministic error.
  Separately, `Connection::fail_all_pending` now also drains `in_flight_publish_snapshots` (installing `OpOutcome::Terminal`), mirroring `fail_producer_open_with_broker_error`'s existing snapshot drain, so a relocated send resolves promptly when the supervisor gives up even if the woken send future has not yet re-registered its waker.
  (#369)
- **A stalled outbound socket no longer starves the driver's read and timer paths:** the driver's write ran unconditionally at the top of every loop iteration in both engines, ahead of `select!` — safe only as long as the write always completed quickly.
  A peer that accepted the connection and then simply stopped draining its receive window parked that write forever, blocking the ENTIRE loop: the read arm, the `driver_waker` arm, and the timer arm (the sole caller of `Connection::handle_timeout`, which drives the keepalive watchdog, the `send_timeout` sweep, and the `ack_response_timeout` backstop) never ran again, so `mark_disconnected()` was never reached and `is_connected()` kept reporting `true` on a functionally dead connection.
  The write is now its own bounded, cancellation-safe `select!` arm (after read and the waker, before the timer), capped by both `DRIVER_WRITE_BUDGET_BYTES` and `Connection::operation_timeout()` (30s default; NOT `keepalive_interval`, which only detects read-side silence).
  Expiry is treated as an I/O failure routed through the same `mark_disconnected()` path every other write error already takes, so the auto-reconnect supervisor redials exactly as it does for any other write failure, instead of the connection wedging as still-connected forever.
  (#370; ADR-0083, amends ADR-0070 and ADR-0074)

- **PIP-74 `Auto` receiver-queue scaling never armed on a connection with continuous inbound traffic:** the `Auto` adjust schedule had no bootstrap trigger.
  `ConsumerState::arm_adjust_clock` had exactly one caller — the `None =>` fallback arm inside `Connection::handle_timeout` — so the first arm required a `poll_timeout()` deadline to elapse, and for a fresh `Auto` consumer whose adjust deadline is still `None` the only deadline `poll_timeout()` can return is the keepalive one.
  Every decoded inbound frame refreshes `last_activity` (ADR-0058's single refresh site), so a connection carrying message deliveries — or the `CommandAckResponse` stream produced by a consumer that awaits each individual ack — deferred the keepalive deadline indefinitely: `handle_timeout` never ran, the schedule never armed, and `Auto` never scaled, regardless of the configured `keepalive_interval`.
  The schedule was parasitic on whichever unrelated deadline fired first, and a busy connection has none.
  `Connection::initial_flow` now arms the schedule at initial-flow time, so the bootstrap no longer depends on an unrelated timer.
  The production impact is measured, not inferred: with the fix reverted, `e2e_auto_adjust_arms_under_continuous_ack_response_traffic` against a real Pulsar 4.0.4 broker leaves the auto-tuned target pinned at its floor of 20 for the whole 300-message drain, so an individually-awaited-ack receive loop — a common pattern — disabled PIP-74 auto-scaling outright.
  **BREAKING CHANGE** (`magnetar-proto` only, re-exported as `magnetar::proto`): `Connection::initial_flow` and `Connection::abandon_consumer_subscribe_waiter` each gain a `now: Instant` parameter — any direct caller of these sans-io APIs (outside the `magnetar-runtime-tokio` / `magnetar-runtime-moonpool` engines, which are updated in this changeset) must pass the current instant at the call site.
  The façade and both engines' `Consumer` surfaces are unaffected.
  (docs/follow-ups.md §4; ADR-0084, amends ADR-0071's arming premise)

- **A dial whose handshake completed too quickly hung for the full `operation_timeout` instead of returning:** `ConnectedFut` — the future behind `wait_connected`, which every bootstrap and every pool dial parks on until the broker answers CONNECT — enrolled for the driver's wakeup from a _spawned helper task_. The driver announces handshake completion with `notify_waiters()`, which stores no permit (ARCHITECTURE.md § "Enroll-before-drain wakeup discipline"), so the helper only enrolled once the runtime scheduled it and a pulse that landed first was lost outright.
  A freshly dialled connection is silent after CONNECTED, so nothing ever pulsed again and the wait burned the whole 30 s `operation_timeout`, surfacing at the caller as `Other("open_producer: timed out: producer target resolution exceeded operation_timeout")`.
  Losing the race needs CPU contention, which is why it reproduced on 4-vCPU CI runners and not on an idle dev box: it hit 5 of the 10 dependabot runs off base `5d6c39f` on 2026-07-22, each in a different, unrelated e2e test.
  `ConnectedFut` now owns an `OwnedNotified` on `event_waker` and polls it BEFORE inspecting connection state, exactly like its sibling `EventWaitFut` and the same discipline already applied to `await_reconnect_or_terminal`, the PIP-33 marker accessor, and the subscribe-readiness waiter; the event drain also narrows to `poll_event_if(Connected | Closed)` so unrelated events stay queued for their own waiter.
  Signal coverage is unchanged — `event_waker` is pulsed at every site that pulses `driver_waker`.
  The moonpool engine was structurally immune: `handshake_plain` completes the handshake inline, before the driver task is spawned.
  (#372)

### Changed

- **The moonpool engine's broker-URL rejection message now names the accepted shapes instead of asserting an unrecognised scheme.** `proxy_broker_authority` / `direct_broker_authority` previously emitted `broker-advertised URL '<url>' carries an unrecognised scheme (expected 'pulsar://' or 'pulsar+ssl://'); refusing to derive a proxy authority from it` for their single rejection case.
  Delegating the parse to `magnetar_proto::probe_authority` folds three rejection classes into one — unrecognised scheme, empty input, and a scheme with no authority behind it — and the old wording would have been actively misleading on the two new ones (`''` does not carry an unrecognised scheme).
  The message is now `broker-advertised URL '<url>' is not a usable authority (expected 'pulsar://host[:port]', 'pulsar+ssl://host[:port]', or a bare 'host:port'); refusing to derive a proxy authority from it`.
  The error type is unchanged (`ClientError::Other`), so only log-scraping on the old string is affected; the tokio engine's own `parse_direct_broker_url` message is untouched.
  (ADR-0087)

- **The moonpool sim-coverage gate now measures a real scope, enforces its verdict, and runs on every pull request.** Three changes compose, and only the third makes the gate bite.
  Its LCOV report was widened from a narrow slice to the whole closure the sim run compiles — six crates, 63 `SF:` records measured 2026-07-31 (`magnetar-proto` 28, `magnetar-runtime-tokio` 12, `magnetar-runtime-moonpool` 12, `magnetar-auth-athenz` 5, `magnetar-differential` 4, `magnetar-auth-sasl` 2) — so a `magnetar-proto` addition is now measured where before it was invisible.
  Execution is unchanged and still runs only the `magnetar-runtime-moonpool` + `magnetar-differential` test binaries, so a `magnetar-proto` or `magnetar-runtime-tokio` line counts as covered only when a sim test reaches it transitively; those crates' own unit tests never run under the gate and can never satisfy it.
  `SIM_COVERAGE_ENFORCES_UNCOVERED` then flipped to `true`, so an uncovered added line inside the reported scope is printed in full with a count and **fails** the check rather than being advisory.
  The flip alone would have changed nothing — the gate diffs against `main`, so its scheduled `main` run compares `main` against itself and short-circuits with "nothing to verify" — so the same changeset gave it a per-PR home: `.github/workflows/ci.yml` runs `check-sim-coverage --enforce` on every `pull_request`.
  `--enforce` is now redundant (it only ORs into the constant) and is retained so existing invocations keep working.
  Additions the run never compiles — the `magnetar` façade above all, plus the generated `crates/magnetar-proto/src/pb/` — still print as advisory `not gated` and do not fail the check: that report is a scope limit, not a verdict, and the four-layer ADR-0024 test policy is what carries the requirement there.
  `main` carries no branch protection as of 2026-08-01, so a red job does not yet mechanically block a merge; ADR-0092 records the admin step that would.
  (#385, #388; ADR-0092, ADR-0090, ADR-0088, refining ADR-0024)

- **New `cargo run -p xtask -- check-e2e-container-memory` gate.** Every `pulsar standalone` container the e2e suite starts is supposed to carry `PULSAR_MEM = -Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g` (docs/testing.md § "e2e container memory budget"), but nothing enforced it.
  The e2e helpers are copy-paste duplicated across 52 files, so a new `e2e_*.rs` cloned from a pre-cap template — or a chain that drops the `.with_env_var` call — silently reintroduced a ~2.3 GiB stock-heap container, whose failure mode is a flaky timeout in whichever unrelated test happens to be running: expensive to diagnose, cheap to misread as "just a flake".
  The gate resolves the image rather than matching text.
  (#379)

- **Dependency refresh.** `tokio` `^1.52.3` → `^1.53.1`, `tokio-util` `^0.7.18` → `^0.7.19`, `futures` / `futures-util` `^0.3.32` → `^0.3.33`, `serde` `^1.0.228` → `^1.0.229`, `serde_json` `^1.0.150` → `^1.0.151`, `schemars` `^1.2.1` → `^1.2.2`, `aws-lc-rs` `^1.17.1` → `^1.17.3`, `rustls-pki-types` `^1.15.0` → `^1.15.1`, `clap` `^4.6.2` → `^4.6.4`, `anyhow` `^1.0.103` → `^1.0.104`, and `base64` `^0.22.1` → `^0.23.0`.
  `base64` is the only semver-major move, and it reaches no public signature: it is a `[dev-dependencies]` entry in the `magnetar` façade and an optional dependency of `magnetar-auth-athenz` used only inside function bodies, so no downstream code can observe the bump.
  (#371)

## [1.2.3] - 2026-07-20

### Added

- **Configurable operation retry policy:** `ClientBuilder::operation_retry(OperationRetryConfig)` now applies operation-specific broker retries across lookup, partition metadata, producer-open, and subscribe, independently from transport supervision.
  Producer-open additionally retries both producer-quota variants and `ProducerBusy`; subscribe additionally retries `ConsumerBusy`.
  Provisional attachment retries re-run lookup and routing with a fresh handle, while established reattachment remains driver-owned.
  One provider-backed `OperationDeadline` spans every setup stage and composite child, preserving the newest retryable broker error if a later deadline fires.
  Tokio and Moonpool share the policy with injected-time parity.
  ([#343](https://github.com/CleverCloud/magnetar/issues/343), ADR-0080)
- **`ConsumerEventListener` (Failover becameActive/becameInactive parity):** `ConsumerBuilder::consumer_event_listener(...)` + `subscribe_with_event_listener()` mirror Java's `ConsumerBuilder#consumerEventListener`.
  `ConsumerEvent::{BecameActive,BecameInactive}` fires from a detached poller task, sequentially, once per broker `CommandActiveConsumerChange` — the same push-delivery poller shape ADR-0064's `MessageListener` uses, driving the new `Consumer::next_active_change()` future instead of `receive()`.
  `Consumer::is_active()` exposes the last-reported state synchronously (`None` until the first transition).
  Proto-side, `ConsumerState` gains a bounded per-slot active-change ring (`ACTIVE_CHANGES_CAP = 32`, oldest dropped) recorded under the SAME per-slot lock the #307 reflow predicate already holds — one lock acquisition, not two.
  Also closes a latent leak: `ConnectionEvent::ActiveConsumerChanged` previously accumulated unbounded in the proto event queue (neither driver drained it); it is now silently consumed like `ChecksumMismatch`.
  (#348; ADR-0081)

### Fixed

- **`Auto` receiver-queue policy never scaled up under real load:** the consumer's permit mirror (`ConsumerState::available_permits`) was purely additive — bumped on every grant but never decremented as messages actually arrived — so the `FlowStats::available_permits == 0` starvation signal `Auto::adjust` needs was reachable only via a churn-window reset, never via a broker genuinely exhausting its grant.
  The field is now split: `ConsumerState::granted_permits` (renamed, semantics unchanged — the additive grant mirror the #307 failover-reflow gate and the want-have delta still use) and a new `ConsumerState::permit_balance` (the REAL balance: grants minus one unit per broker dispatch — plain message, batch member, chunk, or PIP-33 marker), which `flow_stats` now feeds into `FlowStats::available_permits`.
  A new churn-window guard skips the adjust tick entirely when `granted_permits == 0` (reset / terminal-failure / same-broker `CloseConsumer`), so that window is never mistaken for load starvation.
  `Auto::adjust` itself is unchanged — only the signal it was fed was wrong.
  (#349; ADR-0082)
- **Consumer final-clone resource release:** dropping the last clone of a consumer now stages a best-effort `CloseConsumer` and wakes the existing driver, allowing ownership-driven teardown to unregister the broker-side consumer when the frame reaches the broker and is accepted.
  Intermediate clone drops leave surviving consumers usable, explicit `close().await` remains the reliable acknowledgement-bearing path, and forgotten close responses never accumulate undrained outcomes.
  (#342; ADR-0077)
- **`aggregate_stats()` no longer zeroes the rate, latency-percentile, and `pending_batch_acks` fields:** `MultiTopicsConsumer::aggregate_stats` / `PartitionedConsumer::aggregate_stats` and `PartitionedProducer::aggregate_stats` previously summed only a handful of cumulative totals, silently leaving `msgs_per_sec`, `bytes_per_sec`, `receive_latency_p50_ms`/`p99_ms`, `send_latency_p50_ms`/`p99_ms`, and `pending_batch_acks` at their zeroed default regardless of the children's real values.
  New `ConsumerState::receive_latency_histogram` / `ProducerState::send_latency_histogram` accessors expose each child's raw latency distribution, and new `ConsumerStats::fold` / `ProducerStats::fold` associated functions (exhaustive per-field destructuring, so a future field addition is a compile error until this fold picks a rule for it) aggregate every child snapshot: the cumulative totals sum (saturating), the rolling rates sum as `f64`, `*_latency_max_ms` is the exact max, and `*_latency_p50_ms`/`p99_ms` are recomputed from a real `hdrhistogram::Histogram::add` merge of every child's histogram — summing or maxing percentiles directly is not statistically sound.
  Both `ConsumerApi` and `ProducerApi` gain a `{receive,send}_latency_histogram` accessor (implemented on both the tokio and moonpool engines) so the façade rewrites are thin collect-then-fold wrappers.
  (#347)
- **Ack orphaned by same-broker `CloseConsumer` + no deadline:** a same-broker bundle reassignment (`CommandCloseConsumer` with `assigned_broker_service_url = None`, the #307 root cause) tears the old consumer id down without ever answering an ack in flight against it, parking the caller's `ack().await` forever.
  The close-handler now fails every pending ack for the torn-down handle immediately (`code: -1, message: "ack orphaned by broker consumer close"`) before the in-place re-subscribe runs.
  As a generic backstop for any other cause of a dropped `CommandAckResponse`, `Connection::ack` now takes an injected `now: Instant` (ADR-0011) and a new `ConnectionConfig::ack_response_timeout` knob (default `Some(30s)`, mirroring the #304 `send_timeout` default; `None` disables it) reaps a pending ack whose response never arrives, mirroring the existing `send_timeout` sweep.
  **BREAKING CHANGE** (`magnetar-proto` only): `Connection::ack`, `Connection::close_consumer`, and `Connection::close_consumer_forget` are `pub fn` and now take an additional `now: Instant` parameter — any direct caller of these sans-io APIs (outside the `magnetar-runtime-tokio` / `magnetar-runtime-moonpool` engines, which are updated in this changeset) must pass the current instant at the call site.
  The `magnetar`/`magnetar-driver` façade and `magnetar_runtime_{tokio,moonpool}::Consumer::{ack,close}` are unaffected — their own signatures are unchanged.
  (#346)

## [1.2.2] - 2026-07-13

### Fixed

- **Chunked-consumer flow replenishment:** accepted incomplete PIP-37 chunks now repay their broker permits immediately, matching Java's per-chunk accounting and preventing queue-2,000 consumers from reaching zero permits below the logical-message refill threshold.
  (#331; ADR-0076)

## [1.2.1] - 2026-07-06

### Added

- **`ConsumerStats::pending_batch_acks`:** live count of PIP-54 per-batch ack-bitset entries (`batch_ack_tracker`).
  Magnetar-specific gauge (no Java counterpart): bounded by the un-acked window under a correctly-pruning ack path, so a monotonically growing value is the signature of the #326 leak class.
  (#326)

### Changed

- **Dependencies:** bumped workspace manifest floors — `rustls-pki-types` 1.14.1→1.15.0, `uuid` 1.23.3→1.23.4, `aws-lc-rs` 1.17.0→1.17.1, and `anyhow` 1.0.102→1.0.103 — and refreshed `Cargo.lock`; the `anyhow` bump resolves `RUSTSEC-2026-0190` flagged by `cargo-deny`.
  (#325)

### Fixed

- **Cumulative ack now prunes every `batch_ack_tracker` entry it covers:** the cumulative branch of `Connection::ack` removed only the tracker entry keyed on the acked id's exact `(ledger_id, entry_id)`, so a consumer acking exclusively via cumulative watermarks (never an individual ack) leaked one PIP-54 `BatchAckEntry` per batched broker entry for the lifetime of the connection — only a reconnect cleared the map.
  The fix prunes all entries at or below the cumulative position (`retain`, once per cumulative ack).
  Surfaced by the otelgw accesslogs converter OOMing at ~24 GiB every 4-6 h under a batched-topic, cumulative-only workload.
  (#326)
- **Driver write fairness: bounded write turns before reads:** the per-connection driver now retains staged outbound bytes and writes at most 256 KiB per loop turn before returning to the read-first `select!`, so already-persisted `CommandSendReceipt`s stay observable under large producer bursts instead of being starved behind an unbounded write drain — without splitting the single socket owner.
  Matched Tokio/Moonpool unit guards and an e2e burst that crosses the write budget keep both engines in `EventStream` parity; recorded as ADR-0074.
  (#319, #324)

## [1.2.0] - 2026-06-29

### Added

- **Consumer name on the multi-child consumer builders:** `PartitionedConsumerBuilder`, `MultiTopicsConsumerBuilder`, and `PatternConsumerBuilder` gain a `.name(impl Into<String>)` setter that propagates the consumer name verbatim to every per-partition / per-topic child via `ConsumerTemplate` (no per-partition suffix — every child subscribes with the same `consumer_name`, matching the Java client).
  Broker `topics stats` now reports a non-empty `consumerName` for each child, so a multi-instance Failover (or Shared) partitioned consumer is attributable to an instance.
  Previously only the inner per-topic `ConsumerBuilder` exposed `.name()`, leaving partitioned consumers stuck at `consumer_name: None`.
  (#300)
- **Pluggable consumer receiver-queue policy (PIP-74 auto-scaled queue):** a new `ReceiverQueuePolicy` trait in `magnetar-proto` makes the consumer receiver-queue size pluggable.
  `Fixed(usize)` is the DEFAULT and is byte-for-byte identical to the previous client; `Auto { min, max_bytes }` opts into PIP-74 `autoScaledReceiverQueueSizeEnabled` parity — growing the target by bounded doubling under starvation (`available_permits == 0`) and shrinking under a buffered-bytes guard, with hysteresis so it converges without thrashing.
  `adjust` is a pure function of `FlowStats` (no clock/RNG/I/O), so both engines stay bit-reproducible; the adjust tick rides the injected clock inside the per-slot consumer loop.
  Builder sugar: `receiver_queue_size(n)` resolves to `Fixed(n)`; `receiver_queue_policy(Arc::new(Auto::new(min, max_bytes)))` opts in (5s default tick, overridable).
  Threaded through partitioned / multi-topics / pattern consumers.
  (#301)
- **`ClientBuilder::connections_per_broker(n)` (Java `connectionsPerBroker` parity):** magnetar opened a single connection per broker, capping a logical producer fleet at one connection's send pipeline and forcing applications to hand-roll a pool of `PulsarClient`s.
  The new knob (default 1) opens up to `n` connections per broker and deterministically round-robins producers AND consumers across them via a shared cursor, fanning out both data-plane surfaces.
  Runtime-only — never reaches the sans-io proto core; lookups and redirect dials always pin index 0.
  The default of 1 is a byte-identical no-op.
  (#314)

### Changed

- **Producer `send_timeout` now defaults to 30s (Java `sendTimeoutMs = 30000` parity):** `CreateProducerRequest::send_timeout` previously defaulted to `None`, which disabled the per-send timeout sweep — a send whose `CommandSendReceipt` was lost or corrupted in flight (receipts carry no CRC32C) hung `Poll::Pending` forever.
  The canonical default is now `Some(30s)`, inherited by the v4 `ProducerBuilder`, `PartitionedProducerBuilder`, and `TypedProducerBuilder` (the V5 surface already used 30s); a timed-out send resolves with `SendError { code: -1, "send timeout" }` and wakes the parked waker.
  `ProducerBuilder::disable_send_timeout()` restores the previous unbounded behavior.
  (#304)
- **Dependencies:** raised workspace floors — `bytes` 1.11.1→1.12.0, `rustls` 0.23.40→0.23.41, `rustls-native-certs` 0.8.3→0.8.4, `zeroize` 1.8.2→1.9.0, and an explicit `opentelemetry` 0.32.0 patch pin — and refreshed `Cargo.lock`.
  (#312, #315)

### Fixed

- **Consumer wedge on same-broker bundle reassignment (broker-initiated `CloseConsumer`):** a code=6 bundle reassignment closes the consumer on the LIVE socket (no TCP drop, so the supervised reconnect / `rebuild_consumers` path never runs), which previously left a Failover/standby consumer parked at zero permits against a non-empty backlog — `receive()` frozen, `availablePermits=0`, `msgRateOut=0` — until a process restart.
  The sans-io core now: re-syncs the client permit mirror to zero on the broker-initiated close; re-subscribes the running consumer in place on the same connection (resuming from the last acked id, deferring the initial `CommandFlow` to the re-subscribe `Success`); re-arms flow when a Failover standby is promoted to active while holding zero permits; and wakes the driver after a single-message `receive()` so a queued replenishment `CommandFlow` is flushed at a buffered-inbound window boundary.
  Both engines inherit the fix from the proto seam.
  (#307, #317, #318)
- **Reconnect: bounded transient-open retry + recoverable-receive gating:** a transient producer-open / subscribe failure is now retried on bounded exponential backoff (2s initial, 8s cap) off the injected clock and, past the cap, terminalized via `fail_producer_open` / `fail_consumer_subscribe` so `send()` / `receive()` return an error instead of hanging forever (#302); and a `receive()` outstanding across a supervised drop no longer resolves `Err(Closed)` during the recoverable reconnect window — new sans-io predicates re-park it and resolve with the post-reconnect message after the rebuild replays `CommandSubscribe` (#299).
  (#299, #302, #313)
- **Driver read-arm fairness under sustained publish load:** the per-connection driver now polls the inbound read arm BEFORE the waker arm in its biased `select!`, so already-arrived `CommandSendReceipt`s are read promptly instead of sitting behind a near-always-pending send waker — collapsing `send().await` tail latency from hundreds of milliseconds back to broker-persist time.
  The outbound path is not starved (`poll_transmit` + `write_all` run at the top of every loop iteration) and the reorder is identical on both engines, so differential `EventStream` parity holds.
  (#303)

## [1.1.1] - 2026-06-17

### Added

- **`magnetar-admin` topic stats — full rate/throughput/size surface:** `TopicStats` now decodes the high-signal `PersistentTopicStats` metrics it previously dropped: `msgRateIn`, `msgRateOut`, `msgThroughputIn`, `msgThroughputOut`, `averageMsgSize`, `storageSize`, and `backlogSize` (alongside the existing `msgInCounter` / `bytesInCounter`).
  `magnetarctl admin topics stats <topic>` emits all of them in its JSON output, so `jq '.msgRateIn'` (and the out-rate, throughput, and storage/backlog sizes) now work for both non-partitioned and partitioned topics.
  Fields default to `0` when a broker release omits them.
  (#293)
- **`magnetarctl` message-id output — `segmentId` no longer dropped:** under the `scalable-topics` feature, `topics terminate` and `topics get-message-id-by-index` now surface the PIP-460 `segmentId` (JSON `null` when absent) instead of silently omitting it; both commands share one `message_id_to_json` renderer so their shapes can't drift.
  (#293)

### Changed

- **CLI (`magnetarctl`) default log level lowered to `warn`:** the default floor dropped from `magnetar=info` to `magnetar=warn`, so `magnetarctl` is quiet by default and surfaces only degraded-state warnings and errors.
  The whole `-v` ladder shifted down one rung — no capability is lost: `-v` now maps to `info` (the old default), `-vv` to `debug`, `-vvv` to `trace`, and `-vvvv`+ widen into the transport stack (reqwest/hyper/rustls/h2).
  Scripts that relied on the prior `info`-level default output must now pass `-v`.
  `docs/cli.md` and `docs/logging.md` updated to match.
  (#292)
- **Dependencies:** bumped `zeroize` 1.8.2→1.9.0.
  (#288)

## [1.1.0] - 2026-06-16

### Added

- **Admin client (`magnetar-admin`) OAuth2 + TLS:** `AdminClientBuilder` gains `oauth2(...)`, `tls_trust_cert_pem(...)`, and `tls_allow_insecure(...)`; a new `AdminAuth::OAuth2` arm refreshes the cached token on demand and attaches it as a bearer credential (erroring clearly on an empty access token).
  `magnetar-admin` now depends on `magnetar-auth-oauth2` (acyclic) and forwards each `crypto-*` feature to it so the token-exchange client binds the same rustls provider.
  (#281)
- **CLI (`magnetarctl`) pulsarctl config file + contexts:** `magnetarctl` now reads the standard pulsarctl config (`--config` > `MAGNETAR_CONFIG` > `$XDG_CONFIG_HOME` > `$HOME/.config/pulsar/config`) and ships a `context` command group (`use`/`set`/`delete`/`get`/`current`/`rename`, with `create`/`del`/`update` aliases) matching the pulsarctl output strings — a working pulsarctl setup now works with zero extra flags.
  Unknown keys and key casing round-trip untouched, so a magnetarctl-written file stays pulsarctl-readable.
  New global flags `--config`, `--context`, `--token-file`, `--tls-trust-cert-path`, `--tls-allow-insecure`, `--tls-enable-hostname-verification`, `--tls-cert-file`, `--tls-key-file`, and `-s` (short for `--admin-url`); the active context supplies the admin URL + auth + TLS, and the data-plane URL is derived from the admin-service-url (`http` → `pulsar://…:6650`, `https` → `pulsar+ssl://…:6651`) unless an explicit `--service-url` is given.
  (#281, #284; ADR-0068)
- **CLI `context rename --force` (`-f`):** opt into overwriting an existing destination context; the destination then fully becomes the source (endpoint + credentials, clearing any stale destination credential), with a warning printed.
  (#284)

### Changed

- **`magnetar-admin` (`AdminError`):** added a new `AdminError::Decode { method, url, status, content_type, snippet, source }` variant carried by the JSON decoders, and added `method: String` + `url: String` fields to the existing `AdminError::Status` variant.
  `AdminError` stays exhaustive (no `#[non_exhaustive]`), so any exhaustive `match` over it or any `Status { code, body }` destructure without `..` must be updated.
  The existing `AdminError::Json` variant is now reserved for request-body **encode** failures only (its `#[error]` text changed from `json decode: …` to `json encode: …`); response **decode** failures route through `AdminError::Decode`.
  (#282)
- **`magnetar-admin` (`AdminError`):** added an `AdminError::Auth(String)` variant for OAuth2 token-acquisition failures.
  Since `AdminError` is exhaustive, downstream exhaustive matches must add this arm.
  (#281)

### Fixed

- **Admin client (`magnetar-admin`):** non-JSON admin responses now surface the request method, URL, HTTP status, `Content-Type`, and a truncated body snippet instead of the bare `serde_json` message (`json decode: expected value at line 1 column 1`).
  Hitting the wrong endpoint, a reverse proxy, or an auth-redirect on a 2xx is now self-diagnosing.
  Non-success statuses (`AdminError::Status`) also name the method + URL.
  (#282)
- **CLI (`magnetarctl`) config/context correctness:** `context rename` refuses to overwrite an existing destination instead of silently destroying its endpoint + credentials (use `--force` to opt in); `context set` no longer persists an inherited `MAGNETAR_TOKEN` (only an explicit `--token` is written) and clears mutually-exclusive auth fields when switching auth mode so a stale higher-precedence credential cannot shadow the one just configured.
  (#281, #284)

### Security

- **CLI (`magnetarctl`) credential safety:** OAuth2 rejects a non-`https` `issuer_endpoint` up front so the `client_secret` is never POSTed over plaintext; `AuthInfo` carries a redacting `Debug` so a `{:?}` of the config never leaks the bearer token; `config save` forces `0600` on a pre-existing world-readable config before writing credentials; an empty token-file token is rejected rather than sending a malformed `Authorization: Bearer ` header to the broker; and the CLI warns when `tls_allow_insecure` is inherited from a context (silent verification downgrade).
  (#281, #284)

## [1.0.1] - 2026-06-15

### Changed

- Renamed the published crates.io packages to avoid a name collision (the `magnetar` name is held by an unrelated, abandoned crate): the façade ships as **`magnetar-driver`** and the CLI as **`magnetarctl`** (binary command `magnetarctl`).
  The façade's library/import name is unchanged — `use magnetar::*` still works; only the dependency line differs (`magnetar-driver = "1.0.1"`).
  No API, behavior, or wire-format change.
  (ADR-0067)

## [1.0.0] - 2026-06-15

First stable release.
Magnetar is a from-scratch Apache Pulsar client driver for Rust with full Apache Pulsar Java-client parity, a sans-io protocol core, and two interchangeable runtime engines.
See the [parity matrix](README.md#java-client-parity-matrix) for the per-feature status snapshot.

### Added

- Initial public release of magnetar, a from-scratch Apache Pulsar client driver for Rust.
  Targets Apache Pulsar 4.0+ LTS, advertises CONNECT `ProtocolVersion::V21` with downgrade fallback, and ships as a 12-crate workspace (façade `magnetar`, sans-io core `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-admin`, `magnetarctl`, `magnetar-fakes`, `magnetar-auth-oauth2`, `magnetar-auth-sasl`, `magnetar-auth-athenz`, `magnetar-messagecrypto`) plus `xtask`.
  (75f7c16)
- Sans-io protocol core (`magnetar-proto`): a `quinn-proto`-style pure state machine (`handle_bytes` / `poll_transmit` / `poll_event` / `poll_timeout`) for Connection, Producer, and Consumer, with zero I/O dependencies and injected clocks (`now: Instant`, `wall_clock` provider).
  The same state machine drives both engines.
  (123b8db, 10cb025; ADR-0004, ADR-0011)
- Dual runtime engines selected at the type level via `PulsarClient<E: Engine = TokioEngine>`: a production tokio engine and a deterministic-simulation moonpool engine over `moonpool_core::Providers`.
  The moonpool engine reaches full façade parity (driver loop + transport, Client lookup / partitioned-metadata / topic-watch, Producer send/flush/close, Consumer receive/ack/seek) plus a rustls-over-bytepipe TLS adapter.
  (405d2cd, 9555113, 1eba8e1, f59032f, e01d676, 3a119e7)
- No-channels concurrency architecture: all `mpsc`/`broadcast`/`watch`/`oneshot` and third-party channel crates are banned and replaced with `Arc<parking_lot::Mutex<…>>` + `tokio::sync::Notify` + `Waker` slabs.
  A split connection mutex enforces global→per-slot lock ordering so the `Producer::send` hot path takes only the per-slot mutex.
  (3275b41; ADR-0003, ADR-0038)
- Producer Java-client parity: `send`/`sendAsync`, batching with `batchingMaxPublishDelay` flush timer, message chunking (PIP-37, chunks-never-batched with bounded consumer reassembly cap), LZ4/ZSTD/Snappy/ZLIB compression, `initialSequenceId`, `sendTimeout`, producer access modes Shared/Exclusive/WaitForExclusive/Fencing (PIP-68), custom `MessageRouter` with Murmur3/JavaStringHash, an interceptor SPI, `TypedMessageBuilder`, and hdrhistogram p50/p99/max stats.
  Best-effort `CloseProducer` is sent on last-clone drop.
  (#243; ADR-0057, ADR-0063)
- Consumer Java-client parity: Exclusive/Shared/Failover/Key_Shared subscriptions, `receive`/`batchReceive`, the full ack family (individual/cumulative/batch/with-properties/under-txn) including batch-index ack (PIP-54/391), negative-ack with `MultiplierRedeliveryBackoff` and an ack-timeout tracker (PIP-37), `reconsumeLater` retry-letter (PIP-58), dead-letter policy (PIP-22/58/124/409), seek by id/timestamp/earliest/latest and per-partition, pause/resume, `readCompacted`, key-shared sticky/auto-split/hash policy (PIP-34/119/282/379), subscription properties, `replicateSubscriptionState`, force-unsubscribe (PIP-313), and `MessageListener` push delivery across single/typed/multi-topic/partitioned/pattern consumers.
  (fe33784; ADR-0064)
- Reader, partitioned producer/consumer, multi-topic, pattern (regex, PIP-145), and `TableView` surfaces, all generic over `E: Engine`, with `auto_update_partitions_interval` tickers for partition growth.
  (8cfd1e3, 844655b, fe5d8c0, b51680a, 31f9cbe, 2b7570c, f09f23c)
- Transactions (PIP-31) end-to-end: a `TxnClient` coordinator with begin/commit/abort, `ADD_PARTITION_TO_TXN` / `ADD_SUBSCRIPTION_TO_TXN`, publish-under-txn, ack-under-txn, and `END_TXN` cleanup; the `Transaction` surface is engine-generic.
  (71e81e9, 19a8df5, ab9041b)
- Schema layer with full Java parity: Bytes/String/JSON/Avro/Protobuf/ProtobufNative/KeyValue/AutoConsume (PIP-87 broker lookup)/AutoProduceBytes plus all primitives.
  AVRO/JSON are canonicalised via the broker canonical form (apache-avro 0.21); PROTOBUF_NATIVE and KeyValue output is byte-identical to the Java client.
  (f3eb61b, d265a06, 08f5702)
- Authentication provider parity: Token, mTLS, OAuth2 `ClientCredentialsFlow` with token caching, SASL-PLAIN (RFC 4616), SASL-Kerberos via `libgssapi` multi-round `AUTH_CHALLENGE`, and Athenz (pre-fetched role token plus opt-in ZTS round-trip).
  In-band `AUTH_CHALLENGE` credential refresh implements PIP-30/PIP-292.
  (48a65b4, 122298e; ADR-0014, ADR-0029, ADR-0030, ADR-0041)
- Pluggable rustls crypto providers selected at compile time on the façade: `crypto-aws-lc-rs` (default, post-quantum hybrid key exchange), `crypto-ring`, `crypto-openssl` (`rustls-openssl` wrapper), and `crypto-fips` (`aws-lc-fips-sys`). rustls-only — no native-tls.
  (3f392af, b6f9cbe, closes issue #9; ADR-0005, ADR-0035)
- End-to-end message encryption (PIP-4, `magnetar-messagecrypto`): AES-GCM payload encryption with RSA-OAEP key wrapping, `MessageEncryptor`/`MessageDecryptor` traits on producer and consumer, and `cryptoFailureAction` Fail/Discard/Consume wired end-to-end, including a moonpool message-crypto bridge.
  (1bfc7e3, 6039251; ADR-0044)
- Admin REST client (`magnetar-admin`, reqwest + rustls) and a kubectl-style `magnetar` CLI: namespace/topic policy endpoints (retention, backlog-quota, TTL, persistence, dispatch-rate, dedup, compaction, delayed-delivery, max-producers/consumers/unacked), schema registry, rack-aware brokers/bookies, Functions/Sources/Sinks/Packages, subscription ops, and PIP-415 `getMessageIdByIndex`.
  (d315c20, d26028b)
- Resilience: supervised reconnect with `Connection::reset` and transparent producer/consumer rebuild, keepalive watchdog, terminal fast-fail, lookup-retry on session-lost, ack-gated re-attach replay, and a handshake-failure budget.
  Memory limit with `FailImmediately` atomic CAS and a `ProducerBlock` `Waker` slab, cluster failover (PIP-121 `ServiceUrlProvider`, Controlled/Auto), and `TOPIC_MIGRATED` supervised reconnect (PIP-188).
  (#263, 5dcc6f9, 6013320; ADR-0016, ADR-0017, ADR-0018, ADR-0020, ADR-0028, ADR-0060, ADR-0061)
- Additional PIPs: broker-entry metadata (PIP-90), shadow topics (PIP-180 — admin CRUD, producer `send_with_source_message_id`, consumer `MessageReceivedFromShadow`), and replicated subscriptions (PIP-33 — `replicate_subscription_state` field plus marker filter, with a two-cluster e2e fixture).
  (bc7ea94, 01d0afd; ADR-0033, ADR-0034)
- Experimental, default-off surfaces: PIP-466 V5 client (`magnetar::v5` behind `experimental-v5-client`, wraps v4 with no wire change) and the PIP-460 scalable-topics scaffold (behind `scalable-topics`: `topic://` URLs, DAG watch, `StreamConsumer`, `magnetar topic-info`).
  No released broker ships PIP-460.
  (b3c581e, d3684ac; ADR-0031, ADR-0032)
- Observability and proxy support: OpenTelemetry context propagation behind the `opentelemetry` feature (auto-injects `traceparent`/`tracestate` into message properties at the send boundary), and Apache Pulsar Proxy support via a per-broker connection pool with lookup-driven routing.
  (#151, #17; ADR-0039, ADR-0053)
- Structured logging across the driver: every error/warn/info log carries at least one structured field (xtask-enforced), with subscriber-side rate-limiting/sampling guidance.
  (#218, #280; ADR-0054, ADR-0065)
- Cross-runtime test and coverage policy: every behavioral change ships proto-unit + tokio-integration + moonpool-integration + differential-equivalence + e2e tests, with 100% moonpool patch coverage and a strict 1:1 tokio↔moonpool test count, all xtask-gated.
  The deterministic-simulation harness adds buggify fault injection, swizzle-clog workload, bit-flip survivability, and a seed sweep.
  (0c8c26c, fec933b; ADR-0024, ADR-0036, ADR-0048, ADR-0050, ADR-0055)

### Changed

- Migrated to Rust Edition 2024 and raised the MSRV to 1.88.
  (ADR-0007, ADR-0042)
- Refactored the client façade into dedicated `builders.rs` / `client_builder.rs` modules.
  (9b83a00, a69830c)
- Repinned the moonpool dependency to the published crates.io 0.7.0 (from a floating git dependency) and adopted vectored writes.
  (#242, 6a0e24b; ADR-0043, ADR-0056)
- Bumped runtime dependencies: `libgssapi` 0.7.2→0.9.1, `http` 1.4.0→1.4.1, and `rcgen` 0.13.2→0.14.8.
  (#13, #152)

### Removed

- Removed dead scaffolding and consolidated the crypto traits into `magnetar-proto`.
  (2a07f07)
- Removed `tls_trust_certs_file_path` from `ClientBuilder`.
  (1736be8)
- Dropped the earlier multi-step (0.1.0 / 0.2.0) release-planning artifacts, superseded by this single 1.0.0 release.
  (fd6e62d)

### Fixed

- Pre-release audit correctness fixes: decompression-bomb size cap, `Instant`-overflow guard, partition-hash correctness, and multi-topic receive starvation.
  (#279, 9347c39)
- Closed transaction parity gaps so the transactions e2e suite passes.
  (19a8df5)
- Hardened consumer behavior during seek and across transient broker-close, and fixed partitioned-topic auto-detection on topic delete.
  (issue #65, 780349c, 6ec47a1)
- The ack-timeout tracker now drops nacked message ids to prevent double redelivery.
  (7ce5e25)
- Resolved moonpool deterministic-simulation seed-sweep failures and hardened swizzle seed replay.
  (#244, #262, #264)
- The reconnect supervisor now persists its backoff across reconnects and resets only after the drop-grace window is stable.
  (#16)

### Security

- Secrets are redacted from `Debug` output: passwords and private keys (CWE-532), Athenz `private_key_pem`, and `AdminAuth::Token`, each guarded by secret-scan log-capture tests.
  (3406f7d, e92994e, f5ae060, 28711ef)
- All `panic!` and `debug_assert!` calls are removed from `magnetar-proto` production paths; every path returns `Result`/`Option`.
  (a561203, cac2199)
- CRC32C verify-or-drop on frames with magic `0x0e01`: a checksum mismatch emits a `ChecksumMismatch` event and drops the frame.
- Exposed `tls_allow_insecure_connection` and `tls_hostname_verification_enable` for Java parity, and cleared cargo-audit advisories (`time` 0.3.45 CVE, `rustls-pemfile` unmaintained).
  (2a9fafb, abc7aad)

[1.4.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.4.0
[1.3.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.3.0
[1.2.3]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.3
[1.2.2]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.2
[1.2.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.1
[1.2.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.0
[1.1.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.1.1
[1.1.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.1.0
[1.0.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.1
[1.0.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.0
