# ADR-0116 — Run the e2e suite on Pulsar 4.2.5 and 5.0.0 per pull request and on 4.0.14 nightly

- **Status**: Accepted
- **Date**: 2026-10-12
- **Decider**: Florentin Dubois
- **Tags**: testing, e2e, ci, docker, compatibility, xtask

## Context

[ADR-0109](0109-pin-the-e2e-broker-image-tag.md) pinned the e2e broker to `4.2.4` because `latest` had moved to Pulsar 5.0.0 and the suite broke on it.
[ADR-0115](0115-share-one-version-agnostic-pulsar-e2e-fixture.md) removed the reason it could not be changed: one shared fixture, a readiness wait that holds on every version, and a gate that keeps the tag constants, CI's pre-pulls, the performance campaign and the PIP-33 compose pin in step.
It left the question of _which_ versions to run open, and the answer had changed in the meantime:

- Clever Cloud runs **4.2.5** in production.
  It is the last release before 5.0, and it carries the broker fix for apache/pulsar#26416 (issue #462) that `4.2.4` lacked.
- **5.0.0** was published on 2026-10-01 as the new upstream LTS.
  A client that has never run its Docker-bound suite against it is making a compatibility claim it has not tested.
- **4.0.14** is the last release of the 4.0 LTS line, which [ADR-0009](0009-pulsar-4-minimum.md) makes the minimum supported broker.
  Since the 4.0.4 pin went away, nothing but three suites pinned to `4.0.13` ran against that line, so the stated minimum was barely tested.
- Per the Apache Pulsar release policy as of 2026-10-12: 4.0 has active support until 2026-10-21 and security fixes until 2027-10-21; 4.2 is a feature line and has been out of support since 2026-09-24; 5.0 has active support until 2028-10-05.
  The two lines worth testing are the two LTS lines, plus the version production runs.
- `4.2.4` and `4.0.13` are superseded releases of lines whose newest releases are `4.2.5` and `4.0.14`; there is no reason left for CI to pull them (the performance nightly keeps them for one cycle, decision 7).

Two of the pinned suites had no reason to be pinned.
`e2e_consumer_drop.rs` and `e2e_reconnect_safety.rs` sat on `4.0.13` since before ADR-0115, with no comment saying a 4.0 broker was needed; only the refusal test of `e2e_scalable_topic.rs` (a pre-5.0 broker must refuse scalable topics) and the PIP-460 tests really need a specific broker.
A suite pinned for no reason runs on exactly one version, which is the thing a version matrix exists to prevent.

The per-pull-request `test` job already runs five cells (ADR-0098): one non-e2e and four e2e shards.
The e2e cells measured about 10–16 minutes each on run 38227469856.

## Decision

1. **The versions.**

   | Version                  | Where it runs                                                                | Why                                                                                                              |
   | ------------------------ | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
   | `4.2.5`                  | every pull request, as `DEFAULT_TAG`; also the PIP-33 fixture                | what Clever Cloud runs; the last release before 5.0                                                              |
   | `5.0.0`                  | every pull request, as a second axis                                         | the new upstream LTS                                                                                             |
   | `4.0.14`                 | nightly at 05:47 UTC and on `workflow_dispatch`                              | the 4.0 LTS and the minimum supported broker (ADR-0009), `MIN_SUPPORTED_TAG`                                     |
   | `4.2.5` (`V4_LTS_TAG`)   | the one test that needs a pre-5.0 broker, whatever the axis                  | `e2e_scalable_topic_unsupported_on_v4_broker`                                                                    |
   | `5.0.0` (`SCALABLE_TAG`) | the PIP-460 tests of `e2e_scalable_topic.rs`, on every run, nightly included | Pulsar 5.0.0 GA carries PIP-460; the tests ran on the 5.0.0-M1 milestone until the vendored protocol moved to GA |

   Exact tags only, never `latest`, a minor alias or a digest.
   `SCALABLE_TAG` moved from `5.0.0-M1` to `5.0.0` when the vendored protocol moved to the GA release (PR #880): PIP-460 e2e runs on Pulsar 5.0.0 GA, `5.0.0-M1` is no longer a constant and is kept in the performance campaign's `FIXTURE_TAGS` only as part of the one-cycle grace set of decision 7.

2. **Every suite runs on every axis unless it cannot.**
   `Broker::Default` is the matrix broker: CI exports the axis tag as `MAGNETAR_PULSAR_IMAGE_TAG`, which the fixture already honours.
   `e2e_consumer_drop.rs` and `e2e_reconnect_safety.rs` move from `Broker::V4Lts` to `Broker::Default`.
   The only suites that pin are the PIP-460 tests (`Broker::Scalable`) and the v4-refusal test (`Broker::V4Lts`, now `4.2.5`).
   `V4_LTS_TAG` is a literal of its own and not an alias of `DEFAULT_TAG`, so moving the default to a 5.x tag cannot drag the refusal test onto a broker that supports scalable topics.
   `Broker::V4Lts` / `V4_LTS_TAG` now mean "the newest pre-5.0 broker" (4.2.5) and no longer the 4.0 LTS; both carry a doc comment saying so.
   They keep their spelling for now: the performance nightly overlays the candidate's `common/pulsar.rs` onto its base checkout, and the base only advances to a head that includes this ADR after the first green nightly that follows it.
   A base that has the shared fixture but not this change would still name `Broker::V4Lts` in its suites, and a rename would stop it from compiling.
   They are renamed with the removals of decision 7.

3. **One job, called twice.**
   The four e2e shards of ADR-0098 move into the reusable workflow `.github/workflows/e2e-pulsar.yml` (`workflow_call`, inputs `pulsar` and `pip33`).
   `ci.yml` calls it from an `e2e` job whose matrix lists `4.2.5` and `5.0.0`, and `e2e-nightly.yml` calls it for `4.0.14`; the `test` job keeps only the non-e2e cell.
   The shard script, the pre-pulls, the PIP-33 steps and the 180-minute timeout therefore exist once.
   Cells render as `e2e (Pulsar 5.0.0) / e2e 3/4`, so the version is in every name, and every matrix has `fail-fast: false`.
   `ci.yml` does not run on a schedule, so the nightly is a separate workflow file and not a cron on `ci.yml`, which would have started every other job of `ci.yml` with it.
   05:47 UTC collides with none of the other schedules: performance 01:17, moonpool seed sweep 03:17, the Monday PIP-33 weekly 03:47, xtask gates 04:17.

4. **`e2e_replicated_subscriptions` runs only on the 4.2.5 axis.**
   The PIP-33 two-cluster compose (`docker-compose.replicated-subs.yml`) is a 4.2.5 fixture, pinned to `DEFAULT_TAG`, and the test connects to its fixed ports and never boots a broker of the axis.
   Only the `4.2.5` run brings the fixture up (`pip33: true`, shard 4) and selects `e2e_replicated_subscriptions`; on `5.0.0` and `4.0.14` the shard script skips that one target and prints a notice.
   This is the one operator-accepted target skip of the matrix, and it is not a pass on those versions: PIP-33 has no 5.0 or 4.0 coverage until its compose is ported, which is separate work.
   The weekly workflow of ADR-0075 is unchanged, and it also runs on the default tag.

5. **An axis lands green, and stays green.**
   A Pulsar version joins the per-pull-request axis only when the whole suite passes on it.
   There is no `continue-on-error`, no per-version `#[ignore]`, no loosened assertion and no raised timeout to make an axis pass, and no skipped test other than the one target of decision 4 ([ADR-0021](0021-no-silent-test-ignore-or-remove.md), [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)).
   A failure on one version is one of three things, and each has its own fix: a client bug (fix the client), a test assumption the broker legitimately changed (change the test, with the broker change named in a comment, so that it holds on every axis), or a broker defect (a tracked issue and a decision recorded here; never a silent skip).
   A red nightly is a regression on the minimum broker and is treated like a red pull-request cell.
   The `#[ignore]` on the permit-accounting suite for apache/pulsar#26416 is not part of this decision.
   `4.2.5` and `4.0.14` carry the upstream fix, so the ignore can be revisited, but separately.
   Landing the axes found the three test assumptions listed under "5.0 / 4.2.5 behaviour changes the suites had to accommodate"; each is a change of the third kind above, made without touching an assertion.

6. **The gate keeps it honest.**
   `cargo run -p xtask -- check-e2e-fixture` (rule 3 of ADR-0115) now reads the `pulsar: "<exact version>"` values of the e2e jobs as the axis and fails closed on:
   - a `docker pull apachepulsar/pulsar:<tag>` that is neither a tag constant nor an axis value, and a tag constant (other than `DEFAULT_TAG`, pulled by the axis cell that boots it) without its own pull; the axis is pre-pulled only through the one spelling `${{ inputs.pulsar }}`, and any other expression is unreadable;
   - an axis value, or a tag constant, missing from `FIXTURE_TAGS`;
   - `MIN_SUPPORTED_TAG` (`4.0.14`, declared once in `common/pulsar.rs` as the minimum supported broker of ADR-0009) that is not an axis value of a workflow other than `ci.yml`, so deleting or retargeting the nightly fails the gate; it is a nightly axis value and never pulled by name in a pull-request cell;
   - `DEFAULT_TAG` that is not an axis value of `ci.yml`, so the default broker really does run the whole suite on every pull request;
   - a workflow that pre-pulls the axis without exporting `MAGNETAR_PULSAR_IMAGE_TAG: ${{ inputs.pulsar }}` to the suite, or the reverse, or that hard-codes that variable: the cell would pull one broker and test another;
   - an unquoted, single-quoted, `latest`, non-exact or list-valued `pulsar:` (an unquoted `4.10` is a YAML float), a `pulsar:` that opens a block list, a `pulsar` key written inside a flow mapping or flow list, a quoted `pulsar` key, and a compose pin that differs from `DEFAULT_TAG`.

7. **The performance harness follows.**
   `FIXTURE_TAGS` in `scripts/performance_campaign.py` holds every tag constant, every axis value and the grace set below.
   The campaign no longer names a tag: it reads `IMAGE_REPO` and `DEFAULT_TAG` from the candidate's `common/pulsar.rs` to find the broker image of its private PIP-33 fixture and of its scenario container.
   The grace set is `4.2.4` and `4.0.13`, which are not constants.
   The nightly measures the head of the last successful scheduled run as its base ([ADR-0113](0113-run-performance-measurement-nightly.md)), so the first nightly after this ADR uses `a154636`, which predates the shared fixture and boots its own per-file pins that `common/` does not override.
   The set is computed from that tree with `git grep -h -oE 'apachepulsar/pulsar:[^"]+|DEFAULT_IMAGE_TAG: &str = "[^"]+"|PULSAR_TAG[^=]*= "[^"]+"|_IMAGE_TAG: &str = "[^"]+"' a154636 -- crates/magnetar/tests scripts`: `4.2.4` in 59 suites and the compose fixture, `4.0.13` in `e2e_consumer_drop`, `e2e_reconnect_safety` and the v4 test of `e2e_scalable_topic`, and `5.0.0-M1`, which was `SCALABLE_TAG` then.
   `latest`, `4.0.4` and `4.2.3` appeared there only in the old `FIXTURE_TAGS` and in prose, so they are removed.
   **Removal condition:** after the first green performance nightly whose base includes this ADR, delete the `4.2.4`, `4.0.13` and `5.0.0-M1` entries of `FIXTURE_TAGS` and rename `Broker::V4Lts` / `V4_LTS_TAG` (decision 2) in one change.

8. **Bumping a version is a constants-and-matrix change.**
   Changing the default or an axis means editing `DEFAULT_TAG` / `V4_LTS_TAG` / `MIN_SUPPORTED_TAG` / `SCALABLE_TAG` in `common/pulsar.rs`, the `pulsar:` values of `ci.yml` or `e2e-nightly.yml`, the literal pre-pulls in `e2e-pulsar.yml`, `FIXTURE_TAGS`, and the six compose pins, in one changeset.
   The gate fails until all of them agree.
   A new major also needs its own review of the suite, per ADR-0109 §3 as carried by ADR-0115, and lands only green (decision 5).

## 5.0 / 4.2.5 behaviour changes the suites had to accommodate

Running the whole suite on `5.0.0` and on the newest 4.x releases (full run on `5.0.0`, representative suites on `4.2.5` and `4.0.14`, 2026-10-12) found three suites whose assumptions a broker release had changed.
None is a client defect, and none needed an assertion, a timeout or a skipped test to change.

- **Shadow topics are opt-in from 4.0.14, 4.2.5 and 5.0.0.**
  `conf/standalone.conf` of those releases carries `enableShadowTopics=false` ("Enable shadow topic creation, loading and replication. Requires a broker restart."), and the admin REST then answers `405 {"reason":"Shadow topics are disabled"}` to a shadow-topic creation.
  The key is absent from the `standalone.conf` of 4.0.4, 4.0.13, 4.2.3, 4.2.4, 5.0.0-M1 and 5.0.0-M2, and `e2e_shadow_topic` passes on 4.0.13 and 4.2.4 without it, so the default flipped between 4.0.13 and 4.0.14 and between 4.2.4 and 4.2.5.
  `e2e_shadow_topic` and `e2e_shadow_topic_replicator` now set `enableShadowTopics=true` through the fixture's `.config(…)`, which holds on every version.
  The suite was already failing on the default `4.2.5` before this ADR added `5.0.0`.
- **Pulsar 5.0.0 does not acquire a namespace bundle when a producer attaches to it directly.**
  `e2e_moonpool_direct_broker_authority` points a client at a stub bootstrap that answers lookups itself, then attaches to the real broker without a lookup.
  On 5.0.0 the broker logs `Namespace bundle (public/default/0xf8000000_0xffffffff) … not served by this instance:localhost:8080. Please redo the lookup.` (`FAILED_CHECK_OWNERSHIP`) and answers `ServiceUnitNotReady` (code 6); on 4.0.14 and 4.2.5 it logs `Trying to acquire ownership of public/default/0xc0000000_0xffffffff` and the attach succeeds.
  The test now creates and closes a producer on that topic through the real client first, so the broker owns the bundle; the portless-authority assertions are unchanged.
- **`e2e_reconnect`** failed once on `5.0.0` (`e2e_subscribe_during_reconnect_reissues_lookup_transparently`, "subscribe during reconnect did not complete within 30 bounded attempts") while the host load average was about 20, and passed in all 15 reruns (6 on 4.2.5, 9 on 5.0.0, including two under 20 busy loops).
  It is recorded as observed once and not reproduced; nothing was changed.
  If it recurs in CI, capture `RUST_LOG=magnetar=debug` and the broker log: the first suspect is a lookup retry that stalls after a code 6 following the broker restart.

## Consequences

- Every pull request runs the whole Docker-bound suite on two brokers.
  The `test` job's five cells become nine (one non-e2e, eight e2e), which adds four e2e cells per pull request, roughly 40–65 runner-minutes at the measured 10–16 minutes each.
  They run in parallel, so wall-clock time stays about that of one e2e cell.
- Compatibility with 5.0 stops being a claim and becomes a result of every pull request, and the 4.0 minimum is exercised nightly instead of by three suites.
- `4.2.5` is the baseline, so the fix for apache/pulsar#26416 is in the default broker.
- The required-check names change: `test (e2e 1/4)` and its siblings become `e2e (Pulsar 4.2.5) / e2e 1/4` and so on.
  A branch-protection rule that names them must be updated; `main` carried none as of the last record of ADR-0092.
- `e2e_consumer_drop.rs` and `e2e_reconnect_safety.rs` now honour `MAGNETAR_PULSAR_IMAGE_TAG` again; ADR-0115's note that they no longer do is superseded.
- `Broker::V4Lts` / `V4_LTS_TAG` are named for a line they no longer pin (they mean the newest pre-5.0 broker) until the follow-up of decision 7.
- `e2e_replicated_subscriptions` is never selected on `5.0.0` or `4.0.14` (decision 4); PIP-33 therefore has no 5.0 or 4.0 coverage until its compose fixture is ported to those versions.
- A new e2e suite needs no CI edit to join every axis: the shard inventory is still the `e2e_*.rs` glob.

## Alternatives considered

- **Add 4.0.14 to the per-PR matrix.** Rejected: four more cells on every pull request for a line production does not run.
  Its value is catching a regression on the minimum broker, which a nightly does at a day's latency and no per-PR cost.
- **A cron on `ci.yml` with the 4.0.14 cells.** Rejected: a `schedule` trigger would run every job of `ci.yml`, most of them unrelated, or require an `if` on each.
- **Move the shard script to `scripts/` and keep a second job skeleton in the nightly.** Rejected: the script is the smaller half.
  The free-disk step with its pinned action, the toolchain and cache steps, the four pre-pulls (KDC, ZTS and the brokers) and the PIP-33 steps would exist twice and drift; and the gate would read the pre-pull from two spellings.
- **Run PIP-33 on every axis.** Rejected: five containers per cell for a test that does not use the axis broker, and the compose fixture would need a version parameter the gate then has to read as well.
- **`continue-on-error` or a per-version `#[ignore]` so 5.0.0 can land red.** Rejected: ADR-0021, ADR-0046 and the rule of decision 5.
  A required axis that is allowed to fail certifies nothing.
- **Keep tracking `latest`.** Rejected again, for the reason of ADR-0109: a tag that moves changes the suite inside whichever pull request is open.
- **Rename `V4Lts` now.** Rejected for this change: a nightly base that has the shared fixture but not this change would stop compiling (decision 2).

## References

- `crates/magnetar/tests/common/pulsar.rs` — the tag constants and the `MAGNETAR_PULSAR_IMAGE_TAG` hook; `crates/magnetar/tests/pulsar_fixture_contract.rs` — their Docker-free contract.
- `.github/workflows/e2e-pulsar.yml`, `.github/workflows/ci.yml` (`e2e`), `.github/workflows/e2e-nightly.yml` — the reusable job, its per-PR callers and the nightly.
- `xtask/src/main.rs` — `check-e2e-fixture` and its unit tests; `scripts/performance_campaign.py` — `FIXTURE_TAGS`, `default_broker_image`.
- `docs/testing.md` § "The shared Pulsar fixture" and § "The Pulsar version matrix".
- [ADR-0009](0009-pulsar-4-minimum.md) — the minimum broker; [ADR-0075](0075-retain-weekly-two-cluster-pip33-e2e-gate.md) — the weekly PIP-33 workflow; [ADR-0098](0098-parallelize-per-pr-test-execution.md) — the shards; [ADR-0109](0109-pin-the-e2e-broker-image-tag.md) — superseded by this ADR; [ADR-0113](0113-run-performance-measurement-nightly.md) — the nightly base that forces the `4.2.4` grace; [ADR-0115](0115-share-one-version-agnostic-pulsar-e2e-fixture.md) — the fixture and the gate this extends.
