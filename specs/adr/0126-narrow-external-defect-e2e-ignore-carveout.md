# ADR-0126 — Allow one e2e `#[ignore]` for an external defect no available broker has fixed

- **Status**: Accepted (amends [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) §7 and its decision paragraph, narrowly and for one named test — everything else in ADR-0046 remains binding). Its one instance was retired by the 2026-11-03 amendment below; the five conditions remain in force for any new instance.
- **Date**: 2026-11-02
- **Decider**: Florentin Dubois
- **Tags**: testing, e2e, process, pulsar, shared-subscription, issue-462

## Context

[ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) §7 forbids `#[ignore]` on an end-to-end test, and its Consequences state the intended end state as "there are no `#[ignore]`s on e2e tests, full stop".
[`crates/magnetar/tests/e2e_shared_subscription_permit_accounting.rs`](../../crates/magnetar/tests/e2e_shared_subscription_permit_accounting.rs) has carried the suite's only `#[ignore]` since [PR #460](https://github.com/CleverCloud/magnetar/pull/460).
The deviation is documented in the test's own doc comment, in [`docs/testing.md`](../../docs/testing.md) and in `CLAUDE.md`, but the ADR layer never recorded it — so an accepted ADR says "full stop" while the tree holds a counterexample.

[Issue #462](https://github.com/CleverCloud/magnetar/issues/462) exists to close exactly that state, and names two independent decisions: remove the attribute once a published image is reliably green, or reconcile the ADR layer.
This ADR takes **only the second**.
It does not remove the attribute, it does not authorize keeping it once a reliably green image exists, and it leaves issue #462 open to own the removal.

### Why this test is unlike every other e2e test

Every other e2e test asserts something about magnetar.
This one asserts something about the **broker**: that [apache/pulsar#26416](https://github.com/apache/pulsar/issues/26416)'s deferred-flow accounting leak is absent under this client's Flow/Subscribe traffic and timing.
`Consumer.flowPermits` credits the per-consumer counter on the IO thread and hands the subscription-aggregate credit to the broker executor; `PersistentDispatcherMultipleConsumers.internalConsumerFlow` discards that deferred credit when the consumer has already left `consumerSet`, while `removeConsumer` debits the aggregate by the consumer's full counter, discarded credit included.
No client behaviour makes that pass or fail, so leaving it enabled puts CI permanently red on a defect this repository cannot repair — the failure mode where a check stops being read.

### No available broker passes it repeatably

| image                   | observed               | result                                                          |
| ----------------------- | ---------------------- | --------------------------------------------------------------- |
| `4.0.4`                 | 2026-09-18             | red 3/3, minimum −10 to −14                                     |
| `4.2.4` (then `latest`) | 2026-09-18             | red 8/8, minimum −4 to −18                                      |
| `5.0.0-M2`              | 2026-09-18             | green 11/12, twelfth at −2 (raw log not archived)               |
| `4.0.13`                | 2026-09-30             | red 1/1, minimum −16, 79 negative readings of 98                |
| `4.0.14`                | 2026-10-12, 2026-10-16 | green 5/5, then green 3/3                                       |
| `4.2.5`                 | 2026-10-12, 2026-10-16 | green 7/10 with a residual −2, then run 1 green and run 2 at −2 |
| `5.0.0`                 | 2026-10-12             | green 7/10, residual −2                                         |

The 2026-10-16 campaign is recorded with image-index digests and the failing run's raw-log SHA-256 by [PR #885](https://github.com/CleverCloud/magnetar/pull/885), which was still open when this ADR merged and is the PR that brings those two surfaces into conformance; the 2026-10-12 matrix is reported in [PR #881](https://github.com/CleverCloud/magnetar/pull/881).
The test selects `Broker::Default`, so the per-PR axes are `4.2.5` and `5.0.0` — the two cells that still reproduce a negative aggregate in roughly three runs of ten.
`4.0.14` is green in 8 of 8 runs across two campaigns, but it is the nightly axis only.

### The upstream fix is present and still not sufficient

apache/pulsar#26416 closed upstream on 2026-09-16.
Verified on 2026-11-02 by reading each tag's own commit range with `gh api repos/apache/pulsar/compare/<previous-tag>...<tag>`, [apache/pulsar#26289](https://github.com/apache/pulsar/pull/26289) and [apache/pulsar#26422](https://github.com/apache/pulsar/pull/26422) ship in `4.0.14`, `4.2.5` and `5.0.0`, alongside the later permit-leak fixes apache/pulsar#26576, #26606 and #26661; `4.0.13` and `4.2.4` carry neither.

**Carrying the patches is therefore not evidence of a green cell.** `4.2.5` and `5.0.0` both carry them and both still reproduce a residual −2.
This is recorded because the inference is tempting and wrong: an argument from version numbers alone would un-ignore a test that is red in roughly three runs of ten on both per-PR axes.
Only a repeated measurement on the exact image settles it, which is what issue #462 asks for.

## Decision

An end-to-end test may carry `#[ignore]` when **all five** conditions hold.
Short of all five, ADR-0046 §7 applies unchanged.

1. **The assertion is about an external component**, not about magnetar: no change to this repository can make it pass.
2. **No version of that component available to the e2e matrix passes it repeatably**, demonstrated by a recorded measurement per image — not inferred from release notes, a changelog, or the presence of an upstream patch.
3. **The attribute's reason string names** the tracking issue, the counter-evidence that keeps it (the measured value, not a prose claim), and the exact command that reproduces it.
4. **The deviation is recorded** in the test's doc comment and in `docs/testing.md` with the per-image evidence, and in this ADR.
5. **An open issue owns the removal**, and the removal is owed: the carve-out licenses the attribute's existence, never its permanence.

The one instance authorized today is `e2e_shared_subscription_permit_accounting_survives_repeated_churn`, owned by issue #462, reviewed by **2026-12-15**.
Its conditions 3 and 4 are satisfied by PR #885, which rewrites the reason string to name the measured `totalAvailablePermits=-2` and the reproduce command, and the `docs/testing.md` row to carry the per-image evidence with digests.
This ADR landed first, so for a short window `main` carried a reason string still saying `4.0.14 / 4.2.5 unpublished` and an evidence row holding only the 2026-09-30 sample — authorized in principle, not yet conformant in the tree.
PR #885 closed that window: the reason string now names the measured `totalAvailablePermits=-2` with its reproduce command, and the `docs/testing.md` row carries the per-image evidence with index digests and the failing run's raw-log hash. Conditions 3 and 4 are satisfied.

**Reactivation is a measurement, not a release note.** The attribute comes off when both per-PR axes — the `Broker::Default` image and `5.0.0` — pass the unmodified test repeatedly on their published images, with each run's observation count and minimum recorded.
A new broker release that merely contains an upstream patch does not qualify; neither does a single green run.
The removal lands with that evidence in the PR description.

This carve-out is narrow on purpose.
It does not restore ADR-0021 §1's general "environment-dependency" exemption, which ADR-0046 §7 removed for e2e; it does not cover an intermittent test of our own, which is [ADR-0123](0123-quarantine-exact-intermittent-test-scenarios.md)'s register; and it does not cover a test that is merely slow, awkward, or unfinished.

## Amendment — 2026-11-03: the one instance is retired, by correcting its assertion

The reactivation rule above asked for the _unmodified_ test to pass repeatedly. That rule assumed the test asserted an invariant of the broker. It did not. The test required the dispatcher's `totalAvailablePermits` to stay non-negative at every log sample. apache/pulsar#26289, the patch that fixes the leak, makes a negative sample part of the design. A Flow credits the consumer at once and the aggregate later, and `readMoreEntries` dispatches against `max(totalAvailablePermits, firstAvailableConsumerPermits)`, so the broker spends permits the aggregate has not received yet. `Consumer.getAvailablePermitsForDispatcherRemoval` documents the resulting balance as negative and forbids clamping it. Every such dip closes when the pending Flow task runs. The leak never closes, because the credit `internalConsumerFlow` discards is lost.

The test now replays the dispatcher ledger from every line that moves it, including the dispatch debits, which the former parser did not read. It asserts that the settled total equals the sum of the consumers' settled `availablePermits`, and it fails as an infrastructure fault when the replay cannot reproduce a printed total. CI run [41681792538](https://github.com/CleverCloud/magnetar/actions/runs/41681792538) made ten runs per image with every dispatcher line dumped:

| image               | former `min >= 0` | settled total − consumers' sum |
| ------------------- | ----------------- | ------------------------------ |
| `4.2.4` (no #26289) | red 10/10         | −12 to −20 in 10/10            |
| `4.2.5`             | red 9/10          | 0 in 10/10                     |
| `5.0.0`             | red 9/10          | 0 in 10/10                     |

The replay matched every printed total in all thirty runs. Every red run on the patched images failed only the `min >= 0` assertion: delivery, the drained backlog and the per-consumer bounds passed. The corrected assertion still catches the defect it exists for, red on every unpatched run, and is green on both per-PR axes. Condition 2 therefore no longer holds, and the `#[ignore]` is removed. A red run of this test on a matrix image now means that image leaks.

This is not the version-number argument that the alternatives below reject. The test does not pass because a release note claims a fix. It passes because the property it asserted was measured to be the wrong property, and the right one was measured on all three images.

## Consequences

**Positive**

- The ADR layer and the tree agree. ADR-0046 §7 no longer reads "full stop" against a standing counterexample, which is the state issue #462 was opened to close.
- The five conditions are checkable, so the next request for an e2e `#[ignore]` is answered by evidence rather than by precedent — condition 2 in particular refuses the version-number argument that the 2026-11-02 verification shows to be unsound.
- The removal stays owed, with a named review date, rather than becoming permanent by silence.

**Negative**

- A deviation that is recorded is easier to live with than one that is not, and condition 5 is the only thing resisting that drift. The review date is the mitigation; it is not enforced by any gate.
- The aggregate counter goes unmeasured on every pull request. Until the attribute comes off, a regression in this client's Flow/Subscribe traffic that worsens the broker's ledger would be invisible to CI. `e2e_shared_subscription_churn.rs` still asserts that dispatch continues, which is a weaker property.
- One more ADR to read before adding an e2e test.

**Neutral**

- No code, workflow, or gate changes. `cargo test --workspace --all-features` behaves exactly as before, and no xtask gate counts `#[ignore]` attributes.
- Issue #462's decision 1 is untouched and still open.

## Alternatives considered

- **Keep §7 absolute and record nothing (the status quo).** Rejected: it is the state issue #462 exists to close. The attribute has been in the tree since PR #460 with measured justification, so "the ADR forbids it" has been false in practice for months, and a binding rule that is quietly violated teaches that the rules are approximate.
- **Remove the `#[ignore]` now, because the upstream patches are published.** Rejected on measurement: `4.2.5` and `5.0.0` carry both patches and still reach −2 in roughly three runs of ten, so this would put both per-PR axes red intermittently on a third party's defect. This is the specific error condition 2 exists to prevent.
- **Enrol the scenario in ADR-0123's quarantine register instead.** Genuinely attractive, and the recommended next step if the project wants the measurement continuously: the register keeps the assertion in its source file and runs it as a non-gating exact-name diagnostic, which is strictly more information than `#[ignore]`. Not taken here because the register is scoped to intermittent scenarios whose cause is ours to resolve, and its reactivation semantics ("establish that the unchanged scenario no longer reproduces") do not fit a defect only an upstream release can close. Enrolling a third-party defect needs that scope widened deliberately, in its own ADR, and the full churn campaign costs real CI time on two axes per pull request.
- **Pin the test to `4.0.14`, the one image green in 8 of 8 runs.** Rejected: ADR-0115 makes a suite select a `Broker`, never a tag, and `check-e2e-fixture` enforces it. Pinning would also assert the property only on the nightly axis's image while the per-PR axes ship unmeasured, which is the weaker guarantee dressed as the stronger one.
