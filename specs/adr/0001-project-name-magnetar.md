# ADR-0001 — Name the project `magnetar`

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: naming, identity

## Context

The first draft of the project proposed the name `quasar-pulsar` (so the façade
crate would be `quasar-pulsar`, the sans-io core `quasar-pulsar-proto`, etc.).
Two problems showed up during research:

1. The bare crate `quasar` is **already taken** on crates.io (an unrelated
   actor-system experiment), so we couldn't publish the natural short name.
2. `quasar-pulsar` is awkward — the prefix-suffix repetition makes every
   sub-crate longer than needed (`quasar-pulsar-runtime-tokio`,
   `quasar-pulsar-auth-oauth2`, …).

`magnetar` is free on crates.io, evocative of "magnetic compass" /
"high-energy stream" (close to Pulsar's identity), and short enough that
prefixed sub-crates stay readable.

## Decision

- The workspace is named **`magnetar`**.
- The local source path is `/home/florentin/Sources/github.com/CleverCloud/magnetar/`.
- The GitHub repo is `github.com/CleverCloud/magnetar`.
- Published crates: `magnetar`, `magnetar-proto`, `magnetar-runtime-tokio`,
  `magnetar-runtime-moonpool`, `magnetar-admin`, `magnetar-fakes`,
  `magnetar-cli`, `magnetar-auth-oauth2`, `magnetar-auth-sasl`,
  `magnetar-auth-athenz`, `magnetar-messagecrypto`.

## Consequences

- Every doc reference using `quasar*` is now wrong and gets rewritten in the
  same change-set as the rename.
- The original ask-skill transcripts in `~/.claude/plans/` still mention
  `quasar-pulsar` historically; the promoted `docs/research.md` etc. preserve
  that wording as a record of the pre-decision reality.
- Memory in `~/.claude/projects/-home-florentin-Sources-github-com-me-quasar/`
  retains the old directory id because Claude's memory store is keyed on the
  original path. That's harmless.

## References

- [`docs/decisions-log.md` §"Project identity"](../../docs/decisions-log.md)
- [`docs/research.md`](../../docs/research.md) (crate name landscape)
