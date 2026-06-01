# ADR-0044 — Port the PIP-4 message-crypto bridge to the moonpool engine

- **Status**: Accepted
- **Date**: 2026-05-29
- **Decider**: Florentin Dubois
- **Tags**: moonpool, encryption, pip-4, engine-parity, sans-io

## Context

PIP-4 end-to-end encryption (AES-GCM) shipped first on the tokio engine.
`magnetar-messagecrypto` owns the AES-GCM primitive; the façade's `MessageCryptoBridge` ([`crates/magnetar/src/crypto_bridge.rs`](../../crates/magnetar/src/crypto_bridge.rs)) adapts it to the engine-owned `MessageEncryptor` / `MessageDecryptor` trait pair, and `magnetar-runtime-tokio::crypto` wires the encrypt-on-send / decrypt-on-receive path into the tokio producer / consumer.

The moonpool engine had no counterpart.
Its engine crypto API (`MessageEncryptorApi` / `MessageDecryptorApi`) resolved to `NoEncryption`, so a moonpool producer / consumer could not encrypt or decrypt.
Two consequences followed:

1. The moonpool engine was **not** at PIP-4 parity with tokio, contrary to the engine-parity train tracked in [`README.md` §"Engine-by-engine surface coverage"](../../README.md#engine-by-engine-surface-coverage).
2. The `magnetar-differential` harness could not assert tokio ↔ moonpool equivalence for the encrypted path — both engines have to drive the crypto for the equivalence claim to mean anything.
   The `cryptoFailureAction` matrix golden trace was blocked on exactly this (former `docs/follow-ups.md` §3).

`magnetar-runtime-moonpool` cannot depend on `magnetar-messagecrypto` directly (the façade is the layer that owns the crypto-provider feature matrix per [ADR-0035](0035-pluggable-crypto-provider.md)), so the engine defines its own thin trait pair and lets the façade bridge supply the implementation — exactly as the tokio engine already does.

## Decision

The moonpool engine gains the PIP-4 message-crypto bridge, mirroring the tokio engine.

- **Engine trait pair.** [`crates/magnetar-runtime-moonpool/src/crypto.rs`](../../crates/magnetar-runtime-moonpool/src/crypto.rs) defines `MessageEncryptor` / `MessageDecryptor` / `EncryptError`, the moonpool counterparts of `magnetar-runtime-tokio::crypto`.
  The producer / consumer hold an `Arc<dyn MessageEncryptor>` / `Arc<dyn MessageDecryptor>` populated from the façade.
- **Producer (encrypt-on-send).** Encrypt the payload, stamping `pb::MessageMetadata` `encryption_keys` / `encryption_algo` / `encryption_param`.
  This mirrors the tokio producer's **compression → encryption** ordering for the encryption step.
  Compression itself is not yet wired on the moonpool engine — non-`None` `CompressionKind` is refused on send until the runtime codec lands (M3) — so the moonpool path is encrypt-only in practice.
- **Consumer (decrypt-on-receive).** Decrypt the payload, honoring the three `CryptoFailureAction` arms — `Fail`, `Discard`, `Consume` — identically to tokio, then deliver.
  Compression being refused on send, there is no decompression step to mirror: the path reduces to **decrypt, then deliver** (tokio's decrypt-first → decompress ordering with the decompress branch a no-op on moonpool until codecs land).
- **Façade.** `MessageCryptoBridge` now implements **both** engines' trait pairs over `magnetar-messagecrypto::MessageCrypto`, so one bridge value plugs into either engine.
  The moonpool builders gain `.encryption()` / `.create_with_encryption()` (producer) and `.encryption()` / `.subscribe_with_decryption()` (consumer), routing through the new `Client::open_producer_with` / `Client::subscribe_with` entries on the moonpool engine.
- **Engine crypto API is non-stub for both engines.** `MessageEncryptorApi` / `MessageDecryptorApi` now resolve to the real bridge on the moonpool engine.
  `NoEncryption` is retained **only as the documented opt-out** — the resolved API when no bridge is supplied — not as the moonpool default.
- **Equivalence is asserted via the differential harness** per [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).
  The scripted broker ([`crates/magnetar-differential/src/broker.rs`](../../crates/magnetar-differential/src/broker.rs)) round-trips the PIP-4 `MessageMetadata` encryption fields verbatim (mirroring a real broker's PIP-4 opacity).

## Consequences

**Easier**

- The moonpool engine reaches PIP-4 parity with tokio — encryption + decryption + `CryptoFailureAction` work on both engines.
- The `cryptoFailureAction` matrix golden trace is unblocked and lands as a differential equivalence test (former `docs/follow-ups.md` §3 closed).
- A single `MessageCryptoBridge` value is engine-agnostic — callers do not pick a different bridge type per engine.

**Harder / cost**

- The crypto path now has **two** engine implementations to keep in lockstep.
  The four-layer + 1:1 parity gates ([ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)) make drift loud, but the duplication is real (the engine trait pair is mirrored, not shared, because `magnetar-proto` is zero-I/O and the façade owns the provider matrix).

**Test coverage** (ADR-0024 four layers + 1:1)

- Per-engine unit tests: encrypt-on-send, decrypt round-trip, Fail / Discard / Consume, no-decryptor, clone-preserves-decryptor.
- Differential equivalence: [`crypto_roundtrip_equivalence.rs`](../../crates/magnetar-differential/tests/crypto_roundtrip_equivalence.rs) (encrypted round-trip parity) and [`crypto_failure_action_equivalence.rs`](../../crates/magnetar-differential/tests/crypto_failure_action_equivalence.rs) (the 3-arm matrix), pinned by golden trace [`crypto_failure_action.json`](../../crates/magnetar-differential/tests/golden/crypto_failure_action.json).
- The end-to-end PIP-4 + `cryptoFailureAction` coverage stays at [`crates/magnetar/tests/e2e_crypto.rs`](../../crates/magnetar/tests/e2e_crypto.rs).

**Parity guarantees**

- `cargo run -p xtask -- check-runtime-test-parity` keeps the 1:1 tokio ↔ moonpool test count; the crypto unit tests land in matched pairs.

## References

- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer + 1:1 parity policy this port satisfies.
- [ADR-0035](0035-pluggable-crypto-provider.md) — the crypto-provider feature matrix the façade owns (why the engine defines a thin trait pair and the façade supplies the implementation).
- [ADR-0019](0019-engine-scope-and-moonpool-parity.md) — the moonpool engine-parity train this closes one item of.
- `crates/magnetar-runtime-moonpool/src/crypto.rs` — the moonpool engine trait pair (`MessageEncryptor` / `MessageDecryptor` / `EncryptError`).
- `crates/magnetar/src/crypto_bridge.rs` — `MessageCryptoBridge` implementing both engines' trait pairs.
- `crates/magnetar/src/builders.rs` — the moonpool `.encryption()` / `.create_with_encryption()` / `.subscribe_with_decryption()` builders.
- `crates/magnetar-differential/src/broker.rs` — PIP-4 metadata round-tripped verbatim.
- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) §"PIP-4 message-crypto bridge" — the engine-side description.
- [`README.md` §"Engine-by-engine surface coverage"](../../README.md#engine-by-engine-surface-coverage) — the engine-by-engine parity snapshot.
