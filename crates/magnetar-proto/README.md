# magnetar-proto

Sans-io Apache Pulsar wire protocol — encode/decode, framing, state machines.

**Zero I/O dependencies. Zero channels.**

This crate is the protocol heart of the magnetar workspace. It pairs with
runtime crates (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`) which
provide the byte transport.

See the workspace [README](../../README.md) and [ARCHITECTURE.md](../../ARCHITECTURE.md)
for the bigger picture.
