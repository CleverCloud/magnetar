# magnetar-cli

> **Status: stable (1.0.0).** `admin` subcommands are fully wired; `produce` / `consume` are not yet implemented and are excluded from the 1.0 stability guarantee.

`magnetar` — the command-line client for Apache Pulsar built on the magnetar workspace.

See [`docs/cli.md`](../../docs/cli.md) for the canonical reference (install, global flags, subcommands, `--version` semantics, color policy, reproducible builds, quickstart).

## Tests

```sh
cargo test -p magnetar-cli
```

The CLI test suite exercises clap parsing for every subcommand; it does not need a broker.
