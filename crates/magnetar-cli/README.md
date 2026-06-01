# magnetar-cli

> **Status: pre-alpha (M9).** `admin` subcommands fully wired; `produce` / `consume` are stubs until M2+M3 land in the façade.

`magnetar` — the command-line client for Apache Pulsar built on the magnetar workspace.

See [`docs/cli.md`](../../docs/cli.md) for the canonical reference (install, global flags, subcommands, `--version` semantics, color policy, reproducible builds, quickstart).

## Tests

```sh
cargo test -p magnetar-cli
```

The CLI test suite exercises clap parsing for every subcommand; it does not need a broker.
