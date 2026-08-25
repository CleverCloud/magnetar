# magnetarctl

> **Status: stable (1.7.0).** The `produce`, `consume`, and `admin` subcommands are fully wired.

`magnetarctl` — the command-line client for Apache Pulsar built on the magnetar workspace.

See [`docs/cli.md`](../../docs/cli.md) for the canonical reference (install, global flags, subcommands, `--version` semantics, color policy, reproducible builds, quickstart).

## Tests

```sh
cargo test -p magnetarctl
```

The CLI test suite exercises clap parsing for every subcommand; it does not need a broker.
