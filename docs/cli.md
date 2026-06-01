# `magnetar` CLI

> **Status: pre-alpha (M9).** `admin` subcommands fully wired; `produce` / `consume` are stubs until M2+M3 land in the façade.

`magnetar` — the command-line client for Apache Pulsar built on the magnetar workspace.
This page is the canonical CLI reference; for the full subcommand surface, run `magnetar --help` (or `magnetar <subcommand> --help`).

## Install

```sh
cargo install --path crates/magnetar-cli
# or, from inside this workspace
cargo build -p magnetar-cli --release
./target/release/magnetar --help
```

## Global flags

```text
--service-url <url>          Pulsar service URL (data-plane).      [env MAGNETAR_SERVICE_URL]
                             default: pulsar://localhost:6650
--admin-url <url>            Pulsar admin REST URL.                [env MAGNETAR_ADMIN_URL]
                             default: http://localhost:8080
--token <token>              Bearer token for admin auth.          [env MAGNETAR_TOKEN]
--admin-timeout-secs <n>     Admin request timeout (seconds).      default: 60
-v, --verbose                Increase logging verbosity.
```

## Quickstart against a local broker

```sh
docker run --rm -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.0.0 \
  bin/pulsar standalone

magnetar admin tenants list
magnetar admin namespaces create public/scratch
magnetar admin topics create public/scratch/events --partitions 3
magnetar admin topics stats public/scratch/events | jq '.msgInCounter'
```

## `--version` / `-V`

The CLI exposes two forms, modeled on `sozu` and `systemd`:

- **`-V`** prints a single-line, never-colorized identification banner:

  ```
  magnetar 0.1.0-dev.0 (a1b2c3d4e5f6-dirty)
  ```

The parenthesized token is the 12-character git short SHA the binary was built from.
The `-dirty` suffix appears when the working tree had uncommitted changes at build time.
Outside a git checkout (e.g. released tarballs) the SHA is `unknown` and the dirty marker is omitted.

- **`--version`** prints a multi-line build-metadata banner:

  ```
  magnetar 0.1.0-dev.0 (a1b2c3d4e5f6-dirty)
  built 2026-05-26T14:32:11Z · profile=release · rustc=rustc 1.88.0 (…) · target=x86_64-unknown-linux-gnu
  features: +default
  pulsar wire protocol: v21
  os: linux · report bugs at https://github.com/CleverCloud/magnetar
  ```

The lines are intentionally machine-greppable (`rustc=`, `profile=`, `target=`, `features:`) so CI pipelines can pluck the value they need with `grep -oE 'profile=[^ ]+'` and similar.

### Color policy

The long banner is colorized when **both** conditions hold:

1. The `NO_COLOR` environment variable is unset or empty (https://no-color.org).
2. Standard output is a terminal — tested via `IsTerminal::is_terminal` on `stdout`.
   Piping (`magnetar --version | tee …`) automatically suppresses color.

Palette (sozu/systemd convention):

- Program name + version: **bold**.
- Git SHA suffix and footer lines: **dim**.
- `+feature` tokens: green.
- `-feature` tokens: red.
- `pulsar wire protocol`: cyan.

The short form (`-V`) is never colorized.

### Build-time metadata source

The metadata is captured at compile time by `crates/magnetar-cli/build.rs` and exposed via `cargo:rustc-env=` to the binary:

| Variable                   | Source                                                             |
| -------------------------- | ------------------------------------------------------------------ |
| `MAGNETAR_BUILD_GIT_SHA`   | `git rev-parse --short=12 HEAD`, `unknown` outside a git checkout. |
| `MAGNETAR_BUILD_GIT_DIRTY` | `yes` if `git status --porcelain` is non-empty, else `no`.         |
| `MAGNETAR_BUILD_TIMESTAMP` | RFC-3339 UTC at build start. Honors `SOURCE_DATE_EPOCH`.           |
| `MAGNETAR_BUILD_PROFILE`   | Cargo's `PROFILE` env (`debug` / `release`).                       |
| `MAGNETAR_BUILD_TARGET`    | Cargo's `TARGET` env (target triple).                              |
| `MAGNETAR_BUILD_RUSTC`     | First line of `rustc --version`.                                   |
| `MAGNETAR_BUILD_FEATURES`  | Space-joined `+feat` tokens for enabled cargo features.            |

### Reproducible builds

Set `SOURCE_DATE_EPOCH=<unix-seconds>` before invoking `cargo build` to pin `MAGNETAR_BUILD_TIMESTAMP` to a deterministic value.
Combined with a clean working tree (so `git_dirty=no`) and `--locked`, two builds at the same revision produce identical banners.

### Pulsar wire-protocol version

The `pulsar wire protocol: v21` line reflects the value the driver advertises in `CommandConnect.protocol_version`.
Both the driver and the CLI banner read from `magnetar_proto::SUPPORTED_PROTOCOL_VERSION`, so they cannot drift.

## Subcommands

`magnetar --help` lists the full set.
Documented surfaces that need more context than the help text provides:

### `admin <verb>`

Control-plane operations against `/admin/v2/...`.
Wraps [`magnetar_admin::AdminClient`](../crates/magnetar-admin/src/lib.rs).
Output is JSON to stdout; errors go to stderr with a non-zero exit code, so the output is pipeable into `jq`:

```sh
magnetar admin tenants list | jq '.[]'
```

The full verb set:

```sh
magnetar admin clusters list
magnetar admin tenants list
magnetar admin tenants create acme --admin-role alice --admin-role bob --cluster standalone
magnetar admin tenants delete acme

magnetar admin namespaces list acme
magnetar admin namespaces create acme/svc
magnetar admin namespaces delete acme/svc

magnetar admin topics list acme/svc
magnetar admin topics create acme/svc/orders --partitions 4
magnetar admin topics delete acme/svc/orders --force
magnetar admin topics stats acme/svc/orders
```

### `produce` / `consume` (M9 stubs)

```sh
magnetar produce persistent://public/default/x --message hi
magnetar consume persistent://public/default/x --subscription s --count 5
```

These print `not yet wired (M9)` and exit 0 today.
They get implemented once the `Connection` state machine and the tokio engine are integrated into the `magnetar` façade.

### `shadow <verb>` (PIP-180)

PIP-180 shadow-topic admin.
See [`pip-features.md#shadow-topics-pip-180`](pip-features.md#shadow-topics-pip-180) for concepts + caveats.

| Command                                                            | Effect                                                                                                                                           |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `magnetar admin topics shadow create <source> <shadow> [--property key=value]…` | `PUT /admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics` — create a shadow topic on top of a source topic.                          |
| `magnetar admin topics shadow delete <shadow> [--force]`                        | `DELETE /admin/v2/persistent/{tenant}/{namespace}/{shadow}` — remove a shadow topic. `--force` kicks off connected subscribers.                  |
| `magnetar admin topics shadow list <source>`                                    | `GET /admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics` — list the shadows of a source topic.                                      |
| `magnetar admin topics shadow source <shadow>`                                  | `GET /admin/v2/persistent/{tenant}/{namespace}/{shadow}/shadowSource` — resolve a shadow's source topic (returns `null` for a non-shadow topic). |

All four commands share the global `--admin-url` / `--token` / `--admin-timeout-secs` flags with the `admin` subcommand and stream JSON output to stdout.
