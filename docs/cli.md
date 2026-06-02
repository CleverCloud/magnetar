# `magnetar` CLI

> **Status: pre-alpha (M9).** Full `admin` surface wired across V2 + V3 — clusters, tenants, namespaces, topics (+ policies + shadow + PIP-415), subscriptions, brokers (+ dynamic config), bookies, schemas, and the V3 Functions / IO Sources / IO Sinks / Packages families.
> `produce` / `consume` remain stubs until M2 + M3 land in the façade.

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
                             -v     magnetar=debug
                             -vv    magnetar=trace
                             -vvv   + reqwest=debug
                             -vvvv  + hyper=debug,rustls=debug,h2=debug
                             -vvvvv + all four at trace
```

All flags are global — `magnetar admin -vv tenants list` is equivalent to `magnetar -vv admin tenants list`, and either form works.
The `MAGNETAR_*` environment variables seed the same flags so CI pipelines and shell aliases don't have to repeat them.

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

`magnetar --help` (and `magnetar <subcommand> --help` recursively) lists the full set.
This page is the canonical reference; tables below cite every admin verb's REST endpoint so an operator can match the CLI call against the broker-side audit log.

The admin surface is grouped kubectl-style: `magnetar admin <resource> <verb>`.
Output is JSON on stdout (pipeable into `jq`); errors are written to stderr with a non-zero exit code.
Every admin call shares the global `--admin-url` / `--token` / `--admin-timeout-secs` / `-v` flags.

The V2 verbs (`clusters` / `tenants` / `namespaces` / `topics` / `subscriptions` / `brokers` / `bookies` / `schemas`) hit `/admin/v2/...`; the V3 verbs (`functions` / `sources` / `sinks` / `packages`) hit `/admin/v3/...`.
Pulsar's own routing makes the split — `magnetar-admin` keeps two pre-computed base URLs internally so a caller never has to know which family an endpoint belongs to.

### `admin clusters`

| Command                                                       | REST endpoint                                                                  |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `admin clusters list`                                         | `GET /admin/v2/clusters`                                                       |
| `admin clusters list-failure-domains <cluster>`               | `GET /admin/v2/clusters/{cluster}/failureDomains`                              |
| `admin clusters get-failure-domain <cluster> <domain>`        | `GET /admin/v2/clusters/{cluster}/failureDomains/{domain}`                     |
| `admin clusters list-namespace-isolation-policies <cluster>`  | `GET /admin/v2/clusters/{cluster}/namespaceIsolationPolicies`                  |

### `admin tenants`

| Command                                                                            | REST endpoint                  |
| ---------------------------------------------------------------------------------- | ------------------------------ |
| `admin tenants list`                                                               | `GET /admin/v2/tenants`        |
| `admin tenants create <name> --admin-role <r>… --cluster <c>…`                     | `PUT /admin/v2/tenants/{name}` |
| `admin tenants delete <name>`                                                      | `DELETE /admin/v2/tenants/{name}` |

### `admin namespaces`

CRUD plus the full per-namespace policy surface.
Each policy follows a `get-<policy>` / `set-<policy>` / `remove-<policy>` triplet against `/admin/v2/namespaces/{tenant}/{ns}/<suffix>`.

**CRUD**

| Command                                                | REST endpoint                                          |
| ------------------------------------------------------ | ------------------------------------------------------ |
| `admin namespaces list <tenant>`                       | `GET /admin/v2/namespaces/{tenant}`                    |
| `admin namespaces create <tenant>/<ns>`                | `PUT /admin/v2/namespaces/{tenant}/{ns}`               |
| `admin namespaces delete <tenant>/<ns>`                | `DELETE /admin/v2/namespaces/{tenant}/{ns}`            |

**Retention / backlog / TTL**

| Command                                                                                                                              | REST endpoint                                       |
| ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `admin namespaces get-retention <ns>` / `set-retention <ns> --time-minutes N --size-mb M` / `remove-retention <ns>`                  | `/admin/v2/namespaces/{tenant}/{ns}/retention`      |
| `admin namespaces get-backlog-quotas <ns>` / `set-backlog-quota <ns> --type {destination-storage\|message-age} --limit-size N --limit-time T --policy {producer_request_hold\|producer_exception\|consumer_backlog_eviction}` / `remove-backlog-quota <ns> --type T` | `/admin/v2/namespaces/{tenant}/{ns}/backlogQuotaMap` (GET) and `…/backlogQuota?backlogQuotaType=…` (POST / DELETE) |
| `admin namespaces get-message-ttl <ns>` / `set-message-ttl <ns> --ttl-seconds N` / `remove-message-ttl <ns>`                         | `/admin/v2/namespaces/{tenant}/{ns}/messageTTL`     |

**Persistence + rate policies**

| Command                                                                                                                                                                       | REST suffix                          |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `admin namespaces get-persistence` / `set-persistence --ensemble N --write-quorum N --ack-quorum N --max-mark-delete-rate F` / `remove-persistence`                            | `…/persistence`                      |
| `admin namespaces get-dispatch-rate` / `set-dispatch-rate --rate-in-msg N --rate-in-byte N --period-seconds N [--relative-to-publish-rate]` / `remove-dispatch-rate`           | `…/dispatchRate`                     |
| `admin namespaces get-subscription-dispatch-rate` / `set-subscription-dispatch-rate …` / `remove-subscription-dispatch-rate`                                                  | `…/subscriptionDispatchRate`         |
| `admin namespaces get-replicator-dispatch-rate` / `set-replicator-dispatch-rate …` / `remove-replicator-dispatch-rate`                                                        | `…/replicatorDispatchRate`           |
| `admin namespaces get-publish-rate` / `set-publish-rate --rate-in-msg N --rate-in-byte N` / `remove-publish-rate`                                                              | `…/publishRate`                      |

**Limits + dedup + delayed delivery**

| Command                                                                                                            | REST suffix                                    |
| ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------- |
| `admin namespaces get-deduplication` / `set-deduplication --enabled` / `remove-deduplication`                      | `…/deduplication`                              |
| `admin namespaces get-deduplication-snapshot-interval` / `set-deduplication-snapshot-interval --interval N` / `remove-deduplication-snapshot-interval` | `…/deduplicationSnapshotInterval`              |
| `admin namespaces get-compaction-threshold` / `set-compaction-threshold --threshold-bytes N` / `remove-compaction-threshold` | `…/compactionThreshold`                        |
| `admin namespaces get-delayed-delivery` / `set-delayed-delivery --active --tick-time-millis N` / `remove-delayed-delivery` | `…/delayedDelivery`                            |
| `admin namespaces get-max-producers-per-topic` / `set-max-producers-per-topic --max N` / `remove-max-producers-per-topic` | `…/maxProducersPerTopic`                       |
| `admin namespaces get-max-consumers-per-topic` / `set-max-consumers-per-topic --max N` / `remove-max-consumers-per-topic` | `…/maxConsumersPerTopic`                       |
| `admin namespaces get-max-unacked-messages-per-consumer` / `set-max-unacked-messages-per-consumer --max N` / `remove-max-unacked-messages-per-consumer` | `…/maxUnackedMessagesPerConsumer`              |
| `admin namespaces get-max-unacked-messages-per-subscription` / `set-max-unacked-messages-per-subscription --max N` / `remove-max-unacked-messages-per-subscription` | `…/maxUnackedMessagesPerSubscription`          |

### `admin topics`

CRUD + stats + operational verbs + per-topic policy overrides + the PIP-180 shadow surface + the PIP-415 message-id-by-index lookup.

**CRUD + stats**

| Command                                                                                                                          | REST endpoint                                                                                                                 |
| -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `admin topics list <tenant>/<ns>`                                                                                                | `GET /admin/v2/persistent/{tenant}/{ns}`                                                                                       |
| `admin topics create <topic> --partitions N`                                                                                     | `PUT /admin/v2/persistent/{tenant}/{ns}/{topic}/partitions`                                                                    |
| `admin topics delete <topic> [--force]`                                                                                          | `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/partitions?force=…`                                                         |
| `admin topics stats <topic>`                                                                                                     | `GET .../partitions` (probe) → `…/stats` or `…/partitioned-stats` per partition count.                                         |

**Operational verbs**

| Command                                                                  | REST endpoint                                              |
| ------------------------------------------------------------------------ | ---------------------------------------------------------- |
| `admin topics compact <topic>`                                           | `PUT .../compaction`                                       |
| `admin topics compaction-status <topic>`                                 | `GET .../compaction` — returns `LongRunningProcessStatus`. |
| `admin topics unload <topic>`                                            | `PUT .../unload`                                           |
| `admin topics terminate <topic>`                                         | `POST .../terminate` — returns the last `MessageId`.       |
| `admin topics update-partitions <topic> --partitions N`                  | `POST .../partitions` (forward grow only; broker 409s on shrink). |
| `admin topics get-message-id-by-index <topic> --index N`                 | `GET .../getMessageIdByIndex?index=N` (PIP-415).            |

**Per-topic policy overrides**

Same policy taxonomy as `admin namespaces` but at `/admin/v2/persistent/{tenant}/{ns}/{topic}/<suffix>`.
A topic-level policy overrides the namespace default for that one topic.

| Command                                                                                                                                                  | REST suffix                       |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `get-retention` / `set-retention --time-minutes N --size-mb M` / `remove-retention`                                                                       | `…/retention`                     |
| `get-backlog-quotas` / `set-backlog-quota --type … --limit-size … --limit-time … --policy …` / `remove-backlog-quota --type …`                            | `…/backlogQuota`                  |
| `get-message-ttl` / `set-message-ttl --ttl-seconds N` / `remove-message-ttl`                                                                              | `…/messageTTL`                    |
| `get-persistence` / `set-persistence …` / `remove-persistence`                                                                                            | `…/persistence`                   |
| `get-dispatch-rate` / `set-dispatch-rate …` / `remove-dispatch-rate`                                                                                      | `…/dispatchRate`                  |
| `get-subscription-dispatch-rate` / `set-subscription-dispatch-rate …` / `remove-subscription-dispatch-rate`                                              | `…/subscriptionDispatchRate`      |
| `get-replicator-dispatch-rate` / `set-replicator-dispatch-rate …` / `remove-replicator-dispatch-rate`                                                    | `…/replicatorDispatchRate`        |
| `get-publish-rate` / `set-publish-rate …` / `remove-publish-rate`                                                                                         | `…/publishRate`                   |
| `get-max-producers` / `set-max-producers --max N` / `remove-max-producers`                                                                                | `…/maxProducers`                  |
| `get-max-consumers` / `set-max-consumers --max N` / `remove-max-consumers`                                                                                | `…/maxConsumers`                  |

A topic-level GET returns `null` when no override is set — the CLI decodes that as JSON `null` so a downstream script can branch on it.

**Shadow topics (PIP-180)**

PIP-180 lets a "shadow" topic share its ledger storage with a "source" topic and expose a read-only view to consumers — a lightweight fan-out alternative to geo-replication.
See [`docs/shadow-topic.md`](shadow-topic.md) and [`docs/pip-features.md#shadow-topics-pip-180`](pip-features.md#shadow-topics-pip-180) for concepts + caveats.

| Command                                                              | REST endpoint                                                                                                                                    |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `admin topics shadow create <source> <shadow>`                       | `PUT /admin/v2/persistent/{tenant}/{ns}/{source}/shadowTopics` — body is a bare JSON array `[<shadow>]`.                                          |
| `admin topics shadow delete <shadow> [--force]`                      | `DELETE /admin/v2/persistent/{tenant}/{ns}/{shadow}?force=…` — `--force` kicks off connected subscribers.                                         |
| `admin topics shadow list <source>`                                  | `GET /admin/v2/persistent/{tenant}/{ns}/{source}/shadowTopics`                                                                                    |
| `admin topics shadow source <shadow>`                                | `GET /admin/v2/persistent/{tenant}/{ns}/{shadow}/shadowSource` — returns `null` for a non-shadow topic.                                           |

### `admin subscriptions`

Operator-facing subscription management.

| Command                                                                                                                                | REST endpoint                                                                                  |
| -------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `admin subscriptions list <topic>`                                                                                                     | `GET .../{topic}/subscriptions`                                                                |
| `admin subscriptions reset-cursor <topic> <sub> --message-id LEDGER:ENTRY[:PARTITION[:BATCH]] [--is-excluded]`                         | `POST .../{topic}/subscription/{sub}/resetcursor` with `ResetCursorData` body.                 |
| `admin subscriptions reset-cursor-by-timestamp <topic> <sub> --timestamp-millis N`                                                     | `POST .../{topic}/subscription/{sub}/resetcursor/{timestamp}`                                  |
| `admin subscriptions skip <topic> <sub> --count N`                                                                                     | `POST .../{topic}/subscription/{sub}/skip/{count}`                                             |
| `admin subscriptions skip-all <topic> <sub>`                                                                                           | `POST .../{topic}/subscription/{sub}/skip_all` — broker endpoint is snake-case, not kebab.     |
| `admin subscriptions expire <topic> <sub> --expire-time-seconds N`                                                                     | `POST .../{topic}/subscription/{sub}/expireMessages/{seconds}`                                 |
| `admin subscriptions delete <topic> <sub> [--force]`                                                                                   | `DELETE .../{topic}/subscription/{sub}?force=…` — `--force` disconnects active consumers.      |

The `LEDGER:ENTRY[:PARTITION[:BATCH]]` message-id format mirrors pulsarctl: partition and batch default to `-1` (non-partitioned, non-batched) when omitted.

### `admin brokers`

Broker diagnostics + dynamic configuration.

**Diagnostics**

| Command                                                                       | REST endpoint                                              |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `admin brokers list <cluster>`                                                | `GET /admin/v2/brokers/{cluster}`                          |
| `admin brokers leader`                                                        | `GET /admin/v2/brokers/leaderBroker`                       |
| `admin brokers health-check`                                                  | `GET /admin/v2/brokers/health` — returns `"ok"` plain text. |
| `admin brokers owned-namespaces <cluster> <broker>`                           | `GET /admin/v2/brokers/{cluster}/{broker}/ownedNamespaces` |
| `admin brokers runtime-config`                                                | `GET /admin/v2/brokers/configuration/runtime`              |
| `admin brokers internal-config`                                               | `GET /admin/v2/brokers/internal-configuration`             |

**Dynamic configuration**

| Command                                                                       | REST endpoint                                              |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `admin brokers dynamic-config-keys`                                           | `GET /admin/v2/brokers/configuration` — names of mutable knobs. |
| `admin brokers dynamic-config-overrides`                                      | `GET /admin/v2/brokers/configuration/values` — currently-set overrides. |
| `admin brokers set-dynamic-config --name K --value V`                         | `POST /admin/v2/brokers/configuration/{name}/{value}`       |
| `admin brokers delete-dynamic-config --name K`                                | `DELETE /admin/v2/brokers/configuration/{name}`             |

### `admin bookies`

| Command                                                                        | REST endpoint                                          |
| ------------------------------------------------------------------------------ | ------------------------------------------------------ |
| `admin bookies list`                                                           | `GET /admin/v2/bookies/all`                            |
| `admin bookies racks-info`                                                     | `GET /admin/v2/bookies/racks-info`                     |
| `admin bookies set-rack <bookie> --group G --rack R --hostname H`              | `POST /admin/v2/bookies/racks-info/{bookie}` with `BookieInfo` body. |
| `admin bookies delete-rack <bookie>`                                           | `DELETE /admin/v2/bookies/racks-info/{bookie}`         |

### `admin schemas`

| Command                                                                                                              | REST endpoint                                                  |
| -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `admin schemas get-latest <topic>`                                                                                   | `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema`           |
| `admin schemas get-version <topic> --version V`                                                                      | `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema/{version}` |
| `admin schemas list-versions <topic>`                                                                                | `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schemas`          |
| `admin schemas post <topic> --type {AVRO\|JSON\|PROTOBUF\|…} --schema <definition> [--property key=value]…`         | `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/schema` with `PostSchemaPayload` body. |
| `admin schemas delete <topic> [--force]`                                                                             | `DELETE /admin/v2/schemas/{tenant}/{ns}/{topic}/schema?force=…` |
| `admin schemas compatibility <topic> --type … --schema <definition>`                                                 | `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/compatibility`   |

The `--schema` argument carries the schema definition as a string — typically a JSON-stringified AVRO record for `--type AVRO`.

### `admin functions` (V3)

Pulsar Functions — serverless Java / Python / Go user code running inside the broker.
The CLI covers the URL-based register / update path (compiled package fetched by the broker from a URL).
Local-file multipart uploads (`@FormDataParam("data")`) are out of scope today.

| Command                                                                                                                                                                                                | REST endpoint                                                          |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `admin functions list <tenant>/<ns>`                                                                                                                                                                   | `GET /admin/v3/functions/{tenant}/{ns}`                                |
| `admin functions get <tenant>/<ns>/<name>`                                                                                                                                                             | `GET /admin/v3/functions/{tenant}/{ns}/{name}`                         |
| `admin functions status <tenant>/<ns>/<name> [--instance-id N]`                                                                                                                                        | `GET /admin/v3/functions/{tenant}/{ns}/{name}[/{instance_id}]/status`  |
| `admin functions stats <tenant>/<ns>/<name> [--instance-id N]`                                                                                                                                         | `GET /admin/v3/functions/{tenant}/{ns}/{name}[/{instance_id}]/stats`   |
| `admin functions create-with-url --tenant T --namespace N --name X --url U --class-name C --runtime {JAVA\|PYTHON\|GO} --input <topic>… [--output <topic>] [--parallelism N]`                          | `POST /admin/v3/functions/{tenant}/{ns}/{name}` (multipart `url` + `functionConfig`). |
| `admin functions update-with-url …`                                                                                                                                                                    | `PUT /admin/v3/functions/{tenant}/{ns}/{name}` (same multipart shape). |
| `admin functions delete <tenant>/<ns>/<name>`                                                                                                                                                          | `DELETE /admin/v3/functions/{tenant}/{ns}/{name}`                      |
| `admin functions start <tenant>/<ns>/<name> [--instance-id N]`                                                                                                                                         | `POST .../{name}[/{instance_id}]/start`                                |
| `admin functions stop <tenant>/<ns>/<name> [--instance-id N]`                                                                                                                                          | `POST .../{name}[/{instance_id}]/stop`                                 |
| `admin functions restart <tenant>/<ns>/<name>`                                                                                                                                                         | `POST .../{name}/restart`                                              |

The `--url` argument accepts any broker-resolvable scheme (`http(s)://`, `file://`, `function://`) — the broker fetches the package itself.
A `file://` URL is **read by the broker** from its local filesystem, not from the CLI host.

### `admin sources` / `admin sinks` (V3)

Pulsar IO connectors — Sources pull data **into** Pulsar from external systems, Sinks push topic data **out**.
Same verb taxonomy for both families.

| Command (substitute `sources` / `sinks` for `<x>`)                                                                                                                                       | REST endpoint                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `admin <x> list <tenant>/<ns>`                                                                                                                                                            | `GET /admin/v3/<x>/{tenant}/{ns}`                          |
| `admin <x> get <tenant>/<ns>/<name>`                                                                                                                                                      | `GET /admin/v3/<x>/{tenant}/{ns}/{name}`                   |
| `admin <x> status <tenant>/<ns>/<name>`                                                                                                                                                   | `GET /admin/v3/<x>/{tenant}/{ns}/{name}/status`            |
| `admin sources create-with-url --tenant T --namespace N --name X --url U --class-name C --topic-name T [--parallelism N]`                                                                  | `POST /admin/v3/sources/{tenant}/{ns}/{name}` (multipart). |
| `admin sinks create-with-url --tenant T --namespace N --name X --url U --class-name C --input <topic>… [--parallelism N]`                                                                  | `POST /admin/v3/sinks/{tenant}/{ns}/{name}` (multipart).   |
| `admin <x> update-with-url …`                                                                                                                                                             | `PUT .../{name}` (same multipart shape).                   |
| `admin <x> delete <tenant>/<ns>/<name>`                                                                                                                                                   | `DELETE .../{name}`                                        |
| `admin <x> start <tenant>/<ns>/<name>` / `stop` / `restart`                                                                                                                               | `POST .../{name}/{start\|stop\|restart}`                   |

### `admin packages` (V3)

Pulsar Packages — versioned binary registry for Functions / Sources / Sinks JARs.
The `<type>` argument selects which subregistry: `function`, `source`, or `sink`.

| Command                                                                                                                                                                          | REST endpoint                                                                |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `admin packages list <type> <tenant>/<ns>`                                                                                                                                       | `GET /admin/v3/packages/{type}/{tenant}/{ns}`                                |
| `admin packages versions <type> <tenant>/<ns>/<name>`                                                                                                                            | `GET /admin/v3/packages/{type}/{tenant}/{ns}/{name}`                         |
| `admin packages metadata-get <type> <tenant>/<ns>/<name> --version V`                                                                                                            | `GET /admin/v3/packages/{type}/{tenant}/{ns}/{name}/{version}/metadata`      |
| `admin packages metadata-set <type> <tenant>/<ns>/<name> --version V --description D --contact C [--property key=value]…`                                                       | `PUT /admin/v3/packages/{type}/{tenant}/{ns}/{name}/{version}/metadata`      |
| `admin packages delete <type> <tenant>/<ns>/<name> --version V`                                                                                                                  | `DELETE /admin/v3/packages/{type}/{tenant}/{ns}/{name}/{version}`            |

### `produce` / `consume` (M9 stubs)

```sh
magnetar produce persistent://public/default/x --message hi
magnetar consume persistent://public/default/x --subscription s --count 5
```

These print `not yet wired (M9)` and exit 0 today.
They get implemented once the `Connection` state machine and the tokio engine are integrated into the `magnetar` façade.

## Output format

Every admin verb streams a single JSON value (object, array, or primitive) to stdout — pipe it into `jq` for transformation:

```sh
magnetar admin clusters list | jq '.[]'
magnetar admin topics stats acme/svc/orders | jq '.msgInCounter'
magnetar admin functions list acme/svc | jq 'length'
```

Errors print `magnetar: <error>` to stderr followed by indented `caused by:` lines walking the full source chain (so a TLS handshake failure surfaces all the way down to the OS-level `Connection refused`, not just `error sending request for url …`).
The exit code is non-zero on any error.

## Error chain

`reqwest::Error`'s own `Display` only shows its top-level message; the CLI walks `err.source()` recursively so the operator sees the actual cause — a `hyper` connector error, a `rustls` handshake failure, a missing TLS backend, or a DNS resolution — without re-running under tcpdump.
The verbose ladder (`-v` … `-vvvvv`) escalates the tracing filter through `magnetar` → `reqwest` → `hyper` + `rustls` + `h2`, with the highest level putting all four at `trace`.

## Crypto provider

The binary defaults to `crypto-aws-lc-rs` (post-quantum hybrid X25519MLKEM768 KEX via rustls 0.23).
Single-provider builds pick an alternate explicitly:

```sh
cargo build -p magnetar-cli --no-default-features --features crypto-ring
cargo build -p magnetar-cli --no-default-features --features crypto-openssl   # needs system OpenSSL
cargo build -p magnetar-cli --no-default-features --features crypto-fips      # needs cmake + clang
```

A build with `--no-default-features` and no `crypto-*` selected fails fast with a `compile_error!` — the user-facing binary always needs TLS for both the admin REST client (reqwest + rustls) and the data-plane runtime (tokio-rustls).
See [TLS crypto provider](../README.md#tls-crypto-provider) and [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md).
