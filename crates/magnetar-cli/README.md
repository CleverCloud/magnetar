# magnetar-cli

> **Status: pre-alpha (M9).** `admin` subcommands fully wired; `produce` / `consume` are stubs until M2+M3 land in the façade.

`magnetar` — the command-line client for Apache Pulsar built on the magnetar workspace.

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

## Admin subcommands

```sh
magnetar admin cluster-list
magnetar admin tenant-list
magnetar admin tenant-create acme --admin-role alice --admin-role bob --cluster standalone
magnetar admin tenant-delete acme

magnetar admin namespace-list acme
magnetar admin namespace-create acme/svc
magnetar admin namespace-delete acme/svc

magnetar admin topic-list acme/svc
magnetar admin topic-create acme/svc/orders --partitions 4
magnetar admin topic-delete acme/svc/orders --force
magnetar admin topic-stats acme/svc/orders
```

JSON results go to **stdout**, errors and logs to **stderr**, so the output is pipeable into `jq`:

```sh
magnetar admin tenant-list | jq '.[]'
```

## Data-plane subcommands (M9 stubs)

```sh
magnetar produce persistent://public/default/x --message hi
magnetar consume persistent://public/default/x --subscription s --count 5
```

These print `not yet wired (M9)` and exit 0 today. They get implemented once the [`Connection`](https://github.com/FlorentinDUBOIS/magnetar) state machine and the tokio engine are integrated into the `magnetar` façade.

## Quickstart against a local broker

```sh
docker run --rm -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:4.0.0 \
  bin/pulsar standalone

magnetar admin tenant-list
magnetar admin namespace-create public/scratch
magnetar admin topic-create public/scratch/events --partitions 3
magnetar admin topic-stats public/scratch/events | jq '.msgInCounter'
```

## Tests

```sh
cargo test -p magnetar-cli
```

The CLI test suite exercises clap parsing for every subcommand; it does not need a broker.
