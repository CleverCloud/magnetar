// SPDX-License-Identifier: Apache-2.0

//! Clap parsing tests for the `magnetar` CLI.
//!
//! These tests exercise the *shape* of parsed CLI arguments. They do not run
//! the command (which would talk to a broker); they assert the structured
//! representation matches what the broker code below expects.

use clap::Parser;

// The CLI types live in `src/main.rs`. We pull them in via a tiny `mod` re-
// export driven by `path` so the integration test sees the real public-ish
// surface without exposing it as a library crate.
#[path = "../src/main.rs"]
#[allow(dead_code, unused_imports)]
mod cli;

use cli::{
    AdminCmd, Cli, ClustersCmd, Cmd, NamespacesCmd, ShadowCmd, SinksCmd, SourcesCmd,
    SubscriptionsCmd, TenantsCmd, TopicsCmd,
};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("parse")
}

#[test]
fn produce_minimal() {
    let cli = parse(&["magnetar", "produce", "persistent://public/default/x"]);
    match cli.cmd {
        Cmd::Produce { topic, message, .. } => {
            assert_eq!(topic, "persistent://public/default/x");
            assert!(message.is_none());
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn produce_with_message() {
    let cli = parse(&[
        "magnetar",
        "produce",
        "persistent://public/default/x",
        "--message",
        "hello",
    ]);
    match cli.cmd {
        Cmd::Produce {
            message: Some(m), ..
        } => assert_eq!(m, "hello"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn consume_with_defaults() {
    let cli = parse(&[
        "magnetar",
        "consume",
        "persistent://public/default/x",
        "--subscription",
        "s",
    ]);
    match cli.cmd {
        Cmd::Consume {
            topic,
            subscription,
            count,
            ..
        } => {
            assert_eq!(topic, "persistent://public/default/x");
            assert_eq!(subscription, "s");
            assert_eq!(count, 1);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn consume_with_count() {
    let cli = parse(&[
        "magnetar",
        "consume",
        "t",
        "--subscription",
        "s",
        "--count",
        "10",
    ]);
    match cli.cmd {
        Cmd::Consume { count, .. } => assert_eq!(count, 10),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_clusters_list() {
    let cli = parse(&["magnetar", "admin", "clusters", "list"]);
    assert!(matches!(
        cli.cmd,
        Cmd::Admin {
            sub: AdminCmd::Clusters {
                sub: ClustersCmd::List
            }
        }
    ));
}

#[test]
fn admin_tenants_list() {
    let cli = parse(&["magnetar", "admin", "tenants", "list"]);
    assert!(matches!(
        cli.cmd,
        Cmd::Admin {
            sub: AdminCmd::Tenants {
                sub: TenantsCmd::List
            }
        }
    ));
}

#[test]
fn admin_tenants_create_multi_role_multi_cluster() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "tenants",
        "create",
        "acme",
        "--admin-role",
        "alice",
        "--admin-role",
        "bob",
        "--cluster",
        "us-east",
        "--cluster",
        "us-west",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Tenants {
                    sub:
                        TenantsCmd::Create {
                            name,
                            admin_role,
                            cluster,
                        },
                },
        } => {
            assert_eq!(name, "acme");
            assert_eq!(admin_role, vec!["alice".to_owned(), "bob".to_owned()]);
            assert_eq!(cluster, vec!["us-east".to_owned(), "us-west".to_owned()]);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_tenants_delete() {
    let cli = parse(&["magnetar", "admin", "tenants", "delete", "acme"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::Tenants {
                sub: TenantsCmd::Delete { name },
            },
        } => assert_eq!(name, "acme"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_namespaces_list() {
    let cli = parse(&["magnetar", "admin", "namespaces", "list", "acme"]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Namespaces {
                    sub: NamespacesCmd::List { tenant },
                },
        } => assert_eq!(tenant, "acme"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_namespaces_create_and_delete() {
    let create = parse(&["magnetar", "admin", "namespaces", "create", "acme/svc"]);
    match create.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Namespaces {
                    sub: NamespacesCmd::Create { namespace },
                },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
    let del = parse(&["magnetar", "admin", "namespaces", "delete", "acme/svc"]);
    match del.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Namespaces {
                    sub: NamespacesCmd::Delete { namespace },
                },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_list() {
    let cli = parse(&["magnetar", "admin", "topics", "list", "acme/svc"]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub: TopicsCmd::List { namespace },
                },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_create_with_partitions() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topics",
        "create",
        "acme/svc/orders",
        "--partitions",
        "4",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub: TopicsCmd::Create { topic, partitions },
                },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert_eq!(partitions, 4);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_delete_default_no_force() {
    let cli = parse(&["magnetar", "admin", "topics", "delete", "acme/svc/orders"]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub: TopicsCmd::Delete { topic, force },
                },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert!(!force);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_delete_force() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topics",
        "delete",
        "acme/svc/orders",
        "--force",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub: TopicsCmd::Delete { force, .. },
                },
        } => assert!(force),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_stats() {
    let cli = parse(&["magnetar", "admin", "topics", "stats", "acme/svc/orders"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::Topics {
                sub: TopicsCmd::Stats { topic },
            },
        } => assert_eq!(topic, "acme/svc/orders"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_shadow_create() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topics",
        "shadow",
        "create",
        "acme/svc/source",
        "persistent://acme/svc/shadow",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub:
                        TopicsCmd::Shadow {
                            sub: ShadowCmd::Create { source, shadow },
                        },
                },
        } => {
            assert_eq!(source, "acme/svc/source");
            assert_eq!(shadow, "persistent://acme/svc/shadow");
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topics_shadow_list() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topics",
        "shadow",
        "list",
        "acme/svc/source",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Topics {
                    sub:
                        TopicsCmd::Shadow {
                            sub: ShadowCmd::List { source },
                        },
                },
        } => assert_eq!(source, "acme/svc/source"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_subscriptions_list() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "subscriptions",
        "list",
        "acme/svc/orders",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Subscriptions {
                    sub: SubscriptionsCmd::List { topic },
                },
        } => assert_eq!(topic, "acme/svc/orders"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_subscriptions_skip_with_count() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "subscriptions",
        "skip",
        "acme/svc/orders",
        "s-a",
        "--count",
        "50",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Subscriptions {
                    sub:
                        SubscriptionsCmd::Skip {
                            topic,
                            subscription,
                            count,
                        },
                },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert_eq!(subscription, "s-a");
            assert_eq!(count, 50);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_subscriptions_reset_cursor_message_id_full_form() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "subscriptions",
        "reset-cursor",
        "acme/svc/orders",
        "s-a",
        "--message-id",
        "17:42:0:-1",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Subscriptions {
                    sub:
                        SubscriptionsCmd::ResetCursor {
                            topic,
                            subscription,
                            message_id,
                            is_excluded,
                        },
                },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert_eq!(subscription, "s-a");
            assert_eq!(message_id.ledger_id, 17);
            assert_eq!(message_id.entry_id, 42);
            assert_eq!(message_id.partition, 0);
            assert_eq!(message_id.batch_index, -1);
            assert!(!is_excluded);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_subscriptions_reset_cursor_message_id_short_form_defaults_to_neg_one() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "subscriptions",
        "reset-cursor",
        "acme/svc/orders",
        "s-a",
        "--message-id",
        "5:9",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Subscriptions {
                    sub: SubscriptionsCmd::ResetCursor { message_id, .. },
                },
        } => {
            assert_eq!(message_id.ledger_id, 5);
            assert_eq!(message_id.entry_id, 9);
            assert_eq!(message_id.partition, -1);
            assert_eq!(message_id.batch_index, -1);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_subscriptions_delete_force() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "subscriptions",
        "delete",
        "acme/svc/orders",
        "s-a",
        "--force",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Subscriptions {
                    sub: SubscriptionsCmd::Delete { force, .. },
                },
        } => assert!(force),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_flags_are_globals() {
    let cli = parse(&[
        "magnetar",
        "--admin-url",
        "http://broker:8080",
        "--token",
        "secret",
        "admin",
        "tenants",
        "list",
    ]);
    assert_eq!(cli.admin_url, "http://broker:8080");
    assert_eq!(cli.token.as_deref(), Some("secret"));
}

#[test]
fn admin_timeout_default_is_60() {
    let cli = parse(&["magnetar", "admin", "tenants", "list"]);
    assert_eq!(cli.admin_timeout_secs, 60);
}

#[test]
fn admin_timeout_override() {
    let cli = parse(&[
        "magnetar",
        "--admin-timeout-secs",
        "5",
        "admin",
        "tenants",
        "list",
    ]);
    assert_eq!(cli.admin_timeout_secs, 5);
}

#[test]
fn verbose_repetition() {
    let cli = parse(&["magnetar", "-vvv", "admin", "tenants", "list"]);
    assert_eq!(cli.verbose, 3);
}

#[test]
fn root_flags_accepted_after_subcommand() {
    // clap `global = true` lets each root flag be parsed at any level —
    // confirm `--admin-url` / `--token` / `--admin-timeout-secs` / `-v`
    // all flow through when placed after the subcommand chain.
    let cli = parse(&[
        "magnetar",
        "admin",
        "tenants",
        "list",
        "--admin-url",
        "http://broker:8080",
        "--token",
        "secret",
        "--admin-timeout-secs",
        "5",
        "-vv",
    ]);
    assert_eq!(cli.admin_url, "http://broker:8080");
    assert_eq!(cli.token.as_deref(), Some("secret"));
    assert_eq!(cli.admin_timeout_secs, 5);
    assert_eq!(cli.verbose, 2);
    assert!(matches!(
        cli.cmd,
        Cmd::Admin {
            sub: AdminCmd::Tenants {
                sub: TenantsCmd::List
            }
        }
    ));
}

#[test]
fn admin_sources_list_takes_namespace_positional() {
    let cli = parse(&["magnetar", "admin", "sources", "list", "acme/svc"]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Sources {
                    sub: SourcesCmd::List { namespace },
                },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_sources_create_with_url_collects_all_flags() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "sources",
        "create-with-url",
        "--tenant",
        "acme",
        "--namespace",
        "svc",
        "--name",
        "kafka-in",
        "--url",
        "https://repo.example/kafka.nar",
        "--class-name",
        "org.apache.pulsar.io.kafka.KafkaSource",
        "--topic-name",
        "persistent://acme/svc/ingest",
        "--parallelism",
        "3",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Sources {
                    sub:
                        SourcesCmd::CreateWithUrl {
                            tenant,
                            namespace,
                            name,
                            url,
                            class_name,
                            topic_name,
                            parallelism,
                        },
                },
        } => {
            assert_eq!(tenant, "acme");
            assert_eq!(namespace, "svc");
            assert_eq!(name, "kafka-in");
            assert_eq!(url, "https://repo.example/kafka.nar");
            assert_eq!(class_name, "org.apache.pulsar.io.kafka.KafkaSource");
            assert_eq!(topic_name, "persistent://acme/svc/ingest");
            assert_eq!(parallelism, 3);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_sources_restart_takes_positional_id() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "sources",
        "restart",
        "acme/svc/kafka-in",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Sources {
                    sub: SourcesCmd::Restart { source },
                },
        } => assert_eq!(source, "acme/svc/kafka-in"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_sinks_create_with_url_repeats_input_flag() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "sinks",
        "create-with-url",
        "--tenant",
        "acme",
        "--namespace",
        "svc",
        "--name",
        "jdbc-out",
        "--url",
        "https://repo.example/jdbc.nar",
        "--class-name",
        "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink",
        "--input",
        "persistent://acme/svc/orders",
        "--input",
        "persistent://acme/svc/refunds",
        "--parallelism",
        "2",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub:
                AdminCmd::Sinks {
                    sub:
                        SinksCmd::CreateWithUrl {
                            tenant,
                            namespace,
                            name,
                            url,
                            class_name,
                            inputs,
                            parallelism,
                        },
                },
        } => {
            assert_eq!(tenant, "acme");
            assert_eq!(namespace, "svc");
            assert_eq!(name, "jdbc-out");
            assert_eq!(url, "https://repo.example/jdbc.nar");
            assert_eq!(
                class_name,
                "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink"
            );
            assert_eq!(
                inputs,
                vec![
                    "persistent://acme/svc/orders".to_owned(),
                    "persistent://acme/svc/refunds".to_owned(),
                ]
            );
            assert_eq!(parallelism, 2);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_sinks_status_takes_positional_id() {
    let cli = parse(&["magnetar", "admin", "sinks", "status", "acme/svc/jdbc-out"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::Sinks {
                sub: SinksCmd::Status { sink },
            },
        } => assert_eq!(sink, "acme/svc/jdbc-out"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn produce_accepts_global_service_url_after_topic() {
    let cli = parse(&[
        "magnetar",
        "produce",
        "persistent://public/default/x",
        "--service-url",
        "pulsar://broker:6650",
    ]);
    assert_eq!(cli.service_url, "pulsar://broker:6650");
    assert!(matches!(cli.cmd, Cmd::Produce { .. }));
}
