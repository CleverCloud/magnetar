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

use cli::{AdminCmd, Cli, Cmd};

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
fn admin_cluster_list() {
    let cli = parse(&["magnetar", "admin", "cluster-list"]);
    assert!(matches!(
        cli.cmd,
        Cmd::Admin {
            sub: AdminCmd::ClusterList
        }
    ));
}

#[test]
fn admin_tenant_list() {
    let cli = parse(&["magnetar", "admin", "tenant-list"]);
    assert!(matches!(
        cli.cmd,
        Cmd::Admin {
            sub: AdminCmd::TenantList
        }
    ));
}

#[test]
fn admin_tenant_create_multi_role_multi_cluster() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "tenant-create",
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
                AdminCmd::TenantCreate {
                    name,
                    admin_role,
                    cluster,
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
fn admin_tenant_delete() {
    let cli = parse(&["magnetar", "admin", "tenant-delete", "acme"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TenantDelete { name },
        } => assert_eq!(name, "acme"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_namespace_list() {
    let cli = parse(&["magnetar", "admin", "namespace-list", "acme"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::NamespaceList { tenant },
        } => assert_eq!(tenant, "acme"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_namespace_create_and_delete() {
    let create = parse(&["magnetar", "admin", "namespace-create", "acme/svc"]);
    match create.cmd {
        Cmd::Admin {
            sub: AdminCmd::NamespaceCreate { namespace },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
    let del = parse(&["magnetar", "admin", "namespace-delete", "acme/svc"]);
    match del.cmd {
        Cmd::Admin {
            sub: AdminCmd::NamespaceDelete { namespace },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topic_list() {
    let cli = parse(&["magnetar", "admin", "topic-list", "acme/svc"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TopicList { namespace },
        } => assert_eq!(namespace, "acme/svc"),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topic_create_with_partitions() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topic-create",
        "acme/svc/orders",
        "--partitions",
        "4",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TopicCreate { topic, partitions },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert_eq!(partitions, 4);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topic_delete_default_no_force() {
    let cli = parse(&["magnetar", "admin", "topic-delete", "acme/svc/orders"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TopicDelete { topic, force },
        } => {
            assert_eq!(topic, "acme/svc/orders");
            assert!(!force);
        }
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topic_delete_force() {
    let cli = parse(&[
        "magnetar",
        "admin",
        "topic-delete",
        "acme/svc/orders",
        "--force",
    ]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TopicDelete { force, .. },
        } => assert!(force),
        other => panic!("unexpected cmd: {other:?}"),
    }
}

#[test]
fn admin_topic_stats() {
    let cli = parse(&["magnetar", "admin", "topic-stats", "acme/svc/orders"]);
    match cli.cmd {
        Cmd::Admin {
            sub: AdminCmd::TopicStats { topic },
        } => assert_eq!(topic, "acme/svc/orders"),
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
        "tenant-list",
    ]);
    assert_eq!(cli.admin_url, "http://broker:8080");
    assert_eq!(cli.token.as_deref(), Some("secret"));
}

#[test]
fn admin_timeout_default_is_60() {
    let cli = parse(&["magnetar", "admin", "tenant-list"]);
    assert_eq!(cli.admin_timeout_secs, 60);
}

#[test]
fn admin_timeout_override() {
    let cli = parse(&[
        "magnetar",
        "--admin-timeout-secs",
        "5",
        "admin",
        "tenant-list",
    ]);
    assert_eq!(cli.admin_timeout_secs, 5);
}

#[test]
fn verbose_repetition() {
    let cli = parse(&["magnetar", "-vvv", "admin", "tenant-list"]);
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
        "tenant-list",
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
            sub: AdminCmd::TenantList
        }
    ));
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
