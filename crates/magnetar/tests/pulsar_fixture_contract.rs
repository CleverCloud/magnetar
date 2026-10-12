// SPDX-License-Identifier: Apache-2.0

//! Docker-free contract of the shared Pulsar fixture (ADR-0115).
//!
//! `common/pulsar.rs` is compiled into every `e2e_*.rs` binary, where a unit
//! test would run once per suite. These tests live in one binary of their own,
//! with no container, so the selection, version and command logic is checked
//! on every run — including the ones that cannot reach Docker.

mod common;

use common::pulsar::{
    Broker, DEFAULT_TAG, DEFAULT_TAG_ENV, Pulsar, SCALABLE_TAG, SCALABLE_TAG_ENV, V4_LTS_TAG,
    resolve_tag, shell_quote, version_series,
};

/// An environment with every override set to something recognisable.
fn overridden(name: &str) -> Option<String> {
    match name {
        DEFAULT_TAG_ENV => Some("9.9.9-default".to_owned()),
        SCALABLE_TAG_ENV => Some("9.9.9-scalable".to_owned()),
        _ => None,
    }
}

#[test]
fn each_broker_boots_its_pin_without_overrides() {
    let none = |_: &str| None;
    assert_eq!(resolve_tag(Broker::Default, none), DEFAULT_TAG);
    assert_eq!(resolve_tag(Broker::V4Lts, none), V4_LTS_TAG);
    assert_eq!(resolve_tag(Broker::Scalable, none), SCALABLE_TAG);
}

#[test]
fn the_default_and_scalable_overrides_apply_to_their_own_broker_only() {
    assert_eq!(resolve_tag(Broker::Default, overridden), "9.9.9-default");
    assert_eq!(resolve_tag(Broker::Scalable, overridden), "9.9.9-scalable");
}

#[test]
fn the_lts_pin_ignores_every_override() {
    assert_eq!(resolve_tag(Broker::V4Lts, overridden), V4_LTS_TAG);
    assert_eq!(Broker::V4Lts.override_var(), None);
}

#[test]
fn an_empty_override_is_treated_as_unset() {
    let empty = |_: &str| Some(String::new());
    assert_eq!(resolve_tag(Broker::Default, empty), DEFAULT_TAG);
    assert_eq!(resolve_tag(Broker::Scalable, empty), SCALABLE_TAG);
}

#[test]
fn version_series_reads_major_and_minor_of_every_tag_and_broker_version() {
    assert_eq!(version_series("4.2.4"), Some((4, 2)));
    assert_eq!(version_series("4.0.13"), Some((4, 0)));
    assert_eq!(version_series("5.0.0"), Some((5, 0)));
    assert_eq!(version_series("5.0.0-M1"), Some((5, 0)));
    assert_eq!(version_series("5.0"), Some((5, 0)));
    assert_eq!(version_series(" 4.2.4\n"), Some((4, 2)));
}

#[test]
fn version_series_refuses_what_has_no_leading_major_minor() {
    assert_eq!(version_series("latest"), None);
    assert_eq!(version_series("4"), None);
    assert_eq!(version_series("4.x"), None);
    assert_eq!(version_series("4.2-alpine"), None);
    assert_eq!(version_series(""), None);
}

#[test]
fn the_pins_all_have_a_series_to_assert() {
    for tag in [DEFAULT_TAG, V4_LTS_TAG, SCALABLE_TAG] {
        assert!(
            version_series(tag).is_some(),
            "pin {tag} has no MAJOR.MINOR"
        );
    }
}

#[test]
fn a_bare_standalone_runs_the_launcher_directly() {
    assert_eq!(
        Pulsar::new(Broker::Default).command(),
        ["bin/pulsar", "standalone"]
    );
    assert_eq!(
        Pulsar::new(Broker::Default)
            .flag("--no-functions-worker")
            .flag("--no-stream-storage")
            .command(),
        [
            "bin/pulsar",
            "standalone",
            "--no-functions-worker",
            "--no-stream-storage"
        ]
    );
}

#[test]
fn configuration_is_exported_then_applied_before_the_broker_execs() {
    let command = Pulsar::new(Broker::Default)
        .config("subscriptionPatternMaxLength", "200")
        .command();
    assert_eq!(command[..2], ["bash", "-c"]);
    let script = &command[2];
    let export = script
        .find("export PULSAR_PREFIX_subscriptionPatternMaxLength='200'")
        .expect("the key is exported with the PULSAR_PREFIX_ prefix");
    let apply = script
        .find("bin/apply-config-from-env.py conf/standalone.conf")
        .expect("the helper that applies PULSAR_PREFIX_* runs");
    let launch = script
        .find("exec bin/pulsar standalone")
        .expect("the broker is exec'd last");
    assert!(
        export < apply && apply < launch,
        "export, apply, launch: {script}"
    );
    assert!(
        script.starts_with("set -eu"),
        "a failed step must abort: {script}"
    );
}

#[test]
fn pre_start_steps_run_between_the_configuration_and_the_launch() {
    let command = Pulsar::new(Broker::Default)
        .config("transactionCoordinatorEnabled", "true")
        .pre_start("bin/pulsar initialize-transaction-coordinator-metadata --cluster standalone")
        .flag("--no-functions-worker")
        .command();
    let script = &command[2];
    let apply = script.find("apply-config-from-env.py").unwrap();
    let init = script
        .find("initialize-transaction-coordinator-metadata")
        .unwrap();
    let launch = script
        .find("exec bin/pulsar standalone --no-functions-worker")
        .unwrap();
    assert!(apply < init && init < launch, "{script}");
}

#[test]
fn a_pre_start_step_alone_still_gets_a_shell_and_no_config_helper() {
    let command = Pulsar::new(Broker::Default)
        .pre_start("echo patched >> /pulsar/conf/log4j2.yaml")
        .command();
    let script = &command[2];
    assert!(script.contains("echo patched"), "{script}");
    assert!(
        !script.contains("apply-config-from-env.py"),
        "no configuration was given: {script}"
    );
}

#[test]
fn launcher_variables_are_exported_verbatim() {
    let command = Pulsar::new(Broker::Default)
        .env("PULSAR_STANDALONE_FLAVOUR", "two words")
        .advertise_container_address()
        .command();
    let script = &command[2];
    assert!(
        script.contains("export PULSAR_STANDALONE_FLAVOUR='two words'"),
        "{script}"
    );
    assert!(script.contains("set -- $(hostname -i)"), "{script}");
    assert!(
        script.ends_with("exec bin/pulsar standalone --advertised-address \"$1\""),
        "{script}"
    );
}

#[test]
fn zookeeper_metadata_exports_the_launcher_switch_and_nothing_else_does() {
    let with_zookeeper = Pulsar::new(Broker::Default).zookeeper_metadata().command();
    assert_eq!(with_zookeeper[..2], ["bash", "-c"]);
    assert!(
        with_zookeeper[2].contains("export PULSAR_STANDALONE_USE_ZOOKEEPER='1'"),
        "{}",
        with_zookeeper[2]
    );
    // Without it the launcher keeps its RocksDB default and runs unwrapped.
    assert_eq!(
        Pulsar::new(Broker::Default).command(),
        ["bin/pulsar", "standalone"]
    );
}

#[test]
fn values_with_shell_metacharacters_reach_the_broker_unchanged() {
    assert_eq!(shell_quote("plain"), "'plain'");
    assert_eq!(shell_quote("data:;base64,AAA="), "'data:;base64,AAA='");
    assert_eq!(shell_quote("it's"), r"'it'\''s'");
    let command = Pulsar::new(Broker::Default)
        .config("tokenSecretKey", "data:;base64,a'b")
        .command();
    assert!(
        command[2].contains(r"export PULSAR_PREFIX_tokenSecretKey='data:;base64,a'\''b'"),
        "{}",
        command[2]
    );
}

#[test]
#[should_panic(expected = "not a shell-safe name")]
fn a_configuration_key_that_is_not_a_shell_name_is_refused() {
    let _ = Pulsar::new(Broker::Default).config("bad key; rm -rf /", "x");
}

/// The generated script, run under real `bash` against stand-ins for the
/// image's `bin/apply-config-from-env.py` and `bin/pulsar`: shell quoting is
/// only proven by a shell.
#[cfg(unix)]
mod executed {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{Broker, Pulsar};

    /// Spaces, both quote kinds, `$`, command substitution in both spellings,
    /// a backslash and the shell's separators and globs.
    const AWKWARD: &str =
        r#"two  words 'single' "double" $HOME `echo pwned` $(echo pwned) \ ; & | * ? <> ! #"#;

    /// A scratch directory whose `bin/` stands in for the image's.
    struct Image(PathBuf);

    impl Image {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "pulsar-fixture-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(dir.join("bin")).expect("scratch image");
            let record = "printf '%s' \"$PULSAR_PREFIX_secret\"";
            Self::install(
                &dir,
                "apply-config-from-env.py",
                &format!("#!/bin/sh\n{record} > applied\n"),
            );
            Self::install(
                &dir,
                "pulsar",
                &format!(
                    "#!/bin/sh\n{record} > launched\nprintf '%s\\n' \"$@\" > argv\n\
                     printenv PULSAR_STANDALONE_FLAVOUR > flavour || true\n"
                ),
            );
            Self(dir)
        }

        fn install(dir: &Path, name: &str, body: &str) {
            let path = dir.join("bin").join(name);
            std::fs::write(&path, body).expect("stand-in");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("stand-in is executable");
        }

        fn run(&self, script: &str) -> ExitStatus {
            Command::new("bash")
                .arg("-c")
                .arg(script)
                .current_dir(&self.0)
                .status()
                .expect("bash runs")
        }

        fn recorded(&self, name: &str) -> Option<String> {
            std::fs::read_to_string(self.0.join(name)).ok()
        }
    }

    impl Drop for Image {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn an_awkward_config_value_reaches_the_applier_and_the_broker_unchanged() {
        let image = Image::new();
        let command = Pulsar::new(Broker::Default)
            .config("secret", AWKWARD)
            .env("PULSAR_STANDALONE_FLAVOUR", AWKWARD)
            .flag("--no-functions-worker")
            .command();
        assert!(image.run(&command[2]).success());
        assert_eq!(image.recorded("applied").as_deref(), Some(AWKWARD));
        assert_eq!(image.recorded("launched").as_deref(), Some(AWKWARD));
        assert_eq!(
            image.recorded("flavour").as_deref(),
            Some(format!("{AWKWARD}\n").as_str())
        );
        assert_eq!(
            image.recorded("argv").as_deref(),
            Some("standalone\n--no-functions-worker\n")
        );
    }

    #[test]
    fn the_configuration_is_applied_before_the_pre_start_step_and_the_launch() {
        let image = Image::new();
        let command = Pulsar::new(Broker::Default)
            .config("secret", "applied-first")
            .pre_start("test -f applied && cp applied seen-by-pre-start")
            .command();
        assert!(image.run(&command[2]).success());
        assert_eq!(
            image.recorded("seen-by-pre-start").as_deref(),
            Some("applied-first")
        );
        assert_eq!(image.recorded("launched").as_deref(), Some("applied-first"));
    }

    #[test]
    fn a_failing_pre_start_step_stops_the_broker_from_launching() {
        let image = Image::new();
        let command = Pulsar::new(Broker::Default)
            .config("secret", "x")
            .pre_start("false")
            .command();
        assert!(!image.run(&command[2]).success());
        assert_eq!(image.recorded("launched"), None);
    }
}
