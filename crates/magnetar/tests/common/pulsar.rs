// SPDX-License-Identifier: Apache-2.0

//! The one Pulsar broker fixture the façade's `e2e_*.rs` suites share
//! (ADR-0115).
//!
//! This module owns, for every suite: the image repository and tag, the JVM
//! budget (`PULSAR_MEM`), the container command, the exposed ports, the
//! readiness wait and the check that the broker really is the version the tag
//! names. A suite states only what is specific to it — extra `PULSAR_PREFIX_*`
//! broker configuration, a pre-start step, a standalone flag.
//!
//! # Images
//!
//! The three tag constants below are the **only** place a Pulsar image tag
//! appears in the Rust tests; `cargo run -p xtask -- check-e2e-fixture` fails
//! any other `apachepulsar/` image reference under `crates/magnetar/tests/`
//! and keeps these constants equal to the CI pre-pull list and the PIP-33
//! compose fixture.
//! A suite selects a [`Broker`], never a tag:
//!
//! | [`Broker`]            | Tag              | Override                             |
//! | --------------------- | ---------------- | ------------------------------------ |
//! | [`Broker::Default`]   | [`DEFAULT_TAG`]  | `MAGNETAR_PULSAR_IMAGE_TAG`          |
//! | [`Broker::V4Lts`]     | [`V4_LTS_TAG`]   | none — pinned                        |
//! | [`Broker::Scalable`]  | [`SCALABLE_TAG`] | `MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG` |
//!
//! `MAGNETAR_PULSAR_IMAGE_REPO` replaces the repository for all three.
//!
//! [`Broker::Default`] is the broker the whole suite runs on, and
//! `MAGNETAR_PULSAR_IMAGE_TAG` is the matrix hook (ADR-0116): CI exports it
//! from the `pulsar:` axis of its e2e cells — [`DEFAULT_TAG`] and 5.0.0 on
//! every pull request, 4.0.14 nightly — so every suite on [`Broker::Default`]
//! runs on each axis. A suite that needs a 4.x-only or a 5.x-only broker
//! whatever the axis pins it with the matching [`Broker`], so changing the
//! default can neither skip it nor run it on the wrong broker. Only two kinds
//! of suite pin: the PIP-460 tests (a 5.x milestone) and the one test that
//! asserts a pre-5.0 broker refuses scalable topics. A suite with no such
//! reason uses [`Broker::Default`], so it gets every axis.
//!
//! # Readiness
//!
//! Startup never matches a log line. `Created namespace public/default` is
//! printed by 4.0.14, 4.2.4 and 4.2.5 but not by 5.0.0, which logs the
//! structured `PulsarWebResource - Created namespace {clientAppId=null,
//! namespace=public/default}`, and with the functions worker enabled the first
//! `Created namespace` line is for `public/functions`. `/admin/v2/brokers/ready`
//! answers 200 before `public/default` exists. The signal that holds on every
//! version is `GET /admin/v2/namespaces/public/default` returning 200, polled
//! from the host under a four-minute deadline once the container is up.
//!
//! After readiness the fixture asserts that `GET /admin/v2/brokers/version`
//! reports the `MAJOR.MINOR` series of the requested tag (the request is
//! retried under the same deadline; only a version that answers and
//! disagrees fails at once), so a mis-pulled or
//! mislabelled image fails loudly instead of running a suite against the
//! wrong broker. The assertion is skipped — with a notice on stderr — only
//! when the tag has no leading `MAJOR.MINOR` (`latest`, a digest, a mirror's
//! own scheme).
//!
//! # Configuration
//!
//! `PULSAR_PREFIX_<key>` variables reach `conf/standalone.conf` only through
//! `bin/apply-config-from-env.py`; plain `bin/pulsar standalone` ignores them.
//! [`Pulsar::config`] therefore exports each pair inside the container's start
//! script and runs that helper before `exec bin/pulsar standalone`, so a
//! configuration key a suite sets is applied rather than silently dropped.
//!
//! The script is built before the container chain on purpose: `ImageExt`
//! consumes the builder, so a variable number of environment variables cannot
//! be added to the `GenericImage` chain, and `cargo run -p xtask --
//! check-e2e-fixture` verifies the `PULSAR_MEM` cap only when `.start()` sits
//! on the same chain.

use std::time::Duration;

use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

type BoxError = Box<dyn std::error::Error>;

/// Docker Hub repository of the broker image.
pub const IMAGE_REPO: &str = "apachepulsar/pulsar";

/// Tag [`Broker::Default`] boots: the broker baseline the whole suite runs on,
/// and the release Clever Cloud runs in production (ADR-0116).
pub const DEFAULT_TAG: &str = "4.2.5";

/// Tag [`Broker::V4Lts`] boots: the newest pre-5.0 broker (4.2.5), for the suite that
/// must see a broker without PIP-460 whatever the axis is.
///
/// The name is historical and no longer means the 4.0 LTS: this pinned 4.0.13 until
/// ADR-0116, which runs the 4.0 LTS as the nightly axis instead. It is a literal of its
/// own and not an alias of [`DEFAULT_TAG`], so moving the default to a 5.x tag cannot drag
/// the v4-refusal test onto a broker that supports scalable topics.
///
/// The rename (to `V4` / `V4_TAG`) waits for the grace cycle of ADR-0116 decision 7. The
/// performance nightly overlays this file onto its base checkout, and the base only
/// advances to a head that includes ADR-0116 after the first green nightly that follows
/// it: until then a base that already has the shared fixture but not this change still
/// names `Broker::V4Lts` / `V4_LTS_TAG` in its suites, and a rename would stop that base
/// from compiling. Rename it together with the removals the performance campaign's
/// `FIXTURE_TAGS` comment lists.
pub const V4_LTS_TAG: &str = "4.2.5";

/// The minimum supported broker (ADR-0009): the last release of the 4.0 LTS line, which CI
/// runs the whole suite on nightly (ADR-0116).
///
/// Not a [`Broker`]: no suite boots it by name, the nightly selects it as the axis through
/// `MAGNETAR_PULSAR_IMAGE_TAG`. It is declared here so the minimum broker is encoded once, and
/// `cargo run -p xtask -- check-e2e-fixture` fails unless a workflow other than `ci.yml` runs
/// exactly this version, so deleting or retargeting the nightly cannot go unnoticed.
pub const MIN_SUPPORTED_TAG: &str = "4.0.14";

/// Tag [`Broker::Scalable`] boots: Pulsar 5.0.0 GA, the release that carries PIP-460 scalable
/// topics (ADR-0093). It was the 5.0.0-M1 milestone, the first release carrying PIP-460, until
/// the vendored protocol moved to the GA release.
pub const SCALABLE_TAG: &str = "5.0.0";

/// Environment variable replacing [`IMAGE_REPO`] for every [`Broker`].
pub const REPO_ENV: &str = "MAGNETAR_PULSAR_IMAGE_REPO";

/// Environment variable replacing [`DEFAULT_TAG`] for [`Broker::Default`].
pub const DEFAULT_TAG_ENV: &str = "MAGNETAR_PULSAR_IMAGE_TAG";

/// Environment variable replacing [`SCALABLE_TAG`] for [`Broker::Scalable`].
pub const SCALABLE_TAG_ENV: &str = "MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG";

/// Pulsar binary protocol port inside the container.
pub const BROKER_BINARY_PORT: u16 = 6650;

/// Pulsar admin REST port inside the container.
pub const BROKER_HTTP_PORT: u16 = 8080;

/// Embedded `ZooKeeper` client port. Only a standalone started with
/// [`Pulsar::zookeeper_metadata`] listens on it, and only then is it published.
pub const ZOOKEEPER_PORT: u16 = 2181;

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
/// A suite that genuinely needs more heap raises it with [`Pulsar::memory`] and says why.
pub const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

/// Deadline for the broker to serve `public/default` once its container is up.
const READY_TIMEOUT: Duration = Duration::from_mins(4);

/// Pause between two readiness probes.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Per-request ceiling for one readiness or version probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Deadline for the proxy's `Started Pulsar Proxy at` line.
const PROXY_STARTUP_TIMEOUT: Duration = Duration::from_mins(1);

/// Log lines of each stream quoted in a readiness failure.
const LOG_TAIL_LINES: usize = 40;

/// Which broker a suite needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Broker {
    /// The workspace baseline ([`DEFAULT_TAG`]); `MAGNETAR_PULSAR_IMAGE_TAG`
    /// selects another one — CI's matrix axis, or a local compatibility run.
    Default,
    /// The newest pre-5.0 broker ([`V4_LTS_TAG`], 4.2.5), not the 4.0 LTS despite the
    /// name (see the constant); no override, so a suite that needs a 4.x broker never
    /// runs on another major.
    V4Lts,
    /// The PIP-460 broker ([`SCALABLE_TAG`]); `MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG`
    /// selects a later milestone.
    Scalable,
}

impl Broker {
    /// The tag compiled in for this broker.
    pub fn pinned_tag(self) -> &'static str {
        match self {
            Self::Default => DEFAULT_TAG,
            Self::V4Lts => V4_LTS_TAG,
            Self::Scalable => SCALABLE_TAG,
        }
    }

    /// The environment variable that replaces [`Self::pinned_tag`], if any.
    pub fn override_var(self) -> Option<&'static str> {
        match self {
            Self::Default => Some(DEFAULT_TAG_ENV),
            Self::V4Lts => None,
            Self::Scalable => Some(SCALABLE_TAG_ENV),
        }
    }

    /// The tag this broker boots, honouring the process environment.
    pub fn tag(self) -> String {
        resolve_tag(self, |name| std::env::var(name).ok())
    }

    /// The `<repository>:<tag>` image reference this broker boots, for logging.
    pub fn image(self) -> String {
        format!("{}:{}", image_repo(), self.tag())
    }
}

/// [`Broker::tag`] with the environment lookup injected. An empty override is
/// treated as unset.
pub fn resolve_tag(broker: Broker, lookup: impl Fn(&str) -> Option<String>) -> String {
    broker
        .override_var()
        .and_then(lookup)
        .filter(|tag| !tag.is_empty())
        .unwrap_or_else(|| broker.pinned_tag().to_owned())
}

fn image_repo() -> String {
    std::env::var(REPO_ENV)
        .ok()
        .filter(|repo| !repo.is_empty())
        .unwrap_or_else(|| IMAGE_REPO.to_owned())
}

/// The `(major, minor)` series a Pulsar version string or image tag names, or
/// `None` when it does not begin with `MAJOR.MINOR` (`latest`, `4.2-alpine`).
pub fn version_series(version: &str) -> Option<(u32, u32)> {
    let mut parts = version.trim().splitn(3, '.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Install the shared tracing subscriber (`magnetar=info`, or `RUST_LOG`).
///
/// Separate from [`Pulsar::start`] because `e2e_logging.rs` installs its own
/// global capturing subscriber and must not have one installed first.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Start the default broker with no extras and return
/// (`service_url`, `admin_url`, `container_handle`).
///
/// Installs the shared tracing subscriber first. The container is held by the
/// returned guard; dropping it stops the broker.
pub async fn start_pulsar() -> Result<(String, String, ContainerAsync<GenericImage>), BoxError> {
    init_tracing();
    Ok(Pulsar::new(Broker::Default).start().await?.into_parts())
}

/// Builder for one Pulsar container. See the module documentation.
#[derive(Debug, Clone)]
pub struct Pulsar {
    broker: Broker,
    config: Vec<(String, String)>,
    env: Vec<(String, String)>,
    pre_start: Vec<String>,
    flags: Vec<String>,
    zookeeper: bool,
    advertise_container_address: bool,
    memory: String,
    admin_token: Option<String>,
}

impl Pulsar {
    /// A builder for `broker` with the default JVM budget and no extras.
    pub fn new(broker: Broker) -> Self {
        Self {
            broker,
            config: Vec::new(),
            env: Vec::new(),
            pre_start: Vec::new(),
            flags: Vec::new(),
            zookeeper: false,
            advertise_container_address: false,
            memory: PULSAR_MEM_LIMIT.to_owned(),
            admin_token: None,
        }
    }

    /// Set the broker configuration key `key` (`PULSAR_PREFIX_<key>`), applied
    /// to `conf/standalone.conf` (`conf/proxy.conf` for a proxy) before start.
    #[must_use]
    pub fn config(mut self, key: &str, value: impl Into<String>) -> Self {
        assert_identifier(key);
        self.config.push((key.to_owned(), value.into()));
        self
    }

    /// Export `name=value` to the broker process verbatim, for the variables
    /// the launcher reads itself.
    #[must_use]
    pub fn env(mut self, name: &str, value: impl Into<String>) -> Self {
        assert_identifier(name);
        self.env.push((name.to_owned(), value.into()));
        self
    }

    /// Append a shell command run after the configuration is applied and
    /// before the broker starts (transaction-coordinator metadata, a log4j
    /// patch). A failure aborts the start script.
    #[must_use]
    pub fn pre_start(mut self, command: impl Into<String>) -> Self {
        self.pre_start.push(command.into());
        self
    }

    /// Append a `pulsar standalone` flag (`--no-functions-worker`).
    #[must_use]
    pub fn flag(mut self, flag: &str) -> Self {
        self.flags.push(flag.to_owned());
        self
    }

    /// Keep the standalone's metadata in an embedded `ZooKeeper`
    /// (`PULSAR_STANDALONE_USE_ZOOKEEPER=1`) instead of `RocksDB`, and publish
    /// its client port ([`ZOOKEEPER_PORT`], [`Running::zookeeper_port`]) so a
    /// peer container (the proxy) can reach the metadata store.
    #[must_use]
    pub fn zookeeper_metadata(mut self) -> Self {
        self.zookeeper = true;
        self
    }

    /// Start with `--advertised-address <the container's own address>`, so a
    /// peer container (the proxy) can follow the broker's advertised lookup
    /// answers.
    #[must_use]
    pub fn advertise_container_address(mut self) -> Self {
        self.advertise_container_address = true;
        self
    }

    /// Replace the JVM budget. Say why next to the call: the default is the
    /// CI-sized one (`docs/testing.md` § "e2e container memory budget").
    #[must_use]
    pub fn memory(mut self, java_options: impl Into<String>) -> Self {
        self.memory = java_options.into();
        self
    }

    /// Bearer token the readiness and version probes send, for a broker that
    /// enforces `authenticationEnabled` on its admin REST. It must authenticate
    /// as a super-user role.
    #[must_use]
    pub fn admin_token(mut self, token: impl Into<String>) -> Self {
        self.admin_token = Some(token.into());
        self
    }

    /// The container command: the plain launcher when nothing needs a shell,
    /// otherwise one `bash -c` script that exports the configuration, applies
    /// it, runs the pre-start steps and `exec`s the broker.
    pub fn command(&self) -> Vec<String> {
        self.command_for(Role::Standalone)
    }

    fn command_for(&self, role: Role) -> Vec<String> {
        let needs_shell = role == Role::Proxy
            || !self.config.is_empty()
            || !self.env.is_empty()
            || !self.pre_start.is_empty()
            || self.zookeeper
            || self.advertise_container_address;
        if !needs_shell {
            let mut command = vec!["bin/pulsar".to_owned(), role.launcher().to_owned()];
            command.extend(self.flags.iter().cloned());
            return command;
        }
        let mut script = vec!["set -eu".to_owned()];
        if self.zookeeper {
            script.push("export PULSAR_STANDALONE_USE_ZOOKEEPER='1'".to_owned());
        }
        for (name, value) in &self.env {
            script.push(format!("export {name}={}", shell_quote(value)));
        }
        for (key, value) in &self.config {
            script.push(format!("export PULSAR_PREFIX_{key}={}", shell_quote(value)));
        }
        if !self.config.is_empty() {
            script.push(format!("bin/apply-config-from-env.py {}", role.conf()));
        }
        if self.advertise_container_address {
            script.push("set -- $(hostname -i)".to_owned());
            script.push("test \"$#\" -eq 1".to_owned());
        }
        script.extend(self.pre_start.iter().cloned());
        let mut launch = format!("exec bin/pulsar {}", role.launcher());
        for flag in &self.flags {
            launch.push(' ');
            launch.push_str(flag);
        }
        if self.advertise_container_address {
            launch.push_str(" --advertised-address \"$1\"");
        }
        script.push(launch);
        vec!["bash".to_owned(), "-c".to_owned(), script.join("\n")]
    }

    /// Boot a standalone broker, wait until it serves `public/default`, and
    /// check it is the requested version.
    pub async fn start(self) -> Result<Running, BoxError> {
        let tag = self.broker.tag();
        let command = self.command_for(Role::Standalone);
        // The `ZooKeeper` port is published only in `ZooKeeper` mode, so each
        // variant spells out its whole chain. Not because a port cannot be added
        // conditionally (`with_exposed_port` is a `GenericImage` method, so it
        // could be added to a stashed builder), but because `check-e2e-fixture`
        // resolves the image and the `PULSAR_MEM` cap only on a chain that runs
        // from `GenericImage::new(…)` to `.start()` in one expression, and
        // treats a stashed builder as a violation.
        let container = if self.zookeeper {
            GenericImage::new(image_repo(), tag.as_str().to_owned())
                .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
                .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
                .with_exposed_port(ContainerPort::Tcp(ZOOKEEPER_PORT))
                .with_wait_for(WaitFor::Nothing)
                .with_env_var("PULSAR_MEM", self.memory.as_str())
                .with_cmd(command)
                .start()
                .await?
        } else {
            GenericImage::new(image_repo(), tag.as_str().to_owned())
                .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
                .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
                .with_wait_for(WaitFor::Nothing)
                .with_env_var("PULSAR_MEM", self.memory.as_str())
                .with_cmd(command)
                .start()
                .await?
        };

        let host = container.get_host().await?.to_string();
        let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
        let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
        let zookeeper_port = if self.zookeeper {
            Some(container.get_host_port_ipv4(ZOOKEEPER_PORT).await?)
        } else {
            None
        };
        let admin_url = format!("http://{host}:{http_port}");

        await_ready(&container, &admin_url, self.admin_token.as_deref(), &tag).await?;

        Ok(Running {
            service_url: format!("pulsar://{host}:{binary_port}"),
            admin_url,
            host,
            binary_port,
            http_port,
            zookeeper_port,
            container,
        })
    }

    /// Boot a Pulsar proxy of `broker`'s image. It has no admin REST to poll,
    /// so it waits for the proxy's own `Started Pulsar Proxy at` line, which
    /// 4.x and 5.x both print. Configure it with [`Self::config`] only.
    pub async fn start_proxy(self) -> Result<RunningProxy, BoxError> {
        assert!(
            self.env.is_empty()
                && self.pre_start.is_empty()
                && self.flags.is_empty()
                && !self.zookeeper
                && !self.advertise_container_address
                && self.admin_token.is_none(),
            "a proxy is configured with `config` only"
        );
        let tag = self.broker.tag();
        let command = self.command_for(Role::Proxy);
        let container = GenericImage::new(image_repo(), tag.as_str().to_owned())
            .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
            .with_wait_for(WaitFor::message_on_stdout("Started Pulsar Proxy at"))
            .with_startup_timeout(PROXY_STARTUP_TIMEOUT)
            .with_env_var("PULSAR_MEM", self.memory.as_str())
            .with_cmd(command)
            .start()
            .await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
        Ok(RunningProxy {
            service_url: format!("pulsar://{host}:{port}"),
            container,
        })
    }
}

/// A started standalone broker. Dropping it stops the container.
#[derive(Debug)]
pub struct Running {
    /// `pulsar://<host>:<mapped binary port>`.
    pub service_url: String,
    /// `http://<host>:<mapped admin port>`.
    pub admin_url: String,
    /// Host the published ports are reachable on.
    pub host: String,
    /// Mapped host port of [`BROKER_BINARY_PORT`].
    pub binary_port: u16,
    /// Mapped host port of [`BROKER_HTTP_PORT`].
    pub http_port: u16,
    /// Mapped host port of [`ZOOKEEPER_PORT`]; `Some` only after
    /// [`Pulsar::zookeeper_metadata`].
    pub zookeeper_port: Option<u16>,
    /// The container handle.
    pub container: ContainerAsync<GenericImage>,
}

impl Running {
    /// `(service_url, admin_url, container)`, the shape most suites destructure.
    pub fn into_parts(self) -> (String, String, ContainerAsync<GenericImage>) {
        (self.service_url, self.admin_url, self.container)
    }
}

/// A started Pulsar proxy. Dropping it stops the container.
#[derive(Debug)]
pub struct RunningProxy {
    /// `pulsar://<host>:<mapped binary port>`.
    pub service_url: String,
    /// The container handle.
    pub container: ContainerAsync<GenericImage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Standalone,
    Proxy,
}

impl Role {
    fn launcher(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Proxy => "proxy",
        }
    }

    fn conf(self) -> &'static str {
        match self {
            Self::Standalone => "conf/standalone.conf",
            Self::Proxy => "conf/proxy.conf",
        }
    }
}

/// Single-quote `value` for `sh`, so a token carrying `;`, `:` or `'` reaches
/// the broker unchanged.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

fn assert_identifier(name: &str) {
    assert!(
        !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "`{name}` is not a shell-safe name"
    );
}

fn probe_client() -> Result<reqwest::Client, BoxError> {
    Ok(reqwest::Client::builder().timeout(PROBE_TIMEOUT).build()?)
}

fn authorized(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

/// Poll `GET /admin/v2/namespaces/public/default` until it answers 200, then
/// check `GET /admin/v2/brokers/version` reports the series of `tag`.
///
/// Both probes retry under the one [`READY_TIMEOUT`]: the version endpoint can
/// lag the namespace by a request, and a transient failure of either is not a
/// verdict. Only a version that answers and disagrees with the tag is final.
/// Fails early, quoting the broker's logs, when the container exits.
async fn await_ready(
    container: &ContainerAsync<GenericImage>,
    admin_url: &str,
    token: Option<&str>,
    tag: &str,
) -> Result<(), BoxError> {
    let expected = version_series(tag);
    if expected.is_none() {
        eprintln!(
            "pulsar fixture: tag `{tag}` does not begin with MAJOR.MINOR; \
             skipping the broker version assertion"
        );
    }
    let client = probe_client()?;
    let namespace_url = format!("{admin_url}/admin/v2/namespaces/public/default");
    let version_url = format!("{admin_url}/admin/v2/brokers/version");
    let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    loop {
        let last = match probe(&client, &namespace_url, token).await {
            Ok(_) => match expected {
                None => return Ok(()),
                Some(series) => match probe(&client, &version_url, token).await {
                    Ok(version) => return check_series(&version, series, tag),
                    Err(observation) => observation,
                },
            },
            Err(observation) => observation,
        };
        if !container.is_running().await? {
            return Err(failure("the broker container exited", &last, container).await);
        }
        if tokio::time::Instant::now() >= deadline {
            let reason = format!("the broker was not ready within {READY_TIMEOUT:?}");
            return Err(failure(&reason, &last, container).await);
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

/// One GET: the body of a 2xx answer, otherwise a description of what went wrong.
async fn probe(client: &reqwest::Client, url: &str, token: Option<&str>) -> Result<String, String> {
    let describe = |error: reqwest::Error| format!("{url}: request error: {error}");
    let response = authorized(client.get(url), token)
        .send()
        .await
        .map_err(describe)?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("{url}: HTTP {status}"));
    }
    response.text().await.map_err(describe)
}

/// The version the broker reported must be in the `expected` series of `tag`.
fn check_series(reported: &str, expected: (u32, u32), tag: &str) -> Result<(), BoxError> {
    if version_series(reported) == Some(expected) {
        return Ok(());
    }
    Err(format!(
        "the broker reports version `{}` but the image tag `{tag}` names the {}.{} series",
        reported.trim(),
        expected.0,
        expected.1
    )
    .into())
}

async fn failure(reason: &str, last: &str, container: &ContainerAsync<GenericImage>) -> BoxError {
    let stdout = container.stdout_to_vec().await.unwrap_or_default();
    let stderr = container.stderr_to_vec().await.unwrap_or_default();
    format!(
        "{reason}; last probe: {last}\n\
         --- container stdout (last {LOG_TAIL_LINES} lines) ---\n{}\n\
         --- container stderr (last {LOG_TAIL_LINES} lines) ---\n{}",
        tail(&stdout),
        tail(&stderr),
    )
    .into()
}

fn tail(log: &[u8]) -> String {
    let text = String::from_utf8_lossy(log);
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(LOG_TAIL_LINES)..].join("\n")
}
