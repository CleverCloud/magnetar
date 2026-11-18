// SPDX-License-Identifier: Apache-2.0

//! The Docker containers the façade's `e2e_*.rs` suites start, driven through
//! the `docker` command line (ADR-0127).
//!
//! The suites need little from Docker: run an image with environment
//! variables, a command and published ports, wait for a line on its stdout,
//! read the mapped host ports and the logs, run a command inside it, stop it,
//! and remove it when the test is over. The `docker` client every e2e host
//! already has does all of that, so this module adds no crate to the graph.
//! It honours `DOCKER_HOST` and the active Docker context exactly as the
//! client does; [`Container::host`] names the host the published ports
//! are reachable on.
//!
//! Every container is force-removed, with its anonymous volumes, when its
//! [`Container`] is dropped, including on a panic. A test process that is
//! killed leaves its containers behind, as it did before.

use std::process::{Command, Output};
use std::time::Duration;

type BoxError = Box<dyn std::error::Error>;

/// How long [`GenericImage::start`] waits for a [`WaitFor::StdoutMessage`]
/// unless [`GenericImage::with_startup_timeout`] says otherwise.
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_mins(1);

/// Pause between two reads of the logs while waiting for a message.
const LOG_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// A container port to publish on an ephemeral host port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerPort {
    /// A TCP port.
    Tcp(u16),
    /// A UDP port.
    Udp(u16),
}

impl ContainerPort {
    /// The `<port>/<protocol>` form `docker run -p` and `docker port` take.
    fn spec(self) -> String {
        match self {
            Self::Tcp(port) => format!("{port}/tcp"),
            Self::Udp(port) => format!("{port}/udp"),
        }
    }
}

impl From<u16> for ContainerPort {
    fn from(port: u16) -> Self {
        Self::Tcp(port)
    }
}

/// What [`GenericImage::start`] waits for once the container runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitFor {
    /// Return as soon as the container is running.
    Nothing,
    /// Wait until the container's stdout contains this text.
    StdoutMessage(String),
}

impl WaitFor {
    /// Wait until the container's stdout contains `message`.
    pub fn message_on_stdout(message: impl Into<String>) -> Self {
        Self::StdoutMessage(message.into())
    }
}

/// An image to run, with its run-time options.
#[derive(Debug, Clone)]
pub struct GenericImage {
    repository: String,
    tag: String,
    ports: Vec<ContainerPort>,
    wait_for: WaitFor,
    startup_timeout: Duration,
    env: Vec<(String, String)>,
    cmd: Vec<String>,
}

impl GenericImage {
    /// `repository:tag`, run with its own entrypoint and command.
    pub fn new(repository: impl Into<String>, tag: impl Into<String>) -> Self {
        Self {
            repository: repository.into(),
            tag: tag.into(),
            ports: Vec::new(),
            wait_for: WaitFor::Nothing,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            env: Vec::new(),
            cmd: Vec::new(),
        }
    }

    /// Publish `port` on an ephemeral host port.
    #[must_use]
    pub fn with_exposed_port(mut self, port: ContainerPort) -> Self {
        self.ports.push(port);
        self
    }

    /// Wait for `wait_for` before [`Self::start`] returns.
    #[must_use]
    pub fn with_wait_for(mut self, wait_for: WaitFor) -> Self {
        self.wait_for = wait_for;
        self
    }

    /// Bound the [`WaitFor`] wait.
    #[must_use]
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Set the environment variable `name` in the container.
    #[must_use]
    pub fn with_env_var(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((name.into(), value.into()));
        self
    }

    /// Replace the image's command.
    #[must_use]
    pub fn with_cmd(mut self, cmd: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.cmd = cmd.into_iter().map(Into::into).collect();
        self
    }

    /// Run the container (pulling the image when it is missing) and wait for
    /// its [`WaitFor`]. A container that exits or misses its startup timeout
    /// is removed and reported with the tail of its logs.
    pub async fn start(self) -> Result<Container, BoxError> {
        let mut args = vec!["run".to_owned(), "--detach".to_owned()];
        for port in &self.ports {
            args.push("--publish".to_owned());
            args.push(port.spec());
        }
        for (name, value) in &self.env {
            args.push("--env".to_owned());
            args.push(format!("{name}={value}"));
        }
        args.push(format!("{}:{}", self.repository, self.tag));
        args.extend(self.cmd.iter().cloned());
        let host = docker_host().await;
        let id = String::from_utf8(docker_ok(args).await?)?.trim().to_owned();
        let container = Container { id, host };
        if let WaitFor::StdoutMessage(message) = &self.wait_for {
            container
                .await_stdout(message, self.startup_timeout)
                .await?;
        }
        Ok(container)
    }
}

/// A running container. Dropping it force-removes the container.
#[derive(Debug)]
pub struct Container {
    id: String,
    host: String,
}

impl Container {
    /// The container id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The host the published ports are reachable on: the host of a
    /// `tcp://` or `http(s)://` `DOCKER_HOST`, `localhost` otherwise.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The IPv4 host port `port` is published on.
    pub async fn get_host_port_ipv4(
        &self,
        port: impl Into<ContainerPort>,
    ) -> Result<u16, BoxError> {
        let spec = port.into().spec();
        let output = docker_ok(["port".to_owned(), self.id.clone(), spec.clone()]).await?;
        String::from_utf8(output)?
            .lines()
            .filter_map(|binding| binding.trim().rsplit_once(':'))
            .find(|(address, _)| !address.contains(':') && !address.starts_with('['))
            .map(|(_, port)| port.parse())
            .transpose()?
            .ok_or_else(|| format!("container {}: {spec} has no IPv4 binding", self.id).into())
    }

    /// Everything the container has written to stdout so far.
    pub async fn stdout_to_vec(&self) -> Result<Vec<u8>, BoxError> {
        Ok(self.logs().await?.stdout)
    }

    /// Everything the container has written to stderr so far.
    pub async fn stderr_to_vec(&self) -> Result<Vec<u8>, BoxError> {
        Ok(self.logs().await?.stderr)
    }

    /// Whether the container is still running.
    pub async fn is_running(&self) -> Result<bool, BoxError> {
        let output = docker_ok([
            "inspect".to_owned(),
            "--format".to_owned(),
            "{{.State.Running}}".to_owned(),
            self.id.clone(),
        ])
        .await?;
        Ok(String::from_utf8(output)?.trim() == "true")
    }

    /// Run `command` inside the container and wait for it to exit. A command
    /// that exits non-zero is not an error: read [`ExecResult::exit_code`].
    pub async fn exec(
        &self,
        command: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<ExecResult, BoxError> {
        let mut args = vec!["exec".to_owned(), self.id.clone()];
        args.extend(command.into_iter().map(Into::into));
        let output = docker(args).await?;
        Ok(ExecResult {
            exit_code: output.status.code().map(i64::from),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    /// Stop the container, sending `SIGKILL` once `timeout` has passed after
    /// `SIGTERM`. The container is kept, so `docker start` or `docker restart`
    /// can bring it back; it is still removed on drop.
    pub async fn stop(&self, timeout: Duration) -> Result<(), BoxError> {
        docker_ok([
            "stop".to_owned(),
            "--time".to_owned(),
            timeout.as_secs().to_string(),
            self.id.clone(),
        ])
        .await?;
        Ok(())
    }

    async fn logs(&self) -> Result<Output, BoxError> {
        let output = docker(["logs".to_owned(), self.id.clone()]).await?;
        if !output.status.success() {
            return Err(command_error("logs", &output));
        }
        Ok(output)
    }

    async fn await_stdout(&self, message: &str, timeout: Duration) -> Result<(), BoxError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let logs = self.logs().await?;
            if String::from_utf8_lossy(&logs.stdout).contains(message) {
                return Ok(());
            }
            let reason = if !self.is_running().await? {
                "exited"
            } else if tokio::time::Instant::now() >= deadline {
                "missed its startup timeout"
            } else {
                tokio::time::sleep(LOG_POLL_INTERVAL).await;
                continue;
            };
            return Err(format!(
                "container {} {reason} before printing `{message}` on stdout\n\
                 --- stdout ---\n{}\n--- stderr ---\n{}",
                self.id,
                tail(&logs.stdout),
                tail(&logs.stderr),
            )
            .into());
        }
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let removed = Command::new("docker")
            .args(["rm", "--force", "--volumes", &self.id])
            .output();
        match removed {
            Ok(output) if output.status.success() => {}
            Ok(output) => eprintln!(
                "container {}: docker rm failed: {}",
                self.id,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
            Err(error) => eprintln!("container {}: docker rm failed: {error}", self.id),
        }
    }
}

/// What a [`Container::exec`] command left behind.
#[derive(Debug)]
pub struct ExecResult {
    /// The command's exit code, `None` when a signal ended it.
    pub exit_code: Option<i64>,
    /// The command's stdout.
    pub stdout: Vec<u8>,
    /// The command's stderr.
    pub stderr: Vec<u8>,
}

/// Run `docker <args>` off the async worker threads.
async fn docker(args: impl IntoIterator<Item = String>) -> Result<Output, BoxError> {
    let args: Vec<String> = args.into_iter().collect();
    let output =
        tokio::task::spawn_blocking(move || Command::new("docker").args(&args).output()).await??;
    Ok(output)
}

/// [`docker`], failing with its stderr when it exits non-zero.
async fn docker_ok(args: impl IntoIterator<Item = String>) -> Result<Vec<u8>, BoxError> {
    let args: Vec<String> = args.into_iter().collect();
    let subcommand = args.first().cloned().unwrap_or_default();
    let output = docker(args).await?;
    if !output.status.success() {
        return Err(command_error(&subcommand, &output));
    }
    Ok(output.stdout)
}

fn command_error(subcommand: &str, output: &Output) -> BoxError {
    format!(
        "docker {subcommand} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )
    .into()
}

/// The host the published ports are reachable on (see [`Container::host`]).
///
/// A test process that itself runs in a container (`/.dockerenv`, the
/// performance suite runner) reaches the ports Docker publishes on the host
/// through the default `bridge` network's gateway, not through its own
/// `localhost`; this is the resolution `testcontainers` performed.
async fn docker_host() -> String {
    if let Some(host) = std::env::var("DOCKER_HOST")
        .ok()
        .and_then(|url| remote_host(&url))
    {
        return host;
    }
    if std::path::Path::new("/.dockerenv").exists() {
        let gateway = docker_ok([
            "network".to_owned(),
            "inspect".to_owned(),
            "bridge".to_owned(),
            "--format".to_owned(),
            "{{range .IPAM.Config}}{{.Gateway}} {{end}}".to_owned(),
        ])
        .await
        .ok()
        .and_then(|output| {
            String::from_utf8_lossy(&output)
                .split_whitespace()
                .next()
                .map(str::to_owned)
        });
        if let Some(gateway) = gateway {
            return gateway;
        }
    }
    "localhost".to_owned()
}

/// The host part of a `tcp://`, `http://` or `https://` daemon URL.
fn remote_host(url: &str) -> Option<String> {
    let rest = ["tcp://", "http://", "https://"]
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme))?;
    let authority = rest.split('/').next()?;
    let host = match authority.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next()?,
        None => authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host),
    };
    (!host.is_empty()).then(|| host.to_owned())
}

fn tail(log: &[u8]) -> String {
    const LINES: usize = 40;
    let text = String::from_utf8_lossy(log);
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(LINES)..].join("\n")
}
