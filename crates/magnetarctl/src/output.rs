// SPDX-License-Identifier: Apache-2.0

//! Command-specific labels and unit-aware values for human output.

use std::fmt;
use std::fmt::Write as _;

use magnetar_admin::RetentionPolicies;

/// The human view is independent of the serialized broker representation.
pub(crate) trait HumanOutput {
    fn human_fields(&self) -> Vec<(&'static str, String)>;
}

/// Duration stored in minutes; formatting preserves every minute.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DurationMinutes(pub(crate) i64);

impl fmt::Display for DurationMinutes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 < 0 {
            return write!(f, "{} minutes", self.0);
        }
        let days = self.0 / 1440;
        let hours = self.0 % 1440 / 60;
        let minutes = self.0 % 60;
        let mut parts = Vec::new();
        for (value, singular, plural) in [
            (days, "day", "days"),
            (hours, "hour", "hours"),
            (minutes, "minute", "minutes"),
        ] {
            if value != 0 {
                parts.push(format!(
                    "{value} {}",
                    if value == 1 { singular } else { plural }
                ));
            }
        }
        if parts.is_empty() {
            f.write_str("0 minutes")
        } else {
            f.write_str(&parts.join(" "))
        }
    }
}

/// Size stored in megabytes, rendered in the requested MB notation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SizeMegabytes(pub(crate) i64);

impl fmt::Display for SizeMegabytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} MB", self.0)
    }
}

/// A reusable limit: unlimited is separate from the quantity and its unit.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Limit<T> {
    Unlimited,
    Value(T),
}

impl<T> Limit<T> {
    /// Decode the broker's -1 sentinel at the command presentation boundary.
    pub(crate) fn from_sentinel(value: i64, quantity: impl FnOnce(i64) -> T) -> Self {
        if value == -1 {
            Self::Unlimited
        } else {
            Self::Value(quantity(value))
        }
    }
}

impl<T: fmt::Display> fmt::Display for Limit<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unlimited => f.write_str("∞"),
            Self::Value(value) => value.fmt(f),
        }
    }
}

impl HumanOutput for RetentionPolicies {
    fn human_fields(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                "retention duration",
                Limit::from_sentinel(i64::from(self.retention_time_in_minutes), DurationMinutes)
                    .to_string(),
            ),
            (
                "retention size",
                Limit::from_sentinel(self.retention_size_in_mb, SizeMegabytes).to_string(),
            ),
        ]
    }
}

/// Apply color only to padded labels, keeping value columns aligned.
pub(crate) fn render_rows(fields: &[(&str, String)], colored: bool) -> String {
    let width = fields
        .iter()
        .map(|(name, _)| name.to_uppercase().chars().count())
        .max()
        .unwrap_or(0);
    let style = anstyle::Style::new().fg_color(Some(anstyle::AnsiColor::Blue.into()));
    let mut out = String::new();
    for (name, value) in fields {
        let name = name.to_uppercase();
        let padded = format!("{name:<width$}");
        if colored {
            writeln!(out, "{style}{padded}{style:#}  {value}").expect("infallible String write");
        } else {
            writeln!(out, "{padded}  {value}").expect("infallible String write");
        }
    }
    out
}

/// Render any fixed-column table with uppercase colored headers.
pub(crate) fn render_table<const N: usize>(
    headers: [&str; N],
    rows: &[[String; N]],
    colored: bool,
) -> String {
    let headers = headers.map(str::to_uppercase);
    let widths: [usize; N] = std::array::from_fn(|column| {
        rows.iter()
            .map(|row| row[column].chars().count())
            .chain(std::iter::once(headers[column].chars().count()))
            .max()
            .unwrap_or(0)
    });
    let style = anstyle::Style::new().fg_color(Some(anstyle::AnsiColor::Blue.into()));
    let mut out = String::new();
    for column in 0..N {
        if column > 0 {
            out.push_str("  ");
        }
        let header = &headers[column];
        let width = if column + 1 == N { 0 } else { widths[column] };
        if colored {
            write!(out, "{style}{header:<width$}{style:#}").expect("infallible String write");
        } else {
            write!(out, "{header:<width$}").expect("infallible String write");
        }
    }
    out.push('\n');
    for row in rows {
        for column in 0..N {
            if column > 0 {
                out.push_str("  ");
            }
            let value = &row[column];
            let width = if column + 1 == N { 0 } else { widths[column] };
            write!(out, "{value:<width$}").expect("infallible String write");
        }
        out.push('\n');
    }
    out
}

/// Logical topic name and optional declared partition count.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TopicRow {
    pub(crate) name: String,
    pub(crate) partitions: Option<u32>,
}

pub(crate) fn render_topics(topics: &[TopicRow], colored: bool) -> String {
    let rows: Vec<_> = topics
        .iter()
        .map(|topic| {
            [
                topic.name.clone(),
                topic
                    .partitions
                    .map_or_else(|| "—".to_owned(), |count| count.to_string()),
            ]
        })
        .collect();
    render_table(["topic", "partitions"], &rows, colored)
}

/// Candidate parent and partition index; broker metadata confirms membership.
pub(crate) fn partition_parent(topic: &str) -> Option<(&str, u32)> {
    let (parent, index) = topic.rsplit_once("-partition-")?;
    if parent.is_empty() || index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((parent, index.parse().ok()?))
}

/// Collapse physical partitions only when the broker confirms their parent.
/// Counts are declared counts, never inferred from the observed partition set.
pub(crate) fn collapse_partitioned_topics(
    topics: &[String],
    counts: &std::collections::BTreeMap<String, u32>,
) -> Vec<TopicRow> {
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for topic in topics {
        let logical = partition_parent(topic).and_then(|(parent, index)| {
            counts
                .get(parent)
                .copied()
                .filter(|count| index < *count)
                .map(|count| (parent, count))
        });
        if let Some((parent, count)) = logical {
            if seen.insert(parent) {
                rows.push(TopicRow {
                    name: parent.to_owned(),
                    partitions: Some(count),
                });
            }
        } else if seen.insert(topic.as_str()) {
            rows.push(TopicRow {
                name: topic.clone(),
                partitions: None,
            });
        }
    }
    rows
}

/// A byte quantity rendered using decimal SI units and two decimals.
pub(crate) struct SizeBytes(pub(crate) f64);

impl SizeBytes {
    #[expect(
        clippy::cast_precision_loss,
        reason = "Human sizes round to two decimals; raw JSON keeps exact counters"
    )]
    pub(crate) fn from_counter(value: i64) -> Self {
        Self(value as f64)
    }
}

impl fmt::Display for SizeBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_bytes(self.0, "", f)
    }
}

fn format_bytes(mut value: f64, suffix: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut index = 0;
    if value.is_finite() {
        while value.abs() >= 999.995 && index + 1 < units.len() {
            value /= 1000.0;
            index += 1;
        }
    }
    write!(f, "{value:.2} {}{suffix}", units[index])
}

/// A message rate; two decimals unless the caller asks for another precision,
/// so the aggregate rows line up with the per-entity tables and with the byte
/// rates and sizes.
pub(crate) struct MessageRate(pub(crate) f64);

impl fmt::Display for MessageRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let precision = f.precision().unwrap_or(2);
        write!(f, "{:.*} msg/s", precision, self.0)
    }
}

pub(crate) struct ByteRate(pub(crate) f64);

impl fmt::Display for ByteRate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_bytes(self.0, "/s", f)
    }
}

/// Aggregate stats followed by one table row per producer or subscription.
pub(crate) fn render_topic_stats(
    stats: &magnetar_admin::TopicStats,
    partitions: u32,
    colored: bool,
) -> String {
    let fields = vec![
        (
            "partitions",
            if partitions == 0 {
                "—".to_owned()
            } else {
                partitions.to_string()
            },
        ),
        (
            "message rate in",
            MessageRate(stats.msg_rate_in).to_string(),
        ),
        (
            "message rate out",
            MessageRate(stats.msg_rate_out).to_string(),
        ),
        (
            "throughput in",
            ByteRate(stats.msg_throughput_in).to_string(),
        ),
        (
            "throughput out",
            ByteRate(stats.msg_throughput_out).to_string(),
        ),
        (
            "average message size",
            SizeBytes(stats.average_msg_size).to_string(),
        ),
        ("messages received", stats.msg_in_counter.to_string()),
        (
            "bytes received",
            SizeBytes::from_counter(stats.bytes_in_counter).to_string(),
        ),
        (
            "storage size",
            SizeBytes::from_counter(stats.storage_size).to_string(),
        ),
        (
            "backlog size",
            SizeBytes::from_counter(stats.backlog_size).to_string(),
        ),
        ("publishers", stats.publishers.len().to_string()),
        (
            "subscriptions",
            stats
                .subscriptions
                .as_object()
                .map_or(0, serde_json::Map::len)
                .to_string(),
        ),
    ];
    let mut out = render_rows(&fields, colored);
    out.push_str(&render_producers(&stats.publishers, colored));
    out.push_str(&render_subscriptions(&stats.subscriptions, colored));
    out.push_str(&render_consumers(&stats.subscriptions, colored));
    out
}

fn render_producers(publishers: &[serde_json::Value], colored: bool) -> String {
    let mut out = String::new();
    if !publishers.is_empty() {
        let rows: Vec<_> = publishers
            .iter()
            .enumerate()
            .map(|(index, publisher)| {
                [
                    publisher
                        .get("producerName")
                        .and_then(serde_json::Value::as_str)
                        .map_or_else(|| (index + 1).to_string(), str::to_owned),
                    plain_field(publisher, "producerId"),
                    ip_address(publisher),
                    plain_field(publisher, "clientVersion"),
                    numeric_field(publisher, "msgRateIn", |value| {
                        format!("{:.2}", MessageRate(value))
                    }),
                    numeric_field(publisher, "msgThroughputIn", |value| {
                        ByteRate(value).to_string()
                    }),
                    numeric_field(publisher, "averageMsgSize", |value| {
                        SizeBytes(value).to_string()
                    }),
                    numeric_field(publisher, "chunkedMessageRate", |value| {
                        MessageRate(value).to_string()
                    }),
                    plain_field(publisher, "supportsPartialProducer"),
                ]
            })
            .collect();
        out.push('\n');
        out.push_str(&render_table(
            [
                "producer",
                "id",
                "ip address",
                "client version",
                "rate",
                "throughput",
                "avg size",
                "chunked rate",
                "partial",
            ],
            &rows,
            colored,
        ));
    }
    out
}

fn render_subscriptions(subscriptions: &serde_json::Value, colored: bool) -> String {
    let mut out = String::new();
    if let Some(subscriptions) = subscriptions
        .as_object()
        .filter(|subscriptions| !subscriptions.is_empty())
    {
        let rows: Vec<_> = subscriptions
            .iter()
            .map(|(name, details)| {
                [
                    name.clone(),
                    plain_field(details, "type"),
                    details
                        .get("consumers")
                        .and_then(serde_json::Value::as_array)
                        .map_or_else(|| "—".to_owned(), |consumers| consumers.len().to_string()),
                    plain_field(details, "msgBacklog"),
                    plain_field(details, "unackedMessages"),
                    numeric_field(details, "msgRateOut", |value| {
                        MessageRate(value).to_string()
                    }),
                    numeric_field(details, "msgThroughputOut", |value| {
                        ByteRate(value).to_string()
                    }),
                ]
            })
            .collect();
        out.push('\n');
        out.push_str(&render_table(
            [
                "subscription",
                "type",
                "consumers",
                "backlog",
                "unacked",
                "rate",
                "throughput",
            ],
            &rows,
            colored,
        ));
    }
    out
}

/// Consumers belong to subscriptions; retain that association in every row.
fn render_consumers(subscriptions: &serde_json::Value, colored: bool) -> String {
    let mut rows = Vec::new();
    if let Some(subscriptions) = subscriptions.as_object() {
        for (subscription, details) in subscriptions {
            if let Some(consumers) = details
                .get("consumers")
                .and_then(serde_json::Value::as_array)
            {
                for (index, consumer) in consumers.iter().enumerate() {
                    rows.push([
                        consumer
                            .get("consumerName")
                            .and_then(serde_json::Value::as_str)
                            .map_or_else(|| (index + 1).to_string(), str::to_owned),
                        subscription.clone(),
                        ip_address(consumer),
                        plain_field(consumer, "clientVersion"),
                        numeric_field(consumer, "msgRateOut", |value| {
                            format!("{:.2}", MessageRate(value))
                        }),
                        numeric_field(consumer, "msgThroughputOut", |value| {
                            ByteRate(value).to_string()
                        }),
                        plain_field(consumer, "unackedMessages"),
                        plain_field(consumer, "availablePermits"),
                    ]);
                }
            }
        }
    }
    if rows.is_empty() {
        return String::new();
    }
    format!(
        "\n{}",
        render_table(
            [
                "consumer",
                "subscription",
                "ip address",
                "client version",
                "rate",
                "throughput",
                "unacked",
                "permits"
            ],
            &rows,
            colored,
        )
    )
}

fn plain_field(value: &serde_json::Value, key: &str) -> String {
    match value.get(key) {
        None | Some(serde_json::Value::Null) => "—".to_owned(),
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
    }
}

fn numeric_field(
    value: &serde_json::Value,
    key: &str,
    render: impl FnOnce(f64) -> String,
) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .map_or_else(|| "—".to_owned(), render)
}

/// Broker socket addresses commonly have a leading slash; omit their port.
fn ip_address(peer: &serde_json::Value) -> String {
    let Some(address) = peer.get("address").and_then(serde_json::Value::as_str) else {
        return "—".to_owned();
    };
    let address = address.trim_start_matches('/');
    address
        .parse::<std::net::SocketAddr>()
        .map(|socket| socket.ip())
        .or_else(|_| address.parse::<std::net::IpAddr>())
        .map_or_else(|_| "—".to_owned(), |ip| ip.to_string())
}

/// `clusters list-failure-domains`: the broker returns a map
/// `domain → { brokers: [...] }`. Render one row per broker under a `DOMAIN`
/// / `BROKERS` header, naming the domain on the first row of its group only
/// and leaving the cell blank on the following rows, so each group reads as a
/// block; a domain with no brokers prints a single `—` row.
/// Domain order follows the broker payload (`preserve_order`).
pub(crate) fn render_failure_domains(domains: &serde_json::Value, colored: bool) -> String {
    let Some(domains) = domains.as_object() else {
        // Not the documented shape: fall back to pretty JSON so nothing is
        // lost. `to_string_pretty` on a `Value` cannot fail.
        return format!(
            "{}\n",
            serde_json::to_string_pretty(domains).unwrap_or_default()
        );
    };
    let mut rows = Vec::new();
    for (domain, details) in domains {
        let brokers: Vec<String> = details
            .get("brokers")
            .and_then(serde_json::Value::as_array)
            .map(|brokers| brokers.iter().map(plain_value).collect())
            .unwrap_or_default();
        if brokers.is_empty() {
            rows.push([domain.clone(), "—".to_owned()]);
        }
        for (index, broker) in brokers.into_iter().enumerate() {
            let cell = if index == 0 {
                domain.clone()
            } else {
                String::new()
            };
            rows.push([cell, broker]);
        }
    }
    render_table(["domain", "brokers"], &rows, colored)
}

/// A scalar cell: strings bare, anything else as compact JSON.
fn plain_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
