//! The three spike commands (`connect`, `get_topics`, `get_messages`) implemented
//! on rust-rdkafka, kept faithful to `electron/services/kafka.service.ts`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tauri::State;

use crate::util::{
    join_headers, next_offset, sanitize_error, sasl_mechanism, security_protocol, truncate,
    MAX_KEY_SIZE, MAX_VALUE_SIZE,
};

// ---- connection state -------------------------------------------------------

/// A "connection" in the spike is just a validated `ClientConfig`, keyed by id.
///
/// ponytail: kafkajs keeps a live admin + producer per connection here; the spike
/// rebuilds a lightweight consumer per call instead. Pool live clients only if the
/// real migration needs the throughput.
pub struct AppState {
    pub clients: Mutex<HashMap<String, ClientConfig>>,
}

// ---- wire types (mirror shared/types.ts) -----------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    pub ca: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub passphrase: Option<String>,
    pub reject_unauthorized: Option<bool>,
}

/// `ssl` is `boolean | TLSConfig` in the TS connection type.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Ssl {
    Toggle(bool),
    Config(TlsConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslConfig {
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnection {
    pub id: String,
    #[serde(default)]
    pub brokers: Vec<String>,
    pub ssl: Option<Ssl>,
    pub sasl: Option<SaslConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageOptions {
    pub partition: Option<i32>,
    pub from_offset: Option<String>,
    pub from_timestamp: Option<i64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutMessage {
    pub partition: i32,
    pub offset: String,
    pub timestamp: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageFetchResult {
    pub messages: Vec<OutMessage>,
    pub has_more: bool,
    pub next_offset: Option<String>,
    pub next_partition: Option<i32>,
}

// ---- timeouts / limits ------------------------------------------------------

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const META_TIMEOUT: Duration = Duration::from_secs(15);
const WATERMARK_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const OVERALL_TIMEOUT: Duration = Duration::from_secs(30);

const TOPIC_NAME_MAX: usize = 249;
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;

/// Port of `validateTopicName` (electron/main.ts).
fn validate_topic(topic: &str) -> Result<(), String> {
    let ok = !topic.is_empty()
        && topic.len() <= TOPIC_NAME_MAX
        && topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else {
        Err("Invalid topic name".into())
    }
}

// ---- config building --------------------------------------------------------

/// Translate a `KafkaConnection` into a librdkafka `ClientConfig` — faithful to
/// `createKafkaClient` (brokers, security.protocol, SASL, TLS).
pub fn build_config(conn: &KafkaConnection) -> Result<ClientConfig, String> {
    if conn.brokers.is_empty() {
        return Err("At least one broker is required".into());
    }
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", conn.brokers.join(","));
    cfg.set("client.id", format!("topiq-explorer-{}", conn.id));
    cfg.set("socket.timeout.ms", "30000");

    let (has_ssl, tls): (bool, Option<&TlsConfig>) = match &conn.ssl {
        Some(Ssl::Toggle(b)) => (*b, None),
        Some(Ssl::Config(c)) => (true, Some(c)),
        None => (false, None),
    };
    let has_sasl = conn.sasl.is_some();

    cfg.set(
        "security.protocol",
        security_protocol(has_ssl, has_sasl).as_librdkafka(),
    );

    if let Some(sasl) = &conn.sasl {
        // ponytail: secrets are set as plain string config values, which librdkafka
        // never range-validates, so its value-echoing config errors can't leak them
        // past sanitize_error. Keep any future credential keys string-valued too.
        cfg.set("sasl.mechanism", sasl_mechanism(&sasl.mechanism)?);
        cfg.set("sasl.username", sasl.username.clone());
        cfg.set("sasl.password", sasl.password.clone());
    }

    if let Some(tls) = tls {
        if let Some(ca) = &tls.ca {
            cfg.set("ssl.ca.pem", ca.clone());
        }
        if let Some(cert) = &tls.cert {
            cfg.set("ssl.certificate.pem", cert.clone());
        }
        if let Some(key) = &tls.key {
            cfg.set("ssl.key.pem", key.clone());
        }
        if let Some(pass) = &tls.passphrase {
            cfg.set("ssl.key.password", pass.clone());
        }
        if tls.reject_unauthorized == Some(false) {
            cfg.set("enable.ssl.certificate.verification", "false");
        }
    }

    Ok(cfg)
}

static GROUP_SEQ: AtomicU64 = AtomicU64::new(0);

/// An assign-only consumer (never `subscribe`d, so no real group coordination),
/// but librdkafka still requires a `group.id`, so hand it a unique throwaway one.
fn make_consumer(cfg: &ClientConfig) -> Result<BaseConsumer, String> {
    let seq = GROUP_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut c = cfg.clone();
    c.set("group.id", format!("topiq-explorer-spike-{nanos}-{seq}"));
    c.set("enable.auto.commit", "false");
    c.create::<BaseConsumer>()
        .map_err(|e| sanitize_error(&e.to_string()))
}

fn get_config(state: &State<'_, AppState>, connection_id: &str) -> Result<ClientConfig, String> {
    state
        .clients
        .lock()
        .map_err(|_| "connection state poisoned".to_string())?
        .get(connection_id)
        .cloned()
        .ok_or_else(|| "Not connected".to_string())
}

// ---- commands ---------------------------------------------------------------

#[tauri::command]
pub async fn connect(
    state: State<'_, AppState>,
    connection: KafkaConnection,
) -> Result<(), String> {
    let cfg = build_config(&connection)?;
    let id = connection.id.clone();
    let probe = cfg.clone();
    // Smoke-test connectivity/credentials before storing (mirrors testConnection()).
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let consumer = make_consumer(&probe)?;
        consumer
            .fetch_metadata(None, CONNECT_TIMEOUT)
            .map_err(|e| sanitize_error(&e.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())??;

    state
        .clients
        .lock()
        .map_err(|_| "connection state poisoned".to_string())?
        .insert(id, cfg);
    Ok(())
}

#[tauri::command]
pub async fn get_topics(
    state: State<'_, AppState>,
    connection_id: String,
) -> Result<Vec<String>, String> {
    let cfg = get_config(&state, &connection_id)?;
    tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let consumer = make_consumer(&cfg)?;
        let md = consumer
            .fetch_metadata(None, META_TIMEOUT)
            .map_err(|e| sanitize_error(&e.to_string()))?;
        let mut topics: Vec<String> = md.topics().iter().map(|t| t.name().to_string()).collect();
        topics.sort();
        Ok(topics)
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn get_messages(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    options: Option<MessageOptions>,
) -> Result<MessageFetchResult, String> {
    validate_topic(&topic)?;
    let cfg = get_config(&state, &connection_id)?;
    let opts = options.unwrap_or_default();
    let limit = opts.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    tokio::task::spawn_blocking(move || fetch_messages(&cfg, &topic, &opts, limit))
        .await
        .map_err(|e| e.to_string())?
}

// ---- message fetch ----------------------------------------------------------

fn empty_result() -> MessageFetchResult {
    MessageFetchResult {
        messages: vec![],
        has_more: false,
        next_offset: None,
        next_partition: None,
    }
}

/// Faithful port of `getMessages`: pre-check watermarks, build a per-partition seek
/// map (timestamp- or offset-based), assign, and poll up to `limit` with idle and
/// overall timeouts.
fn fetch_messages(
    cfg: &ClientConfig,
    topic: &str,
    opts: &MessageOptions,
    limit: usize,
) -> Result<MessageFetchResult, String> {
    let consumer = make_consumer(cfg)?;

    // Partitions for the topic, filtered if a specific partition was requested.
    let md = consumer
        .fetch_metadata(Some(topic), META_TIMEOUT)
        .map_err(|e| sanitize_error(&e.to_string()))?;
    let topic_md = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("Topic not found: {topic}"))?;
    // An unknown topic comes back as an entry with an error + zero partitions; surface
    // it instead of returning a silent empty page (the Electron admin path errors here).
    if let Some(err) = topic_md.error() {
        return Err(sanitize_error(&format!("Topic error: {err:?}")));
    }
    let mut partitions: Vec<i32> = topic_md.partitions().iter().map(|p| p.id()).collect();
    if let Some(p) = opts.partition {
        partitions.retain(|&x| x == p);
    }
    partitions.sort_unstable();
    if partitions.is_empty() {
        return Ok(empty_result());
    }

    // Resolve seek offsets and sum the expected message count (capped at limit).
    // `filter(ts > 0)` mirrors the TS `if (fromTimestamp)` truthiness (0 falls through).
    let ts_seek = match opts.from_timestamp.filter(|&ts| ts > 0) {
        Some(ts) => Some(resolve_timestamps(&consumer, topic, &partitions, ts)?),
        None => None,
    };
    // Mirror Electron's validateMessageOptions: fromOffset must be all digits — reject
    // negatives/garbage with an error rather than silently starting from the low offset.
    let from_offset = match opts.from_offset.as_deref() {
        Some(s) => Some(s.parse::<u64>().map(|v| v as i64).map_err(|_| "Invalid offset".to_string())?),
        None => None,
    };

    let mut assignment = TopicPartitionList::new();
    let mut total_expected: i64 = 0;
    for &p in &partitions {
        let (low, high) = consumer
            .fetch_watermarks(topic, p, WATERMARK_TIMEOUT)
            .map_err(|e| sanitize_error(&e.to_string()))?;
        let seek = match &ts_seek {
            // librdkafka returns -1 when no message exists at/after the timestamp.
            Some(map) => match map.get(&p).copied() {
                Some(o) if o >= 0 => o,
                _ => high,
            },
            None => from_offset.unwrap_or(low),
        };
        let seek = seek.clamp(low, high);
        if high - seek > 0 {
            total_expected += high - seek;
        }
        assignment
            .add_partition_offset(topic, p, Offset::Offset(seek))
            .map_err(|e| e.to_string())?;
    }

    if total_expected == 0 {
        return Ok(empty_result());
    }
    let cap = (total_expected as usize).min(limit);

    consumer
        .assign(&assignment)
        .map_err(|e| sanitize_error(&e.to_string()))?;

    let mut messages: Vec<OutMessage> = Vec::with_capacity(cap.min(1024));
    let mut last_offset: Option<i64> = None;
    let mut last_partition: Option<i32> = None;
    let deadline = Instant::now() + OVERALL_TIMEOUT;

    while messages.len() < cap {
        if Instant::now() >= deadline {
            break;
        }
        match consumer.poll(IDLE_TIMEOUT) {
            None => break, // idle: assume the assigned ranges are drained
            Some(Err(e)) => return Err(sanitize_error(&e.to_string())),
            Some(Ok(m)) => {
                let p = m.partition();
                // Defensive: assignment already scoped partitions, but keep the filter.
                if let Some(fp) = opts.partition {
                    if p != fp {
                        continue;
                    }
                }
                last_offset = Some(m.offset());
                last_partition = Some(p);
                messages.push(to_out_message(&m));
            }
        }
    }

    // Mirror the TS: reaching the fetch limit means "more available".
    let has_more = messages.len() >= limit;
    let _ = consumer.unassign();

    Ok(MessageFetchResult {
        next_offset: next_offset(has_more, last_offset),
        next_partition: if has_more { last_partition } else { None },
        has_more,
        messages,
    })
}

fn to_out_message(m: &rdkafka::message::BorrowedMessage<'_>) -> OutMessage {
    let header_pairs: Vec<(String, String)> = m
        .headers()
        .map(|hs| {
            (0..hs.count())
                .map(|i| {
                    let h = hs.get(i);
                    let value = h
                        .value
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    (h.key.to_string(), value)
                })
                .collect()
        })
        .unwrap_or_default();
    let headers = join_headers(header_pairs);
    let key = m.key().map(|b| String::from_utf8_lossy(b).into_owned());
    let value = m.payload().map(|b| String::from_utf8_lossy(b).into_owned());
    let timestamp = m
        .timestamp()
        .to_millis()
        .map(|ms| ms.to_string())
        .unwrap_or_default();
    OutMessage {
        partition: m.partition(),
        offset: m.offset().to_string(),
        timestamp,
        key: truncate(key.as_deref(), MAX_KEY_SIZE),
        value: truncate(value.as_deref(), MAX_VALUE_SIZE),
        headers,
    }
}

/// Resolve a wall-clock timestamp into a per-partition start offset, like kafkajs'
/// `fetchTopicOffsetsByTimestamp`.
fn resolve_timestamps(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    ts: i64,
) -> Result<HashMap<i32, i64>, String> {
    let mut tpl = TopicPartitionList::new();
    for &p in partitions {
        tpl.add_partition_offset(topic, p, Offset::Offset(ts))
            .map_err(|e| e.to_string())?;
    }
    let resolved = consumer
        .offsets_for_times(tpl, META_TIMEOUT)
        .map_err(|e| sanitize_error(&e.to_string()))?;
    let mut out = HashMap::new();
    for elem in resolved.elements() {
        if let Offset::Offset(o) = elem.offset() {
            out.insert(elem.partition(), o);
        }
    }
    Ok(out)
}
