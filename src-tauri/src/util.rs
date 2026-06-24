//! Pure helpers ported 1:1 from the Electron main/service layer. No Kafka broker
//! required — this module is the in-session, runnable "test for bugs" surface
//! (`cargo test --no-default-features --features system`).

use std::sync::LazyLock;

use regex::Regex;

// Large-message protection — identical constants to electron/services/kafka.service.ts.
pub const MAX_VALUE_SIZE: usize = 1_048_576; // 1 MB
pub const MAX_KEY_SIZE: usize = 10_240; // 10 KB
pub const MAX_HEADER_VALUE_SIZE: usize = 10_240; // 10 KB

const TRUNCATED_SUFFIX: &str = "\n...[truncated]";

/// Port of the TS `truncate`: `None` stays `None`; a string longer than `max`
/// is cut to `max` units and gets the truncation marker appended.
///
/// ponytail: counts by `char` (Unicode scalar) where the TS counts UTF-16 code
/// units. Identical for ASCII payloads, and both only ever *bound* size. Switch to
/// byte-exact only if precise parity with the Electron build is ever required.
pub fn truncate(s: Option<&str>, max: usize) -> Option<String> {
    let s = s?;
    if s.chars().count() > max {
        let head: String = s.chars().take(max).collect();
        Some(format!("{head}{TRUNCATED_SUFFIX}"))
    } else {
        Some(s.to_string())
    }
}

static RE_IPV4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?\b").unwrap());
static RE_HOST_PORT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)+:\d{2,5}\b",
    )
    .unwrap()
});
static RE_SASL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)SASL\s+\w+\s+authentication").unwrap());
static RE_STACK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\n\s*at\s+.*").unwrap());

/// Port of `sanitizeErrorMessage` (electron/main.ts) — strip IPs, host:port, SASL
/// mechanism names and stack frames before an error crosses the IPC boundary.
pub fn sanitize_error(message: &str) -> String {
    let s = RE_IPV4.replace_all(message, "<redacted>");
    let s = RE_HOST_PORT.replace_all(&s, "<redacted>");
    let s = RE_SASL.replace_all(&s, "SASL authentication");
    let s = RE_STACK.replace_all(&s, "");
    s.into_owned()
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_librdkafka(self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        }
    }
}

/// Same matrix kafkajs derives implicitly: SSL and/or SASL -> security.protocol.
pub fn security_protocol(has_ssl: bool, has_sasl: bool) -> SecurityProtocol {
    match (has_ssl, has_sasl) {
        (true, true) => SecurityProtocol::SaslSsl,
        (true, false) => SecurityProtocol::Ssl,
        (false, true) => SecurityProtocol::SaslPlaintext,
        (false, false) => SecurityProtocol::Plaintext,
    }
}

/// kafkajs lower-case mechanism -> librdkafka upper-case. Errors on anything the
/// app does not support, mirroring the connection type's allowed set.
pub fn sasl_mechanism(mechanism: &str) -> Result<&'static str, String> {
    match mechanism.to_ascii_lowercase().as_str() {
        "plain" => Ok("PLAIN"),
        "scram-sha-256" => Ok("SCRAM-SHA-256"),
        "scram-sha-512" => Ok("SCRAM-SHA-512"),
        other => Err(format!("Unsupported SASL mechanism: {other}")),
    }
}

/// Port of the TS `nextOffset` calc: `hasMore && lastOffset ? lastOffset + 1 : null`.
pub fn next_offset(has_more: bool, last_offset: Option<i64>) -> Option<String> {
    match (has_more, last_offset) {
        (true, Some(o)) => Some((o + 1).to_string()),
        _ => None,
    }
}

/// Group header (key, value) pairs by key, comma-join duplicates, then truncate.
/// Kafka allows repeated header keys; kafkajs collapses them into a `Buffer[]` and
/// `value.toString()` comma-joins before truncation, so a plain map insert (last
/// value wins) would silently drop earlier values. This mirrors the Electron output.
pub fn join_headers<I>(pairs: I) -> std::collections::HashMap<String, String>
where
    I: IntoIterator<Item = (String, String)>,
{
    use std::collections::HashMap;
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in pairs {
        grouped.entry(k).or_default().push(v);
    }
    grouped
        .into_iter()
        .map(|(k, vals)| {
            (
                k,
                truncate(Some(&vals.join(",")), MAX_HEADER_VALUE_SIZE).unwrap_or_default(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_passes_short_and_none() {
        assert_eq!(truncate(Some("hello"), 10).as_deref(), Some("hello"));
        assert_eq!(truncate(None, 10), None);
        assert_eq!(truncate(Some(""), 10).as_deref(), Some(""));
    }

    #[test]
    fn truncate_cuts_long_strings_and_marks_them() {
        assert_eq!(truncate(Some("abcdef"), 3).unwrap(), format!("abc{TRUNCATED_SUFFIX}"));
        // boundary: exactly `max` chars is NOT truncated
        assert_eq!(truncate(Some("abc"), 3).as_deref(), Some("abc"));
    }

    #[test]
    fn sanitize_strips_ip_host_sasl_and_stack() {
        assert_eq!(
            sanitize_error("connect ECONNREFUSED 10.0.0.5:9092"),
            "connect ECONNREFUSED <redacted>"
        );
        assert_eq!(sanitize_error("broker kafka-1.example.com:9093 down"), "broker <redacted> down");
        assert_eq!(sanitize_error("SASL SCRAM authentication failed"), "SASL authentication failed");
        assert_eq!(sanitize_error("boom\n    at foo (bar.js:1)"), "boom");
    }

    #[test]
    fn security_protocol_matrix() {
        assert_eq!(security_protocol(false, false).as_librdkafka(), "plaintext");
        assert_eq!(security_protocol(true, false).as_librdkafka(), "ssl");
        assert_eq!(security_protocol(false, true).as_librdkafka(), "sasl_plaintext");
        assert_eq!(security_protocol(true, true).as_librdkafka(), "sasl_ssl");
    }

    #[test]
    fn sasl_mechanism_maps_and_rejects() {
        assert_eq!(sasl_mechanism("plain").unwrap(), "PLAIN");
        assert_eq!(sasl_mechanism("SCRAM-SHA-256").unwrap(), "SCRAM-SHA-256");
        assert_eq!(sasl_mechanism("scram-sha-512").unwrap(), "SCRAM-SHA-512");
        assert!(sasl_mechanism("gssapi").is_err());
    }

    #[test]
    fn next_offset_only_when_more() {
        assert_eq!(next_offset(true, Some(41)).as_deref(), Some("42"));
        assert_eq!(next_offset(false, Some(41)), None);
        assert_eq!(next_offset(true, None), None);
    }

    #[test]
    fn join_headers_preserves_duplicate_keys() {
        let h = join_headers([
            ("k".to_string(), "a".to_string()),
            ("k".to_string(), "b".to_string()),
            ("x".to_string(), "y".to_string()),
        ]);
        assert_eq!(h.get("k").unwrap(), "a,b");
        assert_eq!(h.get("x").unwrap(), "y");
    }
}
