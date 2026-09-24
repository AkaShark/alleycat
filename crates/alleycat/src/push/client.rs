//! Signed HTTP calls to the push Worker (spec §5.2 / §7.1) and the mapping
//! from HTTP outcomes to outbox decisions (spec §6.3).

use std::time::Duration;

use iroh::SecretKey;
use reqwest::{Method, StatusCode, Url};

use super::signing::{random_hex32, sign_host_request};

/// Where and how to reach the Worker for one delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTarget {
    pub base_url: Url,
    pub timeout: Duration,
}

impl WorkerTarget {
    pub fn host(&self) -> Option<String> {
        self.base_url.host_str().map(str::to_string)
    }

    /// `aud` for signatures: `scheme://host[:port]`, no trailing slash
    /// (default ports omitted, like WHATWG `URL.origin`).
    pub fn origin(&self) -> String {
        self.base_url.origin().ascii_serialization()
    }
}

/// Accept `https://…`, or `http://` to a loopback host (local testing).
pub fn parse_worker_url(raw: &str) -> Result<Url, String> {
    let url =
        Url::parse(raw.trim()).map_err(|error| format!("invalid push worker_url: {error}"))?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err("push worker_url must not contain a query or fragment".into());
    }
    match url.scheme() {
        "https" => Ok(url),
        "http" if is_loopback(&url) => Ok(url),
        _ => Err("push worker_url must use https (http only for loopback hosts)".into()),
    }
}

fn is_loopback(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Outcome of one attempt, already classified for the outbox.
#[derive(Debug, Clone, PartialEq)]
pub enum Delivery {
    /// 2xx; `body` is the parsed JSON response (or `Null`).
    Success {
        status: u16,
        body: serde_json::Value,
    },
    /// Network error, timeout, 408, 429 or 5xx.
    Retry {
        status: Option<u16>,
        retry_after: Option<Duration>,
        error: String,
        /// The Worker may have processed the request even though we got no
        /// usable answer (408/5xx, timeouts, resets) — as opposed to a
        /// refused connection or a 429 rejected before processing.
        maybe_delivered: bool,
    },
    /// Any other 4xx (or a URL we cannot build): drop and log.
    Permanent { status: Option<u16>, error: String },
}

pub fn classify(status: StatusCode, retry_after: Option<Duration>, body: &str) -> Delivery {
    let code = status.as_u16();
    if status.is_success() {
        return Delivery::Success {
            status: code,
            body: serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
        };
    }
    let error = format!("HTTP {code}: {}", error_code(body));
    if code == 408 || code == 429 || status.is_server_error() {
        Delivery::Retry {
            status: Some(code),
            retry_after,
            error,
            maybe_delivered: code != 429,
        }
    } else {
        Delivery::Permanent {
            status: Some(code),
            error,
        }
    }
}

/// `{"error":"<code>"}` → `<code>`, otherwise a short excerpt.
fn error_code(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
        .unwrap_or_else(|| body.chars().take(120).collect())
}

/// `Retry-After` as delta-seconds. HTTP-dates are ignored (the Worker sends
/// seconds); the exponential backoff still applies.
pub fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    value?.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// Send one signed request. `path` is relative to the Worker base URL (e.g.
/// `/v2/events`); the signature covers the Worker origin and the full URL
/// path the Worker sees. Timestamp and nonce are fresh on every call, so a
/// retry is never a replay.
pub async fn send(
    http: &reqwest::Client,
    secret_key: &SecretKey,
    target: &WorkerTarget,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    now: u64,
) -> Delivery {
    let full = format!("{}{}", target.base_url.as_str().trim_end_matches('/'), path);
    let url = match Url::parse(&full) {
        Ok(url) => url,
        Err(error) => {
            return Delivery::Permanent {
                status: None,
                error: format!("building worker URL: {error}"),
            };
        }
    };
    let body_bytes = body.unwrap_or_default();
    let signed = sign_host_request(
        secret_key,
        &target.origin(),
        method.as_str(),
        url.path(),
        &body_bytes,
        now,
        &random_hex32(),
    );
    let mut request = http.request(method, url).timeout(target.timeout);
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    if !body_bytes.is_empty() {
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_bytes);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return Delivery::Retry {
                status: None,
                retry_after: None,
                error: format!("request failed: {}", without_url(&error)),
                maybe_delivered: !error.is_connect(),
            };
        }
    };
    let status = response.status();
    let retry_after = parse_retry_after(
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
    );
    let text = match response.text().await {
        Ok(text) => text,
        Err(error) if status.is_success() => {
            return Delivery::Retry {
                status: Some(status.as_u16()),
                retry_after,
                error: format!("reading response: {}", without_url(&error)),
                maybe_delivered: true,
            };
        }
        Err(_) => String::new(),
    };
    classify(status, retry_after, &text)
}

fn without_url(error: &reqwest::Error) -> String {
    let mut error_text = error.to_string();
    if let Some(url) = error.url() {
        error_text = error_text.replace(url.as_str(), "<worker>");
    }
    error_text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_follows_spec() {
        assert!(matches!(
            classify(StatusCode::CREATED, None, r#"{"subscriptionId":"sub_1"}"#),
            Delivery::Success { status: 201, body } if body["subscriptionId"] == "sub_1"
        ));
        assert!(matches!(
            classify(StatusCode::OK, None, "not json"),
            Delivery::Success {
                body: serde_json::Value::Null,
                ..
            }
        ));
        for code in [408u16, 429, 500, 502, 503] {
            let status = StatusCode::from_u16(code).unwrap();
            assert!(
                matches!(classify(status, None, ""), Delivery::Retry { status: Some(c), .. } if c == code),
                "{code} should retry"
            );
        }
        for code in [400u16, 401, 403, 404, 410, 413] {
            let status = StatusCode::from_u16(code).unwrap();
            assert!(
                matches!(classify(status, None, r#"{"error":"forbidden"}"#), Delivery::Permanent { status: Some(c), ref error } if c == code && error.contains("forbidden")),
                "{code} should be permanent"
            );
        }
        let retry = classify(
            StatusCode::TOO_MANY_REQUESTS,
            Some(Duration::from_secs(7)),
            r#"{"error":"rate_limited"}"#,
        );
        assert_eq!(
            retry,
            Delivery::Retry {
                status: Some(429),
                retry_after: Some(Duration::from_secs(7)),
                error: "HTTP 429: rate_limited".into(),
                maybe_delivered: false,
            }
        );
        assert!(matches!(
            classify(StatusCode::BAD_GATEWAY, None, ""),
            Delivery::Retry {
                maybe_delivered: true,
                ..
            }
        ));
    }

    #[test]
    fn retry_after_parses_seconds_only() {
        assert_eq!(parse_retry_after(Some("30")), Some(Duration::from_secs(30)));
        assert_eq!(parse_retry_after(Some(" 5 ")), Some(Duration::from_secs(5)));
        assert_eq!(
            parse_retry_after(Some("Wed, 21 Oct 2015 07:28:00 GMT")),
            None
        );
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn origin_drops_path_and_default_port() {
        let target = |raw: &str| WorkerTarget {
            base_url: parse_worker_url(raw).unwrap(),
            timeout: Duration::from_secs(1),
        };
        assert_eq!(
            target("https://push.example.test").origin(),
            "https://push.example.test"
        );
        assert_eq!(
            target("https://push.example.test:443/prefix/").origin(),
            "https://push.example.test"
        );
        assert_eq!(
            target("http://127.0.0.1:8787/x").origin(),
            "http://127.0.0.1:8787"
        );
    }

    #[test]
    fn worker_url_requires_https_except_loopback() {
        assert!(parse_worker_url("https://push.example.workers.dev").is_ok());
        assert!(parse_worker_url("https://push.example/prefix/").is_ok());
        assert!(parse_worker_url("http://127.0.0.1:8787").is_ok());
        assert!(parse_worker_url("http://localhost:8787").is_ok());
        assert!(parse_worker_url("http://[::1]:8787").is_ok());
        assert!(parse_worker_url("http://push.example").is_err());
        assert!(parse_worker_url("ftp://push.example").is_err());
        assert!(parse_worker_url("https://push.example/?a=b").is_err());
        assert!(parse_worker_url("not a url").is_err());
    }
}
