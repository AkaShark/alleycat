//! Reuse connections without pinning a long-lived daemon to stale proxies.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use iroh::SecretKey;
use reqwest::{Client, Method};

use super::client::{self, Delivery, WorkerTarget};

const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct PushHttpClient {
    cached: Mutex<Option<CachedClient>>,
}

struct CachedClient {
    client: Client,
    origin: String,
    created_at: Instant,
}

impl PushHttpClient {
    fn get_or_build(
        &self,
        target: &WorkerTarget,
        now: Instant,
        build: impl FnOnce() -> Result<Client, reqwest::Error>,
    ) -> Result<Client, reqwest::Error> {
        let mut cached = self.cached.lock().unwrap();
        if let Some(entry) = cached.as_ref()
            && entry.origin == target.origin()
            && now.saturating_duration_since(entry.created_at) < REFRESH_INTERVAL
        {
            return Ok(entry.client.clone());
        }
        // Building a new client re-reads the environment and (on macOS) the
        // system HTTP/HTTPS proxy settings. No request runs under this lock.
        let client = build()?;
        *cached = Some(CachedClient {
            client: client.clone(),
            origin: target.origin(),
            created_at: now,
        });
        Ok(client)
    }

    fn client_for(&self, target: &WorkerTarget) -> Result<Client, reqwest::Error> {
        self.get_or_build(target, Instant::now(), || {
            alleycat_bridge_core::http_client::builder_for(target.base_url.as_str())
                .user_agent(format!(
                    "{}/{} push",
                    crate::binary_name(),
                    crate::binary_version()
                ))
                // Signed headers cover this origin/path. Never fall back to
                // Client::default(), which would enable redirects again.
                .redirect(reqwest::redirect::Policy::none())
                .build()
        })
    }

    fn invalidate_after(&self, delivery: &Delivery) {
        if matches!(delivery, Delivery::Retry { status: None, .. }) {
            *self.cached.lock().unwrap() = None;
        }
    }

    pub(super) async fn send(
        &self,
        secret_key: &SecretKey,
        target: &WorkerTarget,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        now: u64,
    ) -> Delivery {
        let http = match self.client_for(target) {
            Ok(http) => http,
            Err(error) => {
                return Delivery::Retry {
                    status: None,
                    retry_after: None,
                    error: format!("building push HTTP client: {error}"),
                    maybe_delivered: false,
                };
            }
        };
        let delivery = client::send(&http, secret_key, target, method, path, body, now).await;
        // Let the existing outbox schedule the retry; only refresh transport.
        self.invalidate_after(&delivery);
        delivery
    }
}

#[cfg(test)]
mod tests;
