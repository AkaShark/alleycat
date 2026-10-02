//! HTTP clients for local agent backends and optional remote endpoints.
//!
//! Loopback services must remain reachable even when the host enables a
//! system/environment proxy. Remote endpoints keep reqwest's proxy policy.

use reqwest::{Client, ClientBuilder, Url};

pub fn builder_for(base_url: &str) -> ClientBuilder {
    bypass_loopback_proxy(Client::builder(), base_url)
}

fn bypass_loopback_proxy(builder: ClientBuilder, base_url: &str) -> ClientBuilder {
    if Url::parse(base_url).is_ok_and(|url| is_loopback(&url)) {
        builder.no_proxy()
    } else {
        builder
    }
}

pub fn is_loopback(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("localhost.")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
            std::net::IpAddr::V4(ip) => ip.is_loopback(),
            std::net::IpAddr::V6(ip) => {
                ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn only_loopback_urls_bypass_proxies() {
        for url in [
            "http://localhost:8080",
            "https://LOCALHOST./",
            "http://127.0.0.1",
            "http://127.1.2.3",
            "http://[::1]",
            "http://[::ffff:127.0.0.1]",
        ] {
            assert!(is_loopback(&Url::parse(url).unwrap()), "{url}");
        }
        for url in [
            "https://worker.example",
            "http://localhost.example",
            "http://192.168.1.2",
            "http://10.0.0.1",
            "http://[2001:db8::1]",
        ] {
            assert!(!is_loopback(&Url::parse(url).unwrap()), "{url}");
        }
    }

    async fn responder(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 4096];
            let n = stream.read(&mut bytes).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8_lossy(&bytes[..n]).to_string()
        });
        (url, task)
    }

    #[tokio::test]
    async fn local_requests_bypass_proxy_and_remote_requests_keep_it() {
        let (local, local_task) = responder("local").await;
        let (proxy, proxy_task) = responder("proxy").await;
        let builder = || {
            Client::builder()
                .proxy(reqwest::Proxy::all(&proxy).unwrap())
                .timeout(Duration::from_secs(2))
        };
        let client = bypass_loopback_proxy(builder(), &local).build().unwrap();
        assert_eq!(
            client
                .get(&local)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "local"
        );
        assert!(local_task.await.unwrap().starts_with("GET / HTTP/1.1"));
        assert!(
            !proxy_task.is_finished(),
            "loopback request reached the proxy"
        );

        let remote = "http://unresolvable.invalid/health";
        let client = bypass_loopback_proxy(builder(), remote).build().unwrap();
        assert_eq!(
            client
                .get(remote)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "proxy"
        );
        assert!(
            proxy_task
                .await
                .unwrap()
                .starts_with("GET http://unresolvable.invalid/health")
        );
    }
}
