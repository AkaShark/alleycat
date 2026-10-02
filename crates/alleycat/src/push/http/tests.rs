use super::*;
use reqwest::{Proxy, Url};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

fn target(url: &str) -> WorkerTarget {
    WorkerTarget {
        base_url: Url::parse(url).unwrap(),
        timeout: Duration::from_secs(2),
    }
}

async fn proxy(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).await.unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            reader
                .get_mut()
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    (url, task)
}

fn via(proxy: &str) -> Result<Client, reqwest::Error> {
    Client::builder()
        .proxy(Proxy::all(proxy)?)
        .timeout(Duration::from_secs(2))
        .build()
}

async fn body(client: Client, target: &WorkerTarget) -> String {
    client
        .get(target.base_url.clone())
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
async fn refresh_picks_up_new_proxy_and_worker_origin() {
    let (a, task_a) = proxy("A").await;
    let (b, task_b) = proxy("B").await;
    let (direct, task_direct) = proxy("direct").await;
    let port = Url::parse(&direct).unwrap().port().unwrap();
    let http = PushHttpClient::default();
    let worker = target(&format!("http://worker.invalid:{port}"));
    let now = Instant::now();
    assert_eq!(
        body(
            http.get_or_build(&worker, now, || via(&a)).unwrap(),
            &worker
        )
        .await,
        "A"
    );
    // Keep pooling within the interval, even if the proxy setting changes.
    assert_eq!(
        body(
            http.get_or_build(&worker, now + Duration::from_secs(29), || via(&b))
                .unwrap(),
            &worker
        )
        .await,
        "A"
    );
    assert_eq!(
        body(
            http.get_or_build(&worker, now + REFRESH_INTERVAL, || via(&b))
                .unwrap(),
            &worker
        )
        .await,
        "B"
    );

    // Disabling a proxy must also take effect: a refreshed client goes to
    // the destination directly instead of keeping the old proxy connection.
    assert_eq!(
        body(
            http.get_or_build(&worker, now + REFRESH_INTERVAL * 2, || {
                Client::builder()
                    .no_proxy()
                    .resolve(
                        "worker.invalid",
                        format!("127.0.0.1:{port}").parse().unwrap(),
                    )
                    .timeout(Duration::from_secs(2))
                    .build()
            })
            .unwrap(),
            &worker
        )
        .await,
        "direct"
    );

    let other = target("http://other-worker.invalid");
    assert_eq!(
        body(
            http.get_or_build(&other, now + REFRESH_INTERVAL * 2, || via(&a))
                .unwrap(),
            &other
        )
        .await,
        "A"
    );
    task_a.abort();
    task_b.abort();
    task_direct.abort();
}

#[tokio::test]
async fn network_failure_refreshes_before_the_next_retry() {
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_proxy = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let (live, task) = proxy("recovered").await;
    let http = PushHttpClient::default();
    let worker = target("http://worker.invalid");
    let now = Instant::now();
    http.get_or_build(&worker, now, || via(&dead_proxy))
        .unwrap();
    let result = http
        .send(&SecretKey::generate(), &worker, Method::GET, "/", None, 1)
        .await;
    assert!(matches!(
        result,
        Delivery::Retry {
            status: None,
            maybe_delivered: false,
            ..
        }
    ));
    assert_eq!(
        body(
            http.get_or_build(&worker, now, || via(&live)).unwrap(),
            &worker
        )
        .await,
        "recovered"
    );
    task.abort();
}

#[tokio::test]
async fn refreshed_clients_never_follow_redirects() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker = target(&format!("http://{}", source.local_addr().unwrap()));
    let location = format!("http://{}", destination.local_addr().unwrap());
    let task = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = source.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                if reader.read_line(&mut line).await.unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            reader.get_mut().write_all(format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ).as_bytes()).await.unwrap();
        }
    });
    let http = PushHttpClient::default();
    for _ in 0..2 {
        let response = http
            .client_for(&worker)
            .unwrap()
            .get(worker.base_url.clone())
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        *http.cached.lock().unwrap() = None;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), destination.accept())
            .await
            .is_err()
    );
    task.await.unwrap();
}

/// Opt-in smoke test against a public URL, without credentials or signatures.
/// Clear proxy env vars and set ALLEYCAT_TEST_PROXY_URL before running this test.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "requires a configured macOS HTTP/HTTPS system proxy and a public test URL"]
async fn macos_system_proxy_smoke() {
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ] {
        assert!(
            std::env::var_os(key).is_none(),
            "unset {key} to test system settings"
        );
    }
    let worker = target(&std::env::var("ALLEYCAT_TEST_PROXY_URL").expect("public test URL"));
    let response = PushHttpClient::default()
        .client_for(&worker)
        .unwrap()
        .get(worker.base_url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .unwrap();
    eprintln!(
        "system proxy: HTTP {}, peer {:?}",
        response.status(),
        response.remote_addr()
    );
}
