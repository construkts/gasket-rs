use gasket::daemon::Daemon;
use gasket::metrics::{Reading, Readings};
use std::fmt::Write as _;
use std::{net::SocketAddr, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::warn;

fn sanitize_stage_name(raw: &str) -> String {
    raw.replace('-', "_")
}

fn write_metric(output: &mut String, stage: &str, metric: &str, kind: &str, value: &str) {
    let name = format!("{}_{}", sanitize_stage_name(stage), metric);

    let _ = writeln!(output, "# HELP {name} {metric} {kind} for stage {stage}");
    let _ = writeln!(output, "# TYPE {name} {kind}");
    let _ = writeln!(output, "{name} {value}");
}

/// Serialize every stage's metrics into the Prometheus text exposition format.
fn render_stages<'a>(stages: impl Iterator<Item = (&'a str, Readings)>) -> String {
    let mut out = String::new();

    for (stage, readings) in stages {
        for (key, reading) in readings {
            match reading {
                Reading::Count(x) => write_metric(&mut out, stage, key, "counter", &x.to_string()),
                Reading::Gauge(x) => write_metric(&mut out, stage, key, "gauge", &x.to_string()),
                _ => (),
            }
        }
    }

    out
}

/// Render the daemon's current metrics into the Prometheus text exposition
/// format. Stages whose metrics can't be read (eg: already torn down) are
/// skipped rather than failing the whole scrape.
fn render(source: &Daemon) -> String {
    render_stages(source.tethers().filter_map(|tether| {
        match tether.read_metrics() {
            Ok(readings) => Some((tether.name(), readings)),
            Err(err) => {
                warn!(stage = tether.name(), ?err, "couldn't read stage metrics");
                None
            }
        }
    }))
}

async fn handle_connection(mut socket: tokio::net::TcpStream, source: &Daemon) -> std::io::Result<()> {
    // Drain (and ignore) the request. Metrics are served on any request, so we
    // only need to consume enough to let the client finish sending.
    let mut buf = [0u8; 1024];
    let _ = socket.read(&mut buf).await?;

    let body = render(source);

    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        body,
    );

    socket.write_all(response.as_bytes()).await?;
    socket.flush().await?;

    Ok(())
}

pub async fn serve(addr: SocketAddr, source: Arc<Daemon>) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("can't bind prometheus metrics endpoint");

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                warn!(?err, "error accepting metrics connection");
                continue;
            }
        };

        let source = Arc::clone(&source);

        tokio::spawn(async move {
            if let Err(err) = handle_connection(socket, &source).await {
                warn!(?err, "error serving metrics request");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gasket::metrics::Reading;

    #[test]
    fn renders_counter_and_gauge_in_text_format() {
        let stages = vec![(
            "my-stage",
            vec![
                ("received_blocks", Reading::Count(42)),
                ("chain_tip", Reading::Gauge(-7)),
            ],
        )];

        let out = render_stages(stages.into_iter());

        assert!(out.contains("# HELP my_stage_received_blocks received_blocks counter for stage my-stage"));
        assert!(out.contains("# TYPE my_stage_received_blocks counter"));
        assert!(out.contains("my_stage_received_blocks 42"));

        assert!(out.contains("# TYPE my_stage_chain_tip gauge"));
        assert!(out.contains("my_stage_chain_tip -7"));
    }

    #[test]
    fn sanitizes_stage_name_but_keeps_help_label() {
        let stages = vec![("a-b-c", vec![("ticks", Reading::Count(1))])];

        let out = render_stages(stages.into_iter());

        assert!(out.contains("# TYPE a_b_c_ticks counter"));
        assert!(out.contains("a_b_c_ticks 1"));
        // the human-readable help keeps the original stage name
        assert!(out.contains("for stage a-b-c"));
    }

    #[test]
    fn ignores_message_readings() {
        let stages = vec![(
            "s",
            vec![
                ("note", Reading::Message("hello".into())),
                ("count", Reading::Count(3)),
            ],
        )];

        let out = render_stages(stages.into_iter());

        assert!(!out.contains("note"));
        assert!(out.contains("s_count 3"));
    }
}
