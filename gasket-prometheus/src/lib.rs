use gasket::daemon::Daemon;
use prometheus_exporter_base::{
    prelude::ServerOptions, render_prometheus, MetricType, PrometheusInstance, PrometheusMetric,
};
use std::{io::Write, net::SocketAddr, sync::Arc};

fn sanitize_stage_name(raw: &str) -> String {
    raw.replace('-', "_")
}

fn write_counter(output: &mut impl Write, stage: &str, metric: &str, value: u64) {
    let name = format!("{}_{}", sanitize_stage_name(stage), metric);
    let help = format!("{} counter for stage {}", metric, stage);

    let mut pc = PrometheusMetric::build()
        .with_name(&name)
        .with_metric_type(MetricType::Counter)
        .with_help(&help)
        .build();

    pc.render_and_append_instance(
        &PrometheusInstance::new()
            .with_value(value)
            .with_current_timestamp()
            .expect("error getting the current UNIX epoch"),
    );

    writeln!(output, "{}", pc.render()).unwrap();
}

fn write_gauge(output: &mut impl Write, stage: &str, metric: &str, value: i64) {
    let name = format!("{}_{}", sanitize_stage_name(stage), metric);
    let help = format!("{} gauge for stage {}", metric, stage);

    let mut pc = PrometheusMetric::build()
        .with_name(&name)
        .with_metric_type(MetricType::Gauge)
        .with_help(&help)
        .build();

    pc.render_and_append_instance(
        &PrometheusInstance::new()
            .with_value(value)
            .with_current_timestamp()
            .expect("error getting the current UNIX epoch"),
    );

    writeln!(output, "{}", pc.render()).unwrap();
}

pub async fn serve(addr: SocketAddr, source: Arc<Daemon>) {
    let server_options = ServerOptions {
        addr,
        authorization: prometheus_exporter_base::prelude::Authorization::None,
    };

    render_prometheus(server_options, source, |_, options| async move {
        let mut out = vec![];

        for tether in options.tethers() {
            for (key, metric) in tether.read_metrics().unwrap() {
                match metric {
                    gasket::metrics::Reading::Count(x) => {
                        write_counter(&mut out, tether.name(), key, x);
                    }
                    gasket::metrics::Reading::Gauge(x) => {
                        write_gauge(&mut out, tether.name(), key, x);
                    }
                    _ => (),
                }
            }
        }

        Ok(String::from_utf8(out).unwrap())
    })
    .await;
}
