use std::{any::Any, sync::Once};

use tracing_error::ErrorLayer;
use tracing_futures::WithSubscriber;
use tracing_subscriber::{Layer, Registry, layer::SubscriberExt, fmt};
use tracing_flame::FlameLayer;
use std::path::Path;
use chrono::prelude::*;

use std::mem::drop;

pub type Guards = Vec<Box<dyn Any>>;

#[must_use]
pub fn setup_logging() -> Result<Guards, anyhow::Error> {
    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let time = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let name = format!("dist_lib_{}", time);
    let (tracer, uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name(&name)
        .install()
        .expect("Couldn't install jaeger telemetry");

    let (flame, flame_guard) = FlameLayer::with_file(&name).expect("Couldn't install FlameLayer");

    let drops: Guards = vec![
        Box::new(uninstall),
        Box::new(flame_guard)
    ];

    let otl = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("raft=info".parse().unwrap())
            .add_directive("runner=debug".parse().unwrap())
            .add_directive("dist_lib::consensus=warn".parse().unwrap())
            // .add_directive("dist_lib::consensus[{important}]=info".parse().unwrap())
            // .add_directive("dist_lib::grpc[{trans}]=debug".parse().unwrap())
            .add_directive("dist_lib::anonymity=info".parse().unwrap())
            .add_directive("dist_lib::grpc=error".parse().unwrap());
            // .add_directive("dist_lib[{vote_granted_too_late}]=off".parse()?)
            // .add_directive("dist_lib[{net_err}]=off".parse()?);


    let fmt_layer = fmt::Layer::new()
        .with_thread_ids(true);

    
    let subscriber = Registry::default()
        .with(ErrorLayer::default())
        .with(otl)
        .with(flame)
        .with(env_filter)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("Couldn't set up default tracing subscriber");
    color_eyre::install().unwrap();

    Ok(drops)
}

pub struct TimeOp {
    start: std::time::Instant,
    desc: String
}

impl TimeOp {
    pub fn new(desc: impl Into<String>) -> Self {
        TimeOp {
            start: std::time::Instant::now(),
            desc: desc.into()
        }
    }

    pub fn finish(self) {
        drop(self)
    }
}

impl Drop for TimeOp {
    fn drop(&mut self) {
        let delta = std::time::Instant::now() - self.start;
        warn!(time_trace=true, "{} finished in {} ms", self.desc, delta.as_millis());
    }
}
