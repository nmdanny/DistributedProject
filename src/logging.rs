use std::{any::Any, sync::Once};

use tracing_error::ErrorLayer;
use tracing_futures::WithSubscriber;
use tracing_subscriber::{Layer, Registry, layer::SubscriberExt, fmt};
use tracing_flame::FlameLayer;
use std::path::{Path, PathBuf};
use std::io::Write;
use prost::Message;
use anyhow::Context;

use std::mem::drop;

/// When dropped, flushes remaining spans/events to disk/network etc
pub type Guards = Vec<Box<dyn Any + Send>>;

#[must_use]
pub fn setup_logging() -> Result<Guards, anyhow::Error> {
    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let name = "dist_lib".to_owned();
    let (tracer, uninstall) = opentelemetry_jaeger::new_pipeline()
        .with_service_name(&name)
        .install()
        .expect("Couldn't install jaeger telemetry");

    std::fs::create_dir_all("traces").expect("Couldn't create traces folder");
    let (flame, flame_guard) = FlameLayer::with_file(PathBuf::from("traces").join(name)).expect("Couldn't install FlameLayer");

    #[cfg(not(test))]
    let (appender, appender_guard) = tracing_appender::non_blocking(std::io::stdout());
    #[cfg(test)]
    let (appender, appender_guard) = tracing_appender::non_blocking(tracing_subscriber::fmt::TestWriter::new());

    let drops: Guards = vec![
        Box::new(uninstall),
        Box::new(flame_guard),
        Box::new(appender_guard)
    ];

    let otl = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter = tracing_subscriber::EnvFilter::from_default_env();

    let fmt_layer = fmt::Layer::new()
        .with_writer(appender)
        .pretty()
        .with_thread_ids(true);

    
    let subscriber = Registry::default()
        .with(ErrorLayer::default())
        .with(otl)
        .with(flame)
        .with(env_filter)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber)
        .unwrap_or_else(|e| {
            error!("Error setting the global tracing subscriber: {:?}", e);
            eprintln!("Error setting the global tracing subscriber: {:?}", e);
        });
    color_eyre::install().unwrap_or_else(|e| {
        error!("Couldn't set up color_eyre: {:?}", e);
    });

    Ok(drops)
}

#[must_use]
pub fn profiler<P: AsRef<Path>>(my_path: P, sample_freq: std::os::raw::c_int) -> impl Drop {
    #[cfg(unix)] {
        struct Profiler<P: AsRef<Path>> {
            profiler: pprof::ProfilerGuard<'static>,
            path: P
        }
        impl <P: AsRef<Path>> Drop for Profiler<P> {
            #[cfg(unix)]
            fn drop(&mut self) {
                let flamegraph_path = self.path.as_ref().with_extension("svg");
                let file = std::fs::File::create(&flamegraph_path)
                    .context(format!("at path {:?}", flamegraph_path))
                    .expect("creating flamegraph file");
                let report = self.profiler.report().build().expect("building report");
                report.flamegraph(file).expect("reporting flamegraph into file");
                println!("Created flamegraph file at {:?}", flamegraph_path);

                let pprof_path = self.path.as_ref().with_extension("pb");
                let mut file = std::fs::File::create(&pprof_path)
                    .context(format!("at path {:?}", pprof_path))
                    .expect("creating pprof file");
                let profile = report.pprof().expect("creating pprof profile");
                let mut content = Vec::new();
                profile.encode(&mut content).unwrap();
                file.write_all(&content).unwrap();
                println!("Created pprof file at {:?}", pprof_path);

            }

        }
        let profiler = pprof::ProfilerGuard::new(sample_freq).expect("making profiler guard");
        Profiler { profiler, path: my_path }
    }
    #[cfg(not(unix))] {
        let _ = my_path;
        let _ = sample_freq;
        struct Noop();
        impl Drop for Noop {
            fn drop(&mut self) {}
        }
        Noop()
    }
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
