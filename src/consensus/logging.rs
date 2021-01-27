use std::sync::Once;

static LOGGING: Once = Once::new();

pub fn setup_logging() -> Result<(), anyhow::Error> {
    LOGGING.call_once(|| {
        color_eyre::install().unwrap();
        use tracing_subscriber::FmtSubscriber;
        let subscriber = FmtSubscriber::builder()
            .pretty()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("raft=info".parse().unwrap())
                .add_directive("dist_lib=off".parse().unwrap())
                .add_directive("dist_lib::anonymity=info".parse().unwrap())
                // .add_directive("dist_lib[{vote_granted_too_late}]=off".parse()?)
                // .add_directive("dist_lib[{net_err}]=off".parse()?)
            )
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Couldn't set up default tracing subscriber");
    });
    Ok(())
}
