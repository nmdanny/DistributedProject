use std::sync::Once;

use tracing_error::ErrorLayer;
use tracing_futures::WithSubscriber;
use tracing_subscriber::layer::SubscriberExt;
static LOGGING: Once = Once::new();

pub fn setup_logging() -> Result<(), anyhow::Error> {
    LOGGING.call_once(|| {
        use tracing_subscriber::FmtSubscriber;
        let subscriber = FmtSubscriber::builder()
            .compact()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("raft=info".parse().unwrap())
                .add_directive("runner=debug".parse().unwrap())
                .add_directive("dist_lib::consensus=warn".parse().unwrap())
                .add_directive("dist_lib::consensus[{important}]=info".parse().unwrap())
                .add_directive("dist_lib::grpc[{trans}]=debug".parse().unwrap())
                .add_directive("dist_lib::anonymity=error".parse().unwrap())
                .add_directive("dist_lib::grpc=error".parse().unwrap())
                // .add_directive("dist_lib[{vote_granted_too_late}]=off".parse()?)
                // .add_directive("dist_lib[{net_err}]=off".parse()?)
            )
            .finish()
            .with(ErrorLayer::default());
        tracing::subscriber::set_global_default(subscriber)
            .expect("Couldn't set up default tracing subscriber");
        color_eyre::install().unwrap();
    });
    Ok(())
}
