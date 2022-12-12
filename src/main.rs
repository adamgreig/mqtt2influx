use clap::Parser;
use mqtt2influx::{Config, Router};

#[derive(clap::Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Configuration file to load, in TOML format.
    config_file: String,

    /// Enable extra logging levels.
    #[arg(short, long, action=clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let env = match args.verbose {
        0 => env_logger::Env::default().default_filter_or("mqtt2influx=warn"),
        1 => env_logger::Env::default().default_filter_or("mqtt2influx=info"),
        2 => env_logger::Env::default().default_filter_or("mqtt2influx=debug"),
        3.. => env_logger::Env::default().default_filter_or("mqtt2influx=trace"),
    };
    env_logger::Builder::from_env(env).init();

    log::info!("Reading config from {:?}", args.config_file);
    let config = Config::from_path(args.config_file)?;

    let mut router = Router::connect(&config).await?;

    loop {
        if let Err(err) = router.poll().await {
            log::warn!("Error processing message: {err:?}");
        }
    }
}
