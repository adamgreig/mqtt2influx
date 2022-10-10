use glow2influx::{Config, Glow, InfluxDb};
use clap::Parser;

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
        0 => env_logger::Env::default().default_filter_or("glow2influx=warn"),
        1 => env_logger::Env::default().default_filter_or("glow2influx=info"),
        2 => env_logger::Env::default().default_filter_or("glow2influx=debug"),
        3.. => env_logger::Env::default().default_filter_or("glow2influx=trace"),
    };
    env_logger::Builder::from_env(env).init();

    log::info!("Reading config from {:?}", args.config_file);
    let config = Config::from_path(args.config_file)?;

    let mut influx = InfluxDb::new(&config)?;

    log::info!("Subscribing to messages from Glow");
    let mut glow = Glow::subscribe(&config).await?;

    loop {
        if let Some(reading) = glow.poll().await {
            log::info!("Got reading: {reading:?}");
            if let Err(err) = influx.submit(reading).await {
                log::warn!("Error submitting to Influx: {err:?}");
            }
        }
    }
}
