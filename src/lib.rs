use std::{fs::File, io::Read, path::Path};

mod influxdb;
mod mqtt;
mod glow;
mod tasmota_plug;
use influxdb::{InfluxDb, InfluxDbConfig};
use mqtt::{Mqtt, MqttConfig, MqttMessage};
use glow::{Glow, GlowConfig};
use tasmota_plug::{TasmotaPlug, TasmotaPlugConfig};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error in InfluxDB HTTP client")]
    Reqwest(#[from] reqwest::Error),
    #[error("Error building InfluxDB HTTP client")]
    ReqwestHeader(#[from] reqwest::header::InvalidHeaderValue),
    #[error("MQTT Configuration error")]
    MqttConfig(#[from] rumqttc::OptionError),
    #[error("MQTT Client error")]
    MqttClient(#[from] rumqttc::ClientError),
    #[error("I/O error")]
    Io(#[from] std::io::Error),
    #[error("TOML parsing error")]
    Toml(#[from] toml::de::Error),
    #[error("Config error: topic must not end in /: {0}")]
    ConfigTopicSlash(String),
}

pub type Result<T> = std::result::Result<T, Error>;

struct Submission {
    line: String,
    bucket: String,
}

trait Submittable {
    /// Trait for structs that can turn a `MqttMessage` into a
    /// string in InfluxDB's line protocol ready for submission.
    fn to_submission(&self, message: &MqttMessage) -> Option<Submission>;
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    mqtt: MqttConfig,
    influxdb: InfluxDbConfig,
    glow: Option<Vec<GlowConfig>>,
    tasmota_plug: Option<Vec<TasmotaPlugConfig>>,
}

impl Config {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;
        Ok(toml::from_str(&data)?)
    }
}

pub struct Router {
    mqtt: Mqtt,
    influx: InfluxDb,
    submittables: Vec<(String, Box<dyn Submittable>)>,
}

impl Router {
    pub async fn connect(config: &Config) -> Result<Self> {
        let influx = InfluxDb::new(&config.influxdb)?;
        let mqtt = Mqtt::connect(&config.mqtt).await?;
        let mut submittables: Vec<(String, Box<dyn Submittable>)> = Vec::new();
        if let Some(glows) = &config.glow {
            for glow in glows.iter() {
                let topic = glow.topic.clone();
                if topic.ends_with('/') {
                    return Err(Error::ConfigTopicSlash(topic.clone()));
                }
                mqtt.subscribe(&format!("{}/#", topic)).await?;
                let glow = Box::new(Glow::new(glow));
                submittables.push((topic, glow));
            }
        }
        if let Some(plugs) = &config.tasmota_plug {
            for plug in plugs.iter() {
                let topic = plug.topic.clone();
                if topic.ends_with('/') {
                    return Err(Error::ConfigTopicSlash(topic.clone()));
                }
                mqtt.subscribe(&format!("{}/#", topic)).await?;
                let plug = Box::new(TasmotaPlug::new(plug));
                submittables.push((topic, plug));
            }
        }
        Ok(Router { mqtt, influx, submittables })
    }

    pub async fn poll(&mut self) -> Result<()> {
        if let Some(message) = self.mqtt.poll().await {
            for (topic, submittable) in self.submittables.iter() {
                if message.topic.starts_with(topic) {
                    if let Some(submission) = submittable.to_submission(&message) {
                        self.influx.submit(&submission).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
