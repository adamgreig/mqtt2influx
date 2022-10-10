use std::{fs::File, path::Path, io::Read};
use rumqttc::{MqttOptions, AsyncClient, Event, mqttbytes::v4::Packet};
use reqwest::header::{HeaderMap, HeaderValue};

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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    mqtt: MqttConfig,
    influxdb: InfluxDbConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct MqttConfig {
    url: String,
    topic: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct InfluxDbConfig {
    url: String,
    org: String,
    bucket: String,
    token: String,
}

impl Config {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;
        Ok(toml::from_str(&data)?)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub enum MeterReading {
    #[serde(rename="electricitymeter")]
    Electricity(ElectricityMeter),

    #[serde(rename="gasmeter")]
    Gas(GasMeter),
}

#[derive(Copy, Clone, Debug, serde::Deserialize)]
#[allow(non_camel_case_types)]
pub enum Units {
    kWh,
    kW,
    m3,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct EnergyImportPrice {
    pub unitrate: f64,
    pub standingcharge: f64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ElectricityMeter {
    #[serde(with="time::serde::rfc3339")]
    pub timestamp: time::OffsetDateTime,
    pub energy: ElectricityEnergy,
    pub power: ElectricityPower,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ElectricityEnergy {
    pub export: ElectricityEnergyExport,
    pub import: ElectricityEnergyImport,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ElectricityEnergyExport {
    pub cumulative: f64,
    pub units: Units,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ElectricityEnergyImport {
    pub cumulative: f64,
    pub day: f64,
    pub week: f64,
    pub month: f64,
    pub units: Units,
    pub mpan: String,
    pub supplier: String,
    pub price: EnergyImportPrice,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ElectricityPower {
    pub value: f64,
    pub units: Units,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GasMeter {
    #[serde(with="time::serde::rfc3339")]
    pub timestamp: time::OffsetDateTime,
    pub energy: GasMeterEnergy,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GasMeterEnergy {
    pub import: GasMeterEnergyImport,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GasMeterEnergyImport {
    pub cumulative: f64,
    pub day: f64,
    pub week: f64,
    pub month: f64,
    pub units: Units,
    pub cumulativevol: f64,
    pub cumulativevolunits: Units,
    pub dayvol: f64,
    pub weekvol: f64,
    pub monthvol: f64,
    pub dayweekmonthvolunits: Units,
    pub mprn: String,
    pub supplier: String,
    pub price: EnergyImportPrice,
}

pub struct Glow {
    event_loop: rumqttc::EventLoop,
    topic: String,
}

impl Glow {
    pub async fn subscribe(config: &Config) -> Result<Self> {
        let random_id: String = std::iter::repeat_with(fastrand::alphanumeric).take(8).collect();
        let url = format!("{}?client_id=glow2influx-{random_id}", config.mqtt.url);
        let mut opts = MqttOptions::parse_url(url)?;
        opts.set_clean_session(true);
        let (client, event_loop) = AsyncClient::new(opts, 12);
        let topic = if config.mqtt.topic.ends_with('/') {
            format!("{}#", config.mqtt.topic)
        } else {
            format!("{}/#", config.mqtt.topic)
        };
        log::debug!("Subscribing to topic {topic:?}");
        client.subscribe(&topic, rumqttc::QoS::AtMostOnce).await?;
        Ok(Self { event_loop, topic })
    }

    pub async fn poll(&mut self) -> Option<MeterReading> {
        match self.event_loop.poll().await {
            Ok(event) => match event {
                Event::Incoming(packet) => match packet {
                    Packet::Publish(publish) => {
                        log::trace!("Got MQTT publish: {publish:?}");
                        return self.process(publish);
                    },
                    _ => {
                        log::trace!("Unhandled incoming MQTT packet: {packet:?}");
                    },
                },
                Event::Outgoing(packet) => {
                    log::trace!("Unhandled outgoing MQTT packet: {packet:?}");
                },
            },
            Err(err) => {
                log::warn!("MQTT connection error: {err:?}");
            },
        }
        None
    }

    fn process(&self, publish: rumqttc::mqttbytes::v4::Publish) -> Option<MeterReading> {
        let topic = publish.topic.strip_prefix(&self.topic.strip_suffix("#").unwrap())?;
        let topic: Vec<&str> = topic.split('/').collect();

        let n = topic.len();
        if n < 2 {
            log::trace!("Ignoring message with less than {n}<2 topic levels");
            return None;
        }

        let message_type = topic[1];
        if message_type != "SENSOR" {
            log::trace!("Ignoring message of type {message_type:?}");
            return None;
        }

        match serde_json::from_slice(&publish.payload) {
            Ok(reading) => {
                Some(reading)
            },
            Err(err) => {
                log::trace!("Error parsing meter reading: {err:?}");
                None
            },
        }
    }
}

pub struct InfluxDb {
    client: reqwest::Client,
    url: String,
}

impl InfluxDb {
    pub fn new(config: &Config) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let token = format!("Token {}", config.influxdb.token);
        let mut auth_value = HeaderValue::from_str(&token)?;
        auth_value.set_sensitive(true);
        headers.insert("Authorization", auth_value);
        headers.insert("Content-Type", HeaderValue::from_static("text/plain; charset=utf-8"));
        headers.insert("Accept", HeaderValue::from_static("application/json"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let base_url = if config.influxdb.url.ends_with('/') {
            config.influxdb.url.clone()
        } else {
            format!("{}/", config.influxdb.url)
        };
        let org = &config.influxdb.org;
        let bucket = &config.influxdb.bucket;
        let url = format!("{base_url}api/v2/write?org={org}&bucket={bucket}&precision=s");

        Ok(Self { client, url })
    }

    pub async fn submit(&mut self, reading: MeterReading) -> Result<()> {
        match reading {
            MeterReading::Electricity(elec) => self.submit_elec(elec).await,
            MeterReading::Gas(gas) => self.submit_gas(gas).await,
        }
    }

    async fn submit_elec(&mut self, elec: ElectricityMeter) -> Result<()> {
        let ts = elec.timestamp.unix_timestamp();
        let mpan = elec.energy.import.mpan;
        let power = elec.power.value;
        let cumulative = elec.energy.import.cumulative;
        let price = elec.energy.import.price.unitrate;
        let standing = elec.energy.import.price.standingcharge;
        let line = format!("electricity,mpan={mpan} \
            power={power},cumulative={cumulative},price={price},standing={standing} \
            {ts}"
        );
        log::trace!("Submitting electricity reading: {line}");
        self.submit_line(&line).await
    }

    async fn submit_gas(&mut self, gas: GasMeter) -> Result<()> {
        let ts = gas.timestamp.unix_timestamp();
        let mprn = gas.energy.import.mprn;
        let cumulative = gas.energy.import.cumulative;
        let price = gas.energy.import.price.unitrate;
        let standing = gas.energy.import.price.standingcharge;
        let line = format!("gas,mprn={mprn} \
            cumulative={cumulative},price={price},standing={standing} \
            {ts}"
        );
        log::trace!("Submitting gas reading: {line}");
        self.submit_line(&line).await
    }

    async fn submit_line(&mut self, line: &str) -> Result<()> {
        let url = self.url.clone();
        let line = line.to_string();
        let response = self.client.post(url).body(line).send().await?;
        if let Err(err) = response.error_for_status_ref() {
            let body = response.text().await?;
            log::debug!("InfluxDB error: {body}");
            Err(err)?
        } else {
            Ok(())
        }
    }
}
