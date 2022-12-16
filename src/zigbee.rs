//! Parse messages from zigbee2mqtt.

use std::collections::HashMap;
use crate::{Error, Result, Mqtt, MqttMessage, Submission, Submittable};

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ZigbeeConfig {
    pub(crate) topic: String,
    pub(crate) name: String,
    pub(crate) bucket: String,
    pub(crate) measurement: String,
    pub(crate) keys: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ZigbeeReading {
    time: time::OffsetDateTime,
    values: HashMap<String, f64>,
}

impl ZigbeeReading {
    fn from_str(cfg: &ZigbeeConfig, payload: &[u8]) -> Option<Self> {
        let msg: serde_json::Value = serde_json::from_slice(payload).ok()?;
        let time = if let Some(t) = msg.get("last_seen") {
            time::serde::rfc3339::deserialize(t).ok()?
        } else {
            time::OffsetDateTime::now_utc()
        };
        let mut values = HashMap::new();
        for k in cfg.keys.iter() {
            if let Some(v) = msg.get(k) {
                if let Some(v) = v.as_f64() {
                    values.insert(k.clone(), v);
                }
            }
        }
        Some(Self { time, values })
    }

    fn to_line(&self, measurement: &str, name: &str) -> String {
        let ts = self.time.unix_timestamp();
        let name = name.replace(',', "\\,").replace('=', "\\=").replace(' ', "\\ ");
        let mut line = format!("{measurement},name={name} ");
        let pairs: Vec<String> = self.values.iter().map(|(k, v)| format!("{k}={v}")).collect();
        line.push_str(&pairs.join(","));
        line.push_str(&format!(" {ts}"));
        line
    }
}

pub(crate) struct Zigbee {
    config: ZigbeeConfig,
}

impl Zigbee {
    pub(crate) fn new(config: &ZigbeeConfig) -> Self {
        Self { config: config.clone() }
    }

    pub(crate) async fn from_configs(
        configs: &[ZigbeeConfig],
        mqtt: &Mqtt,
        submittables: &mut Vec<(String, Box<dyn Submittable>)>,
    ) -> Result<()> {
        for cfg in configs.iter() {
            let topic = cfg.topic.clone();
            if topic.ends_with('/') {
                return Err(Error::ConfigTopicSlash(topic.clone()));
            }
            mqtt.subscribe(&topic).await?;
            let zb = Box::new(Zigbee::new(cfg));
            submittables.push((topic, zb));
        }
        Ok(())
    }
}

impl Submittable for Zigbee {
    fn to_submission(&self, message: &MqttMessage) -> Option<Submission> {
        match ZigbeeReading::from_str(&self.config, &message.payload) {
            Some(reading) => Some(Submission {
                line: reading.to_line(&self.config.measurement, &self.config.name),
                bucket: self.config.bucket.to_string(),
            }),
            None => {
                log::trace!("Error parsing Zigbee message {:?}", message.payload);
                None
            },
        }
    }
}
