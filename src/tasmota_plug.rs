use crate::{Error, Result, MqttMessage, Submission, Submittable};

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TasmotaPlugConfig {
    pub(crate) topic: String,
    pub(crate) name: String,
    pub(crate) bucket: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct TasmotaPlugReading {
    #[serde(with = "time::serde::rfc3339", rename="Time")]
    time: time::OffsetDateTime,
    #[serde(rename = "ENERGY")]
    energy: EnergyReading,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "PascalCase")]
struct EnergyReading {
    total_start_time: String,
    total: f64,
    yesterday: f64,
    today: f64,
    power: f64,
    apparent_power: f64,
    reactive_power: f64,
    factor: f64,
    voltage: f64,
    current: f64,
}

impl TasmotaPlugReading {
    fn to_line(&self, name: &str) -> String {
        let ts = self.time.unix_timestamp();
        let name = name.replace(',', "\\,").replace('=', "\\=").replace(' ', "\\ ");
        let total = self.energy.total;
        let today = self.energy.today;
        let power = self.energy.power;
        let voltage = self.energy.voltage;
        format!(
            "tasmota_plug,name={name} \
            power={power},cumulative={total},today={today},voltage={voltage} \
            {ts}"
        )
    }
}

pub(crate) struct TasmotaPlug {
    config: TasmotaPlugConfig,
}

impl TasmotaPlug {
    pub(crate) fn new(config: &TasmotaPlugConfig) -> Self {
        Self { config: config.clone() }
    }

    pub(crate) fn from_configs(
        configs: &[TasmotaPlugConfig],
        topics: &mut Vec<String>,
        submittables: &mut Vec<(String, Box<dyn Submittable>)>,
    ) -> Result<()> {
        for cfg in configs.iter() {
            let topic = cfg.topic.clone();
            if topic.ends_with('/') {
                return Err(Error::ConfigTopicSlash(topic));
            }
            topics.push(format!("{}/#", topic));
            let plug = Box::new(TasmotaPlug::new(cfg));
            submittables.push((topic, plug));
        }
        Ok(())
    }
}

impl Submittable for TasmotaPlug {
    fn to_submission(&self, message: &MqttMessage) -> Option<Submission> {
        let topic = message.topic.strip_prefix(&self.config.topic)?;
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

        match serde_json::from_slice::<TasmotaPlugReading>(&message.payload) {
            Ok(reading) => Some(Submission {
                line: reading.to_line(&self.config.name),
                bucket: self.config.bucket.to_string(),
            }),
            Err(err) => {
                log::trace!("Error parsing meter reading: {err:?}");
                None
            },
        }
    }
}
