use crate::{Error, Result, Submission, Submittable, Mqtt, MqttMessage};

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct GlowConfig {
    pub(crate) topic: String,
    pub(crate) bucket: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub enum MeterReading {
    #[serde(rename = "electricitymeter")]
    Electricity(ElectricityMeter),

    #[serde(rename = "gasmeter")]
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
    #[serde(with = "time::serde::rfc3339")]
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
    #[serde(with = "time::serde::rfc3339")]
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

impl ElectricityMeter {
    fn to_line(&self) -> String {
        let ts = self.timestamp.unix_timestamp();
        let mpan = &self.energy.import.mpan;
        let power = self.power.value;
        let reading = self.energy.import.cumulative;
        let price = self.energy.import.price.unitrate;
        let standing = self.energy.import.price.standingcharge;
        format!(
            "electricity,mpan={mpan} \
            power={power},cumulative={reading},price={price},standing={standing} \
            {ts}"
        )
    }
}

impl GasMeter {
    fn to_line(&self) -> String {
        let ts = self.timestamp.unix_timestamp();
        let mprn = &self.energy.import.mprn;
        let reading = self.energy.import.cumulative;
        let readingvol = self.energy.import.cumulativevol;
        let price = self.energy.import.price.unitrate;
        let standing = self.energy.import.price.standingcharge;
        format!(
            "gas,mprn={mprn} \
            cumulative={reading},cumulativevol={readingvol},price={price},standing={standing} \
            {ts}"
        )
    }
}

impl MeterReading {
    fn to_line(&self) -> String {
        match self {
            MeterReading::Electricity(elec) => elec.to_line(),
            MeterReading::Gas(gas) => gas.to_line(),
        }
    }
}

pub(crate) struct Glow {
    config: GlowConfig,
}

impl Glow {
    pub(crate) fn new(config: &GlowConfig) -> Self {
        Glow { config: config.clone() }
    }

    pub(crate) async fn from_configs(
        configs: &[GlowConfig],
        mqtt: &Mqtt,
        submittables: &mut Vec<(String, Box<dyn Submittable>)>,
    ) -> Result<()> {
        for cfg in configs.iter() {
            let topic = cfg.topic.clone();
            if topic.ends_with('/') {
                return Err(Error::ConfigTopicSlash(topic.clone()));
            }
            mqtt.subscribe(&format!("{}/#", topic)).await?;
            let glow = Box::new(Glow::new(cfg));
            submittables.push((topic, glow));
        }
        Ok(())
    }
}

impl Submittable for Glow {
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

        match serde_json::from_slice::<MeterReading>(&message.payload) {
            Ok(reading) => Some(Submission {
                line: reading.to_line(),
                bucket: self.config.bucket.to_string(),
            }),
            Err(err) => {
                log::trace!("Error parsing meter reading: {err:?}");
                None
            }
        }
    }
}
