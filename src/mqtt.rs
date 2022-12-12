use rumqttc::{mqttbytes::v4::Packet, AsyncClient, Event, MqttOptions};
use crate::Result;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct MqttConfig {
    pub(crate) url: String,
}

pub(crate) struct Mqtt {
    client: rumqttc::AsyncClient,
    event_loop: rumqttc::EventLoop,
}

#[derive(Clone, Debug)]
pub(crate) struct MqttMessage {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
}

impl Mqtt {
    pub(crate) async fn connect(config: &MqttConfig) -> Result<Self> {
        let random_id: String = std::iter::repeat_with(fastrand::alphanumeric)
            .take(8)
            .collect();
        let url = format!("{}?client_id=mqtt2influx-{random_id}", config.url);
        let mut opts = MqttOptions::parse_url(url)?;
        opts.set_clean_session(true);
        let (client, event_loop) = AsyncClient::new(opts, 12);
        Ok(Self { client, event_loop })
    }

    pub(crate) async fn subscribe(&self, topic: &str) -> Result<()> {
        log::debug!("Subscribing to topic {topic:?}");
        self.client.subscribe(topic, rumqttc::QoS::AtMostOnce).await?;
        Ok(())
    }

    pub(crate) async fn poll(&mut self) -> Option<MqttMessage> {
        match self.event_loop.poll().await {
            Ok(event) => match event {
                Event::Incoming(packet) => match packet {
                    Packet::Publish(publish) => {
                        log::trace!("Got MQTT publish: {publish:?}");
                        return Some(MqttMessage {
                            topic: publish.topic.clone(),
                            payload: publish.payload.into(),
                        });
                    }
                    _ => {
                        log::trace!("Unhandled incoming MQTT packet: {packet:?}");
                    }
                },
                Event::Outgoing(packet) => {
                    log::trace!("Unhandled outgoing MQTT packet: {packet:?}");
                }
            },
            Err(err) => {
                log::warn!("MQTT connection error: {err:?}");
            }
        }
        None
    }
}
