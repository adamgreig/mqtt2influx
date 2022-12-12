use reqwest::header::{HeaderMap, HeaderValue};
use crate::{Submission, Result};

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct InfluxDbConfig {
    pub(crate) url: String,
    pub(crate) org: String,
    pub(crate) token: String,
}

pub(crate) struct InfluxDb {
    client: reqwest::Client,
    url: String,
}

impl InfluxDb {
    pub(crate) fn new(config: &InfluxDbConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let token = format!("Token {}", config.token);
        let mut auth_value = HeaderValue::from_str(&token)?;
        auth_value.set_sensitive(true);
        headers.insert("Authorization", auth_value);
        headers.insert(
            "Content-Type",
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        headers.insert("Accept", HeaderValue::from_static("application/json"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let base_url = if config.url.ends_with('/') {
            config.url.clone()
        } else {
            format!("{}/", config.url)
        };
        let org = &config.org;
        let url = format!("{base_url}api/v2/write?org={org}&precision=s");

        Ok(Self { client, url })
    }

    pub(crate) async fn submit(&mut self, submission: &Submission) -> Result<()> {
        log::trace!("Submitting: {} {}", submission.bucket, submission.line);
        let url = format!("{}&bucket={}", self.url, submission.bucket);
        let response = self.client.post(url).body(submission.line.to_string()).send().await?;
        if let Err(err) = response.error_for_status_ref() {
            let body = response.text().await?;
            log::debug!("InfluxDB error: {body}");
            Err(err)?
        } else {
            Ok(())
        }
    }
}
