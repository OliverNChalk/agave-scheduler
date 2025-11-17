use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) host_name: String,
    pub(crate) nats_servers: Vec<String>,
}
