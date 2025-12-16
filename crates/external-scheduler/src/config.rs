use std::convert::Infallible;
use std::fmt::Debug;
use std::str::FromStr;

use agave_schedulers::batch;
use serde::Deserialize;
use serde_with::serde_as;
use solana_keypair::Pubkey;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) host_name: String,
    pub(crate) nats_servers: Vec<String>,
    pub(crate) scheduler: SchedulerConfig,
}

#[derive(Debug, Deserialize)]
pub(crate) enum SchedulerConfig {
    Batch(BatchSchedulerConfig),
    Fifo(()),
    Greedy(()),
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchSchedulerConfig {
    #[serde(deserialize_with = "serde_with_expand_env::with_expand_envs")]
    keypair: SecretString,
    tip: TipDistributionConfig,
    jito: JitoConfig,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct TipDistributionConfig {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(crate) vote_account: Pubkey,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(crate) merkle_root_upload_authority: Pubkey,
    pub(crate) commission_bps: u16,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JitoConfig {
    pub(crate) http_rpc: String,
    pub(crate) ws_rpc: String,
    pub(crate) block_engine: String,
}

#[derive(Deserialize)]
#[repr(transparent)]
pub(crate) struct SecretString(String);

impl SecretString {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<REDACTED>")
    }
}

impl FromStr for SecretString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}
