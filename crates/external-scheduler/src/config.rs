use std::fmt::Debug;
use std::path::PathBuf;

use serde::Deserialize;
use serde_with::serde_as;
use solana_pubkey::Pubkey;

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub(crate) host_name: String,
    pub(crate) nats_servers: Vec<String>,
    pub(crate) scheduler: SchedulerConfig,
}

#[derive(Debug, Deserialize)]
pub(crate) enum SchedulerConfig {
    Batch(BatchSchedulerConfig),
    Fifo,
    Greedy,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchSchedulerConfig {
    pub(crate) keypair_path: PathBuf,
    pub(crate) tip: TipDistributionConfig,
    pub(crate) jito: JitoConfig,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct TipDistributionConfig {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(crate) vote_account: Pubkey,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub(crate) merkle_authority: Pubkey,
    pub(crate) commission_bps: u16,
}

#[derive(Debug, Deserialize)]
pub(crate) struct JitoConfig {
    pub(crate) http_rpc: String,
    pub(crate) ws_rpc: String,
    pub(crate) block_engine: String,
}
