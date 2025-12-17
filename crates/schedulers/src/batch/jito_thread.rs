use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use eyre::eyre;
use futures::{FutureExt, StreamExt};
use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role, Token};
use jito_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use jito_protos::block_engine::{
    BlockBuilderFeeInfoRequest, SubscribeBundlesRequest, SubscribeBundlesResponse,
    SubscribePacketsRequest, SubscribePacketsResponse,
};
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_packet::PACKET_DATA_SIZE;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::{CommitmentConfig, RpcAccountInfoConfig, UiAccountEncoding};
use solana_rpc_client_types::response::UiAccount;
use tonic::service::Interceptor;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use toolbox::shutdown::Shutdown;
use toolbox::tokio::IntervalStream;
use tracing::{error, info};

use crate::batch::tip_program::TIP_PAYMENT_CONFIG;

#[derive(Debug)]
pub struct JitoArgs {
    pub http_rpc: String,
    pub ws_rpc: String,
    pub block_engine: String,
}

pub(crate) struct JitoThread {
    update_tx: crossbeam_channel::Sender<JitoUpdate>,
    endpoint: Endpoint,
    keypair: &'static Keypair,
}

impl JitoThread {
    pub(crate) fn spawn(
        shutdown: Shutdown,
        update_tx: crossbeam_channel::Sender<JitoUpdate>,
        config: JitoArgs,
        keypair: &'static Keypair,
    ) -> JoinHandle<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let rpc = Box::leak(Box::new(RpcClient::new(config.http_rpc)));

        // Setup the block engine endpoint.
        let enable_tls = config.block_engine.starts_with("https");
        let mut endpoint = Endpoint::from_shared(config.block_engine)
            .unwrap()
            .tcp_keepalive(Some(Duration::from_secs(60)));
        if enable_tls {
            endpoint = endpoint.tls_config(ClientTlsConfig::new()).unwrap();
        }

        std::thread::Builder::new()
            .name("Jito".to_string())
            .spawn(move || {
                let fut = futures::future::select(
                    Box::pin(JitoThread { update_tx, endpoint, keypair }.run(rpc, &config.ws_rpc)),
                    Box::pin(shutdown.cancelled()),
                );

                rt.block_on(fut);
            })
            .unwrap()
    }

    async fn run(self, rpc: &'static RpcClient, ws: &str) {
        loop {
            let Err(err) = self.run_until_err(rpc, ws).await;
            error!(?err, "Jito connection errored");

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn run_until_err(&self, rpc: &'static RpcClient, ws: &str) -> eyre::Result<Infallible> {
        // Connect to the auth service.
        let auth = self.endpoint.connect().await?;
        let mut auth = AuthServiceClient::new(auth);

        // Complete the block engine auth challenge.
        let pubkey = self.keypair.pubkey();
        let challenge_response = auth
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Validator as i32,
                pubkey: pubkey.as_array().to_vec(),
            })
            .await?;
        let formatted_challenge = format!("{pubkey}-{}", challenge_response.into_inner().challenge);
        let signed_challenge = self
            .keypair
            .sign_message(formatted_challenge.as_bytes())
            .as_array()
            .to_vec();

        // Generate auth tokens using the signed challenge.
        let auth_tokens = auth
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: formatted_challenge,
                client_pubkey: pubkey.as_array().to_vec(),
                signed_challenge,
            })
            .await?
            .into_inner();

        // Extract & validate tokens.
        let access = auth_tokens
            .access_token
            .ok_or_else(|| eyre!("Missing access token"))?;
        eyre::ensure!(access.expires_at_utc.is_some(), "Missing access expiry");
        let refresh = auth_tokens
            .refresh_token
            .ok_or_else(|| eyre!("Missing refresh token"))?;
        eyre::ensure!(refresh.expires_at_utc.is_some(), "Missing refresh expiry");

        // Connect to the block engine service.
        let access = Arc::new(Mutex::new(access));
        let block_engine = self.endpoint.connect().await?;
        let mut block_engine = BlockEngineValidatorClient::with_interceptor(
            block_engine,
            AuthInterceptor { access: access.clone() },
        );

        // Fetch block builder config (for now we don't refresh).
        let block_builder_info = block_engine
            .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
            .await?
            .into_inner();
        self.update_tx
            .try_send(JitoUpdate::BuilderConfig(BuilderConfig {
                key: block_builder_info.pubkey.parse().unwrap(),
                commission: block_builder_info.commission,
            }))
            .unwrap();

        // Start the bundle & packet streams.
        let mut bundles = block_engine
            .subscribe_bundles(SubscribeBundlesRequest {})
            .await?
            .into_inner();
        let mut packets = block_engine
            .subscribe_packets(SubscribePacketsRequest {})
            .await?
            .into_inner();

        // Poll recent blockhashes.
        let mut recent_blockhashes = IntervalStream::new(
            tokio::time::interval(Duration::from_secs(30)),
            Box::new(move || {
                async {
                    rpc.get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
                        .await
                }
                .boxed()
            }),
        );

        // Start jito tip config stream.
        let ws = PubsubClient::new(ws).await?;
        let (mut tip_config, _) = ws
            .account_subscribe(
                &TIP_PAYMENT_CONFIG,
                Some(RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    commitment: Some(CommitmentConfig::processed()),
                    min_context_slot: None,
                }),
            )
            .await?;

        info!("Jito connected & subscribed");

        // Consume bundles & packets until error.
        loop {
            tokio::select! {
                biased;

                opt = recent_blockhashes.next() => {
                    let (hash, _) = opt.unwrap()?;

                    self.update_tx.try_send(JitoUpdate::RecentBlockhash(hash)).unwrap();
                },
                opt = tip_config.next() => {
                    let config = opt.ok_or_else(|| eyre!("tip config stream closed"))?.value;

                    self.on_tip_config(&config);
                }
                res = bundles.message() => {
                    let bundles = res?.ok_or_else(|| eyre!("bundle stream closed"))?;

                    self.on_bundles(bundles);
                }
                res = packets.message() => {
                    let packets = res?.ok_or_else(|| eyre!("bundle stream closed"))?;

                    self.on_packets(packets);
                },
            }
        }
    }

    fn on_tip_config(&self, config: &UiAccount) {
        let data = config.data.decode().unwrap();

        let tip_receiver = Pubkey::new_from_array(*arrayref::array_ref![&data, 8, 32]);
        let block_builder = Pubkey::new_from_array(*arrayref::array_ref![&data, 40, 32]);

        self.update_tx
            .try_send(JitoUpdate::TipConfig(TipConfig { tip_receiver, block_builder }))
            .unwrap();
    }

    fn on_bundles(&self, bundles: SubscribeBundlesResponse) {
        for bundle in bundles
            .bundles
            .into_iter()
            .filter_map(|bundle| bundle.bundle)
            .map(|bundle| {
                bundle
                    .packets
                    .into_iter()
                    .map(|packet| packet.data)
                    .inspect(|packet| assert!(packet.len() <= PACKET_DATA_SIZE))
                    .collect::<Vec<_>>()
            })
            .filter(|bundle| !bundle.is_empty())
        {
            self.update_tx.try_send(JitoUpdate::Bundle(bundle)).unwrap();
        }
    }

    fn on_packets(&self, packets: SubscribePacketsResponse) {
        for packet in packets
            .batch
            .into_iter()
            .flat_map(|batch| batch.packets)
            .map(|packet| packet.data)
        {
            self.update_tx.try_send(JitoUpdate::Packet(packet)).unwrap();
        }
    }
}

pub(crate) enum JitoUpdate {
    BuilderConfig(BuilderConfig),
    TipConfig(TipConfig),
    RecentBlockhash(Hash),
    Packet(Vec<u8>),
    Bundle(Vec<Vec<u8>>),
}

pub(crate) struct BuilderConfig {
    pub(crate) key: Pubkey,
    pub(crate) commission: u64,
}

#[derive(Debug)]
pub(crate) struct TipConfig {
    pub(crate) tip_receiver: Pubkey,
    pub(crate) block_builder: Pubkey,
}

struct AuthInterceptor {
    access: Arc<Mutex<Token>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.access.lock().unwrap().value)
                .parse()
                .map_err(|_| Status::invalid_argument("Failed to parse authorization token"))?,
        );

        Ok(request)
    }
}
