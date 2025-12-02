use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use eyre::eyre;
use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role, Token};
use jito_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use jito_protos::block_engine::{
    SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
    SubscribePacketsResponse,
};
use solana_keypair::{Keypair, Signer};
use solana_packet::PACKET_DATA_SIZE;
use tonic::service::Interceptor;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use tracing::error;

pub(crate) struct JitoConfig {
    pub(crate) url: String,
}

pub(crate) struct JitoThread {
    bundle_tx: crossbeam_channel::Sender<Vec<Vec<u8>>>,
    endpoint: Endpoint,
    keypair: Keypair,
}

impl JitoThread {
    pub(crate) fn spawn(
        bundle_tx: crossbeam_channel::Sender<Vec<Vec<u8>>>,
        config: JitoConfig,
        keypair: Keypair,
    ) -> JoinHandle<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Setup the block engine endpoint.
        let enable_tls = config.url.starts_with("https");
        let mut endpoint = Endpoint::from_shared(config.url)
            .unwrap()
            .tcp_keepalive(Some(Duration::from_secs(60)));
        if enable_tls {
            endpoint = endpoint.tls_config(ClientTlsConfig::new()).unwrap();
        }

        std::thread::Builder::new()
            .name("Jito".to_string())
            .spawn(move || rt.block_on(JitoThread { bundle_tx, endpoint, keypair }.run()))
            .unwrap()
    }

    async fn run(self) {
        loop {
            let Err(err) = self.run_until_err().await;
            error!(%err, "Jito connection errored");
        }
    }

    async fn run_until_err(&self) -> eyre::Result<Infallible> {
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

        // Start the bundle & packet streams.
        let mut bundles = block_engine
            .subscribe_bundles(SubscribeBundlesRequest {})
            .await?
            .into_inner();
        let mut packets = block_engine
            .subscribe_packets(SubscribePacketsRequest {})
            .await?
            .into_inner();

        // Consume bundles & packets until error.
        loop {
            tokio::select! {
                biased;

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
            self.bundle_tx.try_send(bundle).unwrap();
        }
    }

    fn on_packets(&self, packets: SubscribePacketsResponse) {
        todo!()
    }
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
