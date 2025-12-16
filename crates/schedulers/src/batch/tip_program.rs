use solana_clock::Epoch;
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_message::{AccountMeta, Instruction};
use solana_pubkey::{Pubkey, pubkey};
use solana_sdk_ids::system_program;
use solana_transaction::Transaction;
use solana_transaction::versioned::VersionedTransaction;

const TIP_PAYMENT_PROGRAM: Pubkey = pubkey!("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt");
pub(crate) const TIP_PAYMENT_CONFIG: Pubkey = pubkey!("HgzT81VF1xZ3FT9Eq1pHhea7Wcfq2bv4tWTP3VvJ8Y9D");
const TIP_PAYMENT_ACCOUNT_0: Pubkey = pubkey!("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5");
const TIP_PAYMENT_ACCOUNT_1: Pubkey = pubkey!("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe");
const TIP_PAYMENT_ACCOUNT_2: Pubkey = pubkey!("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY");
const TIP_PAYMENT_ACCOUNT_3: Pubkey = pubkey!("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49");
const TIP_PAYMENT_ACCOUNT_4: Pubkey = pubkey!("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh");
const TIP_PAYMENT_ACCOUNT_5: Pubkey = pubkey!("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt");
const TIP_PAYMENT_ACCOUNT_6: Pubkey = pubkey!("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL");
const TIP_PAYMENT_ACCOUNT_7: Pubkey = pubkey!("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT");

const TIP_DISTRIBUTION_PROGRAM: Pubkey = pubkey!("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7");
const TIP_DISTRIBUTION_CONFIG: Pubkey = pubkey!("8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt");

#[derive(Debug, Clone, Copy)]
pub struct TipDistributionArgs {
    pub vote_account: Pubkey,
    pub merkle_root_upload_authority: Pubkey,
    pub commission_bps: u16,
}

pub(crate) fn init_tip_distribution(
    keypair: &Keypair,
    TipDistributionArgs { vote_account, merkle_root_upload_authority, commission_bps }: TipDistributionArgs,
    epoch: Epoch,
    recent_blockhash: Hash,
) -> (Pubkey, Vec<u8>) {
    let (distribution_key, distribution_bump) = Pubkey::find_program_address(
        &[
            b"TIP_DISTRIBUTION_ACCOUNT",
            vote_account.to_bytes().as_ref(),
            epoch.to_le_bytes().as_ref(),
        ],
        &TIP_DISTRIBUTION_PROGRAM,
    );

    let discriminator = [120, 191, 25, 182, 111, 49, 179, 55];
    let mut data = Vec::with_capacity(discriminator.len() + 35);
    data.extend_from_slice(&discriminator);
    data.extend(borsh::to_vec(&merkle_root_upload_authority).unwrap());
    data.extend(borsh::to_vec(&commission_bps).unwrap());
    data.extend(borsh::to_vec(&distribution_bump).unwrap());
    let ix = Instruction {
        program_id: TIP_DISTRIBUTION_PROGRAM,
        data,
        accounts: vec![
            AccountMeta::new_readonly(TIP_DISTRIBUTION_CONFIG, false),
            AccountMeta::new(distribution_key, false),
            AccountMeta::new_readonly(vote_account, false),
            AccountMeta::new(keypair.pubkey(), true),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
    };

    let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
        &[ix],
        Some(&keypair.pubkey()),
        &[keypair],
        recent_blockhash,
    ));

    (distribution_key, bincode::serialize(&tx).unwrap())
}

pub(crate) struct ChangeTipReceiverArgs {
    pub(crate) old_tip_receiver: Pubkey,
    pub(crate) new_tip_receiver: Pubkey,
    pub(crate) old_block_builder: Pubkey,
    pub(crate) new_block_builder: Pubkey,
    pub(crate) block_builder_commission: u64,
}

pub(crate) fn change_tip_receiver(
    keypair: &Keypair,
    ChangeTipReceiverArgs {
        old_tip_receiver,
        new_tip_receiver,
        old_block_builder,
        new_block_builder,
        block_builder_commission,
    }: ChangeTipReceiverArgs,
    recent_blockhash: Hash,
) -> Vec<u8> {
    let change_tip_ix = Instruction {
        program_id: TIP_PAYMENT_PROGRAM,
        data: [69, 99, 22, 71, 11, 231, 86, 143].to_vec(),
        accounts: vec![
            AccountMeta::new(TIP_PAYMENT_CONFIG, false),
            AccountMeta::new(old_tip_receiver, false),
            AccountMeta::new(new_tip_receiver, false),
            AccountMeta::new(old_block_builder, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_0, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_1, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_2, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_3, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_4, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_5, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_6, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_7, false),
            AccountMeta::new(keypair.pubkey(), true),
        ],
    };

    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&[134, 80, 38, 137, 165, 21, 114, 123]);
    data.extend(borsh::to_vec(&block_builder_commission).unwrap());
    let change_block_builder_ix = Instruction {
        program_id: TIP_PAYMENT_PROGRAM,
        data,
        accounts: vec![
            AccountMeta::new(TIP_PAYMENT_CONFIG, false),
            // We just set the tip reciever in prior IX.
            AccountMeta::new(new_tip_receiver, false),
            AccountMeta::new(old_block_builder, false),
            AccountMeta::new(new_block_builder, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_0, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_1, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_2, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_3, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_4, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_5, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_6, false),
            AccountMeta::new(TIP_PAYMENT_ACCOUNT_7, false),
            AccountMeta::new(keypair.pubkey(), true),
        ],
    };

    let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
        &[change_tip_ix, change_block_builder_ix],
        Some(&keypair.pubkey()),
        &[keypair],
        recent_blockhash,
    ));

    bincode::serialize(&tx).unwrap()
}
