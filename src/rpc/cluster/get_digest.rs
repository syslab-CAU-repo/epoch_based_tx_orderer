use sha3::{Digest, Keccak256};

use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetDigest {
    pub rollup_id: RollupId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetDigestResponse {
    /// Digest over ordered tx hashes by (epoch, transaction_order).
    pub epoch_digest: [u8; 32],
    /// Digest over ordered tx hashes by (batch_number, batch_tx_order).
    pub batch_digest: [u8; 32],
    /// Number of tx hashes included in `epoch_digest`.
    pub epoch_tx_count: u64,
    /// Number of tx hashes included in `batch_digest`.
    pub batch_tx_count: u64,
}

impl RpcParameter<AppState> for GetDigest {
    type Response = GetDigestResponse;

    fn method() -> &'static str {
        "get_digest"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        let rollup_id = self.rollup_id;

        // === epoch_digest: iterate (epoch, transaction_order) ===
        let epoch_metadata = EpochMetadata::get(&rollup_id)?;

        let mut epoch_hasher = Keccak256::new();

        let mut epoch_tx_count: u64 = 0;
        for (epoch, tx_count_in_epoch) in epoch_metadata.epoch_transaction_orders.iter() {
            for transaction_order in 0..*tx_count_in_epoch {
                let (raw_epoch_tx, _is_direct_sent) =
                    RawEpochTransactionModel::get(&rollup_id, *epoch, transaction_order)?;
                let tx_hash_bytes = raw_epoch_tx
                    .raw_transaction_hash()
                    .as_bytes()
                    .map_err(|e| {
                        RpcError::from(Error::GeneralError(format!("invalid tx hash hex: {e:?}")))
                    })?;
                epoch_hasher.update(tx_hash_bytes);
                epoch_tx_count += 1;
            }
        }

        let epoch_digest: [u8; 32] = epoch_hasher.finalize().into();

        // === batch_digest: iterate (batch_number, batch_tx_order) ===
        let rollup_metadata = RollupMetadata::get(&rollup_id)?;

        let mut batch_hasher = Keccak256::new();

        let mut batch_tx_count: u64 = 0;
        let last_batch_number = rollup_metadata.batch_number;
        for batch_number in 0..=last_batch_number {
            let end_exclusive = if batch_number == last_batch_number {
                rollup_metadata.transaction_order
            } else {
                rollup_metadata.max_transaction_count_per_batch
            };

            for batch_tx_order in 0..end_exclusive {
                let (raw_tx, _is_direct_sent) =
                    RawTransactionModel::get(&rollup_id, batch_number, batch_tx_order)?;
                let tx_hash_bytes = raw_tx
                    .raw_transaction_hash()
                    .as_bytes()
                    .map_err(|e| {
                        RpcError::from(Error::GeneralError(format!("invalid tx hash hex: {e:?}")))
                    })?;
                batch_hasher.update(tx_hash_bytes);
                batch_tx_count += 1;
            }
        }

        let batch_digest: [u8; 32] = batch_hasher.finalize().into();

        Ok(GetDigestResponse {
            epoch_digest,
            batch_digest,
            epoch_tx_count,
            batch_tx_count,
        })
    }
}