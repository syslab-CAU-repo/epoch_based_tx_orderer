use crate::types::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawEpochTransactionModel;

impl RawEpochTransactionModel {
    pub const ID: &'static str = stringify!(RawEpochTransactionModel);

    pub fn put(
        rollup_id: &RollupId,
        epoch: u64,
        transaction_order: u64,
        raw_transaction: RawEpochTransaction,
        is_direct_sent: bool,
    ) -> Result<(), KvStoreError> {
        let key = &(Self::ID, rollup_id, epoch, transaction_order);

        kvstore()?.put(key, &(raw_transaction, is_direct_sent))
    }

    pub fn put_with_transaction_hash(
        rollup_id: &RollupId,
        transaction_hash: &RawTransactionHash,
        raw_transaction: RawEpochTransaction,
        is_direct_sent: bool,
    ) -> Result<(), KvStoreError> {
        let key = &(Self::ID, rollup_id, transaction_hash);

        kvstore()?.put(key, &(raw_transaction, is_direct_sent))
    }

    pub fn get(
        rollup_id: &RollupId,
        epoch: u64,
        transaction_order: u64,
    ) -> Result<(RawEpochTransaction, bool), KvStoreError> {
        let key = &(Self::ID, rollup_id, epoch, transaction_order);

        kvstore()?.get(key)
    }

    pub fn get_with_transaction_hash(
        rollup_id: &RollupId,
        transaction_hash: &str,
    ) -> Result<(RawEpochTransaction, bool), KvStoreError> {
        let key = &(Self::ID, rollup_id, transaction_hash);

        kvstore()?.get(key)
    }
}

