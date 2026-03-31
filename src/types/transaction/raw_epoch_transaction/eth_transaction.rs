use ethers_core::types as eth_types;

use crate::{error::Error, types::prelude::*};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EthRawEpochTransaction {
    pub raw_transaction: String,
    #[serde(default)]
    pub epoch: Option<u64>, // None is required by the clients
}

impl Default for EthRawEpochTransaction {
    fn default() -> Self {
        Self {
            raw_transaction: "".to_string(),
            epoch: None,
        }
    }
}

impl From<String> for EthRawEpochTransaction {
    fn from(value: String) -> Self {
        Self {
            raw_transaction: value,
            epoch: None,
        }
    }
}

impl EthRawEpochTransaction {
    pub fn raw_transaction_hash(&self) -> RawTransactionHash {
        let decoded_transaction = decode_rlp_transaction(&self.raw_transaction).unwrap();

        let transaction_hash = const_hex::encode_prefixed(decoded_transaction.hash);

        RawTransactionHash::from(transaction_hash)
    }

    pub fn rollup_transaction(&self) -> Result<eth_types::Transaction, Error> {
        decode_rlp_transaction(&self.raw_transaction).map_err(|_| Error::InvalidTransaction)
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }
}

