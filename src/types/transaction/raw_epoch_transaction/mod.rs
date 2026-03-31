use crate::{
    error::Error,
    types::prelude::{Deserialize, Serialize},
};

mod eth_bundle_transaction;
mod eth_transaction;
mod model;

pub use eth_bundle_transaction::*;
pub use eth_transaction::*;
pub use model::*;

use crate::types::RawTransactionHash;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct RawEpochTransactionHash(String);

impl Default for RawEpochTransactionHash {
    fn default() -> Self {
        Self(const_hex::encode_prefixed([0; 32]))
    }
}

impl From<[u8; 32]> for RawEpochTransactionHash {
    fn from(value: [u8; 32]) -> Self {
        Self(const_hex::encode_prefixed(value))
    }
}

impl From<String> for RawEpochTransactionHash {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for RawEpochTransactionHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl AsRef<str> for RawEpochTransactionHash {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl RawEpochTransactionHash {
    pub fn new(value: impl AsRef<[u8]>) -> Self {
        Self(const_hex::encode_prefixed(value))
    }

    pub fn as_string(self) -> String {
        self.0
    }

    pub fn as_bytes(self) -> Result<[u8; 32], const_hex::FromHexError> {
        const_hex::decode_to_array::<String, 32>(self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum RawEpochTransaction {
    Eth(EthRawEpochTransaction),
    EthBundle(EthRawEpochBundleTransaction),
}

impl Default for RawEpochTransaction {
    fn default() -> Self {
        RawEpochTransaction::Eth(EthRawEpochTransaction::default())
    }
}

impl From<EthRawEpochTransaction> for RawEpochTransaction {
    fn from(raw_transaction: EthRawEpochTransaction) -> Self {
        RawEpochTransaction::Eth(raw_transaction)
    }
}

impl From<EthRawEpochBundleTransaction> for RawEpochTransaction {
    fn from(raw_transaction: EthRawEpochBundleTransaction) -> Self {
        RawEpochTransaction::EthBundle(raw_transaction)
    }
}

impl RawEpochTransaction {
    pub fn raw_transaction_hash(&self) -> RawTransactionHash {
        match self {
            RawEpochTransaction::Eth(eth) => eth.raw_transaction_hash(),
            RawEpochTransaction::EthBundle(eth_bundle) => eth_bundle.raw_transaction_hash(),
        }
    }

    pub fn get_transaction_gas_limit(&self) -> Result<u64, Error> {
        match self {
            RawEpochTransaction::Eth(eth) => Ok(eth.rollup_transaction()?.gas.as_u64()),
            RawEpochTransaction::EthBundle(_eth_bundle) => todo!(
                "eth_bundle max_gas_limit"
            ),
        }
    }
}

