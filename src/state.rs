use std::{any::Any, collections::BTreeMap, sync::Arc};

use arc_swap::ArcSwap;
use radius_sdk::{
    json_rpc::client::RpcClient,
    kvstore::{CachedKvStore, CachedKvStoreError},
    signature::PrivateKeySigner,
};
use skde::delay_encryption::SkdeParams;

use crate::{
    client::{reward_manager::RewardManagerClient, seeder::SeederClient},
    merkle_tree_manager::MerkleTreeManager,
    profiler::Profiler,
    task::{Decryptor, SharedChannelInfos},
    types::*,
};

pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: Config,
    seeder_client: SeederClient,
    reward_manager_client: RewardManagerClient,
    decryptor: Arc<Decryptor>,
    liveness_service_manager_clients: CachedKvStore,
    validation_service_manager_clients: CachedKvStore,
    // Signers are populated once at boot and read-only afterwards, so they are
    // stored in a lock-free, read-mostly structure to avoid contention on the
    // RPC hot path.
    signers: ArcSwap<BTreeMap<Platform, PrivateKeySigner>>,
    skde_params: SkdeParams,
    profiler: Option<Profiler>,
    rpc_client: Arc<RpcClient>,
    merkle_tree_manager: MerkleTreeManager,
    shared_channel_infos: SharedChannelInfos,
    atomic_epoch_metadata: AtomicEpochMetadataManager,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        seeder_client: SeederClient,
        reward_manager_client: RewardManagerClient,
        decryptor: Arc<Decryptor>,
        liveness_service_manager_clients: CachedKvStore,
        validation_service_manager_clients: CachedKvStore,
        skde_params: SkdeParams,
        profiler: Option<Profiler>,
        rpc_client: Arc<RpcClient>,
        merkle_tree_manager: MerkleTreeManager,
        shared_channel_infos: SharedChannelInfos,
    ) -> Self {
        let inner = AppStateInner {
            config,
            seeder_client,
            reward_manager_client,
            decryptor,
            signers: ArcSwap::from_pointee(BTreeMap::new()),
            liveness_service_manager_clients,
            validation_service_manager_clients,
            skde_params,
            profiler,
            rpc_client,
            merkle_tree_manager,
            shared_channel_infos,
            atomic_epoch_metadata: AtomicEpochMetadataManager::new(),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn seeder_client(&self) -> &SeederClient {
        &self.inner.seeder_client
    }

    pub fn reward_manager_client(&self) -> &RewardManagerClient {
        &self.inner.reward_manager_client
    }

    pub fn skde_params(&self) -> &SkdeParams {
        &self.inner.skde_params
    }

    pub fn profiler(&self) -> Option<Profiler> {
        self.inner.profiler.clone()
    }

    pub fn rpc_client(&self) -> &RpcClient {
        &self.inner.rpc_client
    }

    pub fn merkle_tree_manager(&self) -> &MerkleTreeManager {
        &self.inner.merkle_tree_manager
    }

    pub fn atomic_epoch_metadata(&self) -> &AtomicEpochMetadataManager {
        &self.inner.atomic_epoch_metadata
    }

    pub fn shared_channel_infos(&self) -> &SharedChannelInfos {
        &self.inner.shared_channel_infos
    }

    pub fn decryptor(&self) -> &Arc<Decryptor> {
        &self.inner.decryptor
    }
}

/// Validation client functions
impl AppState {
    pub async fn add_validation_service_manager_client<T>(
        &self,
        platform: Platform,
        validation_service_provider: ValidationServiceProvider,
        validation_service_manager_client: T,
    ) -> Result<(), CachedKvStoreError>
    where
        T: Clone + Any + Send + 'static,
    {
        let key = &(platform, validation_service_provider);

        self.inner
            .validation_service_manager_clients
            .put(key, validation_service_manager_client)
            .await
    }

    pub async fn get_validation_service_manager_client<T>(
        &self,
        platform: &Platform,
        validation_service_provider: &ValidationServiceProvider,
    ) -> Result<T, CachedKvStoreError>
    where
        T: Clone + Any + Send + 'static,
    {
        let key = &(platform, validation_service_provider);

        self.inner.validation_service_manager_clients.get(key).await
    }
}

/// Liveness client functions
impl AppState {
    pub async fn add_liveness_service_manager_client<T>(
        &self,
        platform: Platform,
        liveness_service_provider: LivenessServiceProvider,
        liveness_service_manager_client: T,
    ) -> Result<(), CachedKvStoreError>
    where
        T: Clone + Any + Send + 'static,
    {
        let key = &(platform, liveness_service_provider);

        self.inner
            .liveness_service_manager_clients
            .put(key, liveness_service_manager_client)
            .await
    }

    pub async fn get_liveness_service_manager_client<T>(
        &self,
        platform: Platform,
        liveness_service_provider: LivenessServiceProvider,
    ) -> Result<T, CachedKvStoreError>
    where
        T: Clone + Any + Send + 'static,
    {
        let key = &(platform, liveness_service_provider);

        self.inner.liveness_service_manager_clients.get(key).await
    }
}

/// Signer functions
impl AppState {
    pub async fn add_signer(
        &self,
        platform: Platform,
        signer: PrivateKeySigner,
    ) -> Result<(), CachedKvStoreError> {
        // RCU update: clone the current map, insert, then publish the new map
        // atomically. This only happens at boot, so the clone cost is irrelevant.
        let mut new_signers = BTreeMap::clone(&self.inner.signers.load());
        new_signers.insert(platform, signer);
        self.inner.signers.store(Arc::new(new_signers));

        Ok(())
    }

    pub async fn get_signer(
        &self,
        platform: Platform,
    ) -> Result<PrivateKeySigner, CachedKvStoreError> {
        // Hot path: lock-free load + a cheap `PrivateKeySigner` clone (Arc bump).
        self.inner
            .signers
            .load()
            .get(&platform)
            .cloned()
            .ok_or(CachedKvStoreError::KeyError(std::any::type_name::<
                PrivateKeySigner,
            >()))
    }
}
