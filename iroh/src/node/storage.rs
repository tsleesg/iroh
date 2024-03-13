use std::path::{Path, PathBuf};

use anyhow::Result;
use ed25519_dalek::{SignatureError, VerifyingKey};
use iroh_sync::store::fs::Store as FsDStore;
use iroh_sync::store::memory::Store as MemDStore;
use iroh_sync::store::{
    DownloadPolicy, DownloadPolicyStore, ImportNamespaceOutcome, PublicKeyStore, Query,
    Store as DStore,
};
use iroh_sync::{
    Author, AuthorId, Capability, NamespaceId, NamespaceSecret, PeerIdBytes, Replica, SignedEntry,
};

/// The configuration of storage options for a node.
#[derive(Debug, Default)]
pub enum NodeStorageConfig {
    #[default]
    Mem,
    Disk {
        /// Storage location for blobs.
        blobs: PathBuf,
        /// Storage location for docs.
        docs: PathBuf,
    },
}

/// The types of storage options for a node.
#[derive(Debug, Clone)]
pub struct NodeStorage {
    blobs: BlobStorage,
    docs: DocsStorage,
}

impl Default for NodeStorage {
    fn default() -> Self {
        Self::memory()
    }
}

impl NodeStorage {
    /// Construct the storage from the given configuration.
    pub async fn from_config(config: NodeStorageConfig) -> Result<Self> {
        match config {
            NodeStorageConfig::Mem => Ok(Self::memory()),
            NodeStorageConfig::Disk { blobs, docs } => Self::disk(blobs, docs).await,
        }
    }

    /// Construct a memory version.
    pub fn memory() -> Self {
        Self {
            blobs: BlobStorage::memory(),
            docs: DocsStorage::memory(),
        }
    }

    /// Construct a disk based version.
    pub async fn disk(blob_dir: impl AsRef<Path>, docs_dir: impl AsRef<Path>) -> Result<Self> {
        let blobs = BlobStorage::disk(blob_dir).await?;
        let docs = DocsStorage::disk(docs_dir)?;
        Ok(NodeStorage { blobs, docs })
    }

    /// Returns a references of the used blob storage.
    pub fn blobs(&self) -> &BlobStorage {
        &self.blobs
    }

    /// Returns a reference of the used docs storage.
    pub fn docs(&self) -> &DocsStorage {
        &self.docs
    }

    /// Split into the individual blobs and docs parts.
    pub fn split(self) -> (BlobStorage, DocsStorage) {
        (self.blobs, self.docs)
    }
}

/// Blob storage options.
#[derive(Debug, Clone)]
pub enum BlobStorage {
    /// In memory.
    Mem(iroh_bytes::store::mem::Store),
    /// On disk.
    Disk(iroh_bytes::store::flat::Store),
}

impl Default for BlobStorage {
    fn default() -> Self {
        Self::Mem(Default::default())
    }
}

impl BlobStorage {
    /// Construct a memory based version.
    pub fn memory() -> Self {
        Self::Mem(Default::default())
    }

    /// Construct a disk based version.
    pub async fn disk(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let blobs = iroh_bytes::store::flat::Store::load(&dir)
            .await
            .with_context(|| format!("Failed to load iroh database from {}", dir.display()))?;

        Ok(Self::Disk(blobs))
    }
}

/// Docs storage options.
#[derive(Debug, Clone)]
pub enum DocsStorage {
    /// In memory.
    Mem(iroh_sync::store::memory::Store),
    /// On disk.
    Disk(iroh_sync::store::fs::Store),
}

impl Default for DocsStorage {
    fn default() -> Self {
        Self::Mem(Default::default())
    }
}

impl DocsStorage {
    /// Construct a memory based version.
    pub fn memory() -> Self {
        Self::Mem(Default::default())
    }

    /// Construct a disk based version.
    pub fn disk(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let docs = iroh_sync::store::fs::Store::new(dir)?;

        Ok(Self::Disk(docs))
    }
}

#[derive(Debug, Clone)]
pub enum DocsStorageInstance {
    Mem(<MemDStore as DStore>::Instance),
    Disk(<FsDStore as DStore>::Instance),
}

impl PublicKeyStore for DocsStorageInstance {
    fn public_key(&self, id: &[u8; 32]) -> Result<VerifyingKey, SignatureError> {
        match self {
            Self::Mem(x) => x.public_key(id),
            Self::Disk(x) => x.public_key(id),
        }
    }
}

impl DownloadPolicyStore for DocsStorageInstance {
    fn get_download_policy(&self, namespace: &NamespaceId) -> Result<DownloadPolicy> {
        match self {
            Self::Mem(x) => x.get_download_policy(namespace),
            Self::Disk(x) => x.get_download_policy(namespace),
        }
    }
}

pub enum DocsStorageIterator<X, Y> {
    Mem(X),
    Disk(Y),
}

impl<T, X: Iterator<Item = T>, Y: Iterator<Item = T>> Iterator for DocsStorageIterator<X, Y> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mem(x) => x.next(),
            Self::Disk(x) => x.next(),
        }
    }
}

impl DStore for DocsStorage {
    type Instance = DocsStorageInstance;
    type GetIter<'a> =
        DocsStorageIterator<<MemDStore as DStore>::GetIter<'a>, <FsDStore as DStore>::GetIter<'a>>;
    type ContentHashesIter<'a> = DocsStorageIterator<
        <MemDStore as DStore>::ContentHashesIter<'a>,
        <FsDStore as DStore>::ContentHashesIter<'a>,
    >;
    type NamespaceIter<'a> = DocsStorageIterator<
        <MemDStore as DStore>::NamespaceIter<'a>,
        <FsDStore as DStore>::NamespaceIter<'a>,
    >;
    type AuthorsIter<'a> = DocsStorageIterator<
        <MemDStore as DStore>::AuthorsIter<'a>,
        <FsDStore as DStore>::AuthorsIter<'a>,
    >;
    type LatestIter<'a> = DocsStorageIterator<
        <MemDStore as DStore>::LatestIter<'a>,
        <FsDStore as DStore>::LatestIter<'a>,
    >;
    type PeersIter<'a> = DocsStorageIterator<
        <MemDStore as DStore>::PeersIter<'a>,
        <FsDStore as DStore>::PeersIter<'a>,
    >;

    /// Create a new replica for `namespace` and persist in this store.
    fn new_replica(&self, namespace: NamespaceSecret) -> Result<Replica<Self::Instance>> {
        match self {
            Self::Mem(x) => x
                .new_replica(namespace)
                .map(|r| r.map_store(DocsStorageInstance::Mem)),
            Self::Disk(x) => x
                .new_replica(namespace)
                .map(|r| r.map_store(DocsStorageInstance::Disk)),
        }
    }

    fn import_namespace(&self, capability: Capability) -> Result<ImportNamespaceOutcome> {
        match self {
            Self::Mem(x) => x.import_namespace(capability),
            Self::Disk(x) => x.import_namespace(capability),
        }
    }

    fn list_namespaces(&self) -> Result<Self::NamespaceIter<'_>> {
        match self {
            Self::Mem(x) => x.list_namespaces().map(DocsStorageIterator::Mem),
            Self::Disk(x) => x.list_namespaces().map(DocsStorageIterator::Disk),
        }
    }

    fn open_replica(&self, namespace: &NamespaceId) -> Result<Replica<Self::Instance>, OpenError> {
        match self {
            Self::Mem(x) => x
                .open_replica(namespace)
                .map(|r| r.map_store(DocsStorageInstance::Mem)),
            Self::Disk(x) => x
                .open_replica(namespace)
                .map(|r| r.map_store(DocsStorageInstance::Disk)),
        }
    }

    fn close_replica(&self, replica: Replica<Self::Instance>) {
        match self {
            Self::Mem(x) => x.close_replica(replica),
            Self::Disk(x) => x.close_replica(replica),
        }
    }

    fn remove_replica(&self, namespace: &NamespaceId) -> Result<()> {
        match self {
            Self::Mem(x) => x.remove_replica(namespace),
            Self::Disk(x) => x.remove_replica(namespace),
        }
    }

    fn import_author(&self, author: Author) -> Result<()> {
        match self {
            Self::Mem(x) => x.import_author(author),
            Self::Disk(x) => x.import_author(author),
        }
    }

    fn list_authors(&self) -> Result<Self::AuthorsIter<'_>> {
        match self {
            Self::Mem(x) => x.list_authors().map(DocsStorageIterator::Mem),
            Self::Disk(x) => x.list_authors().map(DocsStorageIterator::Disk),
        }
    }

    fn get_author(&self, author: &AuthorId) -> Result<Option<Author>> {
        match self {
            Self::Mem(x) => x.get_author(author),
            Self::Disk(x) => x.get_author(author),
        }
    }

    fn get_many(
        &self,
        namespace: NamespaceId,
        query: impl Into<Query>,
    ) -> Result<Self::GetIter<'_>> {
        match self {
            Self::Mem(x) => x.get_many(namespace, query).map(DocsStorageIterator::Mem),
            Self::Disk(x) => x.get_many(namespace, query).map(DocsStorageIterator::Disk),
        }
    }

    fn get_exact(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: impl AsRef<[u8]>,
        include_empty: bool,
    ) -> Result<Option<SignedEntry>> {
        match self {
            Self::Mem(x) => x.get_exact(namespace, author, key, include_empty),
            Self::Disk(x) => x.get_exact(namespace, author, key, include_empty),
        }
    }

    fn content_hashes(&self) -> Result<Self::ContentHashesIter<'_>> {
        match self {
            Self::Mem(x) => x.content_hashes().map(DocsStorageIterator::Mem),
            Self::Disk(x) => x.content_hashes().map(DocsStorageIterator::Disk),
        }
    }

    fn get_latest_for_each_author(&self, namespace: NamespaceId) -> Result<Self::LatestIter<'_>> {
        match self {
            Self::Mem(x) => x
                .get_latest_for_each_author(namespace)
                .map(DocsStorageIterator::Mem),
            Self::Disk(x) => x
                .get_latest_for_each_author(namespace)
                .map(DocsStorageIterator::Disk),
        }
    }

    fn register_useful_peer(&self, namespace: NamespaceId, peer: PeerIdBytes) -> Result<()> {
        match self {
            Self::Mem(x) => x.register_useful_peer(namespace, peer),
            Self::Disk(x) => x.register_useful_peer(namespace, peer),
        }
    }

    fn get_sync_peers(&self, namespace: &NamespaceId) -> Result<Option<Self::PeersIter<'_>>> {
        match self {
            Self::Mem(x) => x
                .get_sync_peers(namespace)
                .map(|x| x.map(DocsStorageIterator::Mem)),
            Self::Disk(x) => x
                .get_sync_peers(namespace)
                .map(|x| x.map(DocsStorageIterator::Disk)),
        }
    }

    fn set_download_policy(&self, namespace: &NamespaceId, policy: DownloadPolicy) -> Result<()> {
        match self {
            Self::Mem(x) => x.set_download_policy(namespace, policy),
            Self::Disk(x) => x.set_download_policy(namespace, policy),
        }
    }

    fn get_download_policy(&self, namespace: &NamespaceId) -> Result<DownloadPolicy> {
        match self {
            Self::Mem(x) => x.get_download_policy(namespace),
            Self::Disk(x) => x.get_download_policy(namespace),
        }
    }
}
