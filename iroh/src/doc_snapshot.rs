use iroh_sync::{RecordIdentifier, Record, NamespaceId};

/// A snapshot of a document
pub struct DocsSnapshot {

}

impl DocsSnapshot {
    pub const HEADER: &'static [u8;16] = "DocsSnapshotV0.";
}

/// Metadata for a document snapshot
///
/// This is the wire format for the metadata blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DocsSnapshotMeta {
    // Must be "DocsSnapshotV0."
    header: [u8; 13],
    // The actual entries
    entries: Vec<SignedEntry>,
    // Query prefix for display, not needed for restore
    namespace: Option<NamespaceId>,
    author: Option<AuthorId>,
    prefix: Vec<u8>,
}

impl DocsSnapshotMeta {
    fn new(entries: Vec<SignedEntry>, prefix: Vec<u8>) -> Self {
        Self {
            header: *b"DocSnapshotV0.",
            entries,
            prefix,
        }
    }
}

