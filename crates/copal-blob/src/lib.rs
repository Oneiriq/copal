//! Copal blob plane.
//!
//! Bytes live at content-addressed keys (`ab/cd/<sha256>`) behind the
//! [`BlobStore`] port. Uploads stream through a staging key while the
//! digest accumulates, then finalize with a rename — the content address
//! cannot be known until the last byte, and a crash mid-upload leaves
//! only staging garbage, never a half-written addressed object.
//!
//! [`FsBlobStore`] is the OpenDAL filesystem backend; S3-compatible,
//! Azure, and GCS backends are additional OpenDAL services behind the
//! same port.

use futures::Stream;
use futures::StreamExt as _;
use opendal::{services::Fs, Operator};

use copal_core::{ContentDigest, CopalError, DigestBuilder};

/// Outcome of a finalized streaming upload.
#[derive(Debug, Clone)]
pub struct StoredBlob {
    pub digest: ContentDigest,
    pub size_bytes: u64,
    /// Key the bytes now live under (relative to the store root).
    pub storage_path: String,
}

/// A boxed byte stream from the blob plane.
pub type ByteStream =
    futures::stream::BoxStream<'static, std::result::Result<bytes::Bytes, CopalError>>;

/// The blob-plane port: streaming writes to content-addressed keys,
/// reads back by digest.
pub trait BlobStore: Clone + Send + Sync + 'static {
    /// Stream `body` to storage, computing the digest en route; on
    /// success the bytes are at the content address.
    fn put_streamed<S, E>(
        &self,
        body: S,
    ) -> impl std::future::Future<Output = copal_core::Result<StoredBlob>> + Send
    where
        S: Stream<Item = Result<bytes::Bytes, E>> + Send + Unpin,
        E: std::fmt::Display + Send;

    /// Read a blob's bytes by digest, fully buffered. Prefer
    /// [`BlobStore::open_read`] for anything that might be large.
    fn read(
        &self,
        digest: &ContentDigest,
    ) -> impl std::future::Future<Output = copal_core::Result<bytes::Bytes>> + Send;

    /// Open a blob for streaming: total length plus a byte stream.
    /// The length is known up front (content-addressed objects are
    /// immutable), so responses can carry Content-Length while the
    /// body streams.
    fn open_read(
        &self,
        digest: &ContentDigest,
    ) -> impl std::future::Future<Output = copal_core::Result<(u64, ByteStream)>> + Send;

    /// Whether bytes exist at the digest's address.
    fn exists(
        &self,
        digest: &ContentDigest,
    ) -> impl std::future::Future<Output = copal_core::Result<bool>> + Send;
}

/// Filesystem-backed blob store via OpenDAL.
#[derive(Clone)]
pub struct FsBlobStore {
    op: Operator,
}

impl FsBlobStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: &str) -> copal_core::Result<Self> {
        let builder = Fs::default().root(root);
        let op = Operator::new(builder)
            .map_err(|e| CopalError::Blob(format!("open fs root: {e}")))?
            .finish();
        Ok(Self { op })
    }

    fn addressed(digest: &ContentDigest) -> String {
        format!("objects/{}", digest.storage_key())
    }
}

impl BlobStore for FsBlobStore {
    async fn put_streamed<S, E>(&self, mut body: S) -> copal_core::Result<StoredBlob>
    where
        S: Stream<Item = Result<bytes::Bytes, E>> + Send + Unpin,
        E: std::fmt::Display + Send,
    {
        let staging = format!("staging/{}", ulid::Ulid::new().to_string().to_lowercase());
        let mut writer = self
            .op
            .writer(&staging)
            .await
            .map_err(|e| CopalError::Blob(format!("open staging writer: {e}")))?;

        let mut hasher = DigestBuilder::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| CopalError::Blob(format!("read body: {e}")))?;
            hasher.update(&chunk);
            writer
                .write(chunk)
                .await
                .map_err(|e| CopalError::Blob(format!("write staging: {e}")))?;
        }
        writer
            .close()
            .await
            .map_err(|e| CopalError::Blob(format!("close staging: {e}")))?;

        let (digest, size_bytes) = hasher.finish();
        let target = Self::addressed(&digest);
        // Rename onto the address. If identical content already lives
        // there the overwrite is byte-identical, so a dedupe race is
        // harmless.
        self.op
            .rename(&staging, &target)
            .await
            .map_err(|e| CopalError::Blob(format!("finalize {target}: {e}")))?;

        Ok(StoredBlob {
            digest,
            size_bytes,
            storage_path: target,
        })
    }

    async fn read(&self, digest: &ContentDigest) -> copal_core::Result<bytes::Bytes> {
        let buffer = self
            .op
            .read(&Self::addressed(digest))
            .await
            .map_err(|e| match e.kind() {
                opendal::ErrorKind::NotFound => CopalError::not_found(format!("blob {digest}")),
                _ => CopalError::Blob(format!("read {digest}: {e}")),
            })?;
        Ok(buffer.to_bytes())
    }

    async fn exists(&self, digest: &ContentDigest) -> copal_core::Result<bool> {
        self.op
            .exists(&Self::addressed(digest))
            .await
            .map_err(|e| CopalError::Blob(format!("stat {digest}: {e}")))
    }

    async fn open_read(&self, digest: &ContentDigest) -> copal_core::Result<(u64, ByteStream)> {
        let path = Self::addressed(digest);
        let not_found = |e: &opendal::Error| e.kind() == opendal::ErrorKind::NotFound;
        let stat = self.op.stat(&path).await.map_err(|e| {
            if not_found(&e) {
                CopalError::not_found(format!("blob {digest}"))
            } else {
                CopalError::Blob(format!("stat {digest}: {e}"))
            }
        })?;
        let len = stat.content_length();
        let reader = self
            .op
            .reader(&path)
            .await
            .map_err(|e| CopalError::Blob(format!("open {digest}: {e}")))?;
        let owned = digest.clone();
        let stream = reader
            .into_bytes_stream(0..len)
            .await
            .map_err(|e| CopalError::Blob(format!("stream {digest}: {e}")))?
            .map(move |chunk| chunk.map_err(|e| CopalError::Blob(format!("stream {owned}: {e}"))))
            .boxed();
        Ok((len, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(chunks: &[&'static [u8]]) -> impl Stream<Item = Result<bytes::Bytes, String>> + Unpin {
        futures::stream::iter(
            chunks
                .iter()
                .map(|c| Ok(bytes::Bytes::from_static(c)))
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn streamed_put_lands_at_the_content_address() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();

        let stored = store
            .put_streamed(body(&[b"hello ", b"world"]))
            .await
            .unwrap();
        assert_eq!(stored.digest, ContentDigest::of_bytes(b"hello world"));
        assert_eq!(stored.size_bytes, 11);
        assert!(store.exists(&stored.digest).await.unwrap());

        let back = store.read(&stored.digest).await.unwrap();
        assert_eq!(&back[..], b"hello world");

        // Staging left nothing behind.
        let staging_dir = dir.path().join("staging");
        let leftovers = std::fs::read_dir(&staging_dir)
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(leftovers, 0, "staging must be empty after finalize");
    }

    #[tokio::test]
    async fn duplicate_content_is_one_object() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
        let a = store.put_streamed(body(&[b"same bytes"])).await.unwrap();
        let b = store.put_streamed(body(&[b"same bytes"])).await.unwrap();
        assert_eq!(a.digest, b.digest);
        assert_eq!(a.storage_path, b.storage_path);
    }

    #[tokio::test]
    async fn missing_blob_reads_as_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(dir.path().to_str().unwrap()).unwrap();
        let absent = ContentDigest::of_bytes(b"never stored");
        assert!(!store.exists(&absent).await.unwrap());
        assert!(matches!(
            store.read(&absent).await.unwrap_err(),
            CopalError::NotFound(_)
        ));
    }
}
