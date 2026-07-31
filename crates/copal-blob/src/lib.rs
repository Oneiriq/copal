//! Copal blob plane.
//!
//! Bytes live at content-addressed keys (`ab/cd/<sha256>`) behind the
//! [`BlobStore`] port. Uploads stream through a staging key while the
//! digest accumulates, then finalize with a rename. The content address
//! cannot be known until the last byte, and a crash mid-upload leaves
//! only staging garbage, never a half-written addressed object.
//!
//! [`ObjectStore`] speaks any configured OpenDAL backend through one
//! type: the local filesystem and S3-compatible services today, with
//! Azure and GCS as further services behind the same port.

pub mod crypto;

use futures::Stream;
use futures::StreamExt as _;
use opendal::{services::Fs, services::S3, Operator};

use copal_core::{ContentDigest, CopalError, DigestBuilder};

/// One backend's connection parameters, as a residency configures it.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "scheme", rename_all = "lowercase")]
pub enum BackendConfig {
    /// Local filesystem under a root directory.
    Fs {
        root: String,
        /// Optional 64-hex key sealing this residency's objects. Unset
        /// falls back to the deployment master key.
        #[serde(default)]
        encryption_key: Option<String>,
    },
    /// An S3-compatible service. `endpoint` covers MinIO-style and
    /// other compatible targets; unset means AWS itself.
    S3 {
        bucket: String,
        #[serde(default)]
        root: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        region: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        /// Optional 64-hex key sealing this residency's objects.
        #[serde(default)]
        encryption_key: Option<String>,
    },
}

impl BackendConfig {
    /// The residency's own sealing key, when it carries one.
    pub fn encryption_key(&self) -> Option<&str> {
        match self {
            Self::Fs { encryption_key, .. } | Self::S3 { encryption_key, .. } => {
                encryption_key.as_deref()
            }
        }
    }
}

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

    /// Open one byte range `[start, end)` of a blob: total object
    /// length plus the ranged stream. The caller validates the range
    /// against the total; backends may assume `start < end <= len`.
    fn open_range(
        &self,
        digest: &ContentDigest,
        start: u64,
        end: u64,
    ) -> impl std::future::Future<Output = copal_core::Result<(u64, ByteStream)>> + Send;

    /// Whether bytes exist at the digest's address.
    fn exists(
        &self,
        digest: &ContentDigest,
    ) -> impl std::future::Future<Output = copal_core::Result<bool>> + Send;

    /// Remove the object at the digest's address. Removing an absent
    /// object is a no-op; collection must be replay-safe.
    fn delete(
        &self,
        digest: &ContentDigest,
    ) -> impl std::future::Future<Output = copal_core::Result<()>> + Send;

    /// Append a body to a named staged object (creating it), returning
    /// the staged length afterward. Resumable uploads accumulate here;
    /// bytes are staged as received and seal (when configured) at
    /// promotion.
    fn append_staged<S, E>(
        &self,
        key: &str,
        body: S,
    ) -> impl std::future::Future<Output = copal_core::Result<u64>> + Send
    where
        S: Stream<Item = Result<bytes::Bytes, E>> + Send + Unpin,
        E: std::fmt::Display + Send;

    /// Current length of a staged object; zero when absent.
    fn staged_len(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = copal_core::Result<u64>> + Send;

    /// Open a staged object for streaming: its length plus the byte
    /// stream. Cross-residency promotion reads here and writes through
    /// the target's [`BlobStore::put_streamed`].
    fn open_staged(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = copal_core::Result<(u64, ByteStream)>> + Send;

    /// Promote a staged object to its content address: hash it, seal it
    /// when encryption is configured, land it, and remove the staging
    /// entry.
    fn promote_staged(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = copal_core::Result<StoredBlob>> + Send;

    /// Remove a staged object; absent is a no-op.
    fn discard_staged(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = copal_core::Result<()>> + Send;

    /// Delete staging entries older than `ttl`, returning how many were
    /// removed. Age comes from the ULID staging key itself, not from
    /// backend metadata; every backend gets the same clock.
    fn sweep_staging(
        &self,
        ttl: std::time::Duration,
    ) -> impl std::future::Future<Output = copal_core::Result<u64>> + Send;
}

/// The OpenDAL-backed blob store: one type over every configured
/// backend, with optional encryption at rest (see [`crypto`]).
#[derive(Clone)]
pub struct ObjectStore {
    op: Operator,
    cipher: Option<crypto::BlobCipher>,
}

impl ObjectStore {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: &str) -> copal_core::Result<Self> {
        Self::open_backend(&BackendConfig::Fs {
            root: root.to_owned(),
            encryption_key: None,
        })
    }

    /// Open with encryption at rest: new objects seal under the master
    /// key (64-hex); existing plaintext objects keep serving.
    pub fn open_encrypted(root: &str, key_hex: &str) -> copal_core::Result<Self> {
        let mut store = Self::open(root)?;
        store.cipher = Some(crypto::BlobCipher::from_hex(key_hex)?);
        Ok(store)
    }

    /// Open any configured backend.
    pub fn open_backend(config: &BackendConfig) -> copal_core::Result<Self> {
        let op = match config {
            BackendConfig::Fs { root, .. } => Operator::new(Fs::default().root(root))
                .map_err(|e| CopalError::Blob(format!("open fs root: {e}")))?,
            BackendConfig::S3 {
                bucket,
                root,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                ..
            } => {
                let mut builder = S3::default()
                    .bucket(bucket)
                    .access_key_id(access_key_id)
                    .secret_access_key(secret_access_key);
                if let Some(root) = root {
                    builder = builder.root(root);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = region {
                    builder = builder.region(region);
                }
                Operator::new(builder)
                    .map_err(|e| CopalError::Blob(format!("open s3 backend: {e}")))?
            }
        };
        // A residency's own key seals its objects; deployments
        // without one inherit the master key from the caller. Keys are
        // per residency rather than per tenant because a key change
        // scopes deduplication exactly the way a backend change does,
        // and residencies already carry that scope: a tenant that
        // needs its own key gets its own residency.
        let cipher = match config.encryption_key() {
            Some(key) => Some(crypto::BlobCipher::from_hex(key)?),
            None => None,
        };
        Ok(Self { op, cipher })
    }

    /// Attach the encryption-at-rest cipher to any opened backend.
    /// A key the backend config already carries wins: the residency's
    /// own key is more specific than the deployment master.
    pub fn with_cipher(mut self, key_hex: &str) -> copal_core::Result<Self> {
        if self.cipher.is_none() {
            self.cipher = Some(crypto::BlobCipher::from_hex(key_hex)?);
        }
        Ok(self)
    }

    /// Whether this store seals what it writes.
    pub fn is_encrypted(&self) -> bool {
        self.cipher.is_some()
    }

    /// Move a finished object onto its address. Filesystem backends
    /// rename; backends without rename (S3) copy server-side and drop
    /// the source.
    async fn land(&self, from: &str, to: &str) -> copal_core::Result<()> {
        if self.op.info().capability().rename {
            return self
                .op
                .rename(from, to)
                .await
                .map_err(|e| CopalError::Blob(format!("finalize {to}: {e}")));
        }
        self.op
            .copy(from, to)
            .await
            .map_err(|e| CopalError::Blob(format!("finalize {to}: {e}")))?;
        self.op
            .delete(from)
            .await
            .map_err(|e| CopalError::Blob(format!("drop staging {from}: {e}")))
    }

    /// A plaintext stream over a sealed object's frames covering
    /// `[start, end)`: one ciphertext read and one decrypt per frame,
    /// so memory stays one frame regardless of object size.
    fn sealed_stream(
        &self,
        path: &str,
        prefix: &[u8],
        disk_len: u64,
        start: u64,
        end: u64,
    ) -> copal_core::Result<ByteStream> {
        let Some(master) = &self.cipher else {
            return Err(CopalError::Blob(
                "object is sealed but no encryption key is configured".into(),
            ));
        };
        let salt = crypto::parse_header(prefix)?;
        let object = master.object_cipher(&salt)?;
        let logical = crypto::plaintext_len(disk_len as usize)? as u64;
        let total_frames = crypto::frame_count(logical as usize) as u32;
        let frame = crypto::FRAME as u64;
        let first = (start / frame) as u32;
        let last_frame = (end.saturating_sub(1) / frame) as u32;

        let op = self.op.clone();
        let path = path.to_owned();
        let stream = futures::stream::unfold(first, move |index| {
            let op = op.clone();
            let path = path.clone();
            let object = object.clone();
            async move {
                if index > last_frame {
                    return None;
                }
                let ct_start =
                    crypto::HEADER as u64 + u64::from(index) * crypto::SEALED_FRAME as u64;
                let ct_end = (ct_start + crypto::SEALED_FRAME as u64).min(disk_len);
                let result = async {
                    let buffer = op
                        .read_with(&path)
                        .range(ct_start..ct_end)
                        .await
                        .map_err(|e| CopalError::Blob(format!("read frame {index}: {e}")))?;
                    let mut plain =
                        crypto::open_frames(&object, &buffer.to_vec(), index, total_frames)?;
                    // Trim the window edges on the first and last frames.
                    let frame_base = u64::from(index) * frame;
                    let keep_from = start.saturating_sub(frame_base) as usize;
                    let keep_to = ((end - frame_base).min(frame)) as usize;
                    if keep_from > 0 || keep_to < plain.len() {
                        plain = plain[keep_from..keep_to].to_vec();
                    }
                    Ok::<bytes::Bytes, CopalError>(bytes::Bytes::from(plain))
                }
                .await;
                Some((result, index + 1))
            }
        })
        .boxed();
        Ok(stream)
    }

    /// Read the first bytes of an object, enough to classify it. The
    /// range clamps to the object length so tiny legacy objects read
    /// cleanly.
    async fn read_prefix(&self, path: &str, disk_len: u64) -> copal_core::Result<Vec<u8>> {
        let end = disk_len.min(crypto::HEADER as u64);
        if end == 0 {
            return Ok(Vec::new());
        }
        let buffer = self
            .op
            .read_with(path)
            .range(0..end)
            .await
            .map_err(|e| match e.kind() {
                opendal::ErrorKind::NotFound => CopalError::not_found("blob".to_owned()),
                _ => CopalError::Blob(format!("read prefix: {e}")),
            })?;
        Ok(buffer.to_vec())
    }

    fn addressed(digest: &ContentDigest) -> String {
        format!("objects/{}", digest.storage_key())
    }
}

impl BlobStore for ObjectStore {
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
        match &self.cipher {
            None => {
                while let Some(chunk) = body.next().await {
                    let chunk = chunk.map_err(|e| CopalError::Blob(format!("read body: {e}")))?;
                    hasher.update(&chunk);
                    writer
                        .write(chunk)
                        .await
                        .map_err(|e| CopalError::Blob(format!("write staging: {e}")))?;
                }
            }
            Some(master) => {
                // Seal frame by frame while the digest accumulates over
                // the PLAINTEXT. A frame seals with the non-final nonce
                // only while more than one frame's bytes are buffered,
                // so the final frame (sealed after the stream ends) is
                // the only one carrying the final flag.
                let mut salt = [0u8; 16];
                rand::RngCore::fill_bytes(&mut rand::rng(), &mut salt);
                let object = master.object_cipher(&salt)?;
                let mut header = Vec::with_capacity(crypto::HEADER);
                header.extend_from_slice(crypto::MAGIC);
                header.extend_from_slice(&salt);
                writer
                    .write(header)
                    .await
                    .map_err(|e| CopalError::Blob(format!("write staging: {e}")))?;

                let mut pending: Vec<u8> = Vec::with_capacity(2 * crypto::FRAME);
                let mut index: u32 = 0;
                while let Some(chunk) = body.next().await {
                    let chunk = chunk.map_err(|e| CopalError::Blob(format!("read body: {e}")))?;
                    hasher.update(&chunk);
                    pending.extend_from_slice(&chunk);
                    while pending.len() > crypto::FRAME {
                        let frame: Vec<u8> = pending.drain(..crypto::FRAME).collect();
                        let sealed = crypto::seal_one(&object, index, false, &frame)?;
                        index = index
                            .checked_add(1)
                            .ok_or_else(|| CopalError::Blob("object exceeds frame count".into()))?;
                        writer
                            .write(sealed)
                            .await
                            .map_err(|e| CopalError::Blob(format!("write staging: {e}")))?;
                    }
                }
                let sealed = crypto::seal_one(&object, index, true, &pending)?;
                writer
                    .write(sealed)
                    .await
                    .map_err(|e| CopalError::Blob(format!("write staging: {e}")))?;
            }
        }
        writer
            .close()
            .await
            .map_err(|e| CopalError::Blob(format!("close staging: {e}")))?;

        let (digest, size_bytes) = hasher.finish();
        let target = Self::addressed(&digest);
        // Land onto the address. If identical content already lives
        // there the overwrite is byte-identical, so a dedupe race is
        // harmless.
        self.land(&staging, &target).await?;

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
        let bytes = buffer.to_bytes();
        if crypto::is_sealed(&bytes) {
            let Some(master) = &self.cipher else {
                return Err(CopalError::Blob(
                    "object is sealed but no encryption key is configured".into(),
                ));
            };
            return Ok(bytes::Bytes::from(master.open(&bytes)?));
        }
        Ok(bytes)
    }

    async fn exists(&self, digest: &ContentDigest) -> copal_core::Result<bool> {
        self.op
            .exists(&Self::addressed(digest))
            .await
            .map_err(|e| CopalError::Blob(format!("stat {digest}: {e}")))
    }

    async fn delete(&self, digest: &ContentDigest) -> copal_core::Result<()> {
        // OpenDAL delete is a no-op on absent paths, which is exactly
        // the replay-safety collection needs.
        self.op
            .delete(&Self::addressed(digest))
            .await
            .map_err(|e| CopalError::Blob(format!("delete {digest}: {e}")))
    }

    async fn append_staged<S, E>(&self, key: &str, mut body: S) -> copal_core::Result<u64>
    where
        S: Stream<Item = Result<bytes::Bytes, E>> + Send + Unpin,
        E: std::fmt::Display + Send,
    {
        let mut writer = self
            .op
            .writer_with(key)
            .append(true)
            .await
            .map_err(|e| CopalError::Blob(format!("open append {key}: {e}")))?;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| CopalError::Blob(format!("read body: {e}")))?;
            writer
                .write(chunk)
                .await
                .map_err(|e| CopalError::Blob(format!("append {key}: {e}")))?;
        }
        writer
            .close()
            .await
            .map_err(|e| CopalError::Blob(format!("close append {key}: {e}")))?;
        self.staged_len(key).await
    }

    async fn staged_len(&self, key: &str) -> copal_core::Result<u64> {
        match self.op.stat(key).await {
            Ok(stat) => Ok(stat.content_length()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(CopalError::Blob(format!("stat {key}: {e}"))),
        }
    }

    async fn open_staged(&self, key: &str) -> copal_core::Result<(u64, ByteStream)> {
        let len = self.staged_len(key).await?;
        let reader = self
            .op
            .reader(key)
            .await
            .map_err(|e| CopalError::Blob(format!("open staged {key}: {e}")))?;
        let owned = key.to_owned();
        let stream = reader
            .into_bytes_stream(0..len)
            .await
            .map_err(|e| CopalError::Blob(format!("stream staged {key}: {e}")))?
            .map(move |chunk| chunk.map_err(|e| CopalError::Blob(format!("staged {owned}: {e}"))))
            .boxed();
        Ok((len, stream))
    }

    async fn promote_staged(&self, key: &str) -> copal_core::Result<StoredBlob> {
        // Hash the staged plaintext in one streaming pass.
        let len = self.staged_len(key).await?;
        let reader = self
            .op
            .reader(key)
            .await
            .map_err(|e| CopalError::Blob(format!("open staged {key}: {e}")))?;
        let mut stream = reader
            .into_bytes_stream(0..len)
            .await
            .map_err(|e| CopalError::Blob(format!("stream staged {key}: {e}")))?;
        let mut hasher = DigestBuilder::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| CopalError::Blob(format!("read staged {key}: {e}")))?;
            hasher.update(&chunk);
        }
        let (digest, size_bytes) = hasher.finish();
        let target = Self::addressed(&digest);

        match &self.cipher {
            None => {
                self.land(key, &target).await?;
            }
            Some(_) => {
                // Seal through the ordinary write path into a second
                // staging key, then land it and drop the plaintext.
                let reader = self
                    .op
                    .reader(key)
                    .await
                    .map_err(|e| CopalError::Blob(format!("open staged {key}: {e}")))?;
                let plain = reader
                    .into_bytes_stream(0..len)
                    .await
                    .map_err(|e| CopalError::Blob(format!("stream staged {key}: {e}")))?
                    .map(|chunk| chunk.map_err(|e| format!("staged read: {e}")));
                let stored = self.put_streamed(plain).await?;
                if stored.digest != digest {
                    return Err(CopalError::Blob(
                        "staged content changed during promotion".into(),
                    ));
                }
                self.op
                    .delete(key)
                    .await
                    .map_err(|e| CopalError::Blob(format!("drop staged {key}: {e}")))?;
            }
        }
        Ok(StoredBlob {
            digest,
            size_bytes,
            storage_path: target,
        })
    }

    async fn discard_staged(&self, key: &str) -> copal_core::Result<()> {
        self.op
            .delete(key)
            .await
            .map_err(|e| CopalError::Blob(format!("discard {key}: {e}")))
    }

    async fn sweep_staging(&self, ttl: std::time::Duration) -> copal_core::Result<u64> {
        let cutoff = std::time::SystemTime::now() - ttl;
        let entries = self
            .op
            .list("staging/")
            .await
            .map_err(|e| CopalError::Blob(format!("list staging: {e}")))?;
        let mut removed = 0u64;
        for entry in entries {
            // Listings include the directory itself; only files sweep.
            if entry.path().ends_with('/') {
                continue;
            }
            let name = entry.name();
            // The staging key is a ULID, which embeds its mint time;
            // no dependency on backend last-modified metadata. Anything
            // unparseable is foreign garbage and old by definition.
            let expired = match ulid::Ulid::from_string(&name.to_ascii_uppercase()) {
                Ok(id) => id.datetime() < cutoff,
                Err(_) => true,
            };
            if !expired {
                continue;
            }
            self.op
                .delete(entry.path())
                .await
                .map_err(|e| CopalError::Blob(format!("sweep {}: {e}", entry.path())))?;
            removed += 1;
        }
        Ok(removed)
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
        let disk_len = stat.content_length();

        let prefix = self.read_prefix(&path, disk_len).await?;
        if crypto::is_sealed(&prefix) {
            let logical = crypto::plaintext_len(disk_len as usize)? as u64;
            let stream = self.sealed_stream(&path, &prefix, disk_len, 0, logical)?;
            return Ok((logical, stream));
        }

        let reader = self
            .op
            .reader(&path)
            .await
            .map_err(|e| CopalError::Blob(format!("open {digest}: {e}")))?;
        let owned = digest.clone();
        let stream = reader
            .into_bytes_stream(0..disk_len)
            .await
            .map_err(|e| CopalError::Blob(format!("stream {digest}: {e}")))?
            .map(move |chunk| chunk.map_err(|e| CopalError::Blob(format!("stream {owned}: {e}"))))
            .boxed();
        Ok((disk_len, stream))
    }

    async fn open_range(
        &self,
        digest: &ContentDigest,
        start: u64,
        end: u64,
    ) -> copal_core::Result<(u64, ByteStream)> {
        let path = Self::addressed(digest);
        let not_found = |e: &opendal::Error| e.kind() == opendal::ErrorKind::NotFound;
        let stat = self.op.stat(&path).await.map_err(|e| {
            if not_found(&e) {
                CopalError::not_found(format!("blob {digest}"))
            } else {
                CopalError::Blob(format!("stat {digest}: {e}"))
            }
        })?;
        let disk_len = stat.content_length();

        let prefix = self.read_prefix(&path, disk_len).await?;
        if crypto::is_sealed(&prefix) {
            let logical = crypto::plaintext_len(disk_len as usize)? as u64;
            if start >= end || end > logical {
                return Err(CopalError::validation(format!(
                    "range {start}..{end} exceeds object length {logical}",
                )));
            }
            let stream = self.sealed_stream(&path, &prefix, disk_len, start, end)?;
            return Ok((logical, stream));
        }

        if start >= end || end > disk_len {
            return Err(CopalError::validation(format!(
                "range {start}..{end} exceeds object length {disk_len}",
            )));
        }
        let reader = self
            .op
            .reader(&path)
            .await
            .map_err(|e| CopalError::Blob(format!("open {digest}: {e}")))?;
        let owned = digest.clone();
        let stream = reader
            .into_bytes_stream(start..end)
            .await
            .map_err(|e| CopalError::Blob(format!("stream {digest}: {e}")))?
            .map(move |chunk| chunk.map_err(|e| CopalError::Blob(format!("stream {owned}: {e}"))))
            .boxed();
        Ok((disk_len, stream))
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
        let store = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();

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
        let store = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
        let a = store.put_streamed(body(&[b"same bytes"])).await.unwrap();
        let b = store.put_streamed(body(&[b"same bytes"])).await.unwrap();
        assert_eq!(a.digest, b.digest);
        assert_eq!(a.storage_path, b.storage_path);
    }

    #[tokio::test]
    async fn missing_blob_reads_as_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
        let absent = ContentDigest::of_bytes(b"never stored");
        assert!(!store.exists(&absent).await.unwrap());
        assert!(matches!(
            store.read(&absent).await.unwrap_err(),
            CopalError::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn residency_keys_seal_independently() {
        // A residency carrying its own key seals with it, and a store
        // holding a different key cannot open those bytes: key
        // separation is what makes a residency's data its own.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap().to_owned();
        let own_key = "a".repeat(64);
        let other_key = "b".repeat(64);

        let config = BackendConfig::Fs {
            root: root.clone(),
            encryption_key: Some(own_key.clone()),
        };
        let store = ObjectStore::open_backend(&config).unwrap();
        assert!(store.is_encrypted(), "the residency key applies");
        // A master key does not override a residency's own key.
        let store = store.with_cipher(&other_key).unwrap();
        let stored = store
            .put_streamed(body(&[b"residency bytes"]))
            .await
            .unwrap();
        assert_eq!(
            store.read(&stored.digest).await.unwrap(),
            &b"residency bytes"[..],
        );

        let foreign = ObjectStore::open_backend(&BackendConfig::Fs {
            root,
            encryption_key: Some(other_key),
        })
        .unwrap();
        assert!(
            foreign.read(&stored.digest).await.is_err(),
            "another key cannot open this residency's objects",
        );
    }

    #[tokio::test]
    async fn backend_configs_parse_and_open() {
        // The tagged form residencies configure with.
        let dir = tempfile::tempdir().unwrap();
        let raw = format!(
            "{{\"scheme\": \"fs\", \"root\": {}}}",
            serde_json::Value::from(dir.path().to_str().unwrap()),
        );
        let config: BackendConfig = serde_json::from_str(&raw).unwrap();
        let store = ObjectStore::open_backend(&config).unwrap();
        let stored = store.put_streamed(body(&[b"via config"])).await.unwrap();
        assert_eq!(
            store.read(&stored.digest).await.unwrap(),
            &b"via config"[..]
        );

        // The s3 form constructs without contacting anything.
        let raw = r#"{
            "scheme": "s3",
            "bucket": "tenant-bytes",
            "endpoint": "http://127.0.0.1:9000",
            "region": "us-east-1",
            "access_key_id": "ak",
            "secret_access_key": "sk"
        }"#;
        let config: BackendConfig = serde_json::from_str(raw).unwrap();
        assert!(ObjectStore::open_backend(&config).is_ok());
    }
}
