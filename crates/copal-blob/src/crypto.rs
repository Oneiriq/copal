//! Encryption at rest for stored objects.
//!
//! The design keeps every property the blob plane already promises:
//!
//! - The content digest is computed over PLAINTEXT, so content
//!   addressing and dedupe survive encryption unchanged.
//! - Objects are sealed in 64 KiB frames, each an independent
//!   AES-256-GCM seal, so ranged reads touch only the frames covering
//!   the requested window and every read verifies integrity.
//! - Each object derives its own key from the master key through
//!   HKDF-SHA256 with a random per-object salt, so counter nonces
//!   (frame index, with a final-frame flag) can never repeat across
//!   objects under one master key.
//! - A magic header (`CPE1`) marks encrypted objects. Objects without
//!   it are legacy plaintext and keep serving, so enabling encryption
//!   is non-destructive; new writes seal, old bytes stay readable.
//!
//! Layout on disk:
//!
//! ```text
//! "CPE1" | salt (16 bytes) | frame*
//! frame = AES-256-GCM seal of up to 65536 plaintext bytes, 16-byte tag
//! nonce = frame index as big-endian u32 in bytes 7..11, final flag in byte 11
//! ```

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use rand::RngCore as _;
use sha2::Sha256;

use copal_core::CopalError;

/// Magic bytes opening every encrypted object.
pub const MAGIC: &[u8; 4] = b"CPE1";
/// Plaintext bytes per frame.
pub const FRAME: usize = 64 * 1024;
/// GCM tag length.
pub const TAG: usize = 16;
/// Header length: magic plus salt.
pub const HEADER: usize = 4 + 16;
/// Ciphertext bytes per full frame.
pub const SEALED_FRAME: usize = FRAME + TAG;

/// The object cipher: a master key, cheap to clone.
#[derive(Clone)]
pub struct BlobCipher {
    master: [u8; 32],
}

impl BlobCipher {
    /// Build from a 64-hex master key string.
    pub fn from_hex(raw: &str) -> copal_core::Result<Self> {
        let bytes = hex::decode(raw.trim())
            .map_err(|_| CopalError::validation("blob encryption key must be hex"))?;
        let master: [u8; 32] = bytes
            .try_into()
            .map_err(|_| CopalError::validation("blob encryption key must be 32 bytes"))?;
        Ok(Self { master })
    }

    /// The per-object cipher for a header's salt.
    pub fn object_cipher(&self, salt: &[u8; 16]) -> copal_core::Result<Aes256Gcm> {
        let hk = Hkdf::<Sha256>::new(Some(salt), &self.master);
        let mut key = [0u8; 32];
        hk.expand(b"copal-blob-v1", &mut key)
            .map_err(|e| CopalError::Blob(format!("key derivation: {e}")))?;
        Ok(Aes256Gcm::new(&key.into()))
    }

    /// Seal a whole plaintext into the on-disk format.
    pub fn seal(&self, plaintext: &[u8]) -> copal_core::Result<Vec<u8>> {
        let mut salt = [0u8; 16];
        rand::rng().fill_bytes(&mut salt);
        let cipher = self.object_cipher(&salt)?;

        let mut out = Vec::with_capacity(sealed_len(plaintext.len()));
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&salt);

        let total = frame_count(plaintext.len());
        for index in 0..total {
            let start = index * FRAME;
            let end = ((index + 1) * FRAME).min(plaintext.len());
            let last = index + 1 == total;
            out.extend_from_slice(&seal_frame(
                &cipher,
                index as u32,
                last,
                &plaintext[start..end],
            )?);
        }
        Ok(out)
    }

    /// Open a whole sealed object (header included in `sealed`).
    pub fn open(&self, sealed: &[u8]) -> copal_core::Result<Vec<u8>> {
        let salt = parse_header(sealed)?;
        let cipher = self.object_cipher(&salt)?;
        let body = &sealed[HEADER..];
        if body.is_empty() {
            return Err(CopalError::Blob("sealed object has no frames".into()));
        }
        let total = frame_count(plaintext_len(sealed.len())?);
        open_frames(&cipher, body, 0, total as u32)
    }
}

/// Validate the magic and return the salt from an object's first bytes.
pub fn parse_header(prefix: &[u8]) -> copal_core::Result<[u8; 16]> {
    if prefix.len() < HEADER || &prefix[..4] != MAGIC {
        return Err(CopalError::Blob("object is not in sealed format".into()));
    }
    Ok(prefix[4..HEADER].try_into().expect("sliced to length"))
}

/// Whether an object's first bytes carry the sealed magic.
pub fn is_sealed(prefix: &[u8]) -> bool {
    prefix.len() >= 4 && &prefix[..4] == MAGIC
}

/// Decrypt consecutive frames starting at `first_index`, given the
/// object's total frame count (the final-flag binding needs it).
pub fn open_frames(
    cipher: &Aes256Gcm,
    frames: &[u8],
    first_index: u32,
    total_frames: u32,
) -> copal_core::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(frames.len());
    let mut index = first_index;
    let mut offset = 0usize;
    while offset < frames.len() {
        let end = (offset + SEALED_FRAME).min(frames.len());
        let last = index + 1 == total_frames;
        out.extend_from_slice(&open_frame(cipher, index, last, &frames[offset..end])?);
        offset = end;
        index += 1;
    }
    Ok(out)
}

/// Seal one frame; the streaming writer drives indexes and flags.
pub fn seal_one(
    cipher: &Aes256Gcm,
    index: u32,
    last: bool,
    plaintext: &[u8],
) -> copal_core::Result<Vec<u8>> {
    seal_frame(cipher, index, last, plaintext)
}

fn nonce_bytes(index: u32, last: bool) -> [u8; 12] {
    let mut bytes = [0u8; 12];
    bytes[7..11].copy_from_slice(&index.to_be_bytes());
    bytes[11] = u8::from(last);
    bytes
}

fn seal_frame(
    cipher: &Aes256Gcm,
    index: u32,
    last: bool,
    plaintext: &[u8],
) -> copal_core::Result<Vec<u8>> {
    let nonce = nonce_bytes(index, last);
    cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &[],
            },
        )
        .map_err(|_| CopalError::Blob("frame seal failed".into()))
}

fn open_frame(
    cipher: &Aes256Gcm,
    index: u32,
    last: bool,
    sealed: &[u8],
) -> copal_core::Result<Vec<u8>> {
    let nonce = nonce_bytes(index, last);
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: sealed,
                aad: &[],
            },
        )
        .map_err(|_| {
            CopalError::Blob("object decryption failed (wrong key or tampered bytes)".into())
        })
}

/// Number of frames a plaintext of `len` seals into (one even for an
/// empty object, so truncation to a bare header is detectable).
pub fn frame_count(len: usize) -> usize {
    if len == 0 {
        1
    } else {
        len.div_ceil(FRAME)
    }
}

/// Sealed on-disk length for a plaintext of `len`.
pub fn sealed_len(len: usize) -> usize {
    HEADER + len + frame_count(len) * TAG
}

/// Plaintext length recovered from the on-disk length.
pub fn plaintext_len(disk_len: usize) -> copal_core::Result<usize> {
    if disk_len < HEADER + TAG {
        return Err(CopalError::Blob("sealed object is truncated".into()));
    }
    let body = disk_len - HEADER;
    let frames = (body - 1) / SEALED_FRAME + 1;
    if body < frames * TAG {
        return Err(CopalError::Blob("sealed object is truncated".into()));
    }
    Ok(body - frames * TAG)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> BlobCipher {
        BlobCipher::from_hex(&"a".repeat(64)).unwrap()
    }

    #[test]
    fn round_trips_across_frame_boundaries() {
        let c = cipher();
        for len in [
            0usize,
            1,
            FRAME - 1,
            FRAME,
            FRAME + 1,
            2 * FRAME,
            2 * FRAME + 7,
        ] {
            let plaintext: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let sealed = c.seal(&plaintext).unwrap();
            assert_eq!(sealed.len(), sealed_len(len), "len {len}");
            assert_eq!(plaintext_len(sealed.len()).unwrap(), len, "len {len}");
            assert_eq!(c.open(&sealed).unwrap(), plaintext, "len {len}");
            assert!(is_sealed(&sealed));
        }
    }

    #[test]
    fn ranged_frame_opens_return_exact_windows() {
        let c = cipher();
        let plaintext: Vec<u8> = (0..3 * FRAME + 100).map(|i| (i % 251) as u8).collect();
        let sealed = c.seal(&plaintext).unwrap();
        let salt = parse_header(&sealed).unwrap();
        let object = c.object_cipher(&salt).unwrap();
        let total = frame_count(plaintext.len()) as u32;

        // Frames 1..=2 cover plaintext [FRAME, 3*FRAME).
        let byte_start = HEADER + SEALED_FRAME;
        let byte_end = HEADER + 3 * SEALED_FRAME;
        let window = open_frames(&object, &sealed[byte_start..byte_end], 1, total).unwrap();
        assert_eq!(window, plaintext[FRAME..3 * FRAME]);
    }

    #[test]
    fn tampering_and_wrong_keys_refuse() {
        let c = cipher();
        let mut sealed = c.seal(b"guarded bytes").unwrap();
        let last = sealed.len() - 1;
        sealed[last] ^= 1;
        assert!(c.open(&sealed).is_err(), "tampered tag must refuse");

        let sealed = c.seal(b"guarded bytes").unwrap();
        let other = BlobCipher::from_hex(&"b".repeat(64)).unwrap();
        assert!(other.open(&sealed).is_err(), "wrong key must refuse");
    }

    #[test]
    fn frames_cannot_be_reordered_or_truncated() {
        let c = cipher();
        let plaintext: Vec<u8> = (0..2 * FRAME).map(|i| (i % 251) as u8).collect();
        let sealed = c.seal(&plaintext).unwrap();
        let mut swapped = sealed[..HEADER].to_vec();
        swapped.extend_from_slice(&sealed[HEADER + SEALED_FRAME..]);
        swapped.extend_from_slice(&sealed[HEADER..HEADER + SEALED_FRAME]);
        assert!(c.open(&swapped).is_err(), "reordered frames must refuse");
        let truncated = &sealed[..HEADER + SEALED_FRAME];
        assert!(c.open(truncated).is_err(), "truncation must refuse");
    }

    #[test]
    fn key_parsing_refuses_bad_input() {
        assert!(BlobCipher::from_hex("short").is_err());
        assert!(BlobCipher::from_hex(&"zz".repeat(32)).is_err());
        assert!(BlobCipher::from_hex(&"a".repeat(64)).is_ok());
    }
}
