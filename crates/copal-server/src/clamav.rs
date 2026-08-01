//! A clamd client, spoken directly over TCP.
//!
//! clamd's INSTREAM protocol is small enough to implement honestly:
//! a command, length-prefixed chunks, a zero-length terminator, and
//! one line of verdict. Doing it here avoids a client crate in the
//! dependency graph of a service whose whole point is custody of
//! other people's bytes.
//!
//! Wire shape:
//!
//! ```text
//! -> "zINSTREAM\0"
//! -> u32 big-endian chunk length, chunk bytes   (repeated)
//! -> u32 zero                                    (terminator)
//! <- "stream: OK\0" | "stream: <signature> FOUND\0" | "... ERROR\0"
//! ```

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

use copal_core::CopalError;

/// clamd's answer for one scanned stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Clean,
    /// The signature clamd matched.
    Infected(String),
}

/// Largest chunk written per frame. clamd's own StreamMaxLength
/// governs the total; this only bounds our buffer.
const CHUNK: usize = 64 * 1024;

/// Scan bytes through clamd at `addr` (`host:port`).
///
/// Connection and protocol failures return `Err`, which the calling
/// activity treats as retryable. A scanner that cannot be reached
/// must never read as clean.
pub async fn scan(addr: &str, content: &[u8]) -> copal_core::Result<Verdict> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| CopalError::Store(format!("clamd connect {addr}: {e}")))?;

    stream
        .write_all(b"zINSTREAM\0")
        .await
        .map_err(|e| CopalError::Store(format!("clamd write: {e}")))?;
    for chunk in content.chunks(CHUNK) {
        let len = u32::try_from(chunk.len())
            .map_err(|_| CopalError::Store("clamd chunk too large".into()))?;
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| CopalError::Store(format!("clamd write: {e}")))?;
        stream
            .write_all(chunk)
            .await
            .map_err(|e| CopalError::Store(format!("clamd write: {e}")))?;
    }
    stream
        .write_all(&0u32.to_be_bytes())
        .await
        .map_err(|e| CopalError::Store(format!("clamd terminate: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| CopalError::Store(format!("clamd flush: {e}")))?;

    let mut answer = Vec::new();
    stream
        .read_to_end(&mut answer)
        .await
        .map_err(|e| CopalError::Store(format!("clamd read: {e}")))?;
    parse_verdict(&answer)
}

/// Read clamd's one-line answer.
pub fn parse_verdict(answer: &[u8]) -> copal_core::Result<Verdict> {
    let text = String::from_utf8_lossy(answer);
    let line = text.trim_end_matches('\0').trim();
    if line.ends_with("OK") {
        return Ok(Verdict::Clean);
    }
    if let Some(rest) = line.strip_suffix("FOUND") {
        let signature = rest
            .trim()
            .strip_prefix("stream:")
            .unwrap_or(rest)
            .trim()
            .to_owned();
        return Ok(Verdict::Infected(signature));
    }
    Err(CopalError::Store(format!("clamd said: {line}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_lines_parse() {
        assert_eq!(parse_verdict(b"stream: OK\0").unwrap(), Verdict::Clean);
        assert_eq!(
            parse_verdict(b"stream: Eicar-Test-Signature FOUND\0").unwrap(),
            Verdict::Infected("Eicar-Test-Signature".to_owned()),
        );
        // Anything else is an error, never a silent pass.
        assert!(parse_verdict(b"stream: INSTREAM size limit exceeded ERROR\0").is_err());
        assert!(parse_verdict(b"").is_err());
    }
}
