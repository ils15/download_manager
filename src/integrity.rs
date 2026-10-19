use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use tokio::io::AsyncReadExt;

pub async fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub fn normalize_sha256(value: &str) -> Option<String> {
    let normalized = value.to_ascii_lowercase();
    (normalized.len() == 64 && normalized.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_a_64_digit_sha256_hex_digest() {
        let expected = "a".repeat(64);
        assert_eq!(
            normalize_sha256(&"A".repeat(64)).as_deref(),
            Some(expected.as_str())
        );
        assert_eq!(normalize_sha256(&format!(" {} ", "a".repeat(64))), None);
        assert_eq!(normalize_sha256("sha256:abcd"), None);
        assert_eq!(normalize_sha256(&"g".repeat(64)), None);
    }

    #[tokio::test]
    async fn computes_the_standard_sha256_digest_for_file_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload.bin");
        tokio::fs::write(&path, b"abc").await.unwrap();
        assert_eq!(
            sha256_file(&path).await.unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
