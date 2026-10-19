use crate::url_policy::validate_http_url;

/// Derive a conservative leaf filename from a URL. It never returns a path.
pub fn filename_from_url(raw_url: &str) -> String {
    let candidate = validate_http_url(raw_url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(|mut segments| segments.next_back())
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let decoded = percent_decode(&candidate);
    let leaf = decoded.rsplit(['/', '\\', ':']).next().unwrap_or_default();
    let sanitized: String = leaf
        .chars()
        .filter(|ch| {
            !ch.is_control() && !matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
        })
        .take(100)
        .collect();
    let trimmed = sanitized.trim_matches([' ', '.']);
    if trimmed.is_empty() || matches!(trimmed, "." | "..") {
        "download.bin".to_owned()
    } else if is_windows_device_name(trimmed) {
        format!("_{trimmed}")
    } else {
        trimmed.to_owned()
    }
}

fn is_windows_device_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
    ) || matches!(
        stem.as_str(),
        "COM¹" | "COM²" | "COM³" | "LPT¹" | "LPT²" | "LPT³"
    ) || ["COM", "LPT"].iter().any(|prefix| {
        stem.strip_prefix(prefix).is_some_and(|number| {
            number.len() == 1
                && number.as_bytes()[0].is_ascii_digit()
                && number.as_bytes()[0] != b'0'
        })
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                out.push((high << 4) | low);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_name_is_a_safe_leaf_with_a_fallback() {
        assert_eq!(
            filename_from_url("https://example.com/a%2F..%2Freport.txt?x=1"),
            "report.txt"
        );
        assert_eq!(
            filename_from_url("https://example.com/%2e%2e"),
            "download.bin"
        );
        assert_eq!(filename_from_url("https://example.com/"), "download.bin");
        assert_eq!(filename_from_url("https://example.com/CON.txt"), "_CON.txt");
    }
}
