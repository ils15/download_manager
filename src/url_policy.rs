use thiserror::Error;
use url::Url;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum UrlPolicyError {
    #[error("URL exceeds the 8192-byte limit")]
    TooLong,
    #[error("only absolute HTTP or HTTPS URLs are supported")]
    UnsupportedScheme,
    #[error("URL credentials or authentication-like query parameters are not accepted")]
    Credentials,
    #[error("URL host is missing")]
    MissingHost,
    #[error("URL is malformed")]
    Malformed,
}

pub fn validate_http_url(input: &str) -> Result<Url, UrlPolicyError> {
    if input.len() > 8192 {
        return Err(UrlPolicyError::TooLong);
    }
    let mut url = Url::parse(input).map_err(|_| UrlPolicyError::Malformed)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(UrlPolicyError::UnsupportedScheme);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(UrlPolicyError::Credentials);
    }
    if url.query_pairs().any(|(name, _)| is_auth_query_key(&name)) {
        return Err(UrlPolicyError::Credentials);
    }
    if url.host_str().is_none() {
        return Err(UrlPolicyError::MissingHost);
    }
    // Fragments are client-side only and are not part of an HTTP download request.
    url.set_fragment(None);
    Ok(url)
}

fn is_auth_query_key(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "token"
            | "accesstoken"
            | "access_token"
            | "refreshtoken"
            | "refresh_token"
            | "idtoken"
            | "id_token"
            | "auth"
            | "auth_token"
            | "authorization"
            | "client_secret"
            | "secret"
            | "api_key"
            | "apikey"
            | "key"
            | "signature"
            | "sig"
            | "password"
            | "passwd"
            | "credential"
            | "credentials"
            | "session"
            | "sessionid"
            | "session_id"
            | "sessiontoken"
            | "session_token"
            | "cookie"
            | "awsaccesskeyid"
            | "googleaccessid"
    ) || (name.starts_with("x-amz-")
        && ["credential", "signature", "security-token"]
            .iter()
            .any(|part| name.contains(*part)))
        || (name.starts_with("x-goog-")
            && ["credential", "signature"]
                .iter()
                .any(|part| name.contains(*part)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_http_and_https_but_rejects_other_schemes_and_credentials() {
        assert_eq!(
            validate_http_url("https://example.com/a").unwrap().scheme(),
            "https"
        );
        assert_eq!(
            validate_http_url("http://example.com").unwrap().scheme(),
            "http"
        );
        assert_eq!(
            validate_http_url("https://example.com/file#private-fragment")
                .unwrap()
                .fragment(),
            None
        );
        assert_eq!(
            validate_http_url("file:///secret").unwrap_err(),
            UrlPolicyError::UnsupportedScheme
        );
        assert_eq!(
            validate_http_url("https://user:pass@example.com/").unwrap_err(),
            UrlPolicyError::Credentials
        );
        assert_eq!(
            validate_http_url("https://example.com/a?X-Amz-Signature=private").unwrap_err(),
            UrlPolicyError::Credentials
        );
        assert!(validate_http_url("https://example.com/a?download=full").is_ok());
        assert_eq!(
            validate_http_url(&format!("https://example.com/{}", "a".repeat(8200))).unwrap_err(),
            UrlPolicyError::TooLong
        );
    }
}
