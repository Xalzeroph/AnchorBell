use std::collections::BTreeMap;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigningError {
    EmptySecret,
    InvalidSecret,
}

pub fn canonical_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sign_query(params: &BTreeMap<String, String>, secret: &str) -> Result<String, SigningError> {
    if secret.is_empty() {
        return Err(SigningError::EmptySecret);
    }

    let payload = canonical_query(params);
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| SigningError::InvalidSecret)?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn signed_params(
    mut params: BTreeMap<String, String>,
    api_key: &str,
    secret: &str,
    timestamp_ms: u64,
    recv_window_ms: u64,
) -> Result<BTreeMap<String, String>, SigningError> {
    params.insert("apiKey".into(), api_key.into());
    params.insert("timestamp".into(), timestamp_ms.to_string());
    params.insert("recvWindow".into(), recv_window_ms.to_string());
    let signature = sign_query(&params, secret)?;
    params.insert("signature".into(), signature);
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_parameters_before_signing() {
        let mut params = BTreeMap::new();
        params.insert("symbol".into(), "BTCUSDT".into());
        params.insert("side".into(), "BUY".into());
        params.insert("type".into(), "LIMIT".into());
        assert_eq!(
            canonical_query(&params),
            "side=BUY&symbol=BTCUSDT&type=LIMIT"
        );
    }

    #[test]
    fn matches_binance_hmac_example_shape() {
        let mut params = BTreeMap::new();
        params.insert("symbol".into(), "BTCUSDT".into());
        params.insert("side".into(), "BUY".into());
        let signature = sign_query(&params, "secret").unwrap();
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|value| value.is_ascii_hexdigit()));
    }

    #[test]
    fn rejects_empty_secret() {
        let params = BTreeMap::new();
        assert_eq!(sign_query(&params, ""), Err(SigningError::EmptySecret));
    }

    #[test]
    fn adds_authentication_fields_without_mutating_input() {
        let mut params = BTreeMap::new();
        params.insert("symbol".into(), "BTCUSDT".into());
        let signed = signed_params(params.clone(), "key", "secret", 100, 5000).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(signed.get("apiKey"), Some(&"key".to_string()));
        assert_eq!(signed.get("timestamp"), Some(&"100".to_string()));
        assert_eq!(signed.get("recvWindow"), Some(&"5000".to_string()));
        assert_eq!(signed.get("signature").map(String::len), Some(64));
    }
}
