#[cfg(windows)]
use std::{ffi::c_void, ptr};

#[cfg(any(windows, test))]
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{BinanceCredentials, BinanceEnvironment, CredentialsError};

#[cfg(any(windows, test))]
const TARGET_PREFIX: &str = "AnchorBell/Binance/";
#[cfg(windows)]
const MAX_CREDENTIAL_BLOB_BYTES: usize = 2_560;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistentCredentialStore;

#[derive(Debug, Error)]
pub enum CredentialStoreError {
    #[error("secure credential storage is unavailable on this platform")]
    Unsupported,
    #[error("stored credential data is invalid")]
    InvalidData,
    #[error("credential value is invalid: {0:?}")]
    InvalidCredential(CredentialsError),
    #[error("Windows credential operation {operation} failed with code {code}")]
    Os { operation: &'static str, code: u32 },
}

#[cfg(any(windows, test))]
#[derive(Debug, Serialize, Deserialize)]
struct StoredCredentials {
    api_key: String,
    api_secret: String,
}

impl Default for PersistentCredentialStore {
    fn default() -> Self {
        Self
    }
}

impl PersistentCredentialStore {
    pub fn is_available(&self) -> bool {
        platform_available()
    }

    pub fn load(
        &self,
        environment: BinanceEnvironment,
    ) -> Result<Option<BinanceCredentials>, CredentialStoreError> {
        platform_load(environment)
    }

    pub fn save(
        &self,
        environment: BinanceEnvironment,
        credentials: &BinanceCredentials,
    ) -> Result<(), CredentialStoreError> {
        platform_save(environment, credentials)
    }

    pub fn delete(&self, environment: BinanceEnvironment) -> Result<(), CredentialStoreError> {
        platform_delete(environment)
    }

    pub fn has_saved(&self, environment: BinanceEnvironment) -> Result<bool, CredentialStoreError> {
        Ok(self.load(environment)?.is_some())
    }
}

#[cfg(any(windows, test))]
fn target_name(environment: BinanceEnvironment) -> String {
    format!("{TARGET_PREFIX}{}", environment.as_str())
}

#[cfg(not(windows))]
fn platform_available() -> bool {
    false
}

#[cfg(not(windows))]
fn platform_load(
    _environment: BinanceEnvironment,
) -> Result<Option<BinanceCredentials>, CredentialStoreError> {
    Err(CredentialStoreError::Unsupported)
}

#[cfg(not(windows))]
fn platform_save(
    _environment: BinanceEnvironment,
    _credentials: &BinanceCredentials,
) -> Result<(), CredentialStoreError> {
    Err(CredentialStoreError::Unsupported)
}

#[cfg(not(windows))]
fn platform_delete(_environment: BinanceEnvironment) -> Result<(), CredentialStoreError> {
    Err(CredentialStoreError::Unsupported)
}
#[cfg(windows)]
fn platform_available() -> bool {
    true
}

#[cfg(windows)]
fn platform_load(
    environment: BinanceEnvironment,
) -> Result<Option<BinanceCredentials>, CredentialStoreError> {
    use windows_sys::Win32::{
        Foundation::{GetLastError, ERROR_NOT_FOUND},
        Security::Credentials::{CredFree, CredReadW, CREDENTIALW, CRED_TYPE_GENERIC},
    };

    let target = utf16_null_terminated(&target_name(environment));
    let mut raw: *mut CREDENTIALW = ptr::null_mut();
    let read_ok = unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut raw) };
    if read_ok == 0 {
        let code = unsafe { GetLastError() };
        if code == ERROR_NOT_FOUND {
            return Ok(None);
        }
        return Err(CredentialStoreError::Os {
            operation: "CredReadW",
            code,
        });
    }
    if raw.is_null() {
        return Err(CredentialStoreError::InvalidData);
    }

    let decoded = unsafe {
        let credential = &*raw;
        let blob_size = credential.CredentialBlobSize as usize;
        if credential.CredentialBlob.is_null()
            || blob_size == 0
            || blob_size > MAX_CREDENTIAL_BLOB_BYTES
        {
            Err(CredentialStoreError::InvalidData)
        } else {
            let bytes = std::slice::from_raw_parts(credential.CredentialBlob, blob_size);
            serde_json::from_slice::<StoredCredentials>(bytes)
                .map_err(|_| CredentialStoreError::InvalidData)
                .and_then(to_credentials)
        }
    };
    unsafe {
        CredFree(raw as *const c_void);
    }
    decoded.map(Some)
}

#[cfg(windows)]
fn platform_save(
    environment: BinanceEnvironment,
    credentials: &BinanceCredentials,
) -> Result<(), CredentialStoreError> {
    use windows_sys::Win32::{
        Foundation::GetLastError,
        Security::Credentials::{
            CredWriteW, CREDENTIALW, CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC,
        },
    };

    let stored = StoredCredentials {
        api_key: credentials.api_key.clone(),
        api_secret: credentials.api_secret.clone(),
    };
    let blob = serde_json::to_vec(&stored).map_err(|_| CredentialStoreError::InvalidData)?;
    if blob.is_empty() || blob.len() > MAX_CREDENTIAL_BLOB_BYTES {
        return Err(CredentialStoreError::InvalidData);
    }
    let mut target = utf16_null_terminated(&target_name(environment));
    let credential = CREDENTIALW {
        Type: CRED_TYPE_GENERIC,
        TargetName: target.as_mut_ptr(),
        CredentialBlobSize: blob.len() as u32,
        CredentialBlob: blob.as_ptr() as *mut u8,
        Persist: CRED_PERSIST_LOCAL_MACHINE,
        ..Default::default()
    };
    let write_ok = unsafe { CredWriteW(&credential, 0) };
    if write_ok == 0 {
        return Err(CredentialStoreError::Os {
            operation: "CredWriteW",
            code: unsafe { GetLastError() },
        });
    }
    Ok(())
}
#[cfg(windows)]
fn platform_delete(environment: BinanceEnvironment) -> Result<(), CredentialStoreError> {
    use windows_sys::Win32::{
        Foundation::{GetLastError, ERROR_NOT_FOUND},
        Security::Credentials::{CredDeleteW, CRED_TYPE_GENERIC},
    };

    let target = utf16_null_terminated(&target_name(environment));
    let delete_ok = unsafe { CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) };
    if delete_ok == 0 {
        let code = unsafe { GetLastError() };
        if code == ERROR_NOT_FOUND {
            return Ok(());
        }
        return Err(CredentialStoreError::Os {
            operation: "CredDeleteW",
            code,
        });
    }
    Ok(())
}

#[cfg(windows)]
fn to_credentials(stored: StoredCredentials) -> Result<BinanceCredentials, CredentialStoreError> {
    BinanceCredentials::from_values(stored.api_key, stored.api_secret)
        .map_err(CredentialStoreError::InvalidCredential)
}

#[cfg(windows)]
fn utf16_null_terminated(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_names_are_separate_for_each_environment() {
        assert_eq!(
            target_name(BinanceEnvironment::Testnet),
            "AnchorBell/Binance/testnet"
        );
        assert_eq!(
            target_name(BinanceEnvironment::Production),
            "AnchorBell/Binance/production"
        );
    }

    #[test]
    fn stored_values_round_trip_through_json_without_logging() {
        let stored = StoredCredentials {
            api_key: "key".to_owned(),
            api_secret: "secret".to_owned(),
        };
        let encoded = serde_json::to_vec(&stored).expect("test data serializes");
        let decoded: StoredCredentials =
            serde_json::from_slice(&encoded).expect("test data decodes");
        assert_eq!(decoded.api_key, "key");
        assert_eq!(decoded.api_secret, "secret");
    }
}
