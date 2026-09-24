//! Canonical strings and Ed25519 signatures shared with the push Worker and
//! the mobile client (design spec §5, contract v2, vectors in §12). The
//! strings are UTF-8, `\n` separated, with no trailing newline; signatures
//! are 64 bytes encoded as 128 lowercase hex characters. Every string is
//! bound to the Worker origin (`aud`) so a signature captured by one Worker
//! deployment can't be replayed against another.

use iroh::{PublicKey, SecretKey, Signature};
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const HOST_REQUEST_PREFIX: &str = "agentbuddy-push-host-v2";
pub const GRANT_PREFIX: &str = "agentbuddy-push-grant-v2";

/// Longest grant validity the Worker accepts (`expires - issued`).
pub const MAX_GRANT_LIFETIME_SECS: u64 = 48 * 60 * 60;
/// Clock skew tolerated for a grant issued "in the future".
pub const GRANT_CLOCK_SKEW_SECS: u64 = 300;

pub const HEADER_HOST: &str = "X-AgentBuddy-Host";
pub const HEADER_TIMESTAMP: &str = "X-AgentBuddy-Timestamp";
pub const HEADER_NONCE: &str = "X-AgentBuddy-Nonce";
pub const HEADER_SIGNATURE: &str = "X-AgentBuddy-Signature";

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// 16 CSPRNG bytes as 32 lowercase hex characters (nonces, event ids).
pub fn random_hex32() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `agentbuddy-push-host-v2\n<origin>\n<METHOD>\n<path>\n<unix secs>\n<nonce>\n<sha256(body)>`
pub fn host_request_canonical(
    origin: &str,
    method: &str,
    path: &str,
    timestamp: u64,
    nonce: &str,
    body_sha256_hex: &str,
) -> String {
    format!(
        "{HOST_REQUEST_PREFIX}\n{origin}\n{method}\n{path}\n{timestamp}\n{nonce}\n{body_sha256_hex}"
    )
}

/// Headers that authenticate one host → Worker request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRequest {
    pub host_id: String,
    pub timestamp: u64,
    pub nonce: String,
    pub signature: String,
}

impl SignedRequest {
    pub fn headers(&self) -> [(&'static str, String); 4] {
        [
            (HEADER_HOST, self.host_id.clone()),
            (HEADER_TIMESTAMP, self.timestamp.to_string()),
            (HEADER_NONCE, self.nonce.clone()),
            (HEADER_SIGNATURE, self.signature.clone()),
        ]
    }
}

/// Sign a request with the host key. `origin` is the Worker origin
/// (`scheme://host[:port]`), `path` excludes the query string. Callers must
/// use a fresh `timestamp` and `nonce` for every attempt (the Worker records
/// nonces before processing a request).
pub fn sign_host_request(
    secret_key: &SecretKey,
    origin: &str,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: &str,
) -> SignedRequest {
    let canonical =
        host_request_canonical(origin, method, path, timestamp, nonce, &sha256_hex(body));
    SignedRequest {
        host_id: secret_key.public().to_string(),
        timestamp,
        nonce: nonce.to_string(),
        signature: hex::encode(secret_key.sign(canonical.as_bytes()).to_bytes()),
    }
}

/// Everything covered by a device grant signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantClaims<'a> {
    /// Worker origin the grant is meant for.
    pub aud: &'a str,
    pub host_id: &'a str,
    pub device_id: &'a str,
    /// `ios` | `android`
    pub platform: &'a str,
    /// `sandbox` | `production` | `none`
    pub environment: &'a str,
    /// The opaque sealed target (§5.5); only its SHA-256 is signed.
    pub sealed_target: &'a str,
    pub agent: &'a str,
    pub thread_id: &'a str,
    pub turn_id: &'a str,
    pub issued: u64,
    pub expires: u64,
    pub nonce: &'a str,
}

impl GrantClaims<'_> {
    pub fn canonical(&self) -> String {
        format!(
            "{GRANT_PREFIX}\naud={}\nhost={}\ndevice={}\nplatform={}\nenvironment={}\ntarget_sha256={}\nagent={}\nthread={}\nturn={}\nissued={}\nexpires={}\nnonce={}",
            self.aud,
            self.host_id,
            self.device_id,
            self.platform,
            self.environment,
            sha256_hex(self.sealed_target.as_bytes()),
            self.agent,
            self.thread_id,
            self.turn_id,
            self.issued,
            self.expires,
            self.nonce,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantError {
    MalformedDeviceId,
    MalformedNonce,
    MalformedSignature,
    InvalidLifetime,
    Expired,
    IssuedInFuture,
    BadSignature,
}

impl std::fmt::Display for GrantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::MalformedDeviceId => "device_id must be 64 lowercase hex characters",
            Self::MalformedNonce => "nonce must be 32 lowercase hex characters",
            Self::MalformedSignature => "signature must be 128 lowercase hex characters",
            Self::InvalidLifetime => "grant lifetime must be positive and at most 48h",
            Self::Expired => "grant has expired",
            Self::IssuedInFuture => "grant issued in the future",
            Self::BadSignature => "grant signature does not verify",
        };
        f.write_str(text)
    }
}

/// Verify a device grant: shape, lifetime window and Ed25519 signature by
/// `claims.device_id`. The caller separately checks that `device_id` is the
/// authenticated iroh peer.
pub fn verify_grant(
    claims: &GrantClaims<'_>,
    signature_hex: &str,
    now: u64,
) -> Result<(), GrantError> {
    if !is_lower_hex(claims.device_id, 64) {
        return Err(GrantError::MalformedDeviceId);
    }
    if !is_lower_hex(claims.nonce, 32) {
        return Err(GrantError::MalformedNonce);
    }
    if !is_lower_hex(signature_hex, 128) {
        return Err(GrantError::MalformedSignature);
    }
    if claims.expires <= claims.issued || claims.expires - claims.issued > MAX_GRANT_LIFETIME_SECS {
        return Err(GrantError::InvalidLifetime);
    }
    if claims.expires <= now {
        return Err(GrantError::Expired);
    }
    if claims.issued > now + GRANT_CLOCK_SKEW_SECS {
        return Err(GrantError::IssuedInFuture);
    }
    let mut key = [0u8; 32];
    hex::decode_to_slice(claims.device_id, &mut key).map_err(|_| GrantError::MalformedDeviceId)?;
    let public = PublicKey::from_bytes(&key).map_err(|_| GrantError::MalformedDeviceId)?;
    let mut sig = [0u8; 64];
    hex::decode_to_slice(signature_hex, &mut sig).map_err(|_| GrantError::MalformedSignature)?;
    public
        .verify(claims.canonical().as_bytes(), &Signature::from_bytes(&sig))
        .map_err(|_| GrantError::BadSignature)
}

#[cfg(test)]
pub(crate) mod vectors {
    //! Spec §12 (v2) test vectors, shared with the Worker and mobile tests.
    pub const AUD: &str = "https://push.example.test";
    pub const HOST_SEED: [u8; 32] = [1u8; 32];
    pub const DEVICE_SEED: [u8; 32] = [2u8; 32];
    pub const HOST_ID: &str = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c";
    pub const DEVICE_ID: &str = "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394";

    pub const SEALED_TARGET: &str = "AQGsAbIgnoY1T7hTI3td4PT6sTx_y_QzphwBk2lhf-zxCwUFBQUFBQUFBQUFBZ-odLYTYW6QLwzbORXVqxoBYANUa4EtrUQcvhO-SD6OJtfJpKJt7AB1O9_WrnlX9wxFfUOQylFkMm8ji3OikI9ZvU_wZHaKZN_Mw5iiKA9ThUZYktapcYlLgIb3bFGgQ4_UHttBPnCwqeBa45g0BTJQO33TxFOrTfbK8HkwaDUeG1NPZGxxgDDKI6YdUyR2YDwE4xdNqIM1WIXyg8b0l0ZoFXhpNpBil2Utqj7hSPCUsIFPHGvGJENgkeIM_hWXuwfK_KOP598o6DflDBF60r9C5N-VnaaY1YxF_VkW2-ySG5tjwNtJ2Pyd6UbAwr_wDDh3t9IXNpuWi4W_nUudMfy_znge2419AjQmS42j6jSjXIiiPP9cmbvJi3hx4wyO2TrEifIPigtxn7SG3g";
    pub const SEALED_TARGET_SHA256: &str =
        "3f40a3fbd4fe15e4a6a69c92c3ea5a264f483efa20e82f0fc4826a2734b58fa1";

    pub const EVENT_BODY: &str = r#"{"eventId":"evt_0123456789abcdef0123456789abcdef","agent":"codex","threadId":"thread-1","turnId":"turn-1","type":"completed","reason":null,"occurredAt":1790300000}"#;
    pub const EVENT_BODY_SHA256: &str =
        "b0afc6ab15f7ae40138b3efcecfd209cb704f96abcb25f14807ed800b72732b3";
    pub const HOST_TIMESTAMP: u64 = 1790300000;
    pub const HOST_NONCE: &str = "00112233445566778899aabbccddeeff";
    pub const HOST_CANONICAL: &str = "agentbuddy-push-host-v2\nhttps://push.example.test\nPOST\n/v2/events\n1790300000\n00112233445566778899aabbccddeeff\nb0afc6ab15f7ae40138b3efcecfd209cb704f96abcb25f14807ed800b72732b3";
    pub const HOST_SIGNATURE: &str = "d3040154598d4426ae08efa11f69365fa37cf164458b05eb56e219c694f679bca6356a2dd8dd7f2151e3663c20dc7d354b71109fd8ab9d8dc8a693d8af1c2b05";

    pub const GRANT_ISSUED: u64 = 1790300000;
    pub const GRANT_EXPIRES: u64 = 1790386400;
    pub const GRANT_NONCE: &str = "ffeeddccbbaa99887766554433221100";
    pub const GRANT_CANONICAL: &str = "agentbuddy-push-grant-v2\naud=https://push.example.test\nhost=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c\ndevice=8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394\nplatform=ios\nenvironment=production\ntarget_sha256=3f40a3fbd4fe15e4a6a69c92c3ea5a264f483efa20e82f0fc4826a2734b58fa1\nagent=codex\nthread=thread-1\nturn=turn-1\nissued=1790300000\nexpires=1790386400\nnonce=ffeeddccbbaa99887766554433221100";
    pub const GRANT_SIGNATURE: &str = "40206a5b444e0c4beb95208ceb7c10f6d613ff68f0c9402ccc789c9a71017de9a1beb291f7c4ff488f3d72193fa4731113d6b84693a5c3d4636195277205bf05";

    pub const REVOKE_CANONICAL: &str = "agentbuddy-push-revoke-v2\naud=https://push.example.test\nhost=8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c\ndevice=8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394\nscope=all\ntimestamp=1790300000\nnonce=0f0e0d0c0b0a09080706050403020100";
    pub const REVOKE_SIGNATURE: &str = "8ebac8fa199a1349c15d693b27a835f6a26d3aad210fe152f52470a78a1e2391d86d0fd2091059482df3d63236c24a934e5c8e0c3ffdc883ef0f91e6c7f34f0e";

    pub fn grant_claims() -> super::GrantClaims<'static> {
        super::GrantClaims {
            aud: AUD,
            host_id: HOST_ID,
            device_id: DEVICE_ID,
            platform: "ios",
            environment: "production",
            sealed_target: SEALED_TARGET,
            agent: "codex",
            thread_id: "thread-1",
            turn_id: "turn-1",
            issued: GRANT_ISSUED,
            expires: GRANT_EXPIRES,
            nonce: GRANT_NONCE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::vectors::*;
    use super::*;

    #[test]
    fn seeds_map_to_spec_node_ids() {
        assert_eq!(
            SecretKey::from_bytes(&HOST_SEED).public().to_string(),
            HOST_ID
        );
        assert_eq!(
            SecretKey::from_bytes(&DEVICE_SEED).public().to_string(),
            DEVICE_ID
        );
    }

    #[test]
    fn host_request_vector() {
        assert_eq!(sha256_hex(EVENT_BODY.as_bytes()), EVENT_BODY_SHA256);
        assert_eq!(
            host_request_canonical(
                AUD,
                "POST",
                "/v2/events",
                HOST_TIMESTAMP,
                HOST_NONCE,
                EVENT_BODY_SHA256
            ),
            HOST_CANONICAL
        );
        let signed = sign_host_request(
            &SecretKey::from_bytes(&HOST_SEED),
            AUD,
            "POST",
            "/v2/events",
            EVENT_BODY.as_bytes(),
            HOST_TIMESTAMP,
            HOST_NONCE,
        );
        assert_eq!(signed.host_id, HOST_ID);
        assert_eq!(signed.timestamp, HOST_TIMESTAMP);
        assert_eq!(signed.nonce, HOST_NONCE);
        assert_eq!(signed.signature, HOST_SIGNATURE);
        let headers = signed.headers();
        assert_eq!(headers[0], ("X-AgentBuddy-Host", HOST_ID.to_string()));
        assert_eq!(
            headers[1],
            ("X-AgentBuddy-Timestamp", "1790300000".to_string())
        );
        assert_eq!(headers[2], ("X-AgentBuddy-Nonce", HOST_NONCE.to_string()));
        assert_eq!(
            headers[3],
            ("X-AgentBuddy-Signature", HOST_SIGNATURE.to_string())
        );
    }

    #[test]
    fn host_signature_is_bound_to_the_worker_origin() {
        let key = SecretKey::from_bytes(&HOST_SEED);
        let other = sign_host_request(
            &key,
            "https://other.example.test",
            "POST",
            "/v2/events",
            EVENT_BODY.as_bytes(),
            HOST_TIMESTAMP,
            HOST_NONCE,
        );
        assert_ne!(other.signature, HOST_SIGNATURE);
    }

    #[test]
    fn empty_body_hash_is_sha256_of_nothing() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sealed_target_hash_vector() {
        assert_eq!(sha256_hex(SEALED_TARGET.as_bytes()), SEALED_TARGET_SHA256);
    }

    #[test]
    fn grant_vector_canonical_and_signature() {
        let claims = grant_claims();
        assert_eq!(claims.canonical(), GRANT_CANONICAL);
        let signature = SecretKey::from_bytes(&DEVICE_SEED).sign(GRANT_CANONICAL.as_bytes());
        assert_eq!(hex::encode(signature.to_bytes()), GRANT_SIGNATURE);
        verify_grant(&claims, GRANT_SIGNATURE, GRANT_ISSUED + 10).unwrap();
    }

    #[test]
    fn revoke_vector_signature() {
        // The host never signs revokes (devices do), but the shared vector
        // keeps all three implementations honest about the key material.
        let signature = SecretKey::from_bytes(&DEVICE_SEED).sign(REVOKE_CANONICAL.as_bytes());
        assert_eq!(hex::encode(signature.to_bytes()), REVOKE_SIGNATURE);
    }

    #[test]
    fn grant_rejects_tampering() {
        let now = GRANT_ISSUED + 10;
        let mut claims = grant_claims();
        claims.turn_id = "turn-2";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, now),
            Err(GrantError::BadSignature)
        );

        let mut claims = grant_claims();
        claims.sealed_target = "AQG-swapped-target";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, now),
            Err(GrantError::BadSignature)
        );

        let mut claims = grant_claims();
        claims.aud = "https://other.example.test";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, now),
            Err(GrantError::BadSignature)
        );

        let mut claims = grant_claims();
        claims.host_id = DEVICE_ID; // signed for another host
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, now),
            Err(GrantError::BadSignature)
        );

        let mut claims = grant_claims();
        claims.environment = "sandbox";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, now),
            Err(GrantError::BadSignature)
        );

        // Signature from the wrong key.
        let claims = grant_claims();
        let forged = SecretKey::from_bytes(&HOST_SEED).sign(claims.canonical().as_bytes());
        assert_eq!(
            verify_grant(&claims, &hex::encode(forged.to_bytes()), now),
            Err(GrantError::BadSignature)
        );
    }

    #[test]
    fn grant_rejects_bad_shapes_and_windows() {
        let claims = grant_claims();
        assert_eq!(
            verify_grant(&claims, &GRANT_SIGNATURE.to_uppercase(), GRANT_ISSUED),
            Err(GrantError::MalformedSignature)
        );
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, GRANT_EXPIRES),
            Err(GrantError::Expired)
        );
        assert_eq!(
            verify_grant(
                &claims,
                GRANT_SIGNATURE,
                GRANT_ISSUED - GRANT_CLOCK_SKEW_SECS - 1
            ),
            Err(GrantError::IssuedInFuture)
        );

        let mut claims = grant_claims();
        claims.expires = claims.issued + MAX_GRANT_LIFETIME_SECS + 1;
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, GRANT_ISSUED),
            Err(GrantError::InvalidLifetime)
        );
        let mut claims = grant_claims();
        claims.expires = claims.issued;
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, GRANT_ISSUED),
            Err(GrantError::InvalidLifetime)
        );

        let mut claims = grant_claims();
        claims.device_id = "not-hex";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, GRANT_ISSUED),
            Err(GrantError::MalformedDeviceId)
        );
        let mut claims = grant_claims();
        claims.nonce = "FFEEDDCCBBAA99887766554433221100";
        assert_eq!(
            verify_grant(&claims, GRANT_SIGNATURE, GRANT_ISSUED),
            Err(GrantError::MalformedNonce)
        );
    }

    #[test]
    fn random_hex32_is_lower_hex() {
        let a = random_hex32();
        assert!(is_lower_hex(&a, 32));
        assert_ne!(a, random_hex32());
    }
}
