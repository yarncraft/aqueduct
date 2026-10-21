//! Signing in to Azure Database for PostgreSQL with a managed identity (#140).
//!
//! The server takes a Microsoft Entra access token as the password. A token lives about an hour,
//! so a fresh one (cached until [`REFRESH_MARGIN`] before it expires) is the password of every
//! connection the source opens: the SQL session and the stream transport's replication
//! connection. Two token sources:
//!
//! - [`AzureIdentity::WorkloadIdentity`] (AKS): the federated service-account token in
//!   `AZURE_FEDERATED_TOKEN_FILE`, re-read on every fetch because the kubelet rotates it, is
//!   exchanged at `{AZURE_AUTHORITY_HOST}{AZURE_TENANT_ID}/oauth2/v2.0/token`
//!   (client-credentials grant with a JWT client assertion);
//! - [`AzureIdentity::ContainerApps`]: the local identity endpoint `IDENTITY_ENDPOINT`, with the
//!   `IDENTITY_HEADER` secret.
//!
//! Neither the token, the client assertion nor `IDENTITY_HEADER` is ever logged or shown by
//! `Debug`; errors carry the HTTP status and the OAuth `error` fields only.

mod http;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aqueduct_core::{Error, Result};
use serde_json::Value;

use http::{form, Request, Url};

/// The Entra resource of Azure Database for PostgreSQL.
pub const RESOURCE: &str = "https://ossrdbms-aad.database.windows.net";
/// [`RESOURCE`] as an OAuth 2.0 v2 scope.
pub const SCOPE: &str = "https://ossrdbms-aad.database.windows.net/.default";
/// Default `AZURE_AUTHORITY_HOST` (public cloud).
pub const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com/";
/// Identity endpoint API version used on Container Apps.
pub const IDENTITY_API_VERSION: &str = "2019-08-01";
/// A cached token is replaced this long before it expires.
pub const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);
/// A token is never cached longer than this, whatever lifetime the endpoint claims.
pub const MAX_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

const ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Which managed identity flow signs in (`auth` in the Postgres source config).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzureIdentity {
    /// `azure-workload-identity`: AKS Workload Identity.
    WorkloadIdentity,
    /// `azure-container-apps`: the Container Apps managed identity endpoint.
    ContainerApps,
}

impl AzureIdentity {
    pub fn name(self) -> &'static str {
        match self {
            Self::WorkloadIdentity => "azure-workload-identity",
            Self::ContainerApps => "azure-container-apps",
        }
    }
}

impl FromStr for AzureIdentity {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "azure-workload-identity" => Ok(Self::WorkloadIdentity),
            "azure-container-apps" => Ok(Self::ContainerApps),
            other => Err(format!(
                "auth = {other:?}: expected \"password\", \"azure-workload-identity\" or \
                 \"azure-container-apps\""
            )),
        }
    }
}

/// An Entra access token. `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken(String);

impl AccessToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }
    /// The token, to send as the password.
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AccessToken(<redacted>)")
    }
}

/// A string that `Debug` does not show.
#[derive(Clone)]
struct Hidden(String);

impl std::fmt::Debug for Hidden {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

#[derive(Debug, Clone)]
enum Provider {
    WorkloadIdentity {
        authority: String,
        tenant: String,
        client_id: String,
        token_file: PathBuf,
    },
    ContainerApps {
        endpoint: Url,
        header: Hidden,
        client_id: Option<String>,
    },
}

#[derive(Debug)]
struct Cached {
    token: AccessToken,
    expires_at: Instant,
}

/// Fetches and caches access tokens for one managed identity. Clones share the cache.
#[derive(Clone)]
pub struct AzureCredential {
    provider: Arc<Provider>,
    cache: Arc<tokio::sync::Mutex<Option<Cached>>>,
}

impl std::fmt::Debug for AzureCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The provider's own Debug hides the identity header; the cache is left out.
        f.debug_struct("AzureCredential")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

fn config_err(kind: AzureIdentity, msg: impl std::fmt::Display) -> Error {
    Error::Config(format!("postgres auth = \"{}\": {msg}", kind.name()))
}

/// Refuses header values that could break the request (CR, LF, other control characters).
fn header_safe(kind: AzureIdentity, what: &str, v: &str) -> Result<()> {
    if v.chars().any(|c| c.is_control()) {
        return Err(config_err(
            kind,
            format!("{what} contains control characters"),
        ));
    }
    Ok(())
}

impl AzureCredential {
    /// Reads the variables the platform injects, through `env` (returns `None` for unset or
    /// empty). `client_id` (`azure_client_id`) overrides `AZURE_CLIENT_ID` on AKS and selects a
    /// user-assigned identity on Container Apps. Errors name the missing variable, never a value.
    pub fn from_env(
        kind: AzureIdentity,
        client_id: Option<&str>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let need = |name: &str, hint: &str| {
            env(name).ok_or_else(|| config_err(kind, format!("{name} is not set; {hint}")))
        };
        match kind {
            AzureIdentity::WorkloadIdentity => {
                const HINT: &str = "the workload identity webhook sets it when the pod has the \
                                    label azure.workload.identity/use: \"true\" and a service \
                                    account annotated with azure.workload.identity/client-id";
                let client_id = match client_id {
                    Some(c) => c.to_string(),
                    None => need(
                        "AZURE_CLIENT_ID",
                        "set azure_client_id, or let the workload identity webhook set it",
                    )?,
                };
                let tenant = need("AZURE_TENANT_ID", HINT)?;
                let token_file = need("AZURE_FEDERATED_TOKEN_FILE", HINT)?;
                let authority =
                    env("AZURE_AUTHORITY_HOST").unwrap_or_else(|| DEFAULT_AUTHORITY.into());
                Self::workload_identity(&authority, &tenant, &client_id, token_file.into())
            }
            AzureIdentity::ContainerApps => {
                const HINT: &str = "Container Apps sets it when the app has a managed identity \
                                    (system- or user-assigned)";
                let endpoint = need("IDENTITY_ENDPOINT", HINT)?;
                let header = need("IDENTITY_HEADER", HINT)?;
                Self::container_apps(&endpoint, &header, client_id)
            }
        }
    }

    /// AKS Workload Identity. `authority` must be `https://`, or `http://` to a loopback host
    /// (a test server).
    pub fn workload_identity(
        authority: &str,
        tenant: &str,
        client_id: &str,
        token_file: PathBuf,
    ) -> Result<Self> {
        let kind = AzureIdentity::WorkloadIdentity;
        let authority = if authority.ends_with('/') {
            authority.to_string()
        } else {
            format!("{authority}/")
        };
        Url::parse(&authority)
            .map_err(|e| config_err(kind, format!("AZURE_AUTHORITY_HOST: {e}")))?;
        if tenant.is_empty()
            || !tenant
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
        {
            return Err(config_err(
                kind,
                "AZURE_TENANT_ID must be a tenant ID or domain name",
            ));
        }
        if client_id.trim().is_empty() {
            return Err(config_err(kind, "the client ID is empty"));
        }
        Ok(Self::with(Provider::WorkloadIdentity {
            authority,
            tenant: tenant.to_string(),
            client_id: client_id.to_string(),
            token_file,
        }))
    }

    /// Container Apps. `endpoint` must be `https://`, or `http://` to a loopback host, which is
    /// what Container Apps provides.
    pub fn container_apps(endpoint: &str, header: &str, client_id: Option<&str>) -> Result<Self> {
        let kind = AzureIdentity::ContainerApps;
        let endpoint = Url::parse(endpoint)
            .map_err(|e| config_err(kind, format!("IDENTITY_ENDPOINT: {e}")))?;
        header_safe(kind, "IDENTITY_HEADER", header)?;
        if client_id.is_some_and(|c| c.trim().is_empty()) {
            return Err(config_err(kind, "azure_client_id is empty"));
        }
        Ok(Self::with(Provider::ContainerApps {
            endpoint,
            header: Hidden(header.to_string()),
            client_id: client_id.map(str::to_string),
        }))
    }

    fn with(provider: Provider) -> Self {
        Self {
            provider: Arc::new(provider),
            cache: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn kind(&self) -> AzureIdentity {
        match &*self.provider {
            Provider::WorkloadIdentity { .. } => AzureIdentity::WorkloadIdentity,
            Provider::ContainerApps { .. } => AzureIdentity::ContainerApps,
        }
    }

    /// A token valid for at least [`REFRESH_MARGIN`] if the identity provider allows it: the
    /// cached one, else a new one. A failure is a `Source` error, so the caller's connect fails
    /// and the usual restart applies.
    pub async fn token(&self) -> Result<AccessToken> {
        let mut cache = self.cache.lock().await;
        let now = Instant::now();
        if let Some(c) = cache.as_ref().filter(|c| fresh(c.expires_at, now)) {
            return Ok(c.token.clone());
        }
        let (token, lifetime) = self.fetch().await.map_err(|e| {
            Error::Source(format!(
                "getting a Microsoft Entra token ({}): {e}",
                self.kind().name()
            ))
        })?;
        tracing::info!(
            auth = self.kind().name(),
            expires_in_secs = lifetime.as_secs(),
            "got a Microsoft Entra access token for Postgres"
        );
        // `lifetime` is clamped to MAX_LIFETIME; an overflow would only mean "do not cache".
        *cache = now.checked_add(lifetime).map(|expires_at| Cached {
            token: token.clone(),
            expires_at,
        });
        Ok(token)
    }

    async fn fetch(&self) -> std::result::Result<(AccessToken, Duration), String> {
        let req = match &*self.provider {
            Provider::WorkloadIdentity { token_file, .. } => {
                let assertion = tokio::fs::read_to_string(token_file).await.map_err(|e| {
                    format!(
                        "reading AZURE_FEDERATED_TOKEN_FILE {}: {e}",
                        token_file.display()
                    )
                })?;
                let assertion = assertion.trim();
                if assertion.is_empty() {
                    return Err(format!(
                        "AZURE_FEDERATED_TOKEN_FILE {} is empty",
                        token_file.display()
                    ));
                }
                self.provider.request(assertion)?
            }
            Provider::ContainerApps { .. } => self.provider.request("")?,
        };
        let resp = http::send(&req).await?;
        parse_token(self.kind(), resp.status, &resp.body, unix_now())
    }
}

impl Provider {
    /// The token request. `assertion` is the federated token (Workload Identity only).
    fn request(&self, assertion: &str) -> std::result::Result<Request, String> {
        match self {
            Provider::WorkloadIdentity {
                authority,
                tenant,
                client_id,
                ..
            } => {
                let url = Url::parse(&format!("{authority}{tenant}/oauth2/v2.0/token"))?;
                let body = form(&[
                    ("grant_type", "client_credentials"),
                    ("client_id", client_id),
                    ("scope", SCOPE),
                    ("client_assertion_type", ASSERTION_TYPE),
                    ("client_assertion", assertion),
                ]);
                Ok(Request {
                    method: "POST",
                    url,
                    headers: vec![],
                    body: Some(("application/x-www-form-urlencoded", body)),
                })
            }
            Provider::ContainerApps {
                endpoint,
                header,
                client_id,
            } => {
                let mut q = vec![
                    ("api-version", IDENTITY_API_VERSION),
                    ("resource", RESOURCE),
                ];
                if let Some(c) = client_id {
                    q.push(("client_id", c));
                }
                Ok(Request {
                    method: "GET",
                    url: endpoint.with_query(&form(&q)),
                    headers: vec![("X-IDENTITY-HEADER", header.0.clone())],
                    body: None,
                })
            }
        }
    }
}

/// Whether a token expiring at `expires_at` may still be handed out at `now`.
fn fresh(expires_at: Instant, now: Instant) -> bool {
    now + REFRESH_MARGIN < expires_at
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A number, or a string holding one (Azure endpoints send both).
fn seconds(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// At most `n` characters of an error field from the server.
fn clip(s: &str) -> String {
    const N: usize = 300;
    let mut out: String = s.chars().take(N).collect();
    if s.chars().count() > N {
        out.push('…');
    }
    out
}

/// The token and how long it stays valid, from a token response. `now` is Unix seconds, for
/// `expires_on`. A non-2xx answer becomes an error with the status and the OAuth `error`,
/// `error_description` (or Container Apps `message`) fields, never anything else of the body.
fn parse_token(
    kind: AzureIdentity,
    status: u16,
    body: &[u8],
    now: u64,
) -> std::result::Result<(AccessToken, Duration), String> {
    let json: Option<Value> = serde_json::from_slice(body).ok();
    if !(200..300).contains(&status) {
        let mut msg = format!("the identity endpoint answered HTTP {status}");
        if let Some(j) = &json {
            for field in ["error", "error_description", "message"] {
                if let Some(v) = j.get(field).and_then(Value::as_str) {
                    msg.push_str(&format!("; {field}: {}", clip(v)));
                }
            }
            if let Some(v) = j.pointer("/error/message").and_then(Value::as_str) {
                msg.push_str(&format!("; error: {}", clip(v)));
            }
        }
        return Err(msg);
    }
    let json = json.ok_or("the token response is not JSON")?;
    let token = json
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or("the token response has no access_token")?;
    if token.chars().any(|c| c.is_control()) {
        return Err("the access_token contains control characters".into());
    }
    let lifetime = match kind {
        AzureIdentity::WorkloadIdentity => seconds(json.get("expires_in")),
        AzureIdentity::ContainerApps => seconds(json.get("expires_on"))
            .map(|at| at.saturating_sub(now))
            .or_else(|| seconds(json.get("expires_in"))),
    }
    .ok_or("the token response has no valid expiry (expires_in / expires_on)")?;
    let lifetime = Duration::from_secs(lifetime).min(MAX_LIFETIME);
    Ok((AccessToken::new(token), lifetime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| m.get(k).cloned()
    }

    fn wi_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("AZURE_CLIENT_ID", "11111111-2222-3333-4444-555555555555"),
            ("AZURE_TENANT_ID", "contoso.onmicrosoft.com"),
            (
                "AZURE_FEDERATED_TOKEN_FILE",
                "/var/run/secrets/azure/tokens/azure-identity-token",
            ),
        ]
    }

    fn body(req: &Request) -> String {
        req.body.as_ref().unwrap().1.clone()
    }

    #[test]
    fn auth_names_parse() {
        assert_eq!(
            "azure-workload-identity".parse::<AzureIdentity>(),
            Ok(AzureIdentity::WorkloadIdentity)
        );
        assert_eq!(
            "azure-container-apps".parse::<AzureIdentity>(),
            Ok(AzureIdentity::ContainerApps)
        );
        assert!("azure"
            .parse::<AzureIdentity>()
            .unwrap_err()
            .contains("password"));
    }

    #[test]
    fn workload_identity_request() {
        let env = wi_env();
        let c = AzureCredential::from_env(AzureIdentity::WorkloadIdentity, None, &env_of(&env))
            .unwrap();
        let req = c.provider.request("eyJ.assert.ion").unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.url.host, "login.microsoftonline.com");
        assert!(req.url.https);
        assert_eq!(req.url.target, "/contoso.onmicrosoft.com/oauth2/v2.0/token");
        assert_eq!(
            body(&req),
            "grant_type=client_credentials\
             &client_id=11111111-2222-3333-4444-555555555555\
             &scope=https%3A%2F%2Fossrdbms-aad.database.windows.net%2F.default\
             &client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer\
             &client_assertion=eyJ.assert.ion"
        );
        assert_eq!(
            req.body.as_ref().unwrap().0,
            "application/x-www-form-urlencoded"
        );
    }

    #[test]
    fn workload_identity_overrides() {
        let mut env = wi_env();
        env.push(("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.us"));
        let c = AzureCredential::from_env(
            AzureIdentity::WorkloadIdentity,
            Some("override-id"),
            &env_of(&env),
        )
        .unwrap();
        let req = c.provider.request("a").unwrap();
        assert_eq!(req.url.host, "login.microsoftonline.us");
        assert!(body(&req).contains("client_id=override-id&"));
        // azure_client_id alone is enough without AZURE_CLIENT_ID.
        let env: Vec<_> = wi_env()
            .into_iter()
            .filter(|(k, _)| *k != "AZURE_CLIENT_ID")
            .collect();
        assert!(AzureCredential::from_env(
            AzureIdentity::WorkloadIdentity,
            Some("x"),
            &env_of(&env)
        )
        .is_ok());
    }

    #[test]
    fn container_apps_request() {
        let env = env_of(&[
            ("IDENTITY_ENDPOINT", "http://localhost:42356/msi/token"),
            ("IDENTITY_HEADER", "s3cr3t-header"),
        ]);
        let c = AzureCredential::from_env(AzureIdentity::ContainerApps, None, &env).unwrap();
        let req = c.provider.request("").unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(
            req.url.target,
            "/msi/token?api-version=2019-08-01&resource=https%3A%2F%2Fossrdbms-aad.database.windows.net"
        );
        assert_eq!(
            req.headers,
            vec![("X-IDENTITY-HEADER", "s3cr3t-header".to_string())]
        );
        assert!(req.body.is_none());
        let c =
            AzureCredential::from_env(AzureIdentity::ContainerApps, Some("uami-id"), &env).unwrap();
        assert!(c
            .provider
            .request("")
            .unwrap()
            .url
            .target
            .ends_with("&client_id=uami-id"));
        // The secret header never shows in Debug.
        assert!(!format!("{c:?}").contains("s3cr3t"));
    }

    #[test]
    fn missing_or_bad_env_is_refused_without_values() {
        for missing in [
            "AZURE_CLIENT_ID",
            "AZURE_TENANT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",
        ] {
            let env: Vec<_> = wi_env()
                .into_iter()
                .filter(|(k, _)| *k != missing)
                .collect();
            let e = AzureCredential::from_env(AzureIdentity::WorkloadIdentity, None, &env_of(&env))
                .unwrap_err()
                .to_string();
            assert!(
                e.contains(missing) && e.contains("azure-workload-identity"),
                "{e}"
            );
        }
        for (k, v) in [
            ("AZURE_AUTHORITY_HOST", "http://login.microsoftonline.com/"),
            ("AZURE_TENANT_ID", "a/b"),
            ("AZURE_TENANT_ID", "a?b"),
        ] {
            let mut env = wi_env();
            env.retain(|(name, _)| *name != k);
            env.push((k, v));
            let e = AzureCredential::from_env(AzureIdentity::WorkloadIdentity, None, &env_of(&env))
                .unwrap_err()
                .to_string();
            assert!(e.contains(k), "{e}");
        }
        for missing in ["IDENTITY_ENDPOINT", "IDENTITY_HEADER"] {
            let env: Vec<_> = [
                ("IDENTITY_ENDPOINT", "http://localhost:42356/msi/token"),
                ("IDENTITY_HEADER", "s3cr3t"),
            ]
            .into_iter()
            .filter(|(k, _)| *k != missing)
            .collect();
            let e = AzureCredential::from_env(AzureIdentity::ContainerApps, None, &env_of(&env))
                .unwrap_err()
                .to_string();
            assert!(e.contains(missing) && !e.contains("s3cr3t"), "{e}");
        }
        // The identity header must not reach a remote host in plaintext, nor inject headers.
        for (endpoint, header) in [
            ("http://10.1.2.3:42356/msi/token", "s3cr3t"),
            ("http://localhost:42356/msi/token", "s3cr3t\r\nX-Evil: 1"),
        ] {
            let e = AzureCredential::from_env(
                AzureIdentity::ContainerApps,
                None,
                &env_of(&[("IDENTITY_ENDPOINT", endpoint), ("IDENTITY_HEADER", header)]),
            )
            .unwrap_err()
            .to_string();
            assert!(!e.contains("s3cr3t"), "{e}");
        }
    }

    #[test]
    fn token_responses_parse() {
        let wi = AzureIdentity::WorkloadIdentity;
        let aca = AzureIdentity::ContainerApps;
        let (t, life) = parse_token(
            wi,
            200,
            br#"{"token_type":"Bearer","expires_in":3599,"ext_expires_in":3599,"access_token":"tok"}"#,
            0,
        )
        .unwrap();
        assert_eq!((t.secret(), life), ("tok", Duration::from_secs(3599)));
        let (_, life) =
            parse_token(wi, 200, br#"{"access_token":"t","expires_in":"60"}"#, 0).unwrap();
        assert_eq!(life, Duration::from_secs(60));
        let (t, life) = parse_token(
            aca,
            200,
            br#"{"access_token":"tok","expires_on":"1700003600","resource":"x","token_type":"Bearer"}"#,
            1_700_000_000,
        )
        .unwrap();
        assert_eq!((t.secret(), life), ("tok", Duration::from_secs(3600)));
        // An absurd lifetime is clamped instead of overflowing the expiry instant.
        for body in [
            format!(r#"{{"access_token":"t","expires_in":{}}}"#, u64::MAX),
            format!(r#"{{"access_token":"t","expires_in":"{}"}}"#, u64::MAX),
        ] {
            let (_, life) = parse_token(wi, 200, body.as_bytes(), 0).unwrap();
            assert_eq!(life, MAX_LIFETIME);
            assert!(Instant::now().checked_add(life).is_some());
        }
        let (_, life) = parse_token(
            aca,
            200,
            format!(r#"{{"access_token":"t","expires_on":{}}}"#, u64::MAX).as_bytes(),
            1_700_000_000,
        )
        .unwrap();
        assert_eq!(life, MAX_LIFETIME);
        // Already expired: lifetime zero, so it is never cached.
        let (_, life) =
            parse_token(aca, 200, br#"{"access_token":"t","expires_on":5}"#, 10).unwrap();
        assert_eq!(life, Duration::ZERO);
        for bad in [
            &b"not json"[..],
            br#"{"expires_in":10}"#,
            br#"{"access_token":"","expires_in":10}"#,
            br#"{"access_token":"t"}"#,
            br#"{"access_token":"t","expires_in":"soon"}"#,
        ] {
            assert!(parse_token(wi, 200, bad, 0).is_err());
        }
    }

    #[test]
    fn error_responses_show_status_and_oauth_fields_only() {
        let e = parse_token(
            AzureIdentity::WorkloadIdentity,
            401,
            br#"{"error":"invalid_client","error_description":"AADSTS700016: app not found","access_token":"leak","trace_id":"t"}"#,
            0,
        )
        .unwrap_err();
        assert!(e.contains("HTTP 401"), "{e}");
        assert!(
            e.contains("invalid_client") && e.contains("AADSTS700016"),
            "{e}"
        );
        assert!(!e.contains("leak") && !e.contains("trace_id"), "{e}");
        let e =
            parse_token(AzureIdentity::ContainerApps, 500, b"<html>oops</html>", 0).unwrap_err();
        assert_eq!(e, "the identity endpoint answered HTTP 500");
        let long = format!(r#"{{"message":"{}"}}"#, "x".repeat(1000));
        let e = parse_token(AzureIdentity::ContainerApps, 400, long.as_bytes(), 0).unwrap_err();
        assert!(e.len() < 400, "{}", e.len());
    }

    #[test]
    fn expiry_margin() {
        let now = Instant::now();
        assert!(fresh(now + Duration::from_secs(3600), now));
        assert!(fresh(now + REFRESH_MARGIN + Duration::from_secs(1), now));
        assert!(!fresh(now + REFRESH_MARGIN, now));
        assert!(!fresh(now + Duration::from_secs(60), now));
        assert!(!fresh(now, now));
    }

    #[test]
    fn debug_is_redacted() {
        let t = AccessToken::new("eyJ0eXAi.secret.token");
        assert_eq!(format!("{t:?}"), "AccessToken(<redacted>)");
        let c =
            AzureCredential::container_apps("http://127.0.0.1:1/t", "hdr-secret", None).unwrap();
        let d = format!("{c:?}");
        assert!(
            !d.contains("hdr-secret") && d.contains("ContainerApps"),
            "{d}"
        );
    }

    /// A one-shot-per-connection plain HTTP server on loopback: records each request's head
    /// and body and answers with `reply(n)` for the n-th request.
    async fn mock(
        reply: impl Fn(usize) -> String + Send + Sync + 'static,
    ) -> (String, Arc<AtomicUsize>, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (h, s) = (hits.clone(), seen.clone());
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let n = h.fetch_add(1, Ordering::SeqCst);
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read the head, then Content-Length bytes of body.
                loop {
                    let k = sock.read(&mut chunk).await.unwrap();
                    buf.extend_from_slice(&chunk[..k]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let len = text
                            .lines()
                            .find_map(|l| l.strip_prefix("Content-Length: "))
                            .map(|v| v.trim().parse::<usize>().unwrap())
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + len || k == 0 {
                            s.lock().unwrap().push(text);
                            break;
                        }
                    }
                    if k == 0 {
                        break;
                    }
                }
                sock.write_all(reply(n).as_bytes()).await.unwrap();
                sock.shutdown().await.ok();
            }
        });
        (format!("http://{addr}"), hits, seen)
    }

    fn ok_json(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    #[tokio::test]
    async fn container_apps_fetches_and_caches() {
        let exp = unix_now() + 3600;
        let (base, hits, seen) = mock(move |n| {
            ok_json(&format!(
                r#"{{"access_token":"tok{n}","expires_on":"{exp}","token_type":"Bearer"}}"#
            ))
        })
        .await;
        let c = AzureCredential::container_apps(&format!("{base}/msi/token"), "hdr", Some("uami"))
            .unwrap();
        assert_eq!(c.token().await.unwrap().secret(), "tok0");
        assert_eq!(c.token().await.unwrap().secret(), "tok0");
        // A clone shares the cache.
        assert_eq!(c.clone().token().await.unwrap().secret(), "tok0");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let req = seen.lock().unwrap()[0].clone();
        assert!(req.starts_with(
            "GET /msi/token?api-version=2019-08-01&resource=https%3A%2F%2Fossrdbms-aad.database.windows.net&client_id=uami HTTP/1.1\r\n"
        ), "{req}");
        assert!(req.contains("\r\nX-IDENTITY-HEADER: hdr\r\n"), "{req}");
    }

    #[tokio::test]
    async fn short_lived_tokens_are_fetched_on_every_connect() {
        let exp = unix_now() + 60;
        let (base, hits, _) = mock(move |n| {
            // Chunked, to cover that path end to end.
            let body = format!(r#"{{"access_token":"tok{n}","expires_on":{exp}}}"#);
            format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
                body.len()
            )
        })
        .await;
        let c = AzureCredential::container_apps(&base, "hdr", None).unwrap();
        assert_eq!(c.token().await.unwrap().secret(), "tok0");
        assert_eq!(c.token().await.unwrap().secret(), "tok1");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn workload_identity_rereads_the_token_file_and_reports_errors() {
        let (base, hits, seen) = mock(|n| match n {
            0 => ok_json(r#"{"access_token":"tokA","expires_in":60}"#),
            1 => ok_json(r#"{"access_token":"tokB","expires_in":3600}"#),
            _ => {
                let b = r#"{"error":"invalid_client","error_description":"AADSTS70021: No matching federated identity record found"}"#;
                format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\n\r\n{b}",
                    b.len()
                )
            }
        })
        .await;
        let file = std::env::temp_dir().join(format!("aq-wi-{}.jwt", std::process::id()));
        std::fs::write(&file, "assertion-one\n").unwrap();
        let c = AzureCredential::workload_identity(&base, "tenant-1", "client-1", file.clone())
            .unwrap();
        assert_eq!(c.token().await.unwrap().secret(), "tokA");
        // The kubelet rotated the file; the next fetch (60 s token, not cached) sends the new one.
        std::fs::write(&file, "assertion-two").unwrap();
        assert_eq!(c.token().await.unwrap().secret(), "tokB");
        {
            let seen = seen.lock().unwrap();
            assert!(seen[0].starts_with("POST /tenant-1/oauth2/v2.0/token HTTP/1.1\r\n"));
            assert!(
                seen[0].ends_with("&client_assertion=assertion-one"),
                "{}",
                seen[0]
            );
            assert!(
                seen[1].ends_with("&client_assertion=assertion-two"),
                "{}",
                seen[1]
            );
            assert!(seen[0].contains("grant_type=client_credentials&client_id=client-1&"));
        }
        // Cached now: a third call makes no request.
        assert_eq!(c.token().await.unwrap().secret(), "tokB");
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        let c2 = AzureCredential::workload_identity(&base, "tenant-1", "client-1", file.clone())
            .unwrap();
        let e = c2.token().await.unwrap_err().to_string();
        assert!(e.starts_with("source error"), "{e}");
        assert!(e.contains("HTTP 400") && e.contains("AADSTS70021"), "{e}");
        assert!(!e.contains("assertion-two"), "{e}");
        std::fs::remove_file(&file).unwrap();
        let e = c2.token().await.unwrap_err().to_string();
        assert!(e.contains("AZURE_FEDERATED_TOKEN_FILE"), "{e}");
    }
}
