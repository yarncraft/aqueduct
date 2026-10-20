//! Authentication on a replication connection: trust, cleartext, md5 and SCRAM-SHA-256 with or
//! without channel binding (`-PLUS`). All cryptography is `postgres-protocol`'s; this module only
//! decides what is allowed and frames the messages.
//!
//! Rules (ADR 0003 §6):
//! - cleartext and md5 are refused on a plaintext connection to a non-loopback host, whatever
//!   `sslmode` says: either one hands a replayable secret to anyone on the path;
//! - with TLS up and `SCRAM-SHA-256-PLUS` offered, SCRAM binds to the server certificate
//!   (`tls-server-end-point`) and never falls back to SCRAM without binding, in any `sslmode`:
//!   under `verify-full` the ADR forbids the downgrade, and under `prefer`, `require` and
//!   `verify-ca` channel binding is what would expose a man in the middle. A certificate whose
//!   signature algorithm defines no binding hash (Ed25519, which RFC 5929 gives none) is
//!   therefore refused with an error naming the algorithm; such a server needs an RSA, ECDSA or
//!   RSASSA-PSS certificate;
//! - the server's final SCRAM message is verified before authentication counts as done, and an
//!   `AuthenticationOk` that skips it is refused.

use aqueduct_core::{Error, Result};
use bytes::{Bytes, BytesMut};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{
    ChannelBinding, ScramSha256, SCRAM_SHA_256, SCRAM_SHA_256_PLUS,
};
use postgres_protocol::message::frontend;

use super::client::{FrameRead, StatusSender};
use super::frame::{proto, Backend};
use crate::SslMode;

fn err(msg: impl Into<String>) -> Error {
    Error::Source(format!("replication connection: {}", msg.into()))
}

/// How the session authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// The server asked for no password.
    Trust,
    Cleartext,
    Md5,
    ScramSha256,
    /// SCRAM bound to the server certificate.
    ScramSha256Plus,
}

/// What the authentication exchange may rely on.
pub(crate) struct AuthContext<'a> {
    pub user: &'a str,
    pub password: Option<&'a [u8]>,
    pub mode: SslMode,
    pub encrypted: bool,
    pub local: bool,
    /// `tls-server-end-point` hash of the server certificate, when TLS is up and the
    /// certificate's signature algorithm defines one.
    pub cert_hash: Option<Vec<u8>>,
    /// The certificate's signature algorithm, for errors.
    pub cert_algorithm: Option<String>,
}

/// SCRAM channel-binding choice, before it becomes a `postgres-protocol` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Binding {
    /// `n`: the client cannot bind (no TLS, or no usable certificate hash).
    Unsupported,
    /// `y`: the client could bind but the server did not offer `-PLUS`.
    Unrequested,
    /// `p=tls-server-end-point` with the certificate hash.
    ServerEndPoint(Vec<u8>),
}

impl Binding {
    fn into_protocol(self) -> ChannelBinding {
        match self {
            Binding::Unsupported => ChannelBinding::unsupported(),
            Binding::Unrequested => ChannelBinding::unrequested(),
            Binding::ServerEndPoint(h) => ChannelBinding::tls_server_end_point(h),
        }
    }
}

/// Picks the SASL mechanism and channel binding for what the server offered.
pub(crate) fn choose_mechanism(
    offered: &[String],
    ctx: &AuthContext<'_>,
) -> Result<(&'static str, Binding)> {
    let plus = offered.iter().any(|m| m == SCRAM_SHA_256_PLUS);
    let plain = offered.iter().any(|m| m == SCRAM_SHA_256);
    if ctx.encrypted && plus {
        // No fallback to SCRAM without binding once the server offered it, in any sslmode.
        return match &ctx.cert_hash {
            Some(h) => Ok((SCRAM_SHA_256_PLUS, Binding::ServerEndPoint(h.clone()))),
            None => Err(err(format!(
                "the server offered SCRAM-SHA-256-PLUS, but no tls-server-end-point channel \
                 binding can be computed for its certificate (signature algorithm {}). \
                 Channel binding is required when the server offers it, in every sslmode \
                 (here {}), so the connection is refused; give the server a certificate \
                 signed with RSA, ECDSA or RSASSA-PSS",
                ctx.cert_algorithm.as_deref().unwrap_or("unknown"),
                super::secure::mode_name(ctx.mode),
            ))),
        };
    }
    if plain {
        let binding = if ctx.encrypted && ctx.cert_hash.is_some() {
            // We could bind but the server did not offer it: `y` lets a server that does
            // support it detect that `-PLUS` was stripped on the way.
            Binding::Unrequested
        } else {
            Binding::Unsupported
        };
        return Ok((SCRAM_SHA_256, binding));
    }
    Err(err(format!(
        "the server offered no supported SASL mechanism ({})",
        offered.join(", ")
    )))
}

fn refuse_plaintext_password(method: &str, ctx: &AuthContext<'_>) -> Result<()> {
    if !ctx.encrypted && !ctx.local {
        return Err(Error::Config(format!(
            "the server asked for {method} password authentication over a plaintext connection \
             to a non-loopback host; refusing to send the password. Use TLS (sslmode=verify-full) \
             or SCRAM authentication"
        )));
    }
    Ok(())
}

fn password<'a>(ctx: &AuthContext<'a>) -> Result<&'a [u8]> {
    ctx.password.ok_or_else(|| {
        Error::Config("the server asked for a password but the postgres url has none".into())
    })
}

async fn send(
    writer: &mut StatusSender,
    build: impl FnOnce(&mut BytesMut) -> std::io::Result<()>,
) -> Result<()> {
    let mut buf = BytesMut::new();
    build(&mut buf).map_err(|e| err(format!("building an authentication message: {e}")))?;
    writer.send(&buf).await
}

/// Parses `AuthenticationSASL`'s mechanism list.
fn mechanisms(data: &Bytes) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut rest = &data[..];
    loop {
        let n = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| proto("unterminated SASL mechanism list"))?;
        if n == 0 {
            return Ok(out);
        }
        out.push(String::from_utf8_lossy(&rest[..n]).into_owned());
        rest = &rest[n + 1..];
    }
}

/// Runs the authentication exchange after the startup message, up to `ReadyForQuery`.
pub(crate) async fn authenticate(
    reader: &mut FrameRead,
    writer: &mut StatusSender,
    ctx: &AuthContext<'_>,
) -> Result<AuthMethod> {
    let mut method = AuthMethod::Trust;
    let mut scram: Option<ScramSha256> = None;
    let mut verified = false;
    let mut authenticated = false;
    loop {
        match reader.read().await? {
            Backend::Auth { code: 0, .. } => {
                if scram.is_some() && !verified {
                    return Err(err(
                        "the server accepted SCRAM authentication without proving it knows the \
                         password (no SASL final message); refusing the connection",
                    ));
                }
                authenticated = true;
                reader.set_authenticated();
            }
            Backend::Auth { .. } if authenticated => {
                return Err(proto("authentication request after AuthenticationOk"))
            }
            Backend::Auth { code: 3, .. } => {
                refuse_plaintext_password("cleartext", ctx)?;
                let pw = password(ctx)?;
                send(writer, |b| frontend::password_message(pw, b)).await?;
                method = AuthMethod::Cleartext;
            }
            Backend::Auth { code: 5, data } => {
                refuse_plaintext_password("md5", ctx)?;
                let pw = password(ctx)?;
                let salt: [u8; 4] = data
                    .get(..4)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| proto("md5 request without a salt"))?;
                let hash = md5_hash(ctx.user.as_bytes(), pw, salt);
                send(writer, |b| frontend::password_message(hash.as_bytes(), b)).await?;
                method = AuthMethod::Md5;
            }
            Backend::Auth { code: 10, data } => {
                if scram.is_some() {
                    return Err(proto("second SASL request"));
                }
                let pw = password(ctx)?;
                let (mechanism, binding) = choose_mechanism(&mechanisms(&data)?, ctx)?;
                method = if mechanism == SCRAM_SHA_256_PLUS {
                    AuthMethod::ScramSha256Plus
                } else {
                    AuthMethod::ScramSha256
                };
                let s = ScramSha256::new(pw, binding.into_protocol());
                send(writer, |b| {
                    frontend::sasl_initial_response(mechanism, s.message(), b)
                })
                .await?;
                scram = Some(s);
            }
            Backend::Auth { code: 11, data } => {
                let s = scram
                    .as_mut()
                    .ok_or_else(|| proto("SASL continue without a SASL exchange"))?;
                s.update(&data).map_err(|e| err(format!("SCRAM: {e}")))?;
                send(writer, |b| frontend::sasl_response(s.message(), b)).await?;
            }
            Backend::Auth { code: 12, data } => {
                let s = scram
                    .as_mut()
                    .ok_or_else(|| proto("SASL final without a SASL exchange"))?;
                s.finish(&data).map_err(|e| {
                    err(format!(
                        "SCRAM: the server's signature does not verify ({e}); it does not know \
                         the password or the connection is intercepted"
                    ))
                })?;
                verified = true;
            }
            Backend::Auth { code, .. } => {
                let name = match code {
                    2 => "Kerberos V5",
                    6 => "SCM credentials",
                    7 | 8 => "GSSAPI",
                    9 => "SSPI",
                    _ => "an unknown",
                };
                return Err(err(format!(
                    "the server asked for {name} authentication (code {code}), which the \
                     replication connection does not support"
                )));
            }
            Backend::ParameterStatus | Backend::BackendKeyData | Backend::Notice
                if authenticated => {}
            Backend::Notice => {}
            Backend::ReadyForQuery if authenticated => return Ok(method),
            Backend::Error { code, message } => {
                return Err(err(format!("startup failed: {message} ({code})")))
            }
            other => return Err(proto(format!("unexpected {other:?} during startup"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(
        encrypted: bool,
        local: bool,
        mode: SslMode,
        hash: Option<Vec<u8>>,
    ) -> AuthContext<'static> {
        AuthContext {
            user: "u",
            password: Some(b"pw"),
            mode,
            encrypted,
            local,
            cert_hash: hash,
            cert_algorithm: Some("Ed25519 (1.3.101.112)".into()),
        }
    }

    fn offered(m: &[&str]) -> Vec<String> {
        m.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn plus_is_chosen_whenever_tls_is_up_and_offered() {
        let both = offered(&[SCRAM_SHA_256_PLUS, SCRAM_SHA_256]);
        for mode in [
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            let (m, b) =
                choose_mechanism(&both, &ctx(true, false, mode, Some(vec![1, 2]))).unwrap();
            assert_eq!(
                (m, b),
                (SCRAM_SHA_256_PLUS, Binding::ServerEndPoint(vec![1, 2]))
            );
        }
    }

    #[test]
    fn no_downgrade_from_plus_in_any_ssl_mode() {
        // TLS up, PLUS offered, but the certificate (Ed25519) has no binding hash: refused in
        // every mode, with or without plain SCRAM on offer, and the error names the algorithm.
        for offer in [
            offered(&[SCRAM_SHA_256_PLUS, SCRAM_SHA_256]),
            offered(&[SCRAM_SHA_256_PLUS]),
        ] {
            for mode in [
                SslMode::Prefer,
                SslMode::Require,
                SslMode::VerifyCa,
                SslMode::VerifyFull,
            ] {
                let e = choose_mechanism(&offer, &ctx(true, false, mode, None))
                    .unwrap_err()
                    .to_string();
                assert!(e.contains("refused"), "{mode:?}: {e}");
                assert!(e.contains("Ed25519"), "{mode:?}: {e}");
                assert!(e.contains("channel binding"), "{mode:?}: {e}");
            }
        }
    }

    #[test]
    fn plain_scram_flags() {
        let plain = offered(&[SCRAM_SHA_256]);
        // TLS up, PLUS not offered: `y`.
        assert_eq!(
            choose_mechanism(&plain, &ctx(true, true, SslMode::VerifyFull, Some(vec![1])))
                .unwrap()
                .1,
            Binding::Unrequested
        );
        // No TLS: `n`.
        assert_eq!(
            choose_mechanism(&plain, &ctx(false, true, SslMode::Disable, None))
                .unwrap()
                .1,
            Binding::Unsupported
        );
        // PLUS offered without TLS cannot be used.
        assert!(choose_mechanism(
            &offered(&[SCRAM_SHA_256_PLUS]),
            &ctx(false, true, SslMode::Disable, None)
        )
        .is_err());
        assert!(choose_mechanism(
            &offered(&["OTHER"]),
            &ctx(true, true, SslMode::VerifyFull, None)
        )
        .is_err());
    }

    #[test]
    fn passwords_are_refused_over_plaintext_to_a_remote_host() {
        for method in ["cleartext", "md5"] {
            for mode in [SslMode::Disable, SslMode::Prefer] {
                assert!(refuse_plaintext_password(method, &ctx(false, false, mode, None)).is_err());
            }
            assert!(
                refuse_plaintext_password(method, &ctx(false, true, SslMode::Disable, None))
                    .is_ok()
            );
            assert!(
                refuse_plaintext_password(method, &ctx(true, false, SslMode::Require, None))
                    .is_ok()
            );
        }
    }

    #[test]
    fn mechanism_list_parsing() {
        assert_eq!(
            mechanisms(&Bytes::from_static(
                b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0"
            ))
            .unwrap(),
            offered(&[SCRAM_SHA_256_PLUS, SCRAM_SHA_256])
        );
        assert!(mechanisms(&Bytes::from_static(b"SCRAM")).is_err());
        assert!(mechanisms(&Bytes::from_static(b"\0")).unwrap().is_empty());
    }
}
