//! TLS for a replication connection: `SSLRequest`, then the rustls handshake with the same
//! `aqueduct_tls::client_config` and `SslMode` semantics as the SQL connection.

use aqueduct_core::{Error, Result};
use bytes::BytesMut;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use x509_cert::der::asn1::ContextSpecific;
use x509_cert::der::{Decode, SliceReader, TagNumber};
use x509_cert::spki::AlgorithmIdentifierOwned;

use super::client::BoxIo;
use crate::SslMode;

fn err(msg: impl Into<String>) -> Error {
    Error::Source(format!("replication connection: {}", msg.into()))
}

/// The byte stream after TLS negotiation.
pub(crate) struct Negotiated {
    pub io: BoxIo,
    pub encrypted: bool,
    /// `tls-server-end-point` channel binding data for SCRAM-SHA-256-PLUS.
    pub cert_hash: Option<Vec<u8>>,
    /// The server certificate's signature algorithm, for error messages.
    pub cert_algorithm: Option<String>,
}

/// Sends `SSLRequest` unless `mode` is `disable`, and upgrades to TLS when the server agrees.
/// `N` is accepted only under `prefer`. The plaintext/loopback policy is the caller's
/// (`tls::check_negotiated`).
pub(crate) async fn negotiate(
    mut tcp: TcpStream,
    host: &str,
    mode: SslMode,
    root_cert: Option<&str>,
) -> Result<Negotiated> {
    let plain = |tcp: TcpStream| Negotiated {
        io: Box::new(tcp),
        encrypted: false,
        cert_hash: None,
        cert_algorithm: None,
    };
    if mode == SslMode::Disable {
        return Ok(plain(tcp));
    }
    let mut req = BytesMut::new();
    frontend::ssl_request(&mut req);
    tcp.write_all(&req)
        .await
        .map_err(|e| err(format!("sending SSLRequest: {e}")))?;
    // Exactly one byte, straight from the socket and never through a buffer: anything the
    // server (or someone on the path) sends after it must reach the TLS handshake, not be
    // read as plaintext protocol (CVE-2021-23214).
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer)
        .await
        .map_err(|e| err(format!("reading the SSLRequest answer: {e}")))?;
    match answer[0] {
        b'S' => {}
        b'N' if mode == SslMode::Prefer => return Ok(plain(tcp)),
        b'N' => {
            return Err(Error::Config(format!(
                "server does not support TLS, and sslmode={} requires it",
                mode_name(mode)
            )));
        }
        b'E' => return Err(err("the server rejected the TLS request")),
        other => return Err(err(format!("unexpected answer {other:#04x} to SSLRequest"))),
    }
    let config = aqueduct_tls::client_config(root_cert, mode.verify(root_cert.is_some()))
        .map_err(Error::Config)?;
    let name = ServerName::try_from(host.to_string())
        .map_err(|e| Error::Config(format!("invalid TLS server name {host:?}: {e}")))?;
    let tls = TlsConnector::from(config)
        .connect(name, tcp)
        .await
        .map_err(|e| err(format!("TLS handshake with {host}: {e}")))?;
    let leaf = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certs| certs.first())
        .map(|cert| cert.as_ref().to_vec());
    let cert_hash = leaf.as_deref().and_then(end_point_hash);
    let cert_algorithm = leaf.as_deref().map(signature_algorithm_name);
    Ok(Negotiated {
        io: Box::new(tls),
        encrypted: true,
        cert_hash,
        cert_algorithm,
    })
}

pub(crate) fn mode_name(mode: SslMode) -> &'static str {
    match mode {
        SslMode::Disable => "disable",
        SslMode::Prefer => "prefer",
        SslMode::Require => "require",
        SslMode::VerifyCa => "verify-ca",
        SslMode::VerifyFull => "verify-full",
    }
}

/// `tls-server-end-point` (RFC 5929 §4.1): the certificate hashed with its signature
/// algorithm's hash, SHA-256 for MD5 and SHA-1. `None` for algorithms without one (EdDSA),
/// for which Postgres cannot compute the binding either.
pub(crate) fn end_point_hash(der: &[u8]) -> Option<Vec<u8>> {
    let cert = x509_cert::Certificate::from_der(der).ok()?;
    let algorithm = signature_hash(&cert.signature_algorithm)?;
    Some(ring::digest::digest(algorithm, der).as_ref().to_vec())
}

/// A readable name for the certificate's signature algorithm: the OID, with a name for the
/// common ones.
pub(crate) fn signature_algorithm_name(der: &[u8]) -> String {
    let Ok(cert) = x509_cert::Certificate::from_der(der) else {
        return "unreadable certificate".into();
    };
    let oid = cert.signature_algorithm.oid.to_string();
    let name = match oid.as_str() {
        "1.3.101.112" => "Ed25519",
        "1.3.101.113" => "Ed448",
        "1.2.840.113549.1.1.10" => "RSASSA-PSS",
        "1.2.840.113549.1.1.11" => "sha256WithRSAEncryption",
        "1.2.840.10045.4.3.2" => "ecdsa-with-SHA256",
        _ => return oid,
    };
    format!("{name} ({oid})")
}

/// The hash RFC 5929 uses for a certificate signature algorithm.
fn signature_hash(alg: &AlgorithmIdentifierOwned) -> Option<&'static ring::digest::Algorithm> {
    Some(match alg.oid.to_string().as_str() {
        // RSASSA-PSS names its hash in the parameters.
        "1.2.840.113549.1.1.10" => return pss_hash(alg),
        // md5WithRSAEncryption, sha1WithRSAEncryption, sha256WithRSAEncryption
        "1.2.840.113549.1.1.4" | "1.2.840.113549.1.1.5" | "1.2.840.113549.1.1.11" => {
            &ring::digest::SHA256
        }
        // ecdsa-with-SHA1, ecdsa-with-SHA256
        "1.2.840.10045.4.1" | "1.2.840.10045.4.3.2" => &ring::digest::SHA256,
        // sha384WithRSAEncryption, ecdsa-with-SHA384
        "1.2.840.113549.1.1.12" | "1.2.840.10045.4.3.3" => &ring::digest::SHA384,
        // sha512WithRSAEncryption, ecdsa-with-SHA512
        "1.2.840.113549.1.1.13" | "1.2.840.10045.4.3.4" => &ring::digest::SHA512,
        _ => return None,
    })
}

/// `RSASSA-PSS-params ::= SEQUENCE { hashAlgorithm [0] HashAlgorithm DEFAULT sha1, ... }`
/// (RFC 4055). An absent hash means SHA-1, which RFC 5929 replaces with SHA-256.
fn pss_hash(alg: &AlgorithmIdentifierOwned) -> Option<&'static ring::digest::Algorithm> {
    let hash_oid = match &alg.parameters {
        None => None,
        Some(params) => {
            let mut fields = SliceReader::new(params.value()).ok()?;
            ContextSpecific::<AlgorithmIdentifierOwned>::decode_explicit(&mut fields, TagNumber::N0)
                .ok()?
                .map(|h| h.value.oid.to_string())
        }
    };
    Some(match hash_oid.as_deref() {
        // absent (SHA-1 by default), id-sha1, id-sha256
        None | Some("1.3.14.3.2.26") | Some("2.16.840.1.101.3.4.2.1") => &ring::digest::SHA256,
        Some("2.16.840.1.101.3.4.2.2") => &ring::digest::SHA384,
        Some("2.16.840.1.101.3.4.2.3") => &ring::digest::SHA512,
        Some(_) => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pss(params: Option<&[u8]>) -> AlgorithmIdentifierOwned {
        AlgorithmIdentifierOwned {
            oid: "1.2.840.113549.1.1.10".parse().unwrap(),
            parameters: params.map(|p| x509_cert::der::Any::from_der(p).unwrap()),
        }
    }

    /// `SEQUENCE { [0] { SEQUENCE { <hash OID>, NULL } } }`
    fn pss_params(hash_oid: &[u8]) -> Vec<u8> {
        let mut alg = vec![0x06, hash_oid.len() as u8];
        alg.extend(hash_oid);
        alg.extend([0x05, 0x00]);
        let mut alg_seq = vec![0x30, alg.len() as u8];
        alg_seq.extend(alg);
        let mut tagged = vec![0xA0, alg_seq.len() as u8];
        tagged.extend(alg_seq);
        let mut outer = vec![0x30, tagged.len() as u8];
        outer.extend(tagged);
        outer
    }

    #[test]
    fn rsassa_pss_uses_the_hash_in_its_parameters() {
        let sha384 = [0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02];
        let sha512 = [0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03];
        let sha256 = [0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01];
        let sha1 = [0x2B, 0x0E, 0x03, 0x02, 0x1A];
        let md5 = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x02, 0x05];
        let hash = |p: Option<&[u8]>| signature_hash(&pss(p)).map(|a| a.output_len());
        assert_eq!(hash(Some(&pss_params(&sha384))), Some(48));
        assert_eq!(hash(Some(&pss_params(&sha512))), Some(64));
        assert_eq!(hash(Some(&pss_params(&sha256))), Some(32));
        // SHA-1, explicit or by default, becomes SHA-256.
        assert_eq!(hash(Some(&pss_params(&sha1))), Some(32));
        assert_eq!(hash(Some(&[0x30, 0x00])), Some(32));
        assert_eq!(hash(None), Some(32));
        assert_eq!(hash(Some(&pss_params(&md5))), None);
    }

    #[test]
    fn ed25519_has_no_binding_hash() {
        let ed25519 = AlgorithmIdentifierOwned {
            oid: "1.3.101.112".parse().unwrap(),
            parameters: None,
        };
        assert!(signature_hash(&ed25519).is_none());
    }

    #[test]
    fn garbage_is_not_a_certificate() {
        assert_eq!(signature_algorithm_name(b"x"), "unreadable certificate");
        assert_eq!(end_point_hash(b"not a certificate"), None);
        assert_eq!(end_point_hash(&[]), None);
    }
}
