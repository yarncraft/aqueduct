//! Shared rustls client configuration (ring provider, no OpenSSL) for every
//! outbound connection Aqueduct makes.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
};

/// How strictly to verify the server certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verify {
    /// Encrypt only; accept any certificate (libpq `sslmode=require` without a root cert).
    None,
    /// Verify the chain against the trust roots but not the hostname (`verify-ca`).
    Chain,
    /// Verify chain and hostname (`verify-full`).
    Full,
}

/// Build a TLS client config. With `ca_file` (PEM, one or more certs) only
/// those roots are trusted; otherwise the bundled Mozilla roots are used.
pub fn client_config(ca_file: Option<&str>, verify: Verify) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls: {e}"))?;

    let cfg = if verify == Verify::None {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth()
    } else {
        let mut roots = RootCertStore::empty();
        match ca_file {
            Some(path) => {
                let mut n = 0;
                for cert in CertificateDer::pem_file_iter(path)
                    .map_err(|e| format!("tls ca file {path}: {e}"))?
                {
                    let cert = cert.map_err(|e| format!("tls ca file {path}: {e}"))?;
                    roots
                        .add(cert)
                        .map_err(|e| format!("tls ca file {path}: {e}"))?;
                    n += 1;
                }
                if n == 0 {
                    return Err(format!("tls ca file {path}: no certificates found"));
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|e| format!("tls: {e}"))?;
        let verifier: Arc<dyn ServerCertVerifier> = if verify == Verify::Chain {
            Arc::new(IgnoreHostname(inner))
        } else {
            inner
        };
        builder
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    };
    Ok(Arc::new(cfg))
}

/// Chain verification that tolerates a hostname mismatch.
#[derive(Debug)]
struct IgnoreHostname(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for IgnoreHostname {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(Error::InvalidCertificate(CertificateError::NotValidForName))
            | Err(Error::InvalidCertificate(CertificateError::NotValidForNameContext { .. })) => {
                Ok(ServerCertVerified::assertion())
            }
            other => other,
        }
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls12_signature(m, c, d)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.0.verify_tls13_signature(m, c, d)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

/// Accepts any certificate; the handshake signatures are still checked.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_for_each_mode_without_ca_file() {
        for v in [Verify::None, Verify::Chain, Verify::Full] {
            client_config(None, v).unwrap();
        }
    }

    #[test]
    fn missing_or_empty_ca_file_is_an_error() {
        assert!(client_config(Some("/nonexistent/ca.pem"), Verify::Full).is_err());
        let p = std::env::temp_dir().join("aqueduct-tls-empty.pem");
        std::fs::write(&p, "not a cert").unwrap();
        assert!(client_config(Some(p.to_str().unwrap()), Verify::Full).is_err());
    }

    /// Handshake against a real in-process TLS server with a self-signed cert
    /// for `localhost`, exercising each verification mode.
    #[tokio::test]
    async fn verification_modes_against_live_server() {
        use rustls::pki_types::PrivateKeyDer;
        use tokio::net::{TcpListener, TcpStream};

        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let ca_path = std::env::temp_dir().join(format!("aqueduct-tls-{}.pem", std::process::id()));
        std::fs::write(&ca_path, ck.cert.pem()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server_cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![ck.cert.der().clone()],
                PrivateKeyDer::try_from(ck.key_pair.serialize_der()).unwrap(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let _ = acceptor.accept(sock).await;
                });
            }
        });

        let ca = ca_path.to_str().unwrap();
        let connect = |ca: Option<String>, verify: Verify, name: &'static str| async move {
            let cfg = client_config(ca.as_deref(), verify).unwrap();
            let sock = TcpStream::connect(addr).await.unwrap();
            tokio_rustls::TlsConnector::from(cfg)
                .connect(ServerName::try_from(name).unwrap(), sock)
                .await
                .is_ok()
        };
        // trusted CA + matching name
        assert!(connect(Some(ca.to_string()), Verify::Full, "localhost").await);
        // wrong hostname: full fails, chain-only and none succeed
        assert!(!connect(Some(ca.to_string()), Verify::Full, "other.example").await);
        assert!(connect(Some(ca.to_string()), Verify::Chain, "other.example").await);
        assert!(connect(None, Verify::None, "other.example").await);
        // untrusted (public roots only): verifying modes fail
        assert!(!connect(None, Verify::Full, "localhost").await);
        assert!(!connect(None, Verify::Chain, "localhost").await);
        let _ = std::fs::remove_file(ca_path);
    }
}
