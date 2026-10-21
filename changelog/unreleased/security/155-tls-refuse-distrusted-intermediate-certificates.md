- **TLS: refuse distrusted intermediate certificates** (RUSTSEC-2026-0382, RUSTSEC-2026-0381). `rustls-webpki` 0.103.16 and
  `webpki-roots` 1.0.10 replace 0.103.15 and 1.0.9, which could accept distrusted (emSign) intermediates when verifying
  Postgres, Kafka and Entra certificates. No configuration change.
