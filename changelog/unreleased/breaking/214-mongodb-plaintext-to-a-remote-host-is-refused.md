- **MongoDB source: plaintext to a remote host is refused** (#214). A `mongodb://` URI to a host other than `localhost`
  or a loopback address, with no TLS option, now fails at startup instead of sending documents and credentials in
  cleartext. This matches the Postgres source. To upgrade, add `tls=true` to the URI (and `tlsCAFile` for a private CA),
  or add `tls=false` to accept plaintext explicitly, which logs a warning. `tlsInsecure` and
  `tlsAllowInvalidCertificates` also log a warning now. `mongodb+srv://` URIs and local servers are unaffected.
