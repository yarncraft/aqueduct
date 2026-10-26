- **The admin server, the Azure token client and the OTLP client run on hyper** (#262). All three carry credentials
  and each framed HTTP by hand. hyper now parses and frames every request and response; the size caps, timeouts,
  probe reserve, loopback-only plain HTTP and `verify-full` TLS are unchanged. A header value with CR or LF is refused
  before anything is sent, and credential headers are marked sensitive. Header names now go out in lower case, which
  HTTP treats the same. PUT, PATCH and OPTIONS on the admin server get a 405. No action needed.
