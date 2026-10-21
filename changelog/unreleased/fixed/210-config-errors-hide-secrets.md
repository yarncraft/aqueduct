- **A typo in the config file no longer puts secrets in the startup log** (#210). A TOML parse error used to quote the
  offending line, so an unquoted `admin_token`, `sasl_password` or URL ended up in the logs. The error now gives the file,
  line, column and the parser's message, and leaves the message out on keys that may hold a secret (password, token,
  secret, url, uri). `Debug` output of the configs also redacts the admin token and URL passwords. No action needed.
