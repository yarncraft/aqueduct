- **Cockpit: without a token, other web sites can no longer use the admin API through the browser** (#203). On a loopback
  `health_addr` without `admin_token`, a page the operator visited could start a snapshot with a plain form post, and
  DNS rebinding let it read `/api/*`. `/metrics` and `/api/*` now refuse, without a token, a `Host` that is a name other
  than `localhost` or the host of `health_addr` (IP addresses are fine), an `Origin` other than the server's, and a `POST`
  without `Content-Type: application/json`. Scripts that start snapshots without a token must send
  `-H 'Content-Type: application/json'`. With a token nothing changes.
