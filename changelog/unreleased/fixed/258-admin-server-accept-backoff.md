- **Admin server: a failed `accept` no longer spins a core** (#258). When accepting a connection failed (for example
  out of file descriptors), the server retried at once and used a whole core until a connection closed. It now waits
  100 ms between retries and logs the failure once. No action needed.
