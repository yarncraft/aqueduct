- **Postgres: `hostaddr` can no longer bypass the plaintext refusal** (#208). `host=localhost hostaddr=<remote>` counted
  as a loopback host, so a plaintext session to a remote server was accepted. The dialled address now decides. `prefer`
  to a non-loopback host dials as `require`, so a server without TLS is refused before the password is sent.
