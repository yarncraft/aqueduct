- **Compose files no longer default the admin token** (#19). `docker-compose.yml` and `docker-compose.sim.yml` used to
  fall back to the publicly known `dev-token-change-me`. `docker compose up` now fails with
  `set AQUEDUCT_ADMIN_TOKEN to a secret value` unless `AQUEDUCT_ADMIN_TOKEN` is set. To upgrade, export one (or put it in
  a `.env` file next to the compose file):

  ```sh
  export AQUEDUCT_ADMIN_TOKEN=$(openssl rand -hex 32)
  docker compose up --build
  ```

  Open the cockpit with `http://localhost:8080/#token=$AQUEDUCT_ADMIN_TOKEN`. Existing deployments that relied on the
  default token should rotate it, because it was public.
