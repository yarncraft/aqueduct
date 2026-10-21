- **Admin API for snapshots** (#75): `GET`/`POST`/`DELETE /api/snapshot` shows, starts and cancels a Postgres table
  snapshot (with `snapshot = "on_demand"` or `"initial"`), behind the admin token. Progress is in `/api/metrics`
  (`snapshot`) and Prometheus (`aqueduct_snapshot_*`). A request is not durable until the first chunk is
  checkpointed. The admin server now reads a request split over several TCP segments whole (up to 16 KiB).
  - While a snapshot runs the checkpoint file holds JSON (`{"pos": ..., "snap": {...}}`); a version without snapshots
    refuses it. Let a snapshot finish before rolling back, or delete the checkpoint.
