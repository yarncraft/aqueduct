- **Postgres: a checkpoint from another cluster, database or timeline is refused at startup (#63).** Checkpoints now
  record the cluster's `system_identifier`, the database OID and the timeline (`;sys=<id>;db=<oid>;tli=<n>` after the
  key). Startup fails if the cluster or database differs (a re-pointed URL, a rebuilt primary, a major-version
  `pg_upgrade`), and also after a failover or PITR of the same cluster (a new timeline), because changes may have been
  lost; the error says "restored, recovered to a point in time (PITR) or failed over" and how to resync. See
  DEPLOYMENT.md, "Checkpoint origin check".
  - Upgrade: existing checkpoints lack these fields; they are accepted with a warning and the next checkpoint records
    them.
  - The Aqueduct role must be able to call `pg_control_system()` and `pg_control_checkpoint()`; a plain
    `LOGIN REPLICATION` role can. Where a managed service refuses them, startup fails; set
    `checkpoint_skip_system_check = true` under `[postgres]` to skip the cluster and timeline comparison.
  - After a planned switchover you have verified, delete only the `;tli=` field from the checkpoint file.
  - A slot with the configured name that belongs to another database (or is a physical slot) is now refused instead
    of used.
  - Rollback: a version before this one refuses the new checkpoint (`not a Postgres checkpoint`) and will not start.
    To go back, remove the `;sys=...` suffix from the checkpoint file, or delete the file.
