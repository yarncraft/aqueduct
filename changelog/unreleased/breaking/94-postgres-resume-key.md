- **Postgres: rows were lost on overlapping transactions, multi-row `COPY` and multi-table `TRUNCATE` (#94).**
  Change LSNs are neither commit-ordered nor unique, but the source deduplicated on them. The source now resumes from
  a key `(commit LSN, change LSN, ord)`, checkpointed as text `<commit>@<change>#<ord>`. The event JSON is unchanged.
  - Upgrade: a plain-LSN checkpoint from an older version still starts. The slot replays from its
    `confirmed_flush_lsn`, so some events are delivered again, never lost.
  - **Rollback is not supported.** An older binary refuses the new checkpoint (`not a Postgres LSN`) and will not
    start. To go back, delete the checkpoint file; the slot's `confirmed_flush_lsn` then decides where it resumes.
  - Rows already lost by earlier versions are not recovered by this fix; re-snapshot or backfill the affected tables.
