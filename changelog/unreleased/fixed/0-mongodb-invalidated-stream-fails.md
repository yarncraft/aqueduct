- **MongoDB: a dropped database no longer ends the pipeline silently with success**. Dropping the watched database
  invalidates the change stream; the source skipped the `invalidate` event, then saw the closed cursor as a clean end,
  so Aqueduct exited 0 ("pipeline finished") and, under a restart policy, replayed to the same place forever without
  capturing anything after the database was recreated. It now fails with an error that names the invalidate's resume
  token. To go on, reset the checkpoint as described in "MongoDB change stream invalidated" in `docs/RUNBOOK.md`.
