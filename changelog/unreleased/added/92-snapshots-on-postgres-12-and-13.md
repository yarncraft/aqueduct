- **Snapshots on Postgres 12 and 13** (#92): new `[postgres] snapshot_watermark_table` names a published table whose
  rows carry the watermarks instead of logical messages (Postgres 14+). See DEPLOYMENT.md, "Watermark table".
