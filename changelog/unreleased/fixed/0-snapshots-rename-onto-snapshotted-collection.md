- **MongoDB snapshots: documents are no longer lost when a collection is renamed onto the one being copied**. A rename
  with `dropTarget` (or an `$out` aggregation) into a collection being snapshotted only voided the chunk in flight and
  moved the cursor past it, and a rename between chunks was not noticed at all, so the new documents up to the cursor
  were never delivered while the snapshot reported `finished`. The copy of that collection now starts again from its
  first `_id`; documents copied before the rename are sent twice. No action needed.
