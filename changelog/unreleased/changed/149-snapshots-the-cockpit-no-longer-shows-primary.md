- **Snapshots: the cockpit no longer shows primary-key values of the snapshot cursor.** While a snapshot ran, the
  checkpoint in `/api/metrics`, `/api/stream`, `/api/tables` and the cockpit header was the whole composite
  checkpoint, including `snap.after`, the last chunk's primary-key values (an email or other personal data when that
  is the key). It now shows the stream position plus `(snapshot: <table> <i>/<n>)`. The checkpoint file still holds the
  full value, which resuming needs. No config change.
