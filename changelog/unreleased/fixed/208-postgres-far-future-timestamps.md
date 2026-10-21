- **Postgres: timestamps past the year 294247 are sent as text instead of a wrong number** (#208). They do not fit
  in i64 microseconds; the value wrapped silently. No action needed.
