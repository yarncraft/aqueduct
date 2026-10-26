- **Config: an unknown key now fails startup** (#258). A misspelled key used to be ignored without a word: a route
  with `masks = ["ssn"]` or `wher = "…"` started normally and sent unmasked or unfiltered rows to Kafka. Every table
  now rejects keys it does not know (the top level, `[postgres]`, `[source]` for MongoDB, `[sink]` and each
  `[[sink.routes]]`), and startup names the key (``unknown field `masks` ``). To upgrade, start the new image against
  your config before you deploy it, and fix or remove each key it reports.
