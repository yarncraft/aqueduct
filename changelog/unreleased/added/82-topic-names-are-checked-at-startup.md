- **Topic names are checked at startup** (#82). A route `topic`, `sink.topic` or `topic_fallback` with characters other
  than `A-Z a-z 0-9 . _ -` in its fixed text, or braces that are not a known placeholder (`{schema}`, `{table}`, `{op}`,
  `{row.<col>}`, ...), now fails startup naming the key. Kafka refuses such names anyway, so these configs used to fail
  on the first send instead.
