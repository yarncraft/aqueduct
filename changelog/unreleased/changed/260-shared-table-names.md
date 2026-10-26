- **Events share their table names** (#260). Every Postgres change and snapshot row copied its schema and table name
  from the relation cache and formatted `schema.table` once or twice more for lookups, and the Kafka sink copied each
  record's topic name to group it by partition. The names are now shared with the relation cache and the topic moves
  into its group, so an event allocates none of them. The event JSON is unchanged. No action needed.
