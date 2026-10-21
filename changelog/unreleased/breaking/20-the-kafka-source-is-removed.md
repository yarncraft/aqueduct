- **The Kafka source is removed** (#20). `[source] type = "kafka"` no longer exists, and the
  `aqueduct-source-kafka` crate, its `AQUEDUCT_KAFKA_SOURCE_SASL_USERNAME` / `AQUEDUCT_KAFKA_SOURCE_SASL_PASSWORD`
  variables and the `aqueduct-kafka` service in `docker-compose.sim.yml` (port 8089) are gone. A config that still sets
  `type = "kafka"` now fails at startup with an unknown-variant error. Kafka remains the sink. To upgrade, switch
  `[source]` to `postgres` or `mongodb`, or pin the previous release if you depend on reading Kafka topics. Checkpoint
  files written by the Kafka source are not readable by the other sources, so delete or move them when changing source type.
