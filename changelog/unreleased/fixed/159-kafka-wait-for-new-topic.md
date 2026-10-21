- **Kafka: a new topic that takes a few seconds to appear no longer restarts Aqueduct** (#159). After creating a topic
  the sink waited only about 3 s for it to show up in the broker's metadata, then failed the batch and the container
  restarted. It now waits up to 30 s with backoff. A refused create still fails at once. No action needed.
