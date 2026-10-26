- **Cockpit: throughput no longer dips and spikes after a stall** (#260). The once-a-second sampler made up missed ticks
  in a burst and counted each tick as exactly one second, so after the process stalled the throughput chart showed the
  whole stall's events as one spike, then near zero. A late tick now waits a full second, and events/s and Kafka MB/s
  are divided by the time that really passed, as the CPU tile already was.
