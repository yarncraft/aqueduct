# Architecture

One process runs one pipeline: a **source**, the **Kafka sink**, a **checkpoint file** and the **admin server**,
all inside one container.

```mermaid
flowchart LR
    subgraph db["Source database"]
        PG[("Postgres<br/>logical slot + publication")]
        MG[("MongoDB<br/>change stream")]
    end

    subgraph aq["Aqueduct container (distroless, uid 65532)"]
        SRC["Source<br/>pgoutput decoder / change-stream mapper<br/>snapshot engine (Postgres, MongoDB)"]
        PIPE["run_pipeline<br/>batches of up to 1000 events"]
        SINK["Kafka sink<br/>route plan: match · where filter (op, rows) · topic (template, row fields → fallback) · field ops<br/>Debezium-style envelope (+ tombstones), the row (format = row), or outbox payload (inserts)<br/>key: table + primary key or route key columns (JSON), outbox aggregate id · partition = FNV-1a(key)"]
        CK[("checkpoint file<br/>/var/lib/aqueduct")]
        MET["Metrics"]
        ADM["Admin server :8080<br/>/ · /api/* · /metrics · /healthz · /readyz<br/>/api/snapshot requests"]
        INS["Event inspector"]
    end

    K[("Kafka topics")]
    U(["Browser / Prometheus / probes"])

    PG -- "peek changes + watermarks (poll)<br/>or replication stream (stream)" --> SRC
    SRC -- "snapshot chunks (keyset SELECT)" --> PG
    MG -- "watch (changes + watermarks)" --> SRC
    SRC -- "snapshot chunks (_id index scan) + watermark inserts" --> MG
    SRC --> PIPE --> SINK --> K
    SINK -. "type errors: the route's topic + .dlq" .-> K
    SINK -. "route plan + counts (/api/routes)" .-> ADM
    PIPE -- "save position" --> CK
    PIPE -. "ack(position)" .-> SRC
    SRC -. "slot advance (Postgres)" .-> PG
    PIPE --> MET --> ADM
    ADM -. "snapshot start / cancel" .-> SRC
    K -- "read back recent events<br/>and topic offsets" --> INS --> ADM
    U -- "Bearer for /api/* and /metrics<br/>/, /healthz, /readyz are open" --> ADM
```

## Delivery: at-least-once

The checkpoint is written and the source acknowledged only after Kafka has acknowledged the whole batch. A crash
before that point replays the batch, so consumers must tolerate duplicates.

```mermaid
sequenceDiagram
    autonumber
    participant S as Source
    participant P as run_pipeline
    participant K as Kafka sink
    participant C as Checkpoint file
    Note over P,C: startup: load checkpoint, source.start(position)
    loop until shutdown
        P->>S: next() + try_next() (up to 1000 events)
        S-->>P: batch of ChangeEvents
        P->>K: send_batch(batch) + flush() (60 s timeout)
        K-->>P: broker ack
        P->>C: save(last position) (tmp file, fsync, rename)
        P->>S: ack(last position)
        Note over S: Postgres: slot advances only to a commit-end LSN
    end
```

Heartbeat events (MongoDB, idle or filtered streams) never reach the sink. They only move the checkpoint.

## Crates

```mermaid
flowchart BT
    core["aqueduct-core<br/>events, traits, pipeline, metrics"]
    tls["aqueduct-tls<br/>rustls config"]
    pg["aqueduct-source-postgres"]
    mg["aqueduct-source-mongodb"]
    sinks["aqueduct-sinks<br/>Kafka sink + inspector"]
    bin["aqueduct<br/>binary: config, checkpoint, admin"]
    bench["aqueduct-bench"]

    pg --> core
    pg --> tls
    mg --> core
    sinks --> core
    sinks --> tls
    bin --> core
    bin --> pg
    bin --> mg
    bin --> sinks
    bench --> core
    bench --> sinks
    bench --> pg
```
