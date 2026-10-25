- **jemalloc is the memory allocator** (#253). glibc malloc took about 38% of the per-event CPU on the Postgres-to-Kafka
  path, mostly freeing buffers on a different thread from the one that allocated them. With jemalloc, Aqueduct uses
  about 27% less CPU per event on the bench, whether Postgres is the bottleneck or not. Not used on MSVC (Windows)
  builds. No config change.
