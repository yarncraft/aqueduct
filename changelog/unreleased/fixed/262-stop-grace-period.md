- **Docs: a stop needs 30 s, not docker's default 10 s** (#262). Aqueduct waits up to 25 s for in-flight batches and
  the checkpoint, but a plain `docker stop` kills it after 10 s. The guides now use `docker stop -t 30`,
  `--stop-timeout 30` and `terminationGracePeriodSeconds`, and DEPLOYMENT.md has a "Stopping" section. A cut drain
  only replays from the last checkpoint; nothing is lost.
