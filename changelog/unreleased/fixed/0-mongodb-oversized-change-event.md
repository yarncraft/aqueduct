- **MongoDB: a change event over 16 MB now fails with an explanation** instead of the bare server error. Such an event
  (`BSONObjectTooLarge`, code 10334, usually a large document plus its pre-image) stops the stream at the same place on
  every restart; the error now says so, names the resume token before it, and points to the fixes (`pre_images =
  false`, a smaller document) in "MongoDB change event larger than 16 MB" in `docs/RUNBOOK.md`.
