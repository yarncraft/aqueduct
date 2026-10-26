//! The per-event step: an event in, its Kafka records (or dead letters) out.

use super::*;

impl RoutePlan {
    /// Appends the records for `event` to `out`. Pure: no I/O, and the clock is the caller's
    /// `now_us` (the envelope's top-level `ts_ms`/`ts_us`). Heartbeats produce nothing. An event
    /// no route claims is an error, as before. The route counts stay pending until
    /// [`commit_counts`](Self::commit_counts), so a batch the broker refused is not counted.
    ///
    /// The first matching route owns the event (ADR 0002 §2): its `where` decides on the original
    /// event. A `where` that is false or unknown filters the event (no record), or, when the route
    /// sets `otherwise`, passes it on: to that later route, whose `where` decides next, or to that
    /// topic, rendered by this route as any other row. One that hits a type error sends it to the
    /// dead-letter topic and never passes it on.
    pub fn render_into(
        &self,
        event: &ChangeEvent,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        if event.op == Op::Heartbeat {
            return Ok(());
        }
        let table = self.table(&event.schema, &event.table);
        let (mut route, template) = self.template(table.route).ok_or_else(|| {
            Error::Sink(format!(
                "no kafka route or default topic for {}.{}",
                event.schema, event.table
            ))
        })?;
        // Parses the rows only on first use, so a route that reads no field pays nothing.
        let view = EventView::new(event);
        // `where` first, following `otherwise` from route to route while it passes the row on.
        // `passed_to`: the `otherwise` topic the last route's `where` passed the row on to.
        let mut passed_to = None;
        while let Some(r) = self.routes.get(route) {
            // An outbox publishes inserts, and snapshot reads only when the route has a `where`
            // (checked below, so it must be true for them; ADR 0002 §5, "As built"): updates,
            // deletes and truncates of the outbox table are filtered before anything else.
            if r.outbox.is_some()
                && !(event.op == Op::Insert || (event.op == Op::Read && r.filter.is_some()))
            {
                self.stats
                    .claimed(Outcome::Filtered, route, op_code(event.op));
                return Ok(());
            }
            self.stats.observe(route, &view);
            let Some(filter) = &r.filter else { break };
            match filter.eval(&view) {
                Ok(Tri::True) => break,
                Ok(Tri::False | Tri::Unknown) => match &r.otherwise {
                    Some(PassOn::Route(next)) => {
                        self.stats.passed_on(route, op_code(event.op));
                        route = *next;
                    }
                    // Counted once the record is rendered, like a produced one.
                    Some(PassOn::Topic(t)) => {
                        passed_to = Some(t.as_str());
                        break;
                    }
                    None => {
                        self.stats
                            .claimed(Outcome::Filtered, route, op_code(event.op));
                        return Ok(());
                    }
                },
                Err(e) => {
                    let reason = format!(
                        "route `{}`: where `{}`: {e}",
                        self.stats.names[route],
                        filter.source()
                    );
                    self.dead_letter(route, reason, event, now_us, out)?;
                    self.stats
                        .claimed(Outcome::Errors, route, op_code(event.op));
                    return Ok(());
                }
            }
        }
        // The record key and the partition input, when the route sets its own key.
        let mut route_key = None;
        // Field operations, applied before the topic is chosen, so a row they cannot rewrite goes
        // to the dead-letter topic without taking a content-routing cap slot.
        let mut rewritten = None;
        let mut fell_back = false;
        let mut content = None;
        // The delete's key is not per-row, so the envelope tombstone is left out.
        let mut skip_tombstone = false;
        let topic = match self.routes.get(route) {
            None => template.render(&view),
            Some(r) => {
                if let Some(outbox) = &r.outbox {
                    return self.render_outbox(route, r, outbox, &view, now_us, out);
                }
                // Row format: a truncate, a dropped delete or a change without a row is no record.
                if r.shape.drops(event) {
                    if event.after_is_key && event.op != Op::Delete {
                        self.key_only_dropped(route);
                    }
                    self.stats
                        .claimed(Outcome::Filtered, route, op_code(event.op));
                    return Ok(());
                }
                // A row without its unchanged TOAST columns, where the route wants none: the
                // dead-letter topic. Columns the route drops anyway (excluded, not included) do
                // not count. The reason names the columns, never values.
                let missing: Vec<String> = if r.shape.unchanged_dead_letter {
                    event
                        .unchanged
                        .iter()
                        .filter(|c| {
                            r.fields.as_ref().is_none_or(|f| {
                                !matches!(f.protection(c), Some("excluded" | "not included"))
                            })
                        })
                        .map(|c| format!("`{}`", safe_name(c)))
                        .collect()
                } else {
                    Vec::new()
                };
                if !missing.is_empty() {
                    let cols = missing;
                    let reason = format!(
                        "route `{}`: unchanged = \"dead_letter\": the source left out unchanged column(s) {}",
                        self.stats.names[route],
                        cols.join(", ")
                    );
                    self.dead_letter(route, reason, event, now_us, out)?;
                    self.stats
                        .claimed(Outcome::Errors, route, op_code(event.op));
                    return Ok(());
                }
                route_key = r.key.as_ref().map(|k| self.route_key(k, &view));
                if let Some(f) = &r.fields {
                    match self.rewrite(f, event, route_key.is_none()) {
                        Ok(w) => rewritten = Some(w),
                        Err(e) => {
                            let reason = format!("route `{}`: {e}", self.stats.names[route]);
                            self.dead_letter(route, reason, event, now_us, out)?;
                            self.stats
                                .claimed(Outcome::Errors, route, op_code(event.op));
                            return Ok(());
                        }
                    }
                }
                // A null-value record stands for every record with its key in a compacted topic,
                // so it is produced only under a key that names this one row. Checked before the
                // topic is chosen, so a skipped row-format delete takes no content-routing slot.
                if event.op == Op::Delete
                    && r.shape.null_value_on_delete()
                    && !self.key_is_per_row(r, &view)
                {
                    self.tombstone_skipped(route);
                    if r.shape.format == Format::Row {
                        // The delete is the tombstone itself: nothing safe is left to produce.
                        self.stats
                            .claimed(Outcome::Filtered, route, op_code(event.op));
                        return Ok(());
                    }
                    skip_tombstone = true;
                }
                let (topic, c, fallback) = match passed_to {
                    Some(t) => (t.to_string(), None, false),
                    None => self.route_topic(route, r, &view),
                };
                content = c;
                fell_back = fallback;
                topic
            }
        };
        let shape = self.routes.get(route).map(|r| r.shape).unwrap_or_default();
        let wants_tombstone = shape.tombstones && event.op == Op::Delete && !skip_tombstone;
        let rewritten_key = rewritten.as_mut().and_then(|w| w.key.take());
        let (key, partition_key) = match (route_key, rewritten_key) {
            // A per-route key partitions by the whole key, whatever `partition_by` says.
            (Some(key), _) => (key, PartitionKey::Key),
            // The key object after field operations (a hashed key column keeps per-row order),
            // partitioned like the plain key.
            (None, Some(key)) => (key, self.partition_key(&table)),
            (None, None) => (record_key(event, &self.source), self.partition_key(&table)),
        };
        // The value and headers are fixed here, before the sink may re-route the record to the
        // fallback (`topic_refused` changes only the topic), so a fallback record carries the
        // same rewritten value, key and `aqueduct-hash-key-id` as any other. The rows are the
        // rewritten ones when the route has field operations, so no shape bypasses them.
        let (before, after) = match &rewritten {
            Some(w) => (w.rows.0.as_deref(), w.rows.1.as_deref()),
            None => (event.before.as_deref(), event.after.as_deref()),
        };
        let value = match shape.format {
            Format::Envelope => Some(encode_rows(event, &self.source, now_us, before, after)?),
            Format::Row => match event.op {
                // `deletes = "tombstone"`: the delete itself is the tombstone. `drop` never
                // gets here (`Shape::drops`).
                Op::Delete => (shape.deletes == Deletes::Row).then(|| deleted_row(before)),
                // `Shape::drops` filtered a change without `after`.
                _ => Some(after.map_or("null", |a| a.get()).to_string()),
            },
        };
        let headers = rewritten.map(|w| w.headers).unwrap_or_default();
        let record = OutRecord {
            topic,
            partition_key,
            key,
            value,
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content,
        };
        // Envelope `tombstones`: a null value with the delete's (rewritten) key, topic and
        // partition input, right after it, so it lands on the same partition behind it.
        let tombstone = wants_tombstone.then(|| OutRecord {
            topic: record.topic.clone(),
            key: record.key.clone(),
            partition_key: record.partition_key.clone(),
            value: None,
            op: record.op,
            lsn: record.lsn.clone(),
            commit_ts_micros: record.commit_ts_micros,
            headers: record.headers.clone(),
            content: record.content.as_ref().map(|c| ContentTopic {
                tombstone: true,
                ..c.clone()
            }),
        });
        let op = record.op;
        out.push(record);
        out.extend(tombstone);
        if fell_back {
            self.stats
                .claimed(Outcome::Fallback, route, op_code(event.op));
        } else if passed_to.is_some() {
            self.stats.passed_on(route, op);
        } else {
            self.stats.produced(route, op);
        }
        Ok(())
    }

    /// Whether the record key of a delete on route `r` names exactly one row, so a record with a
    /// null value under it removes only that row from a compacted topic. One check for envelope
    /// tombstones and row-format delete tombstones:
    ///
    /// - `key = [...]`: every listed column has a non-null value in the before-image. The columns
    ///   must also be unique, which cannot be checked.
    /// - Field operations on the key object: there is a primary key, and none of its columns is
    ///   excluded, left out of `include` or masked (all rows would share `"***"`). Hashing (HMAC)
    ///   and renaming keep it unique.
    /// - Otherwise: the event has a primary key (`"key"` is not null).
    pub(super) fn key_is_per_row(&self, r: &CompiledRoute, view: &EventView<'_>) -> bool {
        if let Some(k) = &r.key {
            return k
                .fields
                .iter()
                .all(|f| view.raw(f).is_some_and(|v| v.get().trim() != "null"));
        }
        let Some(obj) = key_object(view.event()) else {
            return false;
        };
        let Ok(Members(cols)) = serde_json::from_str::<Members<'_>>(obj) else {
            return false;
        };
        !cols.is_empty()
            && r.fields.as_ref().is_none_or(|f| {
                cols.iter()
                    .all(|(c, _)| matches!(f.protection(c), None | Some("hashed")))
            })
    }

    /// A delete whose key is not per-row (see [`key_is_per_row`](Self::key_is_per_row)) got no
    /// null-value record: one would erase every row sharing the key (the whole table, for a
    /// null key) from a compacted topic. Counted, and logged once per route.
    pub(super) fn tombstone_skipped(&self, route: usize) {
        self.stats.pend(route, |p| {
            p.tombstones_skipped = p.tombstones_skipped.wrapping_add(1)
        });
        if !self.stats.tombstone_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                "a delete whose record key does not name a single row (no primary key, a missing or null `key` column, or a key column the field operations remove or mask) got no tombstone, since it would erase other rows from a compacted topic; later ones are only counted under tombstones_skipped"
            );
        }
    }

    /// A row-format route dropped a change whose `after` is only the key (a MongoDB update whose
    /// document was deleted before the lookup). Logged once per route; counted as `filtered`.
    pub(super) fn key_only_dropped(&self, route: usize) {
        if !self.stats.key_only_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                "an update without its full document (deleted before the lookup) was dropped: a row-format route never publishes the key alone as the row; later ones are only counted under filtered"
            );
        }
    }

    /// The topic of route `r` for this event, the [`ContentTopic`] of a topic chosen from row
    /// content, and whether the event went to the route's `topic_fallback`.
    pub(super) fn route_topic(
        &self,
        route: usize,
        r: &CompiledRoute,
        view: &EventView<'_>,
    ) -> (String, Option<ContentTopic>, bool) {
        match self.content_topic(route, r, view) {
            Ok((topic, from_row)) => {
                let content = r
                    .topic_fallback
                    .clone()
                    .filter(|_| from_row)
                    .map(|fallback| ContentTopic {
                        route,
                        fallback,
                        tombstone: false,
                    });
                (topic, content, false)
            }
            Err(fallback) => (fallback, None, true),
        }
    }

    /// The topic of route `r` for this event. A content-routed template (one that reads a row
    /// field) gives `Err(topic_fallback)` when a field is null or missing, a value is not one
    /// segment of `[A-Za-z0-9_-]{1,64}` or not in `topic_values`, the result is not a valid topic
    /// name, or the topic would be new beyond `sink.max_dynamic_topics`. Never a row value in the
    /// log line, only the field path.
    pub(super) fn content_topic(
        &self,
        route: usize,
        r: &CompiledRoute,
        view: &EventView<'_>,
    ) -> std::result::Result<(String, bool), String> {
        let Some(fallback) = r.topic_fallback.as_ref().filter(|_| r.topic.reads_row) else {
            return Ok((r.topic.render(view), false));
        };
        let rendered = r
            .topic
            .parsed
            .render_checked(view, |f, v| value_allowed(r, f, v));
        let why = match rendered {
            Ok(t) if !valid_topic_name(&t) => {
                "the rendered topic is not a valid Kafka topic name".to_string()
            }
            // The route's own fallback (the only fixed topic its glob may match): no cap slot.
            Ok(t) if self.fixed.contains(&t) => "the value names a fixed topic".to_string(),
            Ok(t) => match self.dynamic.admit(&t) {
                Admit::Yes => return Ok((t, true)),
                Admit::Collides => {
                    "a topic with the same name up to `.` vs `_` exists, so Kafka would refuse it"
                        .to_string()
                }
                Admit::Cap => "sink.max_dynamic_topics is reached".to_string(),
                Admit::Refused => {
                    "the broker refused to create this topic earlier in this run".to_string()
                }
            },
            Err(e) => e.to_string(),
        };
        if !self.stats.fallback_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                topic = %r.topic.raw,
                fallback = %fallback,
                reason = %why,
                "an event went to the route's topic_fallback; later ones are only counted under fallback"
            );
        }
        Err(fallback.clone())
    }

    /// The broker refused to create a content-routed topic for a reason that will not change on a
    /// retry (`kind`: an invalid name, a create policy, no create permission). Sends `out` to its
    /// route's fallback instead, counts it under `fallback` rather than `produced`, and frees the
    /// name and its cap slot. Logged once per route, with the template, never the value.
    pub fn topic_refused(&self, out: &mut OutRecord, kind: &str) {
        let Some(c) = out.content.take() else {
            return;
        };
        self.dynamic.refuse(&out.topic);
        // A delete's tombstone follows it to the fallback; the event was counted once, with
        // the delete.
        if !c.tombstone {
            self.stats.pend(c.route, |p| {
                p.produced = p.produced.wrapping_sub(1);
                if let Some(i) = OpCounts::slot(out.op) {
                    p.produced_ops[i] = p.produced_ops[i].wrapping_sub(1);
                    let n = &mut p.outcome_ops[Outcome::Fallback.row()][i];
                    *n = n.wrapping_add(1);
                }
                p.fallback = p.fallback.wrapping_add(1);
            });
        }
        if !self.stats.refused_logged[c.route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[c.route],
                topic = %self.routes.get(c.route).map_or("", |r| r.topic.raw.as_str()),
                fallback = %c.fallback,
                error = kind,
                "the broker refused to create a content-routed topic; such events go to the route's topic_fallback (pre-create the topics or set topic_values)"
            );
        }
        out.topic = c.fallback;
    }

    /// A content-routed topic could not be created for a passing reason: free its cap slot, so a
    /// failed create never holds one. The batch fails and is retried.
    pub fn topic_failed(&self, topic: &str) {
        self.dynamic.release(topic);
    }

    /// The sink opened `topic`: it exists, so no other spelling of it may be created.
    pub fn topic_opened(&self, topic: &str) {
        if self.routes.iter().any(|r| r.topic.reads_row) {
            self.dynamic.exists([(topic, false)]);
        }
    }

    /// The record key of a route with `key = [...]`: the usual JSON key with `"key"` built from the
    /// listed columns of `row` (`after`, else `before`, so a delete keys by its before-image), in
    /// order, each value copied as raw JSON and `null` when the row lacks the column. An event
    /// without a row (a truncate) gets the table-only key.
    pub(super) fn route_key(&self, key: &RouteKey, view: &EventView<'_>) -> String {
        let event = view.event();
        if event.after.is_none() && event.before.is_none() {
            return table_key(event, &self.source);
        }
        let mut obj = String::with_capacity(16 * key.columns.len() + 2);
        obj.push('{');
        for (i, (col, field)) in key.columns.iter().zip(&key.fields).enumerate() {
            if i > 0 {
                obj.push(',');
            }
            obj.push_str(&serde_json::to_string(col).unwrap_or_default());
            obj.push(':');
            obj.push_str(view.raw(field).map_or("null", |v| v.get()));
        }
        obj.push('}');
        key_json(event, &self.source, &obj)
    }

    /// The value rows, key and headers of a record of a route with field operations. `own_key`:
    /// the route has no `key = [...]`, so the key object is rewritten too.
    pub(super) fn rewrite(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
        own_key: bool,
    ) -> std::result::Result<Rewritten, FieldError> {
        let rows = self.rewrite_rows(fields, event, true)?;
        let key = if own_key {
            let k = self.rewrite_key(fields, event)?;
            Some(key_json(
                event,
                &self.source,
                k.as_deref().unwrap_or("null"),
            ))
        } else {
            None
        };
        Ok(Rewritten {
            key,
            rows,
            headers: self.hash_headers(fields),
        })
    }

    /// `before` and `after` with the route's field operations applied.
    pub(super) fn rewrite_rows(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
        renames: bool,
    ) -> std::result::Result<Rows, FieldError> {
        let key = self.hash_key.as_ref();
        let row = |r: &Option<Box<RawValue>>| {
            r.as_deref()
                .map(|r| fields.rewrite_raw(r, key, renames))
                .transpose()
        };
        Ok((row(&event.before)?, row(&event.after)?))
    }

    /// The record key's `key` object with the route's field operations applied: excluded columns
    /// removed, masked, hashed and renamed ones as in the value. An object left empty is `None`
    /// (`"key": null`).
    pub(super) fn rewrite_key(
        &self,
        fields: &FieldOps,
        event: &ChangeEvent,
    ) -> std::result::Result<Option<String>, FieldError> {
        let Some(k) = key_object(event) else {
            return Ok(None);
        };
        let mut out = String::with_capacity(k.len());
        let n = fields.rewrite(k, self.hash_key.as_ref(), true, &mut out)?;
        Ok((n > 0).then_some(out))
    }

    /// `aqueduct-hash-key-id` on every record of a route that hashes.
    pub(super) fn hash_headers(&self, fields: &FieldOps) -> Vec<(&'static str, String)> {
        match &self.hash_key {
            Some(k) if fields.hashes() => vec![(HASH_KEY_ID_HEADER, k.id().to_string())],
            _ => Vec::new(),
        }
    }

    /// The dead-letter record for an event route `route` could not evaluate. `reason` names the
    /// route, the expression and the type problem, never a row value: it becomes the
    /// `aqueduct-error` header and a log line. The key is the table-only record key (no row data),
    /// and the partition is hashed from it. The value has the route's field operations applied
    /// (ADR 0002 §5a), so the dead-letter topic never holds data the route would have removed,
    /// masked or hashed: renamed if the renames apply, else under the original names, and with
    /// `before`/`after` null if a row is not an object the operations can rewrite.
    pub(super) fn dead_letter(
        &self,
        route: usize,
        reason: String,
        event: &ChangeEvent,
        now_us: i64,
        out: &mut Vec<OutRecord>,
    ) -> Result<()> {
        let topic = match self.dead_letters.get(route).and_then(Option::as_ref) {
            Some(DeadLetter::Fixed(t)) => t.clone(),
            Some(DeadLetter::Template(t)) => t.render(&EventView::new(event)),
            None => {
                return Err(Error::Sink(format!(
                    "{reason}, and the route has no usable dead-letter topic; set `dead_letter_topic`"
                )));
            }
        };
        // A rendered name (`{schema}`/`{table}` from the event) that Kafka would refuse, or that
        // starts with `_`, fails the batch like any topic that cannot be created.
        if !valid_topic_name(&topic) || topic.starts_with('_') {
            return Err(Error::Sink(format!(
                "{reason}, and its dead-letter topic `{topic}` is not a usable topic name"
            )));
        }
        let key = table_key(event, &self.source);
        let fields = self.routes.get(route).and_then(|r| r.fields.as_ref());
        let mut headers = vec![("aqueduct-error", reason.clone())];
        let value = match fields {
            None => encode(event, &self.source, now_us)?,
            Some(f) => {
                headers.extend(self.hash_headers(f));
                let (before, after) = self
                    .rewrite_rows(f, event, true)
                    .or_else(|_| self.rewrite_rows(f, event, false))
                    .unwrap_or((None, None));
                encode_rows(
                    event,
                    &self.source,
                    now_us,
                    before.as_deref(),
                    after.as_deref(),
                )?
            }
        };
        out.push(OutRecord {
            topic,
            partition_key: PartitionKey::Key,
            key,
            value: Some(value),
            op: op_code(event.op),
            lsn: event.position.to_checkpoint(),
            commit_ts_micros: event.commit_ts_micros,
            headers,
            content: None,
        });
        if !self.stats.error_logged[route].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                route = %self.stats.names[route],
                topic = %out.last().map_or("", |r| r.topic.as_str()),
                error = %reason,
                "an event could not be evaluated and went to the dead-letter topic; later ones are only counted under errors"
            );
        }
        Ok(())
    }

    /// The records for one event, counted at once ([`commit_counts`](Self::commit_counts)): for
    /// callers that deliver nothing. The sink renders with [`render_into`](Self::render_into).
    pub fn render(&self, event: &ChangeEvent, now_us: i64) -> Result<Vec<OutRecord>> {
        let mut out = Vec::with_capacity(1);
        self.render_into(event, now_us, &mut out)?;
        self.commit_counts();
        Ok(out)
    }
}
