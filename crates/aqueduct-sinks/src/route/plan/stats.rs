//! Per-route counters, the cockpit description, and batch counts applied on broker ack.

use super::*;

/// One field path a route references, watched for the unseen-field warning (ADR 0002 §5).
#[derive(Debug)]
pub(super) struct WatchedField {
    pub(super) field: FieldRef,
    /// Rows that had the row the path reads but not the path.
    pub(super) missing: AtomicU64,
    /// Set once a row had the path; a field seen once is never reported.
    pub(super) seen: AtomicBool,
    pub(super) warned: AtomicBool,
}

/// Counters per route, shared between the sink (writer) and the admin server (reader).
#[derive(Debug)]
pub struct RouteStats {
    pub(super) names: Vec<String>,
    pub(super) matched: Vec<AtomicU64>,
    pub(super) produced: Vec<AtomicU64>,
    /// `produced` per operation, `[insert, update, delete, truncate, read]`; sums to `produced`.
    pub(super) produced_ops: Vec<[AtomicU64; 5]>,
    /// `filtered`, `fallback` and `errors` per operation, in that order, like `produced_ops`.
    pub(super) outcome_ops: Vec<[[AtomicU64; 5]; 3]>,
    pub(super) filtered: Vec<AtomicU64>,
    /// The part of `filtered` passed on to the route's `otherwise`.
    pub(super) passed_on: Vec<AtomicU64>,
    /// Events sent to a route's `topic_fallback` (content routing).
    pub(super) fallback: Vec<AtomicU64>,
    pub(super) errors: Vec<AtomicU64>,
    /// Deletes that got no tombstone because their key is null (`tombstones = true`).
    pub(super) tombstones_skipped: Vec<AtomicU64>,
    /// Per route, whether the first skipped tombstone was logged.
    pub(super) tombstone_logged: Vec<AtomicBool>,
    /// Per route, whether the first row-format change with only a key as `after` was logged.
    pub(super) key_only_logged: Vec<AtomicBool>,
    /// Per route, every field path it references. `where` registers its paths here; templates,
    /// `key`, field operations and outbox columns register theirs the same way.
    pub(super) fields: Vec<Vec<WatchedField>>,
    /// Per route, whether the first dead-lettered event was logged.
    pub(super) error_logged: Vec<AtomicBool>,
    /// Per route, whether the first event sent to `topic_fallback` was logged.
    pub(super) fallback_logged: Vec<AtomicBool>,
    /// Per route, whether the first refused topic create was logged.
    pub(super) refused_logged: Vec<AtomicBool>,
    /// Per route, the counts of the batch being rendered, applied to the counters above only once
    /// the broker acknowledged the batch ([`RoutePlan::commit_counts`], or a [`BatchCounts`] taken
    /// for it), so a failed batch the pipeline retries is not counted twice.
    pub(super) pending: Mutex<Vec<PendingCounts>>,
}

/// The counts of one route in a batch the broker has not acknowledged yet. Wrapping arithmetic,
/// so `topic_refused` can move a count from `produced` to `fallback` as a delta.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PendingCounts {
    pub(super) matched: u64,
    pub(super) produced: u64,
    pub(super) produced_ops: [u64; 5],
    pub(super) outcome_ops: [[u64; 5]; 3],
    pub(super) filtered: u64,
    pub(super) passed_on: u64,
    pub(super) fallback: u64,
    pub(super) errors: u64,
    pub(super) tombstones_skipped: u64,
}

/// The final outcome of an event a route claimed, other than `produced`.
#[derive(Debug, Clone, Copy)]
pub(super) enum Outcome {
    Filtered,
    Fallback,
    Errors,
}

impl Outcome {
    /// Its row in `outcome_ops`.
    pub(super) fn row(self) -> usize {
        match self {
            Outcome::Filtered => 0,
            Outcome::Fallback => 1,
            Outcome::Errors => 2,
        }
    }
}

/// The outcome counters of one route. Every event a route claims counts `matched`, then exactly
/// one of `produced`, `filtered`, `fallback` or `errors`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RouteCounts {
    /// Events the route claimed (first match wins).
    pub matched: u64,
    /// Events produced to the route's topic.
    pub produced: u64,
    /// Events dropped by `where` (including a `where` that ends unknown), and outbox changes
    /// other than inserts and snapshot reads.
    pub filtered: u64,
    /// Not an outcome: the part of `filtered` the route passed on to its `otherwise`: a route,
    /// which counts them as `matched`, or a topic they were produced to. Left out of the JSON
    /// while zero.
    #[serde(skip_serializing_if = "is_zero")]
    pub passed_on: u64,
    /// Events sent to the route's `topic_fallback`.
    pub fallback: u64,
    /// Events sent to the route's dead-letter topic because a row could not be evaluated.
    pub errors: u64,
    /// Not an outcome: deletes produced without the tombstone `tombstones = true` asks for,
    /// because their key is null (a table without a primary key), so the tombstone would erase
    /// the whole table from a compacted topic.
    pub tombstones_skipped: u64,
    /// `produced` per operation of the event (a delete's tombstone is not counted again).
    pub ops: OpCounts,
    /// `filtered`, `fallback` and `errors` per operation of the event, in that order, as
    /// `[insert, update, delete, truncate, read]` rows. Kept for the cockpit's rate history, not
    /// part of `/api/routes`.
    #[serde(skip)]
    pub outcome_ops: [[u64; 5]; 3],
}

/// A point-in-time copy of one route's counters. `route` is the route's `name`, else its `match`
/// pattern; the `sink.topic` fallback is `default`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteCount {
    pub route: String,
    #[serde(flatten)]
    pub counts: RouteCounts,
    /// Field paths the route references that no row has had in [`UNSEEN_FIELD_ROWS`] rows.
    pub unseen_fields: Vec<String>,
}

impl RouteStats {
    pub(super) fn new(names: Vec<String>, fields: Vec<Vec<FieldRef>>) -> Self {
        let zeros = || names.iter().map(|_| AtomicU64::new(0)).collect();
        let fields = (0..names.len())
            .map(|i| {
                let mut watched: Vec<WatchedField> = Vec::new();
                for f in fields.get(i).into_iter().flatten() {
                    if watched.iter().all(|w| w.field != *f) {
                        watched.push(WatchedField {
                            field: f.clone(),
                            missing: AtomicU64::new(0),
                            seen: AtomicBool::new(false),
                            warned: AtomicBool::new(false),
                        });
                    }
                }
                watched
            })
            .collect();
        Self {
            matched: zeros(),
            produced: zeros(),
            produced_ops: names
                .iter()
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
            outcome_ops: names
                .iter()
                .map(|_| std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))))
                .collect(),
            filtered: zeros(),
            passed_on: zeros(),
            fallback: zeros(),
            errors: zeros(),
            tombstones_skipped: zeros(),
            tombstone_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            key_only_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            fields,
            error_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            fallback_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            refused_logged: names.iter().map(|_| AtomicBool::new(false)).collect(),
            pending: Mutex::new(vec![PendingCounts::default(); names.len()]),
            names,
        }
    }

    /// Updates the pending counts of `route`.
    pub(super) fn pend(&self, route: usize, f: impl FnOnce(&mut PendingCounts)) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut pending[route]);
    }

    /// Counts an event `route` claimed under `outcome`, together with `matched` and its op
    /// (`op`, an [`op_code`] letter). Called only once the outcome is final (the record is
    /// rendered), so an encode error never leaves `matched` above the sum of the outcomes.
    /// Pending until [`commit`](Self::commit).
    pub(super) fn claimed(&self, outcome: Outcome, route: usize, op: &str) {
        self.pend(route, |p| {
            p.matched = p.matched.wrapping_add(1);
            if let Some(i) = OpCounts::slot(op) {
                let n = &mut p.outcome_ops[outcome.row()][i];
                *n = n.wrapping_add(1);
            }
            let n = match outcome {
                Outcome::Filtered => &mut p.filtered,
                Outcome::Fallback => &mut p.fallback,
                Outcome::Errors => &mut p.errors,
            };
            *n = n.wrapping_add(1);
        });
    }

    /// Counts an event `route`'s `where` rejected and passed on to its `otherwise`: filtered here,
    /// and claimed (`matched`) by the next route once its outcome is final, or already produced
    /// to the `otherwise` topic.
    pub(super) fn passed_on(&self, route: usize, op: &str) {
        self.claimed(Outcome::Filtered, route, op);
        self.pend(route, |p| p.passed_on = p.passed_on.wrapping_add(1));
    }

    /// Counts an event `route` produced to its topic: `matched`, `produced` and its op. Pending
    /// until [`commit`](Self::commit).
    pub(super) fn produced(&self, route: usize, op: &str) {
        self.pend(route, |p| {
            p.matched = p.matched.wrapping_add(1);
            p.produced = p.produced.wrapping_add(1);
            if let Some(i) = OpCounts::slot(op) {
                p.produced_ops[i] = p.produced_ops[i].wrapping_add(1);
            }
        });
    }

    /// Applies the pending counts to the counters and clears them.
    pub(super) fn commit(&self) {
        let counts = self.take();
        self.apply(&counts);
    }

    /// Takes the pending counts out, leaving them cleared.
    pub(super) fn take(&self) -> Vec<PendingCounts> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let n = pending.len();
        std::mem::replace(&mut *pending, vec![PendingCounts::default(); n])
    }

    /// Adds `counts` (one per route) to the counters.
    pub(super) fn apply(&self, counts: &[PendingCounts]) {
        for (i, p) in counts.iter().enumerate() {
            // `fetch_add` wraps like the deltas, so a moved count (`topic_refused`) nets out.
            let add = |v: &[AtomicU64], n: u64| {
                if n != 0 {
                    v[i].fetch_add(n, Ordering::Relaxed);
                }
            };
            add(&self.matched, p.matched);
            add(&self.produced, p.produced);
            add(&self.filtered, p.filtered);
            add(&self.passed_on, p.passed_on);
            add(&self.fallback, p.fallback);
            add(&self.errors, p.errors);
            add(&self.tombstones_skipped, p.tombstones_skipped);
            for (k, &n) in p.produced_ops.iter().enumerate() {
                if n != 0 {
                    self.produced_ops[i][k].fetch_add(n, Ordering::Relaxed);
                }
            }
            for (row, ops) in p.outcome_ops.iter().enumerate() {
                for (k, &n) in ops.iter().enumerate() {
                    if n != 0 {
                        self.outcome_ops[i][row][k].fetch_add(n, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Drops the pending counts: their batch failed, is retried and is counted then.
    pub(super) fn discard(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.fill(PendingCounts::default());
    }

    pub(super) fn counts(&self, i: usize) -> RouteCounts {
        let get = |v: &[AtomicU64]| v[i].load(Ordering::Relaxed);
        RouteCounts {
            matched: get(&self.matched),
            produced: get(&self.produced),
            filtered: get(&self.filtered),
            passed_on: get(&self.passed_on),
            fallback: get(&self.fallback),
            errors: get(&self.errors),
            tombstones_skipped: get(&self.tombstones_skipped),
            ops: OpCounts::from_slots(std::array::from_fn(|k| {
                self.produced_ops[i][k].load(Ordering::Relaxed)
            })),
            outcome_ops: std::array::from_fn(|row| {
                std::array::from_fn(|k| self.outcome_ops[i][row][k].load(Ordering::Relaxed))
            }),
        }
    }

    /// Paths of route `i` reported as unseen.
    pub(super) fn unseen(&self, i: usize) -> Vec<String> {
        self.fields[i]
            .iter()
            .filter(|w| {
                !w.seen.load(Ordering::Relaxed)
                    && w.missing.load(Ordering::Relaxed) >= UNSEEN_FIELD_ROWS
            })
            .map(|w| w.field.as_str().to_string())
            .collect()
    }

    /// Notes, for every watched path of `route` not yet seen, whether this event's row has it.
    /// Cheap once every path has been seen: one atomic load per path.
    pub(super) fn observe(&self, route: usize, view: &EventView<'_>) {
        for w in &self.fields[route] {
            if w.seen.load(Ordering::Relaxed) || !view.has_row(&w.field) {
                continue;
            }
            if view.present(&w.field) {
                w.seen.store(true, Ordering::Relaxed);
                continue;
            }
            let n = w.missing.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= UNSEEN_FIELD_ROWS && !w.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    route = %self.names[route],
                    field = %w.field,
                    rows = n,
                    "a field this route references has not appeared in any row yet; check the spelling"
                );
            }
        }
    }

    pub fn snapshot(&self) -> Vec<RouteCount> {
        (0..self.names.len())
            .map(|i| RouteCount {
                route: self.names[i].clone(),
                counts: self.counts(i),
                unseen_fields: self.unseen(i),
            })
            .collect()
    }
}

/// A static description of the compiled plan with live counts, for `/api/routes` and the
/// cockpit's routing graph. Never holds row data or secrets. Later slices add optional keys
/// (`topic_fallback`, `key`, field operations, `format`, `outbox`) to [`RouteDescription`];
/// readers must ignore keys they do not know.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutesDescription {
    /// `sink.topic`, placeholders included.
    pub default_topic: Option<String>,
    /// `sink.dead_letter_topic`, the global override; each route's own is in its description.
    pub dead_letter_topic: Option<String>,
    /// `key` or `table`.
    pub partition_by: &'static str,
    /// `sink.hash_key_id`, when a hash key is configured. Never the key itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_key_id: Option<String>,
    /// In match order; the `sink.topic` fallback, if any, is last, named `default`.
    pub routes: Vec<RouteDescription>,
}

/// One route of [`RoutesDescription`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RouteDescription {
    /// The route's `name`, else its `match`; `default` for the `sink.topic` fallback.
    pub route: String,
    /// The `match` glob; `null` for the `default` pseudo-route (it takes what no route claims)
    /// and for a route reached only through an earlier route's `otherwise`.
    #[serde(rename = "match")]
    pub pattern: Option<String>,
    /// Where the rows `where` rejects are passed on to, `{"route": ...}` or `{"topic": ...}`;
    /// left out when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otherwise: Option<Otherwise>,
    /// The `where` expression as configured; `null` without one.
    #[serde(rename = "where")]
    pub filter: Option<String>,
    /// The `key` columns, in order; `null` when the record key follows `partition_by`.
    pub key: Option<Vec<String>>,
    /// The topic template, placeholders included.
    pub topic: String,
    /// Where events go whose content-routed topic cannot be used; `null` for other topics.
    pub topic_fallback: Option<String>,
    /// The allow-list of a content-routed topic's values; `null` without one.
    pub topic_values: Option<Vec<String>>,
    /// `outbox = {...}`: the column names and the topic template (also in `topic`); left out
    /// for other routes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outbox: Option<OutboxSpec>,
    /// Field operations: column names (and the rename map), left out when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mask: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rename: Option<BTreeMap<String, String>>,
    /// `row` for a route with `format = "row"`; left out for the envelope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<&'static str>,
    /// With `format = "row"`: `tombstone`, `drop` or `row`, defaults included; left out otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletes: Option<&'static str>,
    /// `true` for an envelope route with `tombstones = true`; left out otherwise.
    #[serde(skip_serializing_if = "is_false")]
    pub tombstones: bool,
    /// `dead_letter` for a route with `unchanged = "dead_letter"`; left out for `omit`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unchanged: Option<&'static str>,
    /// Where rows the route cannot evaluate go: a topic, or a `{schema}`/`{table}` template
    /// rendered per event (`cdc.{schema}.{table}.dlq`). `null` only when the default name is not
    /// a usable topic name and the route cannot dead-letter anyway.
    pub dead_letter_topic: Option<String>,
    pub counts: RouteCounts,
    pub unseen_fields: Vec<String>,
}

pub(super) fn is_false(b: &bool) -> bool {
    !*b
}

pub(super) fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// One batch's route counts, taken by [`RoutePlan::take_counts`]. [`commit`](Self::commit) applies
/// them once the broker acknowledged the batch; dropping them uncommitted drops them, since a
/// failed batch is redelivered and counted then.
#[must_use = "the counts apply only when committed"]
pub struct BatchCounts {
    pub(super) counts: Vec<PendingCounts>,
    pub(super) stats: Arc<RouteStats>,
}

impl BatchCounts {
    pub fn commit(self) {
        self.stats.apply(&self.counts);
    }
}

impl RoutePlan {
    /// The compiled plan and its live counts, for `/api/routes`.
    pub fn describe(&self) -> RoutesDescription {
        let route = |i: usize, pattern: Option<String>, r: Option<&CompiledRoute>, topic: &str| {
            let spec = r.and_then(|r| r.fields.as_ref()).map(FieldOps::spec);
            let list = |v: &Vec<String>| (!v.is_empty()).then(|| v.clone());
            let shape = r.map(|r| r.shape).unwrap_or_default();
            let row = shape.format == Format::Row;
            RouteDescription {
                format: row.then_some("row"),
                deletes: row.then(|| shape.deletes.as_str()),
                tombstones: shape.tombstones,
                unchanged: shape.unchanged_dead_letter.then_some("dead_letter"),
                include: spec.and_then(|s| s.include.clone()),
                exclude: spec.and_then(|s| s.exclude.as_ref().and_then(list)),
                mask: spec.and_then(|s| list(&s.mask)),
                hash: spec.and_then(|s| list(&s.hash)),
                rename: spec.and_then(|s| (!s.rename.is_empty()).then(|| s.rename.clone())),
                route: self.stats.names[i].clone(),
                pattern,
                otherwise: r.and_then(|r| r.otherwise.as_ref()).map(|o| match o {
                    PassOn::Route(j) => Otherwise::Route(self.stats.names[*j].clone()),
                    PassOn::Topic(t) => Otherwise::Topic(t.clone()),
                }),
                filter: r
                    .and_then(|r| r.filter.as_ref())
                    .map(|f| f.source().to_string()),
                key: r.and_then(|r| r.key_columns()).map(<[String]>::to_vec),
                topic: topic.to_string(),
                topic_fallback: r.and_then(|r| r.topic_fallback.clone()),
                topic_values: r.and_then(|r| r.topic_values().map(<[String]>::to_vec)),
                outbox: r.and_then(|r| r.outbox.as_ref()).map(|o| o.spec.clone()),
                dead_letter_topic: self.dead_letter_name(i),
                counts: self.stats.counts(i),
                unseen_fields: self.stats.unseen(i),
            }
        };
        let mut routes: Vec<RouteDescription> = self
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let pattern = (!r.pattern.is_empty()).then(|| r.pattern.clone());
                route(i, pattern, Some(r), &r.topic.raw)
            })
            .collect();
        if let Some(d) = &self.default {
            routes.push(route(self.routes.len(), None, None, &d.raw));
        }
        RoutesDescription {
            default_topic: self.default_topic().map(str::to_string),
            dead_letter_topic: self.dead_letter.clone(),
            partition_by: self.partition_by.as_str(),
            hash_key_id: self.hash_key.as_ref().map(|k| k.id().to_string()),
            routes,
        }
    }

    /// Applies the route counts of everything rendered since the last commit or discard. The sink
    /// calls it once the broker acknowledged the batch, so the counters show delivered events.
    pub fn commit_counts(&self) {
        self.stats.commit();
    }

    /// Drops the route counts of everything rendered since the last commit or discard: the batch
    /// failed and the pipeline retries it, so it is counted when it is delivered.
    pub fn discard_counts(&self) {
        self.stats.discard();
    }

    /// Takes the route counts of everything rendered since the last commit, take or discard, for
    /// a batch that is acknowledged later, while the next batches render: commit them then.
    pub fn take_counts(&self) -> BatchCounts {
        BatchCounts {
            counts: self.stats.take(),
            stats: self.stats.clone(),
        }
    }
}
