//! Which route a table takes, matched once per table and remembered.

use super::*;

/// The most tables [`Resolved`] remembers. Past it a table is matched on every event, as it
/// was before the cache; real databases stay far below it.
pub(super) const RESOLVED_TABLES: usize = 4096;

/// Per table: its route and `schema.table`, worked out on the table's first event instead of
/// on every one. A plan's routes never change after it is built, so neither does the answer.
/// A clone starts empty.
#[derive(Debug, Default)]
pub(super) struct Resolved(pub(super) RwLock<ResolvedTables>);

#[derive(Debug, Default)]
pub(super) struct ResolvedTables {
    /// By schema, then table, so a lookup borrows the event's names.
    pub(super) by_schema: HashMap<Arc<str>, HashMap<Arc<str>, Table>>,
    pub(super) len: usize,
}

impl Clone for Resolved {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// A table as [`RoutePlan::table`] resolved it.
#[derive(Debug, Clone)]
pub(super) struct Table {
    /// The stats index (routes first, the default last); `None` when nothing matches.
    pub(super) route: Option<usize>,
    /// `schema.table`: what the routes match, and the partition input with
    /// `partition_by = "table"`.
    pub(super) name: Arc<str>,
}

impl RoutePlan {
    /// The topic for a table, or `None` when no route matches and there is no default. A
    /// template's `{schema}`/`{table}` are filled; other placeholders (`{op}`, row fields) are
    /// kept, since they depend on the event.
    pub fn topic_for(&self, schema: &str, table: &str) -> Option<String> {
        self.resolve(schema, table)
            .map(|(_, topic)| topic.fill(schema, table))
    }

    /// Index into the stats (routes first, default last) and the topic template.
    pub(super) fn resolve(&self, schema: &str, table: &str) -> Option<(usize, &TopicTemplate)> {
        self.template(self.table(schema, table).route)
    }

    /// The topic template of stats index `i`.
    pub(super) fn template(&self, i: Option<usize>) -> Option<(usize, &TopicTemplate)> {
        let i = i?;
        match self.routes.get(i) {
            Some(r) => Some((i, &r.topic)),
            None => self.default.as_ref().map(|t| (i, t)),
        }
    }

    /// `schema.table`'s route (the first whose pattern matches, else the default) and name,
    /// matched once per table and then read from [`Resolved`]: this runs for every event.
    pub(super) fn table(&self, schema: &str, table: &str) -> Table {
        let read = self.resolved.0.read().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = read.by_schema.get(schema).and_then(|m| m.get(table)) {
            return t.clone();
        }
        drop(read);
        let name: Arc<str> = format!("{schema}.{table}").into();
        let route = self
            .routes
            .iter()
            .position(|r| glob_match(&r.pattern, &name))
            .or_else(|| self.default.as_ref().map(|_| self.routes.len()));
        let t = Table { route, name };
        let mut w = self.resolved.0.write().unwrap_or_else(|e| e.into_inner());
        if w.len < RESOLVED_TABLES {
            let added = w
                .by_schema
                .entry(schema.into())
                .or_default()
                .insert(table.into(), t.clone())
                .is_none();
            w.len += usize::from(added);
        }
        t
    }

    /// What `table`'s records are partitioned by, unless the route keys them itself.
    pub(super) fn partition_key(&self, table: &Table) -> PartitionKey {
        match self.partition_by {
            PartitionBy::Key => PartitionKey::Key,
            PartitionBy::Table => PartitionKey::Table(table.name.clone()),
        }
    }

    /// Globs matching every topic this plan can produce to: each route (a content-routed template
    /// read as a glob covers every topic it can create), the default topic, the fallbacks and the
    /// dead-letter topics (each once), with placeholders turned into `*`.
    pub fn topic_globs(&self) -> Vec<String> {
        let mut globs: Vec<String> = self
            .routes
            .iter()
            .map(|r| r.topic.glob())
            .chain(self.default.as_ref().map(TopicTemplate::glob))
            .chain(self.fallback_topics().map(str::to_string))
            .chain(
                self.routes
                    .iter()
                    .filter_map(CompiledRoute::otherwise_topic)
                    .map(str::to_string),
            )
            .collect();
        for g in self.dead_letters.iter().flatten().map(DeadLetter::glob) {
            if !globs.contains(&g) {
                globs.push(g);
            }
        }
        globs
    }
}
