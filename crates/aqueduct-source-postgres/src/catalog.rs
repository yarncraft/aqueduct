//! Reads what a publication covers: tables, published columns, row filters and primary keys.
//!
//! Snapshots (ADR 0001) need this to know which tables and rows to copy. Nothing in the
//! streaming path calls it yet.

use aqueduct_core::{Error, Result};
use tokio_postgres::Client;

/// One table in a publication.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedTable {
    pub schema: String,
    pub name: String,
    pub oid: u32,
    /// The columns the publication sends, in table order. This is every column unless the
    /// publication has a column list (PG15+). Generated columns are left out, as pgoutput
    /// does not send them.
    pub columns: Vec<String>,
    /// Type OID of each entry of `columns`, for encoding a row exactly as the decoder does.
    pub column_types: Vec<u32>,
    /// SQL type of each entry of `columns` (`format_type`), for binding keyset cursors.
    pub column_type_names: Vec<String>,
    /// Primary-key columns in key order. Empty when the table has no primary key.
    pub primary_key: Vec<String>,
    /// SQL type of each primary-key column (`format_type`), for binding keyset cursors.
    pub primary_key_types: Vec<String>,
    /// `REPLICA IDENTITY` setting: decides which columns key the table's live events.
    pub replica_identity: ReplicaIdentity,
    /// The columns pgoutput flags as key columns, in table order: the primary key under
    /// `DEFAULT`, the index columns under `USING INDEX`, none under `FULL` or `NOTHING`.
    pub identity_columns: Vec<String>,
    /// The publication's `WHERE` expression for this table (PG15+), as Postgres prints it.
    pub row_filter: Option<String>,
    /// The planner's row estimate (`reltuples`). `None` until the table has been analyzed.
    pub estimated_rows: Option<u64>,
    /// Column sets that identify a row: each unique index (primary key included) on plain
    /// columns, without a predicate, whose key columns are all `NOT NULL`. In key order.
    pub unique_keys: Vec<Vec<String>>,
}

impl PublishedTable {
    /// `schema.table` with both parts quoted, safe to splice into SQL.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

/// `pg_class.relreplident`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    fn from_code(c: &str) -> Result<Self> {
        match c {
            "d" => Ok(Self::Default),
            "n" => Ok(Self::Nothing),
            "f" => Ok(Self::Full),
            "i" => Ok(Self::Index),
            other => Err(Error::Source(format!("unknown relreplident {other:?}"))),
        }
    }
}

/// Quotes an identifier the way `quote_ident` does when quoting is needed, always quoting.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// The tables a publication covers, and the server they were read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    /// `server_version_num`, for example `160004`.
    pub server_version_num: u32,
    /// Ordered by schema and name.
    pub tables: Vec<PublishedTable>,
}

/// Column lists and row filters exist from this version on.
const PG15: u32 = 150_000;
/// Generated columns exist from this version on.
const PG12: u32 = 120_000;
/// `pg_index.indnkeyatts` (INCLUDE columns) exists from this version on.
const PG11: u32 = 110_000;

/// `what` of each key column of the index of table `c` matching `filter`, in key order.
fn index_columns(filter: &str, what: &str) -> String {
    format!(
        "COALESCE((SELECT array_agg({what} ORDER BY k.ord)
        FROM pg_index i
        CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = c.oid AND {filter}), ARRAY[]::text[])"
    )
}

fn tables_query(server_version_num: u32) -> String {
    let (listed, filter) = if server_version_num >= PG15 {
        (
            "(p.attnames IS NULL OR a.attname = ANY(p.attnames))",
            "p.rowfilter::text",
        )
    } else {
        ("true", "NULL::text")
    };
    // pgoutput never sends generated columns (before PG18), which exist from PG12 on.
    let generated = if server_version_num >= PG12 {
        "AND a.attgenerated = ''"
    } else {
        ""
    };
    let cols = |what: &str| {
        format!(
            "(SELECT array_agg({what} ORDER BY a.attnum) FROM pg_attribute a
              WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                {generated} AND {listed})"
        )
    };
    // INCLUDE columns (past `indnkeyatts`, PG11+) are not part of a key.
    let key_atts = if server_version_num >= PG11 {
        "i.indnkeyatts"
    } else {
        "i.indnatts"
    };
    let (names, types) = (cols("a.attname::text"), cols("a.atttypid"));
    let type_names = cols("format_type(a.atttypid, a.atttypmod)");
    let pk = index_columns("i.indisprimary", "a.attname::text");
    let pk_types = index_columns("i.indisprimary", "format_type(a.atttypid, a.atttypmod)");
    // pgoutput flags identity columns in table order (`logicalrep_write_attrs`).
    let identity = "COALESCE((SELECT array_agg(a.attname::text ORDER BY a.attnum)
        FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = c.oid
          AND ((c.relreplident = 'd' AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident))),
        ARRAY[]::text[])";
    // Each unique key as its column names joined with the unit separator (0x1f); INCLUDE
    // columns (past `indnkeyatts`) are not part of the key.
    let unique = format!(
        "COALESCE((SELECT array_agg(u.cols) FROM (
        SELECT (SELECT string_agg(a.attname::text, E'\\x1f' ORDER BY k.ord)
                FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE k.ord <= {key_atts}) AS cols
        FROM pg_index i
        WHERE i.indrelid = c.oid AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
            WHERE k.ord <= {key_atts} AND NOT a.attnotnull)) u),
        ARRAY[]::text[])"
    );
    format!(
        "SELECT p.schemaname::text, p.tablename::text, c.oid, COALESCE({names}, ARRAY[]::text[]),
                COALESCE({types}, ARRAY[]::oid[]), {pk}, {pk_types}, c.relreplident::text,
                {identity}, {filter}, c.reltuples::float8,
                COALESCE({type_names}, ARRAY[]::text[]), {unique}
         FROM pg_publication_tables p
         JOIN pg_namespace n ON n.nspname = p.schemaname
         JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename
         WHERE p.pubname = $1
         ORDER BY p.schemaname, p.tablename"
    )
}

/// `reltuples` is -1 for a table that was never analyzed (PG14+) and 0 before that.
fn estimated_rows(reltuples: f64) -> Option<u64> {
    (reltuples >= 0.0).then_some(reltuples as u64)
}

fn err(e: tokio_postgres::Error) -> Error {
    match e.as_db_error() {
        Some(db) => Error::Source(format!("{e}: {} {}", db.code().code(), db.message())),
        None => Error::Source(e.to_string()),
    }
}

/// Reads the tables `publication` covers from the connected server.
pub async fn read_catalog(client: &Client, publication: &str) -> Result<Catalog> {
    let version: String = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(err)?
        .get(0);
    let server_version_num: u32 = version
        .parse()
        .map_err(|_| Error::Source(format!("unreadable server_version_num {version:?}")))?;

    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(err)?;
    if exists.is_none() {
        return Err(Error::Source(format!(
            "publication {publication:?} does not exist"
        )));
    }

    let rows = client
        .query(tables_query(server_version_num).as_str(), &[&publication])
        .await
        .map_err(err)?;
    let tables = rows
        .iter()
        .map(|r| {
            Ok(PublishedTable {
                schema: r.get(0),
                name: r.get(1),
                oid: r.get(2),
                columns: r.get(3),
                column_types: r.get(4),
                primary_key: r.get(5),
                primary_key_types: r.get(6),
                replica_identity: ReplicaIdentity::from_code(r.get(7))?,
                identity_columns: r.get(8),
                row_filter: r.get(9),
                estimated_rows: estimated_rows(r.get(10)),
                column_type_names: r.get(11),
                unique_keys: r
                    .get::<_, Vec<String>>(12)
                    .iter()
                    .map(|k| k.split('\u{1f}').map(str::to_owned).collect())
                    .collect(),
            })
        })
        .collect::<Result<_>>()?;
    Ok(Catalog {
        server_version_num,
        tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_lists_and_row_filters_are_only_read_from_pg15() {
        assert!(tables_query(150_000).contains("p.attnames"));
        assert!(tables_query(150_000).contains("p.rowfilter"));
        let old = tables_query(140_012);
        assert!(!old.contains("p.attnames") && !old.contains("p.rowfilter"));
        assert!(old.contains("NULL::text"));
    }

    #[test]
    fn generated_columns_are_never_listed() {
        for v in [120_000, 140_012, 150_000, 170_000] {
            assert!(tables_query(v).contains("a.attgenerated = ''"), "{v}");
        }
        // PG10 and 11 have neither generated columns nor INCLUDE columns.
        for v in [100_023, 110_022] {
            assert!(!tables_query(v).contains("attgenerated"), "{v}");
        }
        assert!(tables_query(100_023).contains("i.indnatts"));
        assert!(tables_query(110_022).contains("i.indnkeyatts"));
    }

    #[test]
    fn identifiers_are_always_quoted() {
        assert_eq!(quote_ident("Orders"), "\"Orders\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn unanalyzed_tables_have_no_estimate() {
        assert_eq!(estimated_rows(-1.0), None);
        assert_eq!(estimated_rows(0.0), Some(0));
        assert_eq!(estimated_rows(12345.0), Some(12345));
    }
}
