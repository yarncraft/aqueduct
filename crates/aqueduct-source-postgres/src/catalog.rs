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
    /// publication has a column list (PG15+).
    pub columns: Vec<String>,
    /// Primary-key columns in key order. Empty when the table has no primary key.
    pub primary_key: Vec<String>,
    /// The publication's `WHERE` expression for this table (PG15+), as Postgres prints it.
    pub row_filter: Option<String>,
    /// The planner's row estimate (`reltuples`). `None` until the table has been analyzed.
    pub estimated_rows: Option<u64>,
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

const PRIMARY_KEY: &str = "COALESCE((SELECT array_agg(a.attname::text ORDER BY k.ord)
        FROM pg_index i
        CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = c.oid AND i.indisprimary), ARRAY[]::text[])";

fn tables_query(server_version_num: u32) -> String {
    let (columns, filter) = if server_version_num >= PG15 {
        ("p.attnames::text[]", "p.rowfilter::text")
    } else {
        (
            "(SELECT array_agg(a.attname::text ORDER BY a.attnum) FROM pg_attribute a
              WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)",
            "NULL::text",
        )
    };
    format!(
        "SELECT p.schemaname::text, p.tablename::text, c.oid, {columns}, {PRIMARY_KEY}, {filter},
                c.reltuples::float8
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
        .map(|r| PublishedTable {
            schema: r.get(0),
            name: r.get(1),
            oid: r.get(2),
            columns: r.get(3),
            primary_key: r.get(4),
            row_filter: r.get(5),
            estimated_rows: estimated_rows(r.get(6)),
        })
        .collect();
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
    fn unanalyzed_tables_have_no_estimate() {
        assert_eq!(estimated_rows(-1.0), None);
        assert_eq!(estimated_rows(0.0), Some(0));
        assert_eq!(estimated_rows(12345.0), Some(12345));
    }
}
