//! Catalog reads against a real Postgres (S2, #71). Needs a plaintext PG14+ server with
//! user postgres / password aq. Column lists and row filters need PG15:
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test catalog_it -- --ignored
//! ```
//!
//! Every test makes its own tables and publication, because tests share one server.
// The workspace denies these outside tests; this whole crate is one.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use aqueduct_source_postgres::catalog::{PublishedTable, read_catalog};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let addr = std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"));
    let url = format!("postgres://postgres:aq@{addr}/postgres");
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    db
}

fn unique(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("cat_{tag}_{}_{}", std::process::id(), nanos % 1_000_000_000)
}

fn table<'a>(tables: &'a [PublishedTable], name: &str) -> &'a PublishedTable {
    tables
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("{name} not published"))
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
#[ignore]
async fn reads_column_list_row_filter_and_primary_keys() {
    let db = connect().await;
    let version: String = db
        .query_one("SHOW server_version_num", &[])
        .await
        .unwrap()
        .get(0);
    if version.parse::<u32>().unwrap() < 150_000 {
        eprintln!("column lists and row filters need PG15; this server is {version}");
        return;
    }
    let id = unique("full");
    let listed = format!("{id}_listed");
    let filtered = format!("{id}_filtered");
    let nopk = format!("{id}_nopk");
    let composite = format!("{id}_composite");
    let hidden = format!("{id}_hidden");
    let publication = format!("{id}_pub");
    db.batch_execute(&format!(
        "CREATE TABLE {listed} (id int PRIMARY KEY, name text, secret text);
         CREATE TABLE {filtered} (id int PRIMARY KEY, region text, v int);
         CREATE TABLE {nopk} (a int, b text);
         CREATE TABLE {composite} (b int, a int, v text, PRIMARY KEY (a, b));
         CREATE TABLE {hidden} (id int PRIMARY KEY);
         CREATE PUBLICATION {publication} FOR TABLE
             {listed} (id, name),
             {filtered} WHERE (region = 'eu' AND v > 0),
             {nopk}, {composite};
         INSERT INTO {filtered} SELECT g, 'eu', 1 FROM generate_series(1, 500) g;
         ANALYZE {filtered};"
    ))
    .await
    .unwrap();

    let catalog = read_catalog(&db, &publication).await.unwrap();
    assert!(catalog.server_version_num >= 150_000, "needs PG15+");
    assert_eq!(
        catalog.tables.len(),
        4,
        "the unpublished table must not show up"
    );
    assert!(catalog.tables.iter().all(|t| t.name != hidden));

    let t = table(&catalog.tables, &listed);
    assert_eq!(t.schema, "public");
    assert_eq!(
        t.columns,
        strs(&["id", "name"]),
        "column list hides `secret`"
    );
    assert_eq!(t.primary_key, strs(&["id"]));
    assert_eq!(t.row_filter, None);

    let t = table(&catalog.tables, &filtered);
    assert_eq!(t.columns, strs(&["id", "region", "v"]));
    let filter = t.row_filter.as_deref().expect("row filter");
    assert!(
        filter.contains("region") && filter.contains("'eu'") && filter.contains("v > 0"),
        "{filter}"
    );
    assert_eq!(t.estimated_rows, Some(500));

    let t = table(&catalog.tables, &nopk);
    assert_eq!(t.columns, strs(&["a", "b"]));
    assert!(t.primary_key.is_empty(), "no primary key");
    assert_eq!(t.estimated_rows, None, "never analyzed");

    let t = table(&catalog.tables, &composite);
    assert_eq!(
        t.primary_key,
        strs(&["a", "b"]),
        "key order, not table order"
    );
    assert_eq!(t.columns, strs(&["b", "a", "v"]), "table order");

    // Tables come back ordered by schema and name.
    let names: Vec<_> = catalog.tables.iter().map(|t| t.name.clone()).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    db.batch_execute(&format!(
        "DROP PUBLICATION {publication};
         DROP TABLE {listed}, {filtered}, {nopk}, {composite}, {hidden};"
    ))
    .await
    .unwrap();
}

/// What the snapshot engine needs to encode rows and keys like the decoder. Runs on PG14 too.
#[tokio::test]
#[ignore]
async fn reads_types_replica_identity_and_skips_generated_columns() {
    use aqueduct_source_postgres::catalog::ReplicaIdentity;
    let db = connect().await;
    let id = unique("ident");
    let mixed = format!("{id}_Mixed Case");
    let full = format!("{id}_full");
    let index = format!("{id}_index");
    let publication = format!("{id}_pub");
    db.batch_execute(&format!(
        "CREATE TABLE \"{mixed}\" (\"Id\" bigint PRIMARY KEY, n numeric(10,2), g int GENERATED ALWAYS AS (1) STORED, t text);
         CREATE TABLE {full} (id int PRIMARY KEY, v text);
         ALTER TABLE {full} REPLICA IDENTITY FULL;
         CREATE TABLE {index} (id int PRIMARY KEY, b int NOT NULL, a int NOT NULL);
         CREATE UNIQUE INDEX {index}_ab ON {index} (a, b);
         ALTER TABLE {index} REPLICA IDENTITY USING INDEX {index}_ab;
         CREATE PUBLICATION {publication} FOR TABLE \"{mixed}\", {full}, {index};"
    ))
    .await
    .unwrap();

    let catalog = read_catalog(&db, &publication).await.unwrap();
    let t = table(&catalog.tables, &mixed);
    assert_eq!(
        t.columns,
        strs(&["Id", "n", "t"]),
        "generated `g` is not sent"
    );
    assert_eq!(t.column_types, vec![20, 1700, 25]);
    assert_eq!(
        t.column_type_names,
        strs(&["bigint", "numeric(10,2)", "text"])
    );
    assert_eq!(t.primary_key_types, strs(&["bigint"]));
    assert_eq!(t.replica_identity, ReplicaIdentity::Default);
    assert_eq!(t.identity_columns, strs(&["Id"]));
    assert_eq!(t.qualified_name(), format!("\"public\".\"{mixed}\""));
    // The quoted name must work in a query.
    db.query(&format!("SELECT count(*) FROM {}", t.qualified_name()), &[])
        .await
        .unwrap();

    let t = table(&catalog.tables, &full);
    assert_eq!(t.replica_identity, ReplicaIdentity::Full);
    assert!(t.identity_columns.is_empty());
    assert_eq!(t.primary_key, strs(&["id"]));

    let t = table(&catalog.tables, &index);
    assert_eq!(t.replica_identity, ReplicaIdentity::Index);
    assert_eq!(
        t.identity_columns,
        strs(&["b", "a"]),
        "table order, not index order"
    );

    db.batch_execute(&format!(
        "DROP PUBLICATION {publication};
         DROP TABLE \"{mixed}\", {full}, {index};"
    ))
    .await
    .unwrap();
}

#[tokio::test]
#[ignore]
async fn a_missing_publication_is_an_error_and_an_empty_one_is_not() {
    let db = connect().await;
    let id = unique("missing");
    let err = read_catalog(&db, &format!("{id}_nope")).await.unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    let publication = format!("{id}_empty");
    db.batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    let catalog = read_catalog(&db, &publication).await.unwrap();
    assert!(catalog.tables.is_empty());
    db.batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}
