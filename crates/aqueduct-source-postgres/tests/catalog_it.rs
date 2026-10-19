//! Catalog reads against a real Postgres (S2, #71). Needs a plaintext PG15+ server with
//! user postgres / password aq (column lists and row filters do not exist before 15):
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test catalog_it -- --ignored
//! ```
//!
//! Every test makes its own tables and publication, because tests share one server.
use aqueduct_source_postgres::catalog::{read_catalog, PublishedTable};
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
