use aqueduct_core::Op;
use serde_json::{Map, Value};

/// A change-stream event reduced to Aqueduct's shape, before a position is attached.
#[derive(Debug, Clone, PartialEq)]
pub struct Mapped {
    pub schema: String,
    pub table: String,
    pub op: Op,
    pub before: Option<Map<String, Value>>,
    pub after: Option<Map<String, Value>>,
    /// `after` holds only the `documentKey`: an update without a `fullDocument` (the document
    /// was deleted before the lookup). Becomes `ChangeEvent::after_is_key`.
    pub after_is_key: bool,
}

/// Map one change-stream document (as extended JSON) to a `Mapped` event.
/// Returns `None` for event types that carry no row change (e.g. `invalidate`).
pub fn map_event(ev: &Value) -> Option<Mapped> {
    let op = match ev.get("operationType")?.as_str()? {
        "insert" => Op::Insert,
        "update" | "replace" => Op::Update,
        "delete" => Op::Delete,
        _ => return None,
    };
    let ns = ev.get("ns")?;
    let obj = |k: &str| ev.get(k).and_then(Value::as_object).cloned();
    let mut after = obj("fullDocument");
    let before = obj("fullDocumentBeforeChange");
    // A delete has no fullDocument; keep the key so consumers can identify the row.
    let mut before = before;
    if op == Op::Delete && before.is_none() {
        before = obj("documentKey");
    }
    // An update whose document is gone has no fullDocument either; the envelope keeps the key in
    // `after`, and the event says so, so a route that publishes the row alone does not take the
    // key for the document.
    let mut after_is_key = false;
    if op != Op::Delete && after.is_none() {
        after = obj("documentKey");
        after_is_key = after.is_some();
    }
    Some(Mapped {
        schema: ns.get("db")?.as_str()?.to_string(),
        table: ns.get("coll")?.as_str()?.to_string(),
        op,
        before,
        after,
        after_is_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn insert() {
        let m = map_event(
            &json!({"operationType":"insert","ns":{"db":"app","coll":"u"},
            "fullDocument":{"_id":1,"n":"a"},"documentKey":{"_id":1}}),
        )
        .unwrap();
        assert_eq!((m.op, m.table.as_str()), (Op::Insert, "u"));
        assert!(m.before.is_none() && m.after.unwrap()["n"] == "a");
        assert!(!m.after_is_key);
    }

    #[test]
    fn update_without_full_document_keeps_the_key_and_says_so() {
        let m = map_event(
            &json!({"operationType":"update","ns":{"db":"app","coll":"u"},
            "documentKey":{"_id":1},"updateDescription":{"updatedFields":{"n":"b"}}}),
        )
        .unwrap();
        assert_eq!(m.op, Op::Update);
        assert_eq!(m.after, Some(json!({"_id":1}).as_object().unwrap().clone()));
        assert!(m.after_is_key, "after is only the documentKey");
        let full = map_event(
            &json!({"operationType":"update","ns":{"db":"app","coll":"u"},
            "fullDocument":{"_id":1,"n":"b"},"documentKey":{"_id":1}}),
        )
        .unwrap();
        assert!(!full.after_is_key);
    }

    #[test]
    fn delete_keeps_key() {
        let m = map_event(
            &json!({"operationType":"delete","ns":{"db":"app","coll":"u"},
            "documentKey":{"_id":1}}),
        )
        .unwrap();
        assert_eq!(m.op, Op::Delete);
        assert_eq!(m.before.unwrap()["_id"], 1);
        assert!(m.after.is_none());
    }

    #[test]
    fn invalidate_ignored() {
        assert!(map_event(&json!({"operationType":"invalidate"})).is_none());
    }
}
