use crate::config::MongoConfig;
use crate::mapping::{map_event, Mapped};
use aqueduct_core::{ChangeEvent, Error, Position, Result, Source};
use async_trait::async_trait;
use futures_util::StreamExt;
use mongodb::bson::{doc, Bson, Document};
use mongodb::change_stream::event::ResumeToken;
use mongodb::change_stream::ChangeStream;
use mongodb::options::{FullDocumentBeforeChangeType, FullDocumentType};
use mongodb::Client;
use serde_json::value::to_raw_value;
use serde_json::{Map, Value};

type Stream = ChangeStream<Document>;

/// Reads a MongoDB change stream. The position is the event's resume token (as JSON text),
/// so a restart resumes just after the last checkpointed event. The server keeps no
/// consumer state, so `ack` has nothing to commit; durability lives in the checkpoint store.
pub struct MongoSource {
    cfg: MongoConfig,
    stream: Option<Stream>,
}

impl MongoSource {
    pub fn new(cfg: MongoConfig) -> Self {
        Self { cfg, stream: None }
    }
}

fn src_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Source(e.to_string())
}

/// Resume token JSON text -> driver token.
fn parse_token(text: &str) -> Result<ResumeToken> {
    let json: Value = serde_json::from_str(text).map_err(src_err)?;
    let bson = Bson::try_from(json).map_err(src_err)?;
    mongodb::bson::from_bson(bson).map_err(src_err)
}

/// `clusterTime` (a BSON timestamp, seconds in the high word) to microseconds.
fn commit_micros(ev: &Value) -> i64 {
    ev.pointer("/clusterTime/$timestamp/t")
        .and_then(Value::as_i64)
        .map_or(0, |s| s * 1_000_000)
}

fn to_change_event(doc: &Document) -> Result<Option<ChangeEvent>> {
    let ev = Bson::Document(doc.clone()).into_relaxed_extjson();
    let Some(Mapped {
        schema,
        table,
        op,
        before,
        after,
    }) = map_event(&ev)
    else {
        return Ok(None);
    };
    let token = ev
        .get("_id")
        .ok_or_else(|| Error::Source("change event without resume token".into()))?;
    let raw = |m: Option<Map<String, Value>>| -> Result<_> {
        m.map(|m| to_raw_value(&m).map_err(src_err)).transpose()
    };
    Ok(Some(ChangeEvent {
        position: Position::Text(token.to_string()),
        xid: 0,
        commit_ts_micros: commit_micros(&ev),
        schema,
        table,
        op,
        before: raw(before)?,
        after: raw(after)?,
        key: ev.get("documentKey").map(Value::to_string),
    }))
}

#[async_trait]
impl Source for MongoSource {
    async fn start(&mut self, start: Option<Position>) -> Result<()> {
        self.cfg.validate().map_err(Error::Config)?;
        let client = Client::with_uri_str(&self.cfg.uri).await.map_err(src_err)?;
        let db = client.database(&self.cfg.database);
        let mut pipeline = Vec::new();
        if !self.cfg.collections.is_empty() {
            pipeline.push(doc! {"$match": {"ns.coll": {"$in": self.cfg.collections.clone()}}});
        }
        let mut watch = db
            .watch()
            .pipeline(pipeline)
            .full_document(FullDocumentType::UpdateLookup);
        if self.cfg.pre_images {
            watch = watch.full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable);
        }
        if let Some(Position::Text(t)) = start {
            watch = watch.start_after(parse_token(&t)?);
        }
        self.stream = Some(watch.await.map_err(src_err)?.with_type::<Document>());
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ChangeEvent>> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::Source("source not started".into()))?;
        while let Some(doc) = stream.next().await {
            let doc = doc.map_err(src_err)?;
            if let Some(ev) = to_change_event(&doc)? {
                return Ok(Some(ev));
            }
        }
        Ok(None)
    }

    async fn ack(&mut self, _position: Position) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips() {
        let t = r#"{"_data":"8265A1"}"#;
        assert!(parse_token(t).is_ok());
        assert!(parse_token("not json").is_err());
    }

    #[test]
    fn converts_insert_event() {
        let d = doc! {"_id": {"_data": "8265A1"}, "operationType": "insert",
        "ns": {"db": "app", "coll": "u"}, "fullDocument": {"_id": 1, "n": "a"},
        "documentKey": {"_id": 1}};
        let e = to_change_event(&d).unwrap().unwrap();
        assert_eq!(e.table, "u");
        assert_eq!(e.position, Position::Text(r#"{"_data":"8265A1"}"#.into()));
        assert!(e.after.unwrap().get().contains("\"n\":\"a\""));
        assert_eq!(e.key.as_deref(), Some(r#"{"_id":1}"#));
    }

    #[tokio::test]
    async fn srv_uri_is_resolved_not_rejected() {
        // Needs the driver's `dns-resolver` feature; without it the error names that feature.
        let err = Client::with_uri_str("mongodb+srv://aqueduct.invalid/")
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.contains("dns-resolver"), "{err}");
    }
}
