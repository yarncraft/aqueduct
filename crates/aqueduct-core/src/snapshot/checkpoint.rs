//! The composite checkpoint (ADR 0001 §4): snapshot progress rides in the same durable write as
//! the stream position, so there is no second file that can disagree after a crash.
//!
//! `pos` stays opaque text here: the Postgres K (`"<commit>@<change>#<ord>"`), a plain decimal
//! LSN written by older versions, or a MongoDB resume token. The source parses it.

use serde::{Deserialize, Serialize};

/// Snapshot progress. `after` is the last primary key of the last chunk that was fully emitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapState {
    pub id: String,
    pub tables: Vec<String>,
    /// Index into `tables` of the table being read.
    pub table: usize,
    pub after: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub pos: String,
    /// `None` outside a snapshot.
    pub snap: Option<SnapState>,
}

#[derive(Serialize, Deserialize)]
struct Composite {
    pos: String,
    snap: SnapState,
}

impl Checkpoint {
    pub fn plain(pos: impl Into<String>) -> Self {
        Self {
            pos: pos.into(),
            snap: None,
        }
    }

    /// Without snapshot progress this is exactly `pos`, so the non-snapshot format does not change.
    pub fn encode(&self) -> String {
        match &self.snap {
            None => self.pos.clone(),
            Some(snap) => serde_json::to_string(&Composite {
                pos: self.pos.clone(),
                snap: snap.clone(),
            })
            // Strings and integers only: serialising cannot fail.
            .unwrap_or_default(),
        }
    }

    /// Accepts the composite JSON and every plain format (decimal LSN, K text, resume token).
    /// A `{` can only start the JSON form: no plain format begins with one.
    pub fn decode(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("empty checkpoint".into());
        }
        if !text.starts_with('{') {
            return Ok(Self::plain(text));
        }
        let c: Composite =
            serde_json::from_str(text).map_err(|e| format!("bad snapshot checkpoint: {e}"))?;
        if c.snap.table >= c.snap.tables.len() {
            return Err(format!(
                "snapshot checkpoint names table {} of {}",
                c.snap.table,
                c.snap.tables.len()
            ));
        }
        Ok(Self {
            pos: c.pos,
            snap: Some(c.snap),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> SnapState {
        SnapState {
            id: "s1".into(),
            tables: vec!["public.a".into(), "public.b".into()],
            table: 1,
            after: Some(vec!["42".into(), "x\"y".into()]),
        }
    }

    #[test]
    fn composite_round_trips() {
        let c = Checkpoint {
            pos: "16/B374D848@16/B374D8E0#2".into(),
            snap: Some(snap()),
        };
        assert_eq!(Checkpoint::decode(&c.encode()), Ok(c));
    }

    #[test]
    fn composite_without_after_round_trips() {
        let mut s = snap();
        s.after = None;
        let c = Checkpoint {
            pos: "7".into(),
            snap: Some(s),
        };
        assert_eq!(Checkpoint::decode(&c.encode()), Ok(c));
    }

    #[test]
    fn composite_has_the_documented_shape() {
        let c = Checkpoint {
            pos: "7".into(),
            snap: Some(snap()),
        };
        assert_eq!(
            c.encode(),
            r#"{"pos":"7","snap":{"id":"s1","tables":["public.a","public.b"],"table":1,"after":["42","x\"y"]}}"#
        );
    }

    #[test]
    fn plain_formats_still_decode_and_encode_unchanged() {
        for p in [
            "123456",
            "16/B374D848@16/B374D8E0#2",
            "82636f72abcd",
            "~tok",
        ] {
            let c = Checkpoint::decode(p).unwrap();
            assert_eq!(c, Checkpoint::plain(p));
            assert_eq!(c.encode(), p);
        }
        assert_eq!(Checkpoint::decode(" 99\n"), Ok(Checkpoint::plain("99")));
    }

    #[test]
    fn rejects_garbage() {
        for bad in [
            "",
            "   ",
            "{",
            "{}",
            r#"{"pos":"1"}"#,
            r#"{"pos":1,"snap":{"id":"s","tables":["t"],"table":0,"after":null}}"#,
            r#"{"pos":"1","snap":{"id":"s","tables":["t"],"table":1,"after":null}}"#,
            r#"{"pos":"1","snap":{"id":"s","tables":[],"table":0,"after":null}}"#,
        ] {
            assert!(Checkpoint::decode(bad).is_err(), "{bad:?}");
        }
    }
}
