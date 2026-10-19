use super::window::*;

const T: &str = "public.a";

fn rows(keys: &[&str]) -> Vec<ChunkRow<u32>> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| ChunkRow {
            key: (*k).into(),
            row: i as u32,
        })
        .collect()
}

fn keys(o: Outcome<u32>) -> Vec<String> {
    match o {
        Outcome::Emit { rows, .. } => rows.into_iter().map(|r| r.key).collect(),
        Outcome::None => panic!("expected Emit"),
    }
}

/// Run one window: `before` events precede the low watermark, `inside` fall in the window.
fn run(chunk: &[&str], before: &[&str], inside: &[&str], truncate: bool) -> Vec<String> {
    let mut w = Window::new();
    w.expect(1, T).unwrap();
    w.set_chunk(rows(chunk)).unwrap();
    for k in before {
        w.live_key(T, k);
    }
    w.mark(Mark::Low(1)).unwrap();
    for k in inside {
        w.live_key(T, k);
    }
    if truncate {
        w.live_truncate(T);
    }
    keys(w.mark(Mark::High(1)).unwrap())
}

#[test]
fn untouched_chunk_is_emitted_whole_and_in_order() {
    assert_eq!(run(&["1", "2", "3"], &[], &[], false), ["1", "2", "3"]);
}

#[test]
fn rows_named_by_live_events_in_the_window_are_dropped() {
    assert_eq!(run(&["1", "2", "3"], &[], &["2"], false), ["1", "3"]);
    assert_eq!(run(&["1", "2", "3"], &[], &["1", "3", "9"], false), ["2"]);
}

#[test]
fn a_key_changing_update_drops_both_old_and_new_key() {
    assert_eq!(run(&["1", "2", "3"], &[], &["1", "7"], false), ["2", "3"]);
    assert!(run(&["1", "7"], &[], &["1", "7"], false).is_empty());
}

#[test]
fn repeated_live_events_for_one_key_are_harmless() {
    assert_eq!(run(&["1", "2"], &[], &["1", "1", "1"], false), ["2"]);
}

#[test]
fn live_events_before_the_low_watermark_do_not_drop_rows() {
    assert_eq!(run(&["1", "2"], &["1", "2"], &[], false), ["1", "2"]);
}

#[test]
fn truncate_in_the_window_drops_the_whole_chunk() {
    assert!(run(&["1", "2", "3"], &[], &[], true).is_empty());
    assert!(run(&["1", "2"], &[], &["1"], true).is_empty());
}

#[test]
fn truncate_before_the_low_watermark_does_not_drop_the_chunk() {
    let mut w = Window::new();
    w.expect(1, T).unwrap();
    w.live_truncate(T);
    w.set_chunk(rows(&["1"])).unwrap();
    w.mark(Mark::Low(1)).unwrap();
    assert_eq!(keys(w.mark(Mark::High(1)).unwrap()), ["1"]);
}

#[test]
fn empty_chunk_emits_nothing() {
    assert!(run(&[], &[], &["1"], false).is_empty());
}

#[test]
fn stale_watermarks_are_ignored() {
    let mut w = Window::new();
    assert_eq!(w.mark(Mark::High(3)), Ok(Outcome::None)); // nothing in flight
    w.expect(5, T).unwrap();
    w.set_chunk(rows(&["1", "2"])).unwrap();
    assert_eq!(w.mark(Mark::Low(4)), Ok(Outcome::None));
    assert_eq!(w.mark(Mark::High(4)), Ok(Outcome::None)); // replay of chunk 4
    w.live_key(T, "1"); // low of chunk 5 not seen: outside the window
    assert_eq!(w.mark(Mark::Low(5)), Ok(Outcome::None));
    w.live_key(T, "2");
    assert_eq!(w.mark(Mark::High(6)), Ok(Outcome::None)); // a later id is stale too
    assert_eq!(w.in_flight(), Some(5));
    assert_eq!(keys(w.mark(Mark::High(5)).unwrap()), ["1"]);
    assert_eq!(w.in_flight(), None);
}

#[test]
fn a_repeated_low_watermark_keeps_the_window_open() {
    let mut w = Window::new();
    w.expect(1, T).unwrap();
    w.set_chunk(rows(&["1", "2"])).unwrap();
    w.mark(Mark::Low(1)).unwrap();
    w.live_key(T, "1");
    w.mark(Mark::Low(1)).unwrap();
    assert_eq!(keys(w.mark(Mark::High(1)).unwrap()), ["2"]);
}

#[test]
fn window_is_reusable_after_emit() {
    let mut w = Window::new();
    for id in 1..=3 {
        w.expect(id, T).unwrap();
        w.set_chunk(rows(&["a"])).unwrap();
        w.mark(Mark::Low(id)).unwrap();
        assert_eq!(keys(w.mark(Mark::High(id)).unwrap()), ["a"]);
    }
}

#[test]
fn protocol_misuse_is_an_error() {
    let mut w: Window<u32> = Window::new();
    assert_eq!(w.set_chunk(rows(&[])), Err(WindowError::NoChunk));
    w.expect(1, T).unwrap();
    assert_eq!(w.expect(2, T), Err(WindowError::Busy(1)));
    assert_eq!(w.mark(Mark::High(1)), Err(WindowError::HighBeforeLow(1)));
    w.mark(Mark::Low(1)).unwrap();
    assert_eq!(w.mark(Mark::High(1)), Err(WindowError::ChunkMissing(1)));
    // the failed high keeps the chunk in flight, so the source can still finish it
    w.set_chunk(rows(&["1"])).unwrap();
    assert_eq!(w.set_chunk(rows(&[])), Err(WindowError::ChunkAlreadySet(1)));
    assert_eq!(keys(w.mark(Mark::High(1)).unwrap()), ["1"]);
}

/// Random interleavings of live events around the watermarks: no emitted row may share a key
/// with a live event of its window, and every untouched row is emitted exactly once.
#[test]
fn random_interleavings_never_emit_a_row_the_stream_already_carries() {
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    for _ in 0..2000 {
        let n = next(12) as usize;
        let chunk: Vec<String> = (0..n).map(|i| (i * 2).to_string()).collect();
        let hand = |w: &mut Window<u32>| {
            let r = chunk
                .iter()
                .enumerate()
                .map(|(i, k)| ChunkRow {
                    key: k.clone(),
                    row: i as u32,
                })
                .collect();
            w.set_chunk(r).unwrap();
        };
        let mut w = Window::new();
        w.expect(9, T).unwrap();
        // the chunk is handed over before low, inside the window, or just before high
        let read_at = next(3);
        if read_at == 0 {
            hand(&mut w);
        }
        let mut window_keys: Vec<String> = Vec::new();
        let mut truncated = false;
        for phase in 0..2 {
            if phase == 1 {
                w.mark(Mark::Low(9)).unwrap();
                if read_at == 1 {
                    hand(&mut w);
                }
            }
            for _ in 0..next(8) {
                if next(10) == 0 && phase == 1 {
                    w.live_truncate(T);
                    truncated = true;
                } else {
                    let k = next(26).to_string(); // overlaps the chunk's even keys
                    w.live_key(T, &k);
                    if phase == 1 {
                        window_keys.push(k);
                    }
                }
            }
        }
        if read_at == 2 {
            hand(&mut w);
        }
        let got = keys(w.mark(Mark::High(9)).unwrap());
        if truncated {
            assert!(got.is_empty());
            continue;
        }
        let want: Vec<String> = chunk
            .iter()
            .filter(|k| !window_keys.contains(k))
            .cloned()
            .collect();
        assert_eq!(got, want);
    }
}

#[test]
fn live_events_of_other_tables_do_not_touch_the_chunk() {
    let mut w = Window::new();
    w.expect(1, T).unwrap();
    w.set_chunk(rows(&["42", "43"])).unwrap();
    w.mark(Mark::Low(1)).unwrap();
    w.live_key("public.b", "42");
    w.live_truncate("public.b");
    assert_eq!(keys(w.mark(Mark::High(1)).unwrap()), ["42", "43"]);
}

#[test]
fn emit_names_the_last_row_read_even_when_none_survive() {
    let mut w = Window::new();
    w.expect(1, T).unwrap();
    w.set_chunk(rows(&["1", "2", "3"])).unwrap();
    w.mark(Mark::Low(1)).unwrap();
    w.live_key(T, "3");
    w.live_key(T, "1");
    w.live_key(T, "2");
    assert_eq!(
        w.mark(Mark::High(1)).unwrap(),
        Outcome::Emit {
            rows: Vec::new(),
            last_key: Some("3".into())
        }
    );
    w.expect(2, T).unwrap();
    w.set_chunk(rows(&["4"])).unwrap();
    w.mark(Mark::Low(2)).unwrap();
    w.live_truncate(T);
    assert_eq!(
        w.mark(Mark::High(2)).unwrap(),
        Outcome::Emit {
            rows: Vec::new(),
            last_key: Some("4".into())
        }
    );
    w.expect(3, T).unwrap();
    w.set_chunk(rows(&[])).unwrap();
    w.mark(Mark::Low(3)).unwrap();
    assert_eq!(
        w.mark(Mark::High(3)).unwrap(),
        Outcome::Emit {
            rows: Vec::new(),
            last_key: None
        }
    );
}
