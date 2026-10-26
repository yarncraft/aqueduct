use super::*;
use aqueduct_core::Position;

fn route(p: &str, t: &str) -> KafkaRoute {
    KafkaRoute {
        name: None,
        pattern: p.into(),
        topic: t.into(),
        ..Default::default()
    }
}

fn named(n: &str, p: &str, t: &str) -> KafkaRoute {
    KafkaRoute {
        name: Some(n.into()),
        ..route(p, t)
    }
}

#[test]
fn first_match_wins_then_default_with_placeholders() {
    let p = RoutePlan::new(
        vec![
            route("public.users", "users-topic"),
            route("public.*", "cdc.{table}"),
        ],
        Some("all.{schema}.{table}".into()),
    )
    .unwrap();
    assert_eq!(
        p.topic_for("public", "users").as_deref(),
        Some("users-topic")
    );
    assert_eq!(
        p.topic_for("public", "orders").as_deref(),
        Some("cdc.orders")
    );
    assert_eq!(p.topic_for("sales", "x").as_deref(), Some("all.sales.x"));
}

/// A table is matched once: later events read its route and name from the cache, and a
/// table that matches nothing is remembered as such too.
#[test]
fn a_table_is_matched_once_and_a_clone_starts_empty() {
    let mut p = RoutePlan::new(
        vec![
            route("public.users", "users-topic"),
            route("public.*", "cdc.{table}"),
        ],
        None,
    )
    .unwrap();
    p.set_partition_by(PartitionBy::Table);
    let tables = |p: &RoutePlan| p.resolved.0.read().unwrap().len;
    for _ in 0..3 {
        assert_eq!(
            p.topic_for("public", "users").as_deref(),
            Some("users-topic")
        );
        assert_eq!(
            p.topic_for("public", "orders").as_deref(),
            Some("cdc.orders")
        );
        assert_eq!(p.topic_for("sales", "x"), None);
    }
    assert_eq!(tables(&p), 3);
    // Every record of a table shares one `schema.table` as its partition input.
    let e = crate::sample();
    let (a, b) = (p.render(&e, 0).unwrap(), p.render(&e, 0).unwrap());
    match (&a[0].partition_key, &b[0].partition_key) {
        (PartitionKey::Table(x), PartitionKey::Table(y)) => {
            assert_eq!(&**x, "public.users");
            assert!(Arc::ptr_eq(x, y));
        }
        other => panic!("{other:?}"),
    }
    let c = p.clone();
    assert_eq!(tables(&c), 0);
    assert_eq!(
        c.topic_for("public", "users").as_deref(),
        Some("users-topic")
    );
}

#[test]
fn topic_globs_cover_routes_and_default() {
    let p = RoutePlan::new(
        vec![
            route("public.users", "users"),
            route("*.audit", "a.{schema}"),
        ],
        Some("cdc.{schema}.{table}".into()),
    )
    .unwrap();
    // Then each route's dead-letter topic: `<topic>.dlq`, templates as globs.
    assert_eq!(
        p.topic_globs(),
        vec![
            "users",
            "a.*",
            "cdc.*.*",
            "users.dlq",
            "a.*.dlq",
            "cdc.*.*.dlq"
        ]
    );
}

#[test]
fn unrouted_event_without_default_is_an_error() {
    let p = RoutePlan::new(vec![route("public.*", "p")], None).unwrap();
    assert_eq!(p.topic_for("sales", "x"), None);
    let mut e = crate::sample();
    e.schema = "sales".into();
    let err = p.render(&e, 0).unwrap_err().to_string();
    assert!(
        err.contains("no kafka route or default topic for sales.users"),
        "{err}"
    );
    assert!(RoutePlan::new(vec![], None).is_err());
}

#[test]
fn route_names_are_validated() {
    assert!(RoutePlan::new(vec![named("", "a.*", "t")], None).is_err());
    assert!(RoutePlan::new(vec![named("x", "a.*", "t"), named("x", "b.*", "u")], None).is_err());
    let err = |routes: Vec<KafkaRoute>, default: Option<&str>| {
        RoutePlan::new(routes, default.map(Into::into))
            .map(|_| ())
            .unwrap_err()
            .to_string()
    };
    // Unnamed routes label by pattern: the same `match` twice can never reach the second.
    let e = err(vec![route("a.*", "t"), route("a.*", "u")], None);
    assert!(e.contains("the second can never match"), "{e}");
    // A name may not collide with another route's pattern label, in either order.
    for routes in [
        vec![route("a.*", "t"), named("a.*", "b.*", "u")],
        vec![named("a.*", "b.*", "u"), route("a.*", "t")],
    ] {
        let e = err(routes, None);
        assert!(e.contains("duplicate sink route label `a.*`"), "{e}");
    }
    // `default` is the `sink.topic` fallback's label, with or without a `sink.topic`.
    for default in [None, Some("rest")] {
        let e = err(vec![named("default", "a.*", "t")], default);
        assert!(e.contains("reserved"), "{e}");
        let e = err(vec![route("default", "t")], default);
        assert!(e.contains("reserved"), "{e}");
    }
    // Distinct labels are fine, and every Prometheus series is unique.
    let p = RoutePlan::new(
        vec![named("x", "a.*", "t"), route("a.*", "u"), route("b.*", "v")],
        Some("rest".into()),
    )
    .unwrap();
    let labels: Vec<String> = p.stats().snapshot().into_iter().map(|c| c.route).collect();
    assert_eq!(labels, ["x", "a.*", "b.*", "default"]);
}

#[test]
fn heartbeats_render_nothing() {
    let p = RoutePlan::new(vec![], Some("t".into())).unwrap();
    let hb = ChangeEvent::heartbeat(Position::Text("x".into()), 0);
    assert!(p.render(&hb, 0).unwrap().is_empty());
    assert!(p.stats().snapshot().iter().all(|c| c.counts.matched == 0));
}

#[test]
fn stats_count_per_route_and_default() {
    let p = RoutePlan::new(
        vec![
            named("users", "public.users", "u"),
            route("sales.*", "s.{table}"),
        ],
        Some("rest".into()),
    )
    .unwrap();
    let e = |schema: &str, table: &str| {
        let mut e = crate::sample();
        e.schema = schema.into();
        e.table = table.into();
        e
    };
    for ev in [
        e("public", "users"),
        e("public", "users"),
        e("sales", "x"),
        e("hr", "y"),
    ] {
        p.render(&ev, 0).unwrap();
    }
    let c = |route: &str, n| RouteCount {
        route: route.into(),
        counts: RouteCounts {
            matched: n,
            produced: n,
            ops: OpCounts {
                insert: n,
                ..Default::default()
            },
            ..Default::default()
        },
        unseen_fields: vec![],
    };
    assert_eq!(
        p.stats().snapshot(),
        vec![c("users", 2), c("sales.*", 1), c("default", 1)]
    );
}

// --- filter: where (op, schema, table, rows), dead-letter topic (R3) ------------------------

const DLQ: &str = "aqueduct.dlq";

fn filtered(name: &str, p: &str, filter: Option<&str>) -> KafkaRoute {
    KafkaRoute {
        filter: filter.map(Into::into),
        ..named(name, p, &format!("{name}.topic"))
    }
}

fn plan_with(routes: Vec<KafkaRoute>) -> RoutePlan {
    let mut p = RoutePlan::with_dead_letter(routes, Some("rest".into()), Some(DLQ.into())).unwrap();
    p.set_source(pg());
    p
}

fn row(table: &str, op: Op, json: &str) -> ChangeEvent {
    let mut e = event("public", table, op, Some(r#"{"id":1}"#), Position::Lsn(7));
    let r = raw(json);
    match op {
        Op::Delete => (e.before, e.after) = (r, None),
        Op::Truncate => (e.before, e.after, e.key) = (None, None, None),
        _ => (e.before, e.after) = (None, r),
    }
    e
}

/// What became of one event: the topic it was produced to, `filtered`, or `dlq`.
fn outcome(p: &RoutePlan, e: &ChangeEvent) -> String {
    let out = p.render(e, NOW).unwrap();
    match out.as_slice() {
        [] => "filtered".into(),
        [r] if r.topic == DLQ => "dlq".into(),
        [r] => r.topic.clone(),
        _ => panic!("one event, several records"),
    }
}

#[test]
fn where_filters_ops_and_rows_per_route_table_driven() {
    let p = plan_with(vec![
        filtered("ins", "public.ins", Some("op == 'insert'")),
        filtered("w", "public.orders", Some("row.status == 'paid'")),
        filtered(
            "both",
            "public.both",
            Some("op in ['insert', 'update'] and row.amount > 10"),
        ),
        filtered(
            "keep_null",
            "public.k",
            Some("row.region is null or not (row.region == 'eu')"),
        ),
        filtered("neg", "public.neg", Some("not (row.region == 'eu')")),
        filtered(
            "nodel",
            "public.nodel",
            Some("not (op in ['delete', 'truncate'])"),
        ),
    ]);
    let cases: &[(&str, Op, &str, &str)] = &[
        // op
        ("ins", Op::Insert, r#"{"id":1}"#, "ins.topic"),
        ("ins", Op::Update, r#"{"id":1}"#, "filtered"),
        ("ins", Op::Delete, r#"{"id":1}"#, "filtered"),
        ("ins", Op::Truncate, "{}", "filtered"),
        ("ins", Op::Read, r#"{"id":1}"#, "filtered"),
        ("nodel", Op::Update, r#"{"id":1}"#, "nodel.topic"),
        ("nodel", Op::Read, r#"{"id":1}"#, "nodel.topic"),
        ("nodel", Op::Delete, r#"{"id":1}"#, "filtered"),
        ("nodel", Op::Truncate, "{}", "filtered"),
        // where: true, false, unknown (missing field), null field
        ("orders", Op::Insert, r#"{"status":"paid"}"#, "w.topic"),
        ("orders", Op::Insert, r#"{"status":"open"}"#, "filtered"),
        ("orders", Op::Insert, r#"{"id":1}"#, "filtered"),
        ("orders", Op::Insert, r#"{"status":null}"#, "filtered"),
        ("orders", Op::Truncate, "{}", "filtered"),
        // where on a delete reads the before-image through `row`
        ("orders", Op::Delete, r#"{"status":"paid"}"#, "w.topic"),
        // type error: a number literal against text that is not a number
        ("orders", Op::Insert, r#"{"status":1}"#, "dlq"),
        // op and row data in one predicate
        ("both", Op::Insert, r#"{"amount":11}"#, "both.topic"),
        ("both", Op::Update, r#"{"amount":"10.5"}"#, "both.topic"),
        ("both", Op::Insert, r#"{"amount":9}"#, "filtered"),
        ("both", Op::Delete, r#"{"amount":11}"#, "filtered"),
        ("both", Op::Insert, r#"{"amount":"abc"}"#, "dlq"),
        // a type error wins over the op part too (no short-circuit, ADR 0002 §3)
        ("both", Op::Delete, r#"{"amount":"abc"}"#, "dlq"),
        // `not` over null is unknown, so filtered; `is null or` keeps the row
        ("neg", Op::Insert, r#"{"region":"us"}"#, "neg.topic"),
        ("neg", Op::Insert, r#"{"region":"eu"}"#, "filtered"),
        ("neg", Op::Insert, r#"{"id":1}"#, "filtered"),
        ("k", Op::Insert, r#"{"id":1}"#, "keep_null.topic"),
        ("k", Op::Insert, r#"{"region":"eu"}"#, "filtered"),
        // tables no filtered route claims go to the default, unfiltered
        ("other", Op::Delete, r#"{"id":1}"#, "rest"),
    ];
    for (table, op, json, want) in cases {
        let e = row(table, *op, json);
        assert_eq!(&outcome(&p, &e), want, "{table} {op:?} {json}");
    }
}

#[test]
fn a_claimed_event_never_falls_through_to_a_later_route() {
    // The second route would pass everything; the first owns public.orders.
    let p = plan_with(vec![
        filtered("strict", "public.orders", Some("row.n > 0")),
        route("public.*", "everything"),
    ]);
    assert_eq!(
        outcome(&p, &row("orders", Op::Insert, r#"{"n":0}"#)),
        "filtered"
    );
    assert_eq!(
        outcome(&p, &row("orders", Op::Insert, r#"{"n":"x"}"#)),
        "dlq"
    );
    assert_eq!(
        outcome(&p, &row("users", Op::Insert, r#"{"n":0}"#)),
        "everything"
    );
}

#[test]
fn dead_letter_record_carries_the_reason_but_no_row_data() {
    let p = plan_with(vec![filtered(
        "orders",
        "public.orders",
        Some("row.amount > 10 and row.status == 'paid'"),
    )]);
    let mut e = row(
        "orders",
        Op::Update,
        r#"{"amount":"secret-text","status":"paid"}"#,
    );
    e.key = Some(r#"{"id":4242}"#.into());
    let out = p.render(&e, NOW).unwrap();
    assert_eq!(out.len(), 1);
    let r = &out[0];
    assert_eq!(r.topic, DLQ);
    // Table-only key, partitioned by it: no primary-key values in the dead-letter key.
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"orders","key":null}"#
    );
    assert_eq!(r.partition_input(), r.key);
    // The route has no field operations, so the value is the envelope as is.
    assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
    assert_eq!((r.op, r.lsn.as_str()), ("u", "7"));
    assert_eq!(r.headers.len(), 1);
    let (name, reason) = &r.headers[0];
    assert_eq!(*name, "aqueduct-error");
    assert_eq!(
        reason,
        "route `orders`: where `row.amount > 10 and row.status == 'paid'`: type error: `row.amount` is not a finite number"
    );
    assert!(!reason.contains("secret-text") && !reason.contains("4242"));
}

/// `big` passes what its `where` rejects on to `eu`, `eu` on to `other`: each `where` decides
/// on the original row in turn, and a type error dead-letters at the route that hit it.
fn chained() -> RoutePlan {
    plan_with(vec![
        KafkaRoute {
            otherwise: Some(Otherwise::Route("eu".into())),
            ..filtered("big", "public.orders", Some("row.total > 100"))
        },
        KafkaRoute {
            otherwise: Some(Otherwise::Route("other".into())),
            ..filtered("eu", "", Some("row.region == 'eu'"))
        },
        named("other", "", "orders.other"),
        route("public.*", "p"),
    ])
}

#[test]
fn otherwise_passes_rejected_rows_on_down_the_chain() {
    let p = chained();
    for (json, want) in [
        (r#"{"total":200,"region":"eu"}"#, "big.topic"),
        (r#"{"total":5,"region":"eu"}"#, "eu.topic"),
        (r#"{"total":5,"region":"us"}"#, "orders.other"),
        // Unknown at both: passed on twice.
        (r#"{}"#, "orders.other"),
        // A type error stops the chain where it happens.
        (r#"{"total":"x"}"#, "dlq"),
        (r#"{"total":5,"region":1}"#, "dlq"),
    ] {
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, json)),
            want,
            "{json}"
        );
    }
    // Only `big` resolves; the continuations take nothing directly.
    assert_eq!(outcome(&p, &row("users", Op::Insert, "{}")), "p");

    let snap = p.stats().snapshot();
    let got: Vec<_> = snap
        .iter()
        .map(|c| {
            let n = &c.counts;
            (
                c.route.as_str(),
                [n.matched, n.produced, n.filtered, n.passed_on, n.errors],
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("big", [6, 1, 4, 4, 1]),
            ("eu", [4, 1, 2, 2, 1]),
            ("other", [2, 2, 0, 0, 0]),
            ("public.*", [1, 1, 0, 0, 0]),
            ("default", [0, 0, 0, 0, 0]),
        ]
    );
    for c in &snap {
        let n = &c.counts;
        assert_eq!(
            n.matched,
            n.produced + n.filtered + n.fallback + n.errors,
            "{}",
            c.route
        );
    }

    let d = serde_json::to_value(p.describe()).unwrap();
    let r = &d["routes"];
    assert_eq!(r[0]["match"], "public.orders");
    assert_eq!(r[0]["otherwise"], serde_json::json!({ "route": "eu" }));
    assert_eq!(r[1]["match"], serde_json::Value::Null);
    assert_eq!(r[1]["otherwise"], serde_json::json!({ "route": "other" }));
    assert_eq!(r[2]["match"], serde_json::Value::Null);
    assert!(r[2].get("otherwise").is_none());
    assert!(r[3].get("otherwise").is_none());
}

/// An `otherwise` topic takes the rejected rows as the route renders any row (its key, its
/// field operations), counted as `filtered` and `passed_on`, never `produced`.
#[test]
fn otherwise_topic_takes_the_rejected_rows_shaped_by_the_route() {
    let p = plan_with(vec![
        KafkaRoute {
            key: Some(vec!["region".into()]),
            fields: FieldSpec {
                exclude: Some(vec!["secret".into()]),
                ..FieldSpec::default()
            },
            otherwise: Some(Otherwise::Topic("orders.small".into())),
            ..filtered("big", "public.orders", Some("row.total > 100"))
        },
        route("public.*", "p"),
    ]);
    for (json, want) in [
        (r#"{"total":200,"region":"eu"}"#, "big.topic"),
        (r#"{"total":5,"region":"eu"}"#, "orders.small"),
        (r#"{"region":"us"}"#, "orders.small"),
        (r#"{"total":"x"}"#, "dlq"),
    ] {
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, json)),
            want,
            "{json}"
        );
    }
    let out = p
        .render(
            &row(
                "orders",
                Op::Insert,
                r#"{"total":5,"region":"eu","secret":"s3"}"#,
            ),
            NOW,
        )
        .unwrap();
    assert_eq!(out[0].topic, "orders.small");
    assert!(
        out[0].key.ends_with(r#""key":{"region":"eu"}}"#),
        "{}",
        out[0].key
    );
    assert!(!out[0].value.as_deref().unwrap().contains("s3"));

    let snap = p.stats().snapshot();
    let n = &snap[0].counts;
    assert_eq!(
        [n.matched, n.produced, n.filtered, n.passed_on, n.errors],
        [5, 1, 3, 3, 1]
    );
    let d = serde_json::to_value(p.describe()).unwrap();
    assert_eq!(
        d["routes"][0]["otherwise"],
        serde_json::json!({ "topic": "orders.small" })
    );
    assert!(p.topic_globs().iter().any(|g| g == "orders.small"));
}

#[test]
fn otherwise_is_checked_at_startup() {
    let next = |name: &str, p: &str, filter: Option<&str>, to: &str| KafkaRoute {
        otherwise: Some(Otherwise::Route(to.into())),
        ..filtered(name, p, filter)
    };
    let to_topic = |t: &str| KafkaRoute {
        otherwise: Some(Otherwise::Topic(t.into())),
        ..filtered("a", "public.a", Some("op == 'insert'"))
    };
    let cases: Vec<(Vec<KafkaRoute>, &str)> = vec![
        (
            vec![next("a", "public.a", Some("op == 'insert'"), "nope")],
            "sink route `a`: `otherwise = { route = \"nope\" }` names no route",
        ),
        (
            vec![to_topic("bad topic")],
            "`otherwise = { topic = \"bad topic\" }` is not a valid Kafka topic name",
        ),
        (
            vec![to_topic("__internal")],
            "`otherwise = { topic = \"__internal\" }` must not start with `_`",
        ),
        (
            vec![
                to_topic("orders.eu"),
                KafkaRoute {
                    topic_fallback: Some("orders.other".into()),
                    ..named("o", "public.o", "orders.{row.region}")
                },
            ],
            "could produce `orders.eu`, which is route `a`'s `otherwise` topic",
        ),
        (vec![to_topic(DLQ)], "is also a route's `otherwise` topic"),
        (
            vec![
                named("b", "", "b.t"),
                next("a", "public.a", Some("op == 'insert'"), "b"),
            ],
            "`otherwise` must name a later route",
        ),
        (
            vec![next("a", "public.a", Some("op == 'insert'"), "a")],
            "`otherwise` must name a later route",
        ),
        (
            vec![
                next("a", "public.a", Some("op == 'insert'"), "b"),
                named("b", "public.b", "b.t"),
            ],
            "`otherwise` route `b` has a `match`",
        ),
        (
            vec![next("a", "public.a", None, "b"), named("b", "", "b.t")],
            "`otherwise` needs a `where`",
        ),
        (
            vec![
                KafkaRoute {
                    outbox: Some(OutboxSpec {
                        topic: "outbox.events".into(),
                        ..OutboxSpec::default()
                    }),
                    otherwise: Some(Otherwise::Route("b".into())),
                    filter: Some("op == 'insert'".into()),
                    ..named("a", "public.outbox", "")
                },
                named("b", "", "b.t"),
            ],
            "`outbox` and `otherwise` are mutually exclusive",
        ),
        (
            vec![named("orphan", "", "o.t")],
            "sink route `orphan` has no `match`; set one, or name it in an earlier route's `otherwise`",
        ),
        (
            vec![route("", "o.t")],
            "a sink route needs `match`, or a `name`",
        ),
    ];
    for (routes, want) in cases {
        let err = RoutePlan::with_dead_letter(routes, None, Some(DLQ.into()))
            .map(|_| ())
            .expect_err(want)
            .to_string();
        assert!(err.contains(want), "{err}\nshould contain: {want}");
    }
    // One continuation may be named by several routes.
    assert!(
        RoutePlan::with_dead_letter(
            vec![
                next("a", "public.a", Some("op == 'insert'"), "c"),
                next("b", "public.b", Some("op == 'insert'"), "c"),
                named("c", "", "c.t"),
            ],
            None,
            Some(DLQ.into()),
        )
        .is_ok()
    );
}

#[test]
fn counters_add_up_per_route() {
    let p = plan_with(vec![
        filtered("o", "public.orders", Some("op == 'insert' and row.n > 1")),
        route("public.*", "p"),
    ]);
    for (table, op, json) in [
        ("orders", Op::Insert, r#"{"n":2}"#),   // produced
        ("orders", Op::Insert, r#"{"n":1}"#),   // filtered by where
        ("orders", Op::Update, r#"{"n":2}"#),   // filtered by op
        ("orders", Op::Insert, r#"{"n":"x"}"#), // error
        ("orders", Op::Insert, r#"{}"#),        // unknown: filtered
        ("users", Op::Insert, r#"{}"#),         // second route
        ("x", Op::Insert, r#"{}"#),             // also the second route (public.*)
    ] {
        p.render(&row(table, op, json), NOW).unwrap();
    }
    let mut sales = row("x", Op::Insert, "{}");
    sales.schema = "sales".into();
    p.render(&sales, NOW).unwrap();
    let snap = p.stats().snapshot();
    for c in &snap {
        let n = &c.counts;
        let sums = n.outcome_ops.map(|row| row.iter().sum::<u64>());
        assert_eq!(sums, [n.filtered, n.fallback, n.errors], "{}", c.route);
    }
    let counts: Vec<(&str, RouteCounts)> = snap
        .iter()
        .map(|c| {
            let mut n = c.counts;
            n.outcome_ops = Default::default();
            (c.route.as_str(), n)
        })
        .collect();
    let c = |matched, produced, filtered, errors| RouteCounts {
        matched,
        produced,
        filtered,
        fallback: 0,
        errors,
        passed_on: 0,
        tombstones_skipped: 0,
        // Every produced event here is an insert.
        ops: OpCounts {
            insert: produced,
            ..Default::default()
        },
        outcome_ops: Default::default(),
    };
    assert_eq!(
        counts,
        vec![
            ("o", c(5, 1, 3, 1)),
            ("public.*", c(2, 2, 0, 0)),
            ("default", c(1, 1, 0, 0)),
        ]
    );
    for c in &snap {
        let k = c.counts;
        assert_eq!(k.matched, k.produced + k.filtered + k.fallback + k.errors);
        assert_eq!(k.ops.total(), k.produced);
    }
}

#[test]
fn route_counts_wait_for_the_broker_ack() {
    let p = plan_with(vec![filtered("o", "public.orders", Some("row.n > 1"))]);
    let batch = [
        row("orders", Op::Insert, r#"{"n":2}"#),   // produced
        row("orders", Op::Insert, r#"{"n":1}"#),   // filtered
        row("orders", Op::Insert, r#"{"n":"x"}"#), // error
    ];
    let render = || {
        let mut out = Vec::new();
        for e in &batch {
            p.render_into(e, NOW, &mut out).unwrap();
        }
    };
    let counts = || p.stats().snapshot()[0].counts;
    // The sink renders a batch the broker then refuses: nothing is counted, and the retry
    // starts from a clean slate.
    render();
    assert_eq!(counts(), RouteCounts::default());
    p.discard_counts();
    p.commit_counts();
    assert_eq!(counts(), RouteCounts::default());
    // The retry is acknowledged: the batch counts once.
    render();
    p.commit_counts();
    let c = counts();
    assert_eq!(
        (c.matched, c.produced, c.filtered, c.errors, c.ops.insert),
        (3, 1, 1, 1, 1)
    );
    p.commit_counts();
    assert_eq!(counts(), c);
}

#[test]
fn taken_counts_apply_on_commit_and_are_dropped_on_failure() {
    let p = plan_with(vec![route("public.orders", "orders")]);
    let one = row("orders", Op::Insert, r#"{"n":1}"#);
    let render = || p.render_into(&one, NOW, &mut Vec::new()).unwrap();
    let produced = || p.stats().snapshot()[0].counts.produced;
    // Two batches in flight: the first fails, the second is acknowledged.
    render();
    let failed = p.take_counts();
    render();
    render();
    let acked = p.take_counts();
    assert_eq!(
        produced(),
        0,
        "nothing counts before its batch is acknowledged"
    );
    drop(failed);
    acked.commit();
    assert_eq!(produced(), 2, "only the acknowledged batch counts");
    // Taking cleared the pending counts, so a commit now adds nothing.
    p.commit_counts();
    assert_eq!(produced(), 2);
}

#[test]
fn produced_counts_per_operation_with_tombstones_under_their_delete() {
    let p = plan_with(vec![KafkaRoute {
        tombstones: true,
        ..route("public.*", "p")
    }]);
    for (op, n) in [
        (Op::Insert, 3),
        (Op::Update, 2),
        (Op::Delete, 2),
        (Op::Truncate, 1),
        (Op::Read, 4),
    ] {
        for _ in 0..n {
            p.render(&row("orders", op, r#"{"n":1}"#), NOW).unwrap();
        }
    }
    // Heartbeats are no event of any route.
    p.render(&row("orders", Op::Heartbeat, "{}"), NOW).unwrap();
    let c = p.stats().snapshot()[0].counts;
    // A delete and its tombstone are one produced event, counted under delete.
    assert_eq!(c.ops.slots(), [3, 2, 2, 1, 4]);
    assert_eq!(c.ops.total(), c.produced);
    assert_eq!(
        serde_json::to_value(c.ops).unwrap(),
        serde_json::json!({"insert": 3, "update": 2, "delete": 2, "truncate": 1, "read": 4})
    );
}

#[test]
fn where_is_validated_at_startup() {
    let dlq = Some(DLQ.to_string());
    let build = |r: KafkaRoute, dlq: Option<String>| {
        RoutePlan::with_dead_letter(vec![r], Some("rest".into()), dlq)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    // (where, dead_letter_topic, expected error or "" for ok)
    let cases: &[(&str, Option<&str>, &str)] = &[
        // `op`, `schema` and `table` alone cannot fail, so no dead-letter topic is needed.
        ("op in ['insert', 'update', 'read']", None, ""),
        (
            "op == 'delete' or schema == 'audit' and table != 'log'",
            None,
            "",
        ),
        ("op is not null", None, ""),
        (
            "op == 'upsert'",
            None,
            "sink route `r`: invalid `where`: unknown op `upsert`",
        ),
        ("op in ['insert', 'Update']", None, "unknown op `Update`"),
        ("op == 1", None, "sink route `r`: invalid `where`"),
        ("table in [true]", None, "`table` is a string"),
        ("op < 'insert'", None, "ordering a string is not supported"),
        // Reading row data needs a dead-letter topic, and there always is one: the route's
        // `r.topic.dlq` without `sink.dead_letter_topic`.
        ("row.n > 1", Some(DLQ), ""),
        ("row.n > 1", None, ""),
        ("op == 'insert' and before.n is null", None, ""),
        ("row.n >", Some(DLQ), "sink route `r`: invalid `where`"),
        (
            "row.ts < '2024-01-01'",
            Some(DLQ),
            "sink route `r`: invalid `where`",
        ),
        ("", Some(DLQ), "sink route `r`: invalid `where`"),
        ("row.n > 1", Some("_dlq"), "must not start with `_`"),
        ("op == 'insert'", Some("_dlq"), "must not start with `_`"),
        (
            "row.n > 1",
            Some("r.topic"),
            "also a topic events are routed to",
        ),
        (
            "row.n > 1",
            Some("rest"),
            "also a topic events are routed to",
        ),
        (
            "row.n > 1",
            Some("bad topic"),
            "not a valid Kafka topic name",
        ),
        ("row.n > 1", Some(""), "not a valid Kafka topic name"),
    ];
    for (filter, dlq_topic, want) in cases {
        let r = filtered("r", "public.*", Some(filter));
        let got = build(r, dlq_topic.map(str::to_string));
        if want.is_empty() {
            assert_eq!(got, Ok(()), "{filter:?} {dlq_topic:?}");
        } else {
            let err = got.expect_err(want);
            assert!(err.contains(want), "{err} should contain {want}");
        }
    }
    // A dead-letter topic matching a templated topic is rejected too.
    let r = KafkaRoute {
        filter: Some("row.n > 1".into()),
        ..route("public.*", "cdc.{table}")
    };
    assert!(build(r.clone(), Some("cdc.dlq".into())).is_err());
    assert!(build(r, dlq.clone()).is_ok());
    // A dead-letter topic without any `where` is allowed (and still checked).
    assert!(RoutePlan::with_dead_letter(vec![], Some("t".into()), Some("_x".into())).is_err());
    assert!(RoutePlan::with_dead_letter(vec![], Some("t".into()), dlq).is_ok());
}

#[test]
fn unseen_where_fields_are_reported_after_enough_rows() {
    let p = plan_with(vec![
        filtered(
            "o",
            "public.orders",
            Some("row.stauts == 'paid' or row.status == 'paid' or op == 'delete' or before.n > 0"),
        ),
        route("public.*", "p"),
    ]);
    let ev = row("orders", Op::Insert, r#"{"status":"paid"}"#);
    for _ in 0..UNSEEN_FIELD_ROWS - 1 {
        p.render(&ev, NOW).unwrap();
    }
    assert!(p.stats().snapshot()[0].unseen_fields.is_empty(), "not yet");
    p.render(&ev, NOW).unwrap();
    // `op` is not a row field; `before.n` never had a before-image to look in.
    assert_eq!(p.stats().snapshot()[0].unseen_fields, vec!["row.stauts"]);
    assert_eq!(p.describe().routes[0].unseen_fields, vec!["row.stauts"]);
    // Seen once, the field is no longer reported.
    p.render(&row("orders", Op::Insert, r#"{"stauts":"x"}"#), NOW)
        .unwrap();
    assert!(p.stats().snapshot()[0].unseen_fields.is_empty());
    assert!(p.stats().snapshot()[1].unseen_fields.is_empty());
}

#[test]
fn describe_shows_the_plan_and_live_counts_without_row_data() {
    let mut p = plan_with(vec![
        filtered(
            "orders",
            "public.orders",
            Some("op in ['insert', 'update'] and row.status == 'paid'"),
        ),
        keyed("public.*", "cdc.{table}", &["id", "tenant"]),
    ]);
    p.set_partition_by(PartitionBy::Table);
    p.render(
        &row("orders", Op::Insert, r#"{"status":"paid","card":"4111"}"#),
        NOW,
    )
    .unwrap();
    let v = serde_json::to_value(p.describe()).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "default_topic": "rest",
            "dead_letter_topic": DLQ,
            "partition_by": "table",
            "routes": [
                {"route": "orders", "match": "public.orders",
                 "where": "op in ['insert', 'update'] and row.status == 'paid'", "key": null, "topic": "orders.topic", "topic_fallback": null, "topic_values": null,
                 "dead_letter_topic": DLQ,
                 "counts": {"matched": 1, "produced": 1, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                            "ops": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
                 "unseen_fields": []},
                {"route": "public.*", "match": "public.*", "where": null,
                 "key": ["id", "tenant"], "topic": "cdc.{table}", "topic_fallback": null, "topic_values": null,
                 "dead_letter_topic": DLQ,
                 "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                            "ops": {"insert": 0, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
                 "unseen_fields": []},
                {"route": "default", "match": null, "where": null, "key": null,
                 "topic": "rest", "topic_fallback": null, "topic_values": null,
                 "dead_letter_topic": DLQ,
                 "counts": {"matched": 0, "produced": 0, "filtered": 0, "fallback": 0, "errors": 0, "tombstones_skipped": 0,
                            "ops": {"insert": 0, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
                 "unseen_fields": []}
            ]
        })
    );
    assert!(!v.to_string().contains("4111"));
    // The metrics snapshot is flat: {route, matched, ..., unseen_fields}.
    let m = serde_json::to_value(p.stats().snapshot()).unwrap();
    assert_eq!(m[0]["route"], "orders");
    assert_eq!(m[0]["produced"], 1);
    assert_eq!(m[0]["fallback"], 0);
    assert!(m[0]["unseen_fields"].as_array().unwrap().is_empty());
}

#[test]
fn dead_letter_topic_is_among_the_inspected_topics() {
    let p = plan_with(vec![route("public.users", "users")]);
    assert_eq!(p.topic_globs(), vec!["users", "rest", DLQ]);
    assert_eq!(p.dead_letter_topic(), Some(DLQ));
}

// --- per-route key (R5) -----------------------------------------------------------------------

fn keyed(p: &str, t: &str, cols: &[&str]) -> KafkaRoute {
    KafkaRoute {
        key: Some(cols.iter().map(|c| c.to_string()).collect()),
        ..route(p, t)
    }
}

fn keyed_plan(info: SourceInfo, by: PartitionBy) -> RoutePlan {
    let mut p = RoutePlan::new(
        vec![keyed("*.orders", "orders", &["tenant", "id"])],
        Some("rest".into()),
    )
    .unwrap();
    p.set_source(info);
    p.set_partition_by(by);
    p
}

#[test]
fn key_columns_make_the_record_key_in_order_and_the_partition_input() {
    for by in [PartitionBy::Key, PartitionBy::Table] {
        let p = keyed_plan(pg(), by);
        let e = row(
            "orders",
            Op::Insert,
            r#"{"id":5,"n":"x","tenant":"acme","addr":{"c":"eu"}}"#,
        );
        let r = p.render(&e, NOW).unwrap().remove(0);
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"orders","key":{"tenant":"acme","id":5}}"#
        );
        // The whole key is the partition input, whatever `partition_by` says.
        assert_eq!(r.partition_input(), r.key, "{by:?}");
        assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
    }
}

#[test]
fn deletes_key_by_the_before_image_and_updates_by_the_after_image() {
    let p = keyed_plan(pg(), PartitionBy::Key);
    let ins = row("orders", Op::Insert, r#"{"id":5,"tenant":"acme","n":1}"#);
    let del = row("orders", Op::Delete, r#"{"tenant":"acme","id":5}"#);
    let mut upd = row("orders", Op::Update, r#"{"id":5,"tenant":"acme","n":2}"#);
    upd.before = raw(r#"{"id":4,"tenant":"old"}"#);
    let keys: Vec<String> = [ins, upd, del]
        .iter()
        .map(|e| p.render(e, NOW).unwrap().remove(0).key)
        .collect();
    assert_eq!(keys[0], keys[1], "an update keys by `after`");
    assert_eq!(keys[0], keys[2], "a delete keys by `before`");
    // So all three land on the same partition.
    let parts: Vec<usize> = keys
        .iter()
        .map(|k| crate::kafka::partition_index(k, 12))
        .collect();
    assert!(parts.iter().all(|&x| x == parts[0]));
}

#[test]
fn key_values_are_raw_json_and_missing_columns_are_null() {
    let mut p = RoutePlan::new(
        vec![keyed("public.*", "t", &["a", "missing", "b", "c", "d"])],
        None,
    )
    .unwrap();
    p.set_source(pg());
    let e = row(
        "x",
        Op::Insert,
        r#"{"d":null,"c":"é\"q","b":{"k":[1,2.50]},"a":1.0e3}"#,
    );
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"x","key":{"a":1.0e3,"missing":null,"b":{"k":[1,2.50]},"c":"é\"q","d":null}}"#
    );
    let v: serde_json::Value = serde_json::from_str(&r.key).unwrap();
    assert_eq!(v["key"]["c"], "é\"q");
}

#[test]
fn truncates_get_the_table_key() {
    let p = keyed_plan(pg(), PartitionBy::Key);
    let r = p
        .render(&row("orders", Op::Truncate, "{}"), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"orders","key":null}"#
    );
    assert_eq!(r.partition_input(), r.key);
}

#[test]
fn mongodb_keys_name_the_database_and_collection() {
    let p = keyed_plan(SourceInfo::Mongodb, PartitionBy::Table);
    let mut e = row(
        "orders",
        Op::Insert,
        r#"{"_id":{"$oid":"6530"},"tenant":"acme","id":7}"#,
    );
    e.schema = "shop".into();
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(
        r.key,
        r#"{"db":"shop","collection":"orders","key":{"tenant":"acme","id":7}}"#
    );
    assert_eq!(r.partition_input(), r.key);
}

#[test]
fn a_keyed_route_with_where_keys_what_passes() {
    let p = plan_with(vec![KafkaRoute {
        key: Some(vec!["id".into()]),
        ..filtered("o", "public.orders", Some("row.n > 1"))
    }]);
    let r = p
        .render(&row("orders", Op::Insert, r#"{"id":3,"n":2}"#), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"orders","key":{"id":3}}"#
    );
    // Dead-letter records keep the table-only key.
    let r = p
        .render(&row("orders", Op::Insert, r#"{"id":3,"n":"x"}"#), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(r.topic, DLQ);
    assert!(r.key.ends_with(r#""key":null}"#), "{}", r.key);
}

#[test]
fn key_lists_are_validated_at_startup() {
    let build = |cols: &[&str]| {
        RoutePlan::new(vec![keyed("public.*", "t", cols)], None)
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    assert_eq!(build(&["id"]), Ok(()));
    assert_eq!(build(&["tenant", "id"]), Ok(()));
    for (cols, want) in [
        (&[][..], "sink route `public.*`: `key` is empty"),
        (&["a.b"][..], "column `a.b` is a dotted path"),
        (&["id", "x", "id"][..], "names column `id` twice"),
        (&[""][..], "has an empty column name"),
    ] {
        let err = build(cols).expect_err(want);
        assert!(err.contains(want), "{err} should contain {want}");
    }
}

#[test]
fn key_columns_are_exposed_for_field_operation_checks() {
    let p = keyed_plan(pg(), PartitionBy::Key);
    assert_eq!(
        p.routes[0].key_columns(),
        Some(&["tenant".to_string(), "id".to_string()][..])
    );
    let p = RoutePlan::new(vec![route("a.*", "t")], None).unwrap();
    assert_eq!(p.routes[0].key_columns(), None);
}

#[test]
fn unseen_key_columns_are_reported_on_routes_without_where() {
    let p = RoutePlan::new(vec![keyed("public.*", "t", &["id", "tenant_id"])], None).unwrap();
    let ev = row("orders", Op::Insert, r#"{"id":1,"tenant":"acme"}"#);
    for _ in 0..UNSEEN_FIELD_ROWS {
        p.render(&ev, NOW).unwrap();
    }
    assert_eq!(p.stats().snapshot()[0].unseen_fields, vec!["row.tenant_id"]);
    // Truncates have no row and never count.
    let q = RoutePlan::new(vec![keyed("public.*", "t", &["id"])], None).unwrap();
    for _ in 0..UNSEEN_FIELD_ROWS {
        q.render(&row("orders", Op::Truncate, "{}"), NOW).unwrap();
    }
    assert!(q.stats().snapshot()[0].unseen_fields.is_empty());
}

// --- content routing (R4) ---------------------------------------------------------------------

fn content(name: &str, topic: &str, fallback: Option<&str>, values: Option<&[&str]>) -> KafkaRoute {
    KafkaRoute {
        topic_fallback: fallback.map(Into::into),
        topic_values: values.map(|v| v.iter().map(|s| s.to_string()).collect()),
        ..named(name, "public.orders", topic)
    }
}

fn content_plan(values: Option<&[&str]>, cap: usize) -> RoutePlan {
    let mut p = RoutePlan::new(
        vec![content(
            "o",
            "orders.{row.region}",
            Some("orders.unknown"),
            values,
        )],
        Some("rest".into()),
    )
    .unwrap();
    p.set_source(pg());
    p.set_max_dynamic_topics(cap);
    p
}

#[test]
fn content_routing_picks_the_topic_from_the_row_or_falls_back() {
    let p = content_plan(None, 100);
    let long = "a".repeat(MAX_SEGMENT);
    let too_long = "a".repeat(MAX_SEGMENT + 1);
    let cases: Vec<(Op, String, &str)> = vec![
        (Op::Insert, r#"{"region":"eu"}"#.into(), "orders.eu"),
        (
            Op::Update,
            r#"{"region":"us-east_1"}"#.into(),
            "orders.us-east_1",
        ),
        // `row` is the before-image on a delete
        (Op::Delete, r#"{"region":"apac"}"#.into(), "orders.apac"),
        (Op::Insert, r#"{"region":7}"#.into(), "orders.7"),
        (Op::Insert, r#"{"region":true}"#.into(), "orders.true"),
        (
            Op::Insert,
            format!(r#"{{"region":"{long}"}}"#),
            "orders.aaaa",
        ),
        // null, missing, no row at all
        (Op::Insert, r#"{"region":null}"#.into(), "orders.unknown"),
        (Op::Insert, r#"{"id":1}"#.into(), "orders.unknown"),
        (Op::Truncate, "{}".into(), "orders.unknown"),
        // not one segment of [A-Za-z0-9_-]{1,64}
        (
            Op::Insert,
            r#"{"region":"x.payments"}"#.into(),
            "orders.unknown",
        ),
        (Op::Insert, r#"{"region":""}"#.into(), "orders.unknown"),
        (Op::Insert, r#"{"region":"e u"}"#.into(), "orders.unknown"),
        (Op::Insert, r#"{"region":"../x"}"#.into(), "orders.unknown"),
        (Op::Insert, r#"{"region":1.5}"#.into(), "orders.unknown"),
        (Op::Insert, r#"{"region":"é"}"#.into(), "orders.unknown"),
        (
            Op::Insert,
            format!(r#"{{"region":"{too_long}"}}"#),
            "orders.unknown",
        ),
        (Op::Insert, r#"{"region":{"a":1}}"#.into(), "orders.unknown"),
    ];
    for (op, json, want) in &cases {
        let e = row("orders", *op, json);
        let r = p.render(&e, NOW).unwrap().remove(0);
        if *want == "orders.aaaa" {
            assert_eq!(r.topic, format!("orders.{long}"));
        } else {
            assert_eq!(r.topic, *want, "{op:?} {json}");
        }
        // Topic aside, the record is the same: key, value, headers.
        assert_eq!(r.key, record_key(&e, &pg()));
        assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
        assert!(r.headers.is_empty());
    }
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (17, 6, 11));
    // Tables no content route claims are unaffected.
    assert_eq!(
        outcome(&p, &row("users", Op::Insert, r#"{"region":"eu"}"#)),
        "rest"
    );
}

#[test]
fn topic_values_allow_list() {
    let p = content_plan(Some(&["eu", "us"]), 100);
    for (json, want) in [
        (r#"{"region":"eu"}"#, "orders.eu"),
        (r#"{"region":"us"}"#, "orders.us"),
        (r#"{"region":"de"}"#, "orders.unknown"),
        (r#"{"region":"EU"}"#, "orders.unknown"),
    ] {
        assert_eq!(
            outcome(&p, &row("orders", Op::Insert, json)),
            want,
            "{json}"
        );
    }
}

#[test]
fn every_substituted_row_value_is_checked_but_schema_and_table_are_not() {
    let mut p = RoutePlan::new(
        vec![KafkaRoute {
            topic_fallback: Some("fb".into()),
            ..route("*.*", "cdc.{schema}.{table}.{row.a}.{after.b}")
        }],
        None,
    )
    .unwrap();
    p.set_source(pg());
    let r = |json: &str| outcome(&p, &row("orders", Op::Insert, json));
    assert_eq!(r(r#"{"a":"x","b":"y"}"#), "cdc.public.orders.x.y");
    assert_eq!(r(r#"{"a":"x","b":"y.z"}"#), "fb");
    assert_eq!(r(r#"{"a":"x"}"#), "fb");
}

#[test]
fn the_dynamic_topic_cap_sends_new_topics_to_the_fallback() {
    let p = content_plan(None, 2);
    let r = |region: &str| {
        outcome(
            &p,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("a"), "orders.a");
    assert_eq!(r("b"), "orders.b");
    assert_eq!(r("c"), "orders.unknown", "a third topic is over the cap");
    assert_eq!(r("a"), "orders.a", "known topics stay usable");
    assert_eq!(p.dynamic_topic_count(), 2);
    // A clone (the admin server's copy) shares the count.
    assert_eq!(p.clone().dynamic_topic_count(), 2);
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.produced, c.fallback), (3, 1));
}

#[test]
fn the_cap_counts_topics_already_on_the_broker() {
    let p = content_plan(None, 2);
    let n = p.add_existing_topics([
        "orders.a",
        "orders.b",
        // its own fallback is a fixed topic, not a content-routed one
        "orders.unknown",
        "rest",
        "other.x",
    ]);
    assert_eq!(n, 2);
    let r = |region: &str| {
        outcome(
            &p,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("c"), "orders.unknown");
    assert_eq!(r("b"), "orders.b");
    // Topics above the cap that exist already stay usable (a lowered cap never strands them).
    let q = content_plan(None, 1);
    assert_eq!(q.add_existing_topics(["orders.a", "orders.b"]), 2);
    let r = |region: &str| {
        outcome(
            &q,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("b"), "orders.b");
    assert_eq!(r("c"), "orders.unknown");
    // Without content routing nothing is counted.
    let s = RoutePlan::new(vec![route("public.*", "orders.{table}")], None).unwrap();
    assert_eq!(s.add_existing_topics(["orders.a"]), 0);
}

#[test]
fn op_and_table_templates_need_no_fallback() {
    let mut p = RoutePlan::new(
        vec![route("public.*", "cdc.{table}.{op}")],
        Some("all.{op}".into()),
    )
    .unwrap();
    p.set_source(pg());
    assert_eq!(
        outcome(&p, &row("orders", Op::Delete, r#"{"id":1}"#)),
        "cdc.orders.delete"
    );
    let mut e = row("orders", Op::Insert, r#"{"id":1}"#);
    e.schema = "sales".into();
    assert_eq!(outcome(&p, &e), "all.insert");
    assert_eq!(p.topic_for("public", "x").as_deref(), Some("cdc.x.{op}"));
}

#[test]
fn content_routing_is_validated_at_startup() {
    let build = |routes: Vec<KafkaRoute>, default: Option<&str>, dlq: Option<&str>| {
        RoutePlan::with_dead_letter(routes, default.map(Into::into), dlq.map(Into::into))
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    let ok = content("o", "orders.{row.region}", Some("orders.unknown"), None);
    assert_eq!(build(vec![ok.clone()], Some("rest"), Some("dlq")), Ok(()));
    let c =
        |topic: &str, fb: Option<&str>, values: Option<&[&str]>| content("o", topic, fb, values);
    type Case<'a> = (Vec<KafkaRoute>, Option<&'a str>, Option<&'a str>, &'a str);
    let cases: Vec<Case> = vec![
        // prefix: fixed, non-empty, no leading `_`
        (
            vec![c("{row.tenant}", Some("fb"), None)],
            None,
            None,
            "must start with fixed text",
        ),
        (
            vec![c("{schema}.{row.tenant}", Some("fb"), None)],
            None,
            None,
            "must start with fixed text",
        ),
        (
            vec![c("_x.{row.r}", Some("fb"), None)],
            None,
            None,
            "must not start with `_`",
        ),
        (
            vec![c("orders {row.r}", Some("fb"), None)],
            None,
            None,
            "not a valid Kafka topic name",
        ),
        (
            vec![c("orders.{nope}", Some("fb"), None)],
            None,
            None,
            "not a valid template",
        ),
        (
            vec![c("orders.{row.r", Some("fb"), None)],
            None,
            None,
            "not a valid template",
        ),
        // fallback: required, fixed, valid, no leading `_`
        (
            vec![c("orders.{row.r}", None, None)],
            None,
            None,
            "needs `topic_fallback`",
        ),
        (
            vec![c("orders.{row.r}", Some("fb.{table}"), None)],
            None,
            None,
            "`topic_fallback` `fb.{table}` is not a valid",
        ),
        (
            vec![c("orders.{row.r}", Some("bad topic"), None)],
            None,
            None,
            "is not a valid Kafka topic name",
        ),
        (
            vec![c("orders.{row.r}", Some("_fb"), None)],
            None,
            None,
            "`topic_fallback` `_fb` must not start with `_`",
        ),
        // only content-routed topics take topic_fallback / topic_values
        (
            vec![c("orders", Some("fb"), None)],
            None,
            None,
            "`topic_fallback` is only for a `topic` that reads a row field",
        ),
        (
            vec![c("orders.{table}", None, Some(&["eu"]))],
            None,
            None,
            "`topic_values` is only for",
        ),
        // topic_values: non-empty, each one segment
        (
            vec![c("orders.{row.r}", Some("fb"), Some(&[]))],
            None,
            None,
            "`topic_values` is empty",
        ),
        (
            vec![c("orders.{row.r}", Some("fb"), Some(&["eu", "x.y"]))],
            None,
            None,
            "entry `x.y` can never be used",
        ),
        (
            vec![c("orders.{row.r}", Some("fb"), Some(&["eu", ""]))],
            None,
            None,
            "entry `` can never be used",
        ),
        // collisions with fixed topics: another route, sink.topic, another fallback, the DLQ
        (
            vec![ok.clone(), named("eu", "public.eu", "orders.eu")],
            None,
            None,
            "could produce `orders.eu`, which is route `eu`'s `topic`",
        ),
        (
            vec![named("eu", "public.eu", "orders.eu"), ok.clone()],
            None,
            None,
            "could produce `orders.eu`",
        ),
        (
            vec![ok.clone()],
            Some("orders.all"),
            None,
            "could produce `orders.all`, which is sink.topic",
        ),
        (
            vec![
                ok.clone(),
                KafkaRoute {
                    topic_fallback: Some("orders.other".into()),
                    ..named("p", "public.p", "p.{row.x}")
                },
            ],
            None,
            None,
            "could produce `orders.other`, which is route `p`'s `topic_fallback`",
        ),
        (
            vec![ok.clone()],
            None,
            Some("orders.eu"),
            "sink.dead_letter_topic `orders.eu` is also a topic events are routed to",
        ),
        (
            vec![ok.clone()],
            None,
            Some("orders.unknown"),
            "sink.dead_letter_topic `orders.unknown` is also a topic events are routed to",
        ),
        (
            vec![c("p.{row.r}", Some("fb"), None)],
            None,
            Some("fb"),
            "sink.dead_letter_topic `fb` is also a route's `topic_fallback`",
        ),
        // a row-reading sink.topic has nowhere to fall back to
        (
            vec![],
            Some("cdc.{row.r}"),
            None,
            "sink.topic cannot read a row field",
        ),
        // with content routing, no topic starts with `_`
        (
            vec![ok.clone(), route("public.*", "_internal")],
            None,
            None,
            "sink route `public.*`: `topic` `_internal` must not start with `_`",
        ),
        (
            vec![ok.clone()],
            Some("__consumer_offsets"),
            None,
            "sink.topic `__consumer_offsets` must not start with `_`",
        ),
        (
            vec![ok.clone()],
            Some("_cdc.{table}"),
            None,
            "sink.topic `_cdc.{table}` must not start with `_`",
        ),
        (
            vec![route("public.*", "bad topic")],
            None,
            None,
            "not a valid Kafka topic name",
        ),
    ];
    for (routes, default, dlq, want) in cases {
        let got = build(routes, default, dlq);
        let err = got.expect_err(want);
        assert!(err.contains(want), "{err}\nshould contain: {want}");
    }
    // Without content routing, `_`-prefixed topics are accepted as before.
    assert_eq!(
        build(
            vec![
                route("public.*", "_internal"),
                route("hr.*", "_hr.{table}.{op}")
            ],
            Some("__cdc.{schema}"),
            None
        ),
        Ok(())
    );
    // A route's own fallback may match its own template, and `{schema}`/`{table}` templates
    // with other fixed text are fine.
    assert_eq!(
        build(
            vec![ok.clone(), route("public.*", "cdc.{table}")],
            Some("all.{schema}.{table}"),
            None
        ),
        Ok(())
    );
    // A content template may not overlap a `{schema}`/`{table}`/`{op}` template: a row value
    // could write into that route's topics (`cdc.{row.kind}` = `customers` -> `cdc.customers`).
    for (routes, default, want) in [
        (
            vec![
                content("o", "cdc.{row.kind}", Some("fb"), None),
                route("public.*", "cdc.{table}"),
            ],
            None,
            "overlaps route `public.*`'s `topic` `cdc.{table}`",
        ),
        (
            vec![
                route("public.*", "cdc.{table}"),
                content("o", "cdc.{row.kind}", Some("fb"), None),
            ],
            None,
            "overlaps route `public.*`'s `topic` `cdc.{table}`",
        ),
        (
            vec![ok.clone()],
            Some("orders.{schema}.{table}"),
            "overlaps sink.topic `orders.{schema}.{table}`",
        ),
        (
            vec![ok.clone(), route("public.*", "ord{table}")],
            None,
            "overlaps route `public.*`'s `topic` `ord{table}`",
        ),
        (
            vec![ok.clone(), route("public.*", "x.{op}")],
            Some("{table}"),
            "overlaps sink.topic `{table}`",
        ),
    ] {
        let err = build(routes, default, None).expect_err(want);
        assert!(err.contains(want), "{err}\nshould contain: {want}");
    }
}

#[test]
fn glob_intersection() {
    for (a, b, want) in [
        ("orders.*", "orders.*", true),
        ("orders.*", "cdc.*", false),
        ("orders.*", "ord*", true),
        ("orders.*", "*", true),
        ("orders.*", "*.eu", true),
        ("orders.*.x", "orders.y.*", true),
        ("a*b", "a*c", false),
        ("orders.unknown", "orders.*", true),
        ("x", "x", true),
        ("x", "y", false),
        ("*a", "b*", true),
    ] {
        assert_eq!(globs_intersect(a, b), want, "{a} {b}");
        assert_eq!(globs_intersect(b, a), want, "{b} {a}");
    }
}

#[test]
fn a_name_kafka_would_refuse_as_a_collision_goes_to_the_fallback() {
    // Two placeholders: `a_b` + `c` and `a` + `b_c` render names equal up to `.` vs `_`.
    let mut p = RoutePlan::new(
        vec![content(
            "e",
            "events.{row.tenant}.{row.kind}",
            Some("events-fallback"),
            None,
        )],
        None,
    )
    .unwrap();
    p.set_source(pg());
    let r = |tenant: &str, kind: &str| {
        let json = format!(r#"{{"tenant":"{tenant}","kind":"{kind}"}}"#);
        outcome(&p, &row("orders", Op::Insert, &json))
    };
    assert_eq!(r("a_b", "c"), "events.a_b.c");
    assert_eq!(r("a", "b_c"), "events-fallback");
    assert_eq!(
        r("a_b", "c"),
        "events.a_b.c",
        "the first spelling stays usable"
    );
    assert_eq!(
        p.dynamic_topic_count(),
        1,
        "a refused name holds no cap slot"
    );
    // A broker topic that only differs by `.` vs `_`, even one no template produces.
    let q = content_plan(None, 100);
    q.add_existing_topics(["orders_eu", "unrelated"]);
    assert_eq!(q.dynamic_topic_count(), 0);
    let r = |region: &str| {
        outcome(
            &q,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("eu"), "orders.unknown");
    assert_eq!(r("us"), "orders.us");
    // The same against a fixed topic of the config.
    let s = RoutePlan::new(
        vec![
            content("o", "orders.{row.region}", Some("orders.unknown"), None),
            route("hr.*", "orders_us"),
        ],
        None,
    )
    .unwrap();
    assert_eq!(
        outcome(&s, &row("orders", Op::Insert, r#"{"region":"us"}"#)),
        "orders.unknown"
    );
}

#[test]
fn a_value_naming_the_fallback_uses_no_cap_slot() {
    let p = content_plan(None, 1);
    let r = |region: &str| {
        outcome(
            &p,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("unknown"), "orders.unknown");
    assert_eq!(p.dynamic_topic_count(), 0);
    assert_eq!(p.stats().snapshot()[0].counts.fallback, 1);
    assert_eq!(r("eu"), "orders.eu", "the one slot is still free");
}

#[test]
fn only_topics_a_row_could_produce_count_against_the_cap() {
    let long = format!("orders.{}", "a".repeat(MAX_SEGMENT + 1));
    let p = content_plan(None, 100);
    let n = p.add_existing_topics([
        "orders.a",
        "orders.a.b",
        long.as_str(),
        "orders.",
        "orders.é",
    ]);
    assert_eq!(n, 1, "only orders.a is one valid segment");
    let q = content_plan(Some(&["eu"]), 100);
    assert_eq!(
        q.add_existing_topics(["orders.eu", "orders.us"]),
        1,
        "topic_values"
    );
    // Topics a `{schema}`/`{table}` template writes never count.
    let s = RoutePlan::new(
        vec![
            content("o", "orders.{row.region}", Some("fb"), None),
            route("public.*", "cdc.{table}"),
        ],
        None,
    )
    .unwrap();
    assert_eq!(s.add_existing_topics(["cdc.customers", "orders.eu"]), 1);
}

#[test]
fn a_refused_or_failed_create_frees_the_cap_slot() {
    let p = content_plan(None, 1);
    let mut out = p
        .render(&row("orders", Op::Insert, r#"{"region":"eu"}"#), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(out.topic, "orders.eu");
    assert_eq!(
        out.content,
        Some(ContentTopic {
            route: 0,
            fallback: "orders.unknown".into(),
            tombstone: false,
        })
    );
    assert_eq!(p.dynamic_topic_count(), 1);
    // The broker refuses it for good: the record goes to the fallback, counted there.
    p.topic_refused(&mut out, "PolicyViolation");
    assert_eq!(
        (out.topic.as_str(), out.content.as_ref()),
        ("orders.unknown", None)
    );
    assert_eq!(p.dynamic_topic_count(), 0);
    // The fallback record is acknowledged.
    p.commit_counts();
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (1, 0, 1));
    // Its op leaves the produced breakdown with it.
    assert_eq!(c.ops.total(), 0);
    // A passing failure frees the slot too; the batch is retried.
    let us = p
        .render(&row("orders", Op::Insert, r#"{"region":"us"}"#), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(us.topic, "orders.us");
    p.topic_failed("orders.us");
    assert_eq!(p.dynamic_topic_count(), 0);
    // An opened topic is never released, and blocks other spellings.
    p.topic_opened("orders.ap");
    p.topic_failed("orders.ap");
    assert_eq!(
        outcome(&p, &row("orders", Op::Insert, r#"{"region":"ap"}"#)),
        "orders.ap"
    );
    // Records of other routes, and fallbacks, are not content-routed.
    let rest = p
        .render(&row("users", Op::Insert, "{}"), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(rest.content, None);
}

#[test]
fn a_refused_name_goes_to_the_fallback_at_render_time_from_then_on() {
    let p = content_plan(None, 100);
    let eu = || row("orders", Op::Insert, r#"{"region":"eu"}"#);
    let mut out = p.render(&eu(), NOW).unwrap().remove(0);
    assert!(out.content.is_some());
    p.topic_refused(&mut out, "TopicAuthorizationFailed");
    // Every later `eu` row: the fallback, with no content marker, so the sink never tries
    // to create the topic again.
    for _ in 0..3 {
        let r = p.render(&eu(), NOW).unwrap().remove(0);
        assert_eq!(
            (r.topic.as_str(), r.content.as_ref()),
            ("orders.unknown", None)
        );
    }
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (4, 0, 4));
    assert_eq!(p.dynamic_topic_count(), 0);
    // Other values are unaffected.
    assert_eq!(
        outcome(&p, &row("orders", Op::Insert, r#"{"region":"us"}"#)),
        "orders.us"
    );
    // The memory is bounded: the oldest refused name is forgotten first.
    let d = DynamicTopics::new();
    for i in 0..=MAX_REFUSED_TOPICS {
        d.refuse(&format!("orders.t{i}"));
    }
    assert_eq!(d.admit("orders.t0"), Admit::Yes, "the oldest was dropped");
    assert_eq!(d.admit("orders.t1"), Admit::Refused);
    assert_eq!(
        d.admit(&format!("orders.t{MAX_REFUSED_TOPICS}")),
        Admit::Refused
    );
}

#[test]
fn a_zero_cap_allows_only_existing_topics() {
    let p = content_plan(None, 0);
    p.add_existing_topics(["orders.eu"]);
    let r = |region: &str| {
        outcome(
            &p,
            &row("orders", Op::Insert, &format!(r#"{{"region":"{region}"}}"#)),
        )
    };
    assert_eq!(r("eu"), "orders.eu");
    assert_eq!(r("us"), "orders.unknown");
}

#[test]
fn content_routes_are_described_inspected_and_watched() {
    let p = content_plan(Some(&["eu", "us"]), 100);
    // The content route's dead-letter topic is its fallback's: `orders.*` cannot render it.
    assert_eq!(
        p.topic_globs(),
        vec![
            "orders.*",
            "rest",
            "orders.unknown",
            "orders.unknown.dlq",
            "rest.dlq"
        ]
    );
    assert_eq!(
        p.topic_for("public", "orders").as_deref(),
        Some("orders.{row.region}")
    );
    let d = serde_json::to_value(p.describe()).unwrap();
    assert_eq!(d["routes"][0]["topic"], "orders.{row.region}");
    assert_eq!(d["routes"][0]["topic_fallback"], "orders.unknown");
    assert_eq!(
        d["routes"][0]["topic_values"],
        serde_json::json!(["eu", "us"])
    );
    assert_eq!(d["routes"][1]["topic_fallback"], serde_json::Value::Null);
    // For R6: the template's row fields and the allow-list.
    let fields: Vec<(&str, Option<&str>)> = p.routes[0]
        .topic_fields()
        .map(|f| (f.as_str(), f.column()))
        .collect();
    assert_eq!(fields, [("row.region", Some("region"))]);
    assert_eq!(
        p.routes[0].topic_values(),
        Some(&["eu".to_string(), "us".to_string()][..])
    );
    // A misspelled template field is reported like a `where` field.
    let q = RoutePlan::new(
        vec![content(
            "o",
            "orders.{row.regoin}",
            Some("orders.unknown"),
            None,
        )],
        None,
    )
    .unwrap();
    for _ in 0..UNSEEN_FIELD_ROWS {
        q.render(&row("orders", Op::Insert, r#"{"region":"eu"}"#), NOW)
            .unwrap();
    }
    assert_eq!(q.stats().snapshot()[0].unseen_fields, vec!["row.regoin"]);
}

// --- outbox (R8) -----------------------------------------------------------------------------

fn outbox_spec() -> OutboxSpec {
    OutboxSpec {
        topic: "outbox.{aggregate_type}".into(),
        id: Some("id".into()),
        kind: Some("type".into()),
        ..Default::default()
    }
}

fn outbox_route(spec: OutboxSpec) -> KafkaRoute {
    KafkaRoute {
        name: Some("ob".into()),
        pattern: "public.outbox".into(),
        topic_fallback: Some("outbox.unknown".into()),
        outbox: Some(spec),
        ..Default::default()
    }
}

fn outbox_plan() -> RoutePlan {
    plan_with(vec![outbox_route(outbox_spec())])
}

const OUTBOX_ROW: &str = r#"{"id":"0f6e","aggregatetype":"order","aggregateid":"A-1","type":"OrderCreated","payload":{"total":12.50,"items":[1,2]}}"#;

#[test]
fn outbox_publishes_inserts_only() {
    let p = outbox_plan();
    assert_eq!(
        outcome(&p, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "outbox.order"
    );
    // Updates, deletes, truncates and, without a `where`, snapshot reads are filtered.
    for op in [Op::Update, Op::Delete, Op::Truncate, Op::Read] {
        assert_eq!(
            outcome(&p, &row("outbox", op, OUTBOX_ROW)),
            "filtered",
            "{op:?}"
        );
    }
    // Even a row that would be dead-lettered as an insert.
    assert_eq!(outcome(&p, &row("outbox", Op::Update, "{}")), "filtered");
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.filtered), (6, 1, 5));
}

#[test]
fn outbox_publishes_snapshot_reads_only_when_where_is_true_for_them() {
    let with_where = |w: &str| {
        plan_with(vec![KafkaRoute {
            filter: Some(w.into()),
            ..outbox_route(outbox_spec())
        }])
    };
    let p = with_where("op in ['insert', 'read', 'update', 'delete', 'truncate']");
    // A snapshot read is published like an insert, with the `r` op header.
    let r = p
        .render(&row("outbox", Op::Read, OUTBOX_ROW), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(
        (r.topic.as_str(), r.key.as_str(), r.op),
        ("outbox.order", "A-1", "r")
    );
    assert_eq!(r.value.text(), r#"{"total":12.50,"items":[1,2]}"#);
    assert_eq!(
        outcome(&p, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "outbox.order"
    );
    // A `where` true for them never publishes updates, deletes or truncates.
    for op in [Op::Update, Op::Delete, Op::Truncate] {
        assert_eq!(
            outcome(&p, &row("outbox", op, OUTBOX_ROW)),
            "filtered",
            "{op:?}"
        );
    }
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.filtered), (5, 2, 3));
    // `op == 'read'` publishes snapshot reads only.
    let q = with_where("op == 'read'");
    assert_eq!(
        outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
        "outbox.order"
    );
    assert_eq!(
        outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "filtered"
    );
    // A `where` false for reads filters them; so does no `where` (`outbox_plan`).
    let q = with_where("op == 'insert'");
    assert_eq!(
        outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
        "filtered"
    );
    assert_eq!(
        outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "outbox.order"
    );
    assert_eq!(
        outcome(&outbox_plan(), &row("outbox", Op::Read, OUTBOX_ROW)),
        "filtered"
    );
    // A row `where` applies to reads and inserts alike.
    let q = with_where("row.aggregatetype == 'order'");
    assert_eq!(
        outcome(&q, &row("outbox", Op::Read, OUTBOX_ROW)),
        "outbox.order"
    );
    let other = OUTBOX_ROW.replace(r#""order""#, r#""invoice""#);
    assert_eq!(outcome(&q, &row("outbox", Op::Read, &other)), "filtered");
    assert_eq!(outcome(&q, &row("outbox", Op::Insert, &other)), "filtered");
    // A snapshot read without an aggregate id goes to the dead-letter topic too.
    assert_eq!(
        outcome(&p, &row("outbox", Op::Read, r#"{"payload":"p"}"#)),
        "dlq"
    );
}

#[test]
fn outbox_template_errors_point_into_the_template_as_written() {
    assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 1), 1);
    // Inside the replacement: the placeholder's start.
    assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 10), 2);
    // After it: shifted back by the length difference (24 - 16).
    assert_eq!(written_pos("a.{aggregate_type}.{x", 24, 27), 19);
    assert_eq!(
        written_pos("{aggregate_type}.{aggregate_type}.}", 9, 20),
        34
    );
    let err = RoutePlan::with_dead_letter(
        vec![outbox_route(OutboxSpec {
            topic: "outbox.{aggregate_type}.{x".into(),
            ..outbox_spec()
        })],
        None,
        Some(DLQ.into()),
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    assert!(
        err.contains(
            "`outbox.{aggregate_type}.{x` is not a valid template: unclosed `{` (at byte 24)"
        ),
        "{err}"
    );
}

#[test]
fn outbox_key_is_the_raw_aggregate_id_and_value_the_raw_payload() {
    let p = outbox_plan();
    let r = p
        .render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
        .unwrap()
        .remove(0);
    assert_eq!(r.topic, "outbox.order");
    // A string's content, not the JSON key of the table.
    assert_eq!(r.key, "A-1");
    assert_eq!(r.partition_input(), r.key);
    // A `json`/`jsonb` payload is the JSON text as the row holds it.
    assert_eq!(r.value.text(), r#"{"total":12.50,"items":[1,2]}"#);
    assert_eq!((r.op, r.lsn.as_str()), ("c", "7"));

    // A text payload is its string content, unescaped; a numeric aggregate id its JSON text.
    let r = p
        .render(
            &row(
                "outbox",
                Op::Insert,
                r#"{"aggregatetype":"order","aggregateid":42,"payload":"say \"hi\"\n é"}"#,
            ),
            NOW,
        )
        .unwrap()
        .remove(0);
    assert_eq!(r.key, "42");
    assert_eq!(r.value.text(), "say \"hi\"\n é");
    // An escaped string aggregate id is unescaped too; other values keep their JSON text.
    for (id, key) in [
        (r#""a\"bé""#, "a\"bé"),
        ("1.0e3", "1.0e3"),
        ("true", "true"),
        (r#"{"$oid":"6530"}"#, r#"{"$oid":"6530"}"#),
    ] {
        let json = format!(r#"{{"aggregatetype":"o","aggregateid":{id},"payload":"{{}}"}}"#);
        let r = p
            .render(&row("outbox", Op::Insert, &json), NOW)
            .unwrap()
            .remove(0);
        assert_eq!(r.key, key, "{id}");
        assert_eq!(r.value.text(), "{}");
    }
}

#[test]
fn outbox_headers_are_id_and_type_when_configured_and_present() {
    let p = outbox_plan();
    let headers = |json: &str| {
        p.render(&row("outbox", Op::Insert, json), NOW)
            .unwrap()
            .remove(0)
            .headers
    };
    assert_eq!(
        headers(OUTBOX_ROW),
        vec![
            ("id", "0f6e".to_string()),
            ("type", "OrderCreated".to_string())
        ]
    );
    // A null or missing header column leaves the header out.
    assert_eq!(
        headers(r#"{"aggregatetype":"o","aggregateid":"a","payload":"p","id":7,"type":null}"#),
        vec![("id", "7".to_string())]
    );
    // Without `id` and `type` in the spec there are no extra headers.
    let q = plan_with(vec![outbox_route(OutboxSpec {
        topic: "outbox.{aggregate_type}".into(),
        ..Default::default()
    })]);
    let r = q
        .render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
        .unwrap()
        .remove(0);
    assert!(r.headers.is_empty());
    // The sink adds `op` and `lsn` to every record.
    assert_eq!((r.op, r.lsn.as_str()), ("c", "7"));
}

#[test]
fn outbox_rows_without_aggregate_id_or_payload_go_to_the_dead_letter_topic() {
    let p = outbox_plan();
    let cases = [
        (
            r#"{"aggregatetype":"o","payload":"secret"}"#,
            "`aggregate_id` column `aggregateid` is missing or null",
        ),
        (
            r#"{"aggregatetype":"o","aggregateid":null,"payload":"secret"}"#,
            "`aggregate_id` column `aggregateid` is missing or null",
        ),
        (
            r#"{"aggregatetype":"o","aggregateid":"secret"}"#,
            "`payload` column `payload` is missing or null",
        ),
        (
            r#"{"aggregatetype":"o","aggregateid":"secret","payload":null}"#,
            "`payload` column `payload` is missing or null",
        ),
        (
            r#"{"aggregatetype":"o","aggregateid":"\ud800","payload":"secret"}"#,
            "`aggregate_id` column `aggregateid` is a string that is not valid Unicode",
        ),
        (
            r#"{"aggregatetype":"o","aggregateid":"secret","payload":"p","type":"\udc00"}"#,
            "`type` column `type` is a string that is not valid Unicode",
        ),
    ];
    for (json, why) in cases {
        let e = row("outbox", Op::Insert, json);
        let out = p.render(&e, NOW).unwrap();
        assert_eq!(out.len(), 1, "{json}");
        let r = &out[0];
        assert_eq!(r.topic, DLQ, "{json}");
        assert_eq!(
            r.key,
            r#"{"db":"app","schema":"public","table":"outbox","key":null}"#
        );
        assert_eq!(r.value.text(), encode(&e, &pg(), NOW).unwrap());
        assert_eq!(r.headers.len(), 1);
        assert_eq!(r.headers[0].0, "aqueduct-error");
        assert_eq!(r.headers[0].1, format!("route `ob`: outbox {why}"));
        assert!(!r.headers[0].1.contains("secret"));
    }
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.errors, c.produced), (6, 6, 0));
    // A dead-lettered row takes no content-routed topic slot.
    assert_eq!(p.dynamic_topic_count(), 0);
}

#[test]
fn outbox_topic_follows_the_content_routing_rules_and_falls_back() {
    let mut p = plan_with(vec![KafkaRoute {
        topic_values: Some(vec!["order".into(), "invoice".into(), "late".into()]),
        ..outbox_route(outbox_spec())
    }]);
    p.set_max_dynamic_topics(2);
    let r = |agg: &str| {
        outcome(
            &p,
            &row(
                "outbox",
                Op::Insert,
                &format!(r#"{{"aggregatetype":{agg},"aggregateid":"a","payload":"p"}}"#),
            ),
        )
    };
    assert_eq!(r(r#""order""#), "outbox.order");
    assert_eq!(r(r#""invoice""#), "outbox.invoice");
    for bad in [
        r#"null"#,
        r#""x.y""#,
        r#""shipment""#,
        r#""""#,
        r#"{"a":1}"#,
    ] {
        assert_eq!(r(bad), "outbox.unknown", "{bad}");
    }
    assert_eq!(r(r#""late""#), "outbox.unknown", "over max_dynamic_topics");
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.produced, c.fallback), (2, 6));
    // A fallback record is still the outbox record: same key, value and headers.
    let rec = p
        .render(
            &row(
                "outbox",
                Op::Insert,
                r#"{"aggregatetype":null,"aggregateid":"a","payload":"p","id":"e1"}"#,
            ),
            NOW,
        )
        .unwrap()
        .remove(0);
    assert_eq!(
        (rec.topic.as_str(), rec.key.as_str(), rec.value.text()),
        ("outbox.unknown", "a", "p")
    );
    assert_eq!(rec.headers, vec![("id", "e1".to_string())]);

    // A template without row fields needs no fallback; `{schema}`, `{table}` and
    // `{after.<col>}` work too.
    let q = plan_with(vec![KafkaRoute {
        topic_fallback: None,
        ..outbox_route(OutboxSpec {
            topic: "events.{table}".into(),
            ..outbox_spec()
        })
    }]);
    assert_eq!(
        outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "events.outbox"
    );
    let q = plan_with(vec![outbox_route(OutboxSpec {
        topic: "ev.{schema}.{after.type}".into(),
        ..outbox_spec()
    })]);
    assert_eq!(
        outcome(&q, &row("outbox", Op::Insert, OUTBOX_ROW)),
        "ev.public.OrderCreated"
    );
}

#[test]
fn outbox_routes_refuse_every_field_operation() {
    let specs: Vec<(&str, FieldSpec)> = vec![
        (
            "include",
            FieldSpec {
                include: Some(strs(&["payload"])),
                ..Default::default()
            },
        ),
        (
            "exclude",
            FieldSpec {
                exclude: Some(strs(&["secret"])),
                ..Default::default()
            },
        ),
        // An empty `exclude` is still a field key on an outbox route.
        (
            "exclude",
            FieldSpec {
                exclude: Some(vec![]),
                ..Default::default()
            },
        ),
        (
            "mask",
            FieldSpec {
                mask: strs(&["aggregateid"]),
                ..Default::default()
            },
        ),
        (
            "hash",
            FieldSpec {
                hash: strs(&["aggregateid"]),
                ..Default::default()
            },
        ),
        (
            "rename",
            FieldSpec {
                rename: [("payload".to_string(), "body".to_string())].into(),
                ..Default::default()
            },
        ),
    ];
    for (key, fields) in specs {
        let err = RoutePlan::build(
            vec![KafkaRoute {
                fields,
                ..outbox_route(outbox_spec())
            }],
            None,
            Some(DLQ.into()),
            Some(hash_key()),
        )
        .map(|_| ())
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!(
                "sink route `ob`: `outbox` and the field operations (`{key}`) are mutually exclusive"
            )),
            "{key}: {err}"
        );
    }
    // Several at once are all named.
    let err = RoutePlan::build(
        vec![KafkaRoute {
            fields: FieldSpec {
                mask: strs(&["a"]),
                hash: strs(&["b"]),
                ..Default::default()
            },
            ..outbox_route(outbox_spec())
        }],
        None,
        Some(DLQ.into()),
        Some(hash_key()),
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    assert!(err.contains("(`mask`, `hash`)"), "{err}");
}

#[test]
fn outbox_routes_are_validated_at_startup() {
    let build = |r: KafkaRoute, dlq: Option<&str>| {
        RoutePlan::with_dead_letter(vec![r], Some("rest".into()), dlq.map(Into::into))
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    let ok = outbox_route(outbox_spec());
    assert_eq!(build(ok.clone(), Some(DLQ)), Ok(()));
    let spec = |f: fn(&mut OutboxSpec)| {
        let mut s = outbox_spec();
        f(&mut s);
        outbox_route(s)
    };
    // Without `sink.dead_letter_topic`, the outbox dead-letters to `outbox.unknown.dlq`.
    assert_eq!(build(ok.clone(), None), Ok(()));
    let cases: Vec<(KafkaRoute, Option<&str>, &str)> = vec![
        (
            KafkaRoute {
                topic: "t".into(),
                ..ok.clone()
            },
            Some(DLQ),
            "`topic` and `outbox` are mutually exclusive",
        ),
        (
            KafkaRoute {
                key: Some(vec!["id".into()]),
                ..ok.clone()
            },
            Some(DLQ),
            "`key` and `outbox` are mutually exclusive",
        ),
        (
            spec(|s| s.aggregate_id = String::new()),
            Some(DLQ),
            "`aggregate_id` is an empty column name",
        ),
        (
            spec(|s| s.payload = "a.b".into()),
            Some(DLQ),
            "`payload` column `a.b` is not a top-level column name",
        ),
        (
            spec(|s| s.aggregate_type = "{x}".into()),
            Some(DLQ),
            "`aggregate_type` column `{x}` is not a top-level",
        ),
        (
            spec(|s| s.id = Some(String::new())),
            Some(DLQ),
            "`id` is an empty column name",
        ),
        (
            spec(|s| s.kind = Some("t.x".into())),
            Some(DLQ),
            "`type` column `t.x`",
        ),
        (
            spec(|s| s.topic = "outbox.{before.kind}".into()),
            Some(DLQ),
            "reads `before.kind`, but only inserts and snapshot reads are published",
        ),
        (
            KafkaRoute {
                filter: Some("before.status == 'x' or after.n > 1".into()),
                ..ok.clone()
            },
            Some(DLQ),
            "`where` reads `before.status`, but an outbox route only publishes inserts and snapshot reads",
        ),
        (
            spec(|s| s.topic = "outbox.{aggregate_id}".into()),
            Some(DLQ),
            "`outbox` `topic` `outbox.{aggregate_id}` is not a valid template",
        ),
        // The content-routing rules: a fixed prefix, a fallback, no collisions.
        (
            spec(|s| s.topic = "{aggregate_type}".into()),
            Some(DLQ),
            "`topic` `{aggregate_type}` reads a row field, so it must start with fixed text",
        ),
        (
            KafkaRoute {
                topic_fallback: None,
                ..ok.clone()
            },
            Some(DLQ),
            "`topic` `outbox.{aggregate_type}` reads a row field, so it needs `topic_fallback`",
        ),
        (
            spec(|s| s.topic = "outbox.events".into()),
            Some(DLQ),
            "`topic_fallback` is only for a `topic` that reads a row field",
        ),
        (
            spec(|s| s.topic = "bad topic".into()),
            Some(DLQ),
            "is not a valid Kafka topic name",
        ),
    ];
    for (r, dlq, want) in cases {
        let err = build(r, dlq).expect_err(want);
        assert!(err.contains("sink route `ob`"), "{err}");
        assert!(err.contains(want), "{err} should contain {want}");
    }
    // A topic the template could produce cannot be the dead-letter topic or another route's.
    let err = build(ok.clone(), Some("outbox.order")).unwrap_err();
    assert!(
        err.contains("is also a topic events are routed to"),
        "{err}"
    );
    // `dlq` is a reserved value, so `outbox.{aggregate_type}` never renders `outbox.dlq`.
    assert_eq!(build(ok.clone(), Some("outbox.dlq")), Ok(()));
    let err = RoutePlan::with_dead_letter(
        vec![ok.clone(), route("public.x", "outbox.x")],
        None,
        Some(DLQ.into()),
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("could produce `outbox.x`, which is route `public.x`'s `topic`"),
        "{err}"
    );
    // A fixed topic with no fallback is fine; so are Debezium's defaults.
    assert_eq!(
        build(
            KafkaRoute {
                topic_fallback: None,
                ..spec(|s| s.topic = "outbox.events".into())
            },
            Some(DLQ)
        ),
        Ok(())
    );
    assert_eq!(
        build(outbox_route(OutboxSpec::default()), Some(DLQ)),
        Ok(())
    );
}

#[test]
fn outbox_routes_are_described_inspected_and_watched() {
    let p = outbox_plan();
    assert_eq!(
        p.topic_globs(),
        vec!["outbox.*", "rest", "outbox.unknown", DLQ]
    );
    let d = serde_json::to_value(p.describe()).unwrap();
    assert_eq!(d["routes"][0]["topic"], "outbox.{aggregate_type}");
    assert_eq!(d["routes"][0]["topic_fallback"], "outbox.unknown");
    assert_eq!(
        d["routes"][0]["outbox"],
        serde_json::json!({
            "aggregate_type": "aggregatetype", "aggregate_id": "aggregateid",
            "payload": "payload", "topic": "outbox.{aggregate_type}",
            "id": "id", "type": "type"
        })
    );
    // Other routes have no `outbox` key at all.
    assert!(d["routes"][1].get("outbox").is_none());
    // Misspelled outbox columns are reported as unseen fields, on inserts only.
    let q = plan_with(vec![outbox_route(OutboxSpec {
        aggregate_id: "aggregate_id".into(),
        ..outbox_spec()
    })]);
    for _ in 0..UNSEEN_FIELD_ROWS {
        q.render(&row("outbox", Op::Insert, OUTBOX_ROW), NOW)
            .unwrap();
        q.render(&row("outbox", Op::Delete, "{}"), NOW).unwrap();
    }
    assert_eq!(
        q.stats().snapshot()[0].unseen_fields,
        vec!["after.aggregate_id"]
    );
}

// --- fields: include/exclude, mask, hash, rename (R6) ---------------------------------------

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

fn hash_key() -> HashKey {
    HashKey::new(SECRET, "k1").unwrap()
}

/// The expected digest of `text`, computed with ring directly.
fn hmac_hex(text: &str) -> String {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET);
    ring::hmac::sign(&k, text.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn with_fields(name: &str, p: &str, f: FieldSpec) -> KafkaRoute {
    KafkaRoute {
        fields: f,
        ..named(name, p, &format!("{name}.topic"))
    }
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn fields_plan(routes: Vec<KafkaRoute>) -> RoutePlan {
    let mut p = RoutePlan::build(
        routes,
        Some("rest".into()),
        Some(DLQ.into()),
        Some(hash_key()),
    )
    .unwrap();
    p.set_source(pg());
    p
}

/// JSON text in tests: a key, an encoded value, or a record value that is not a tombstone.
trait Text {
    fn text(&self) -> &str;
}

impl Text for str {
    fn text(&self) -> &str {
        self
    }
}

impl Text for String {
    fn text(&self) -> &str {
        self
    }
}

impl Text for Option<String> {
    fn text(&self) -> &str {
        self.as_deref().expect("a record value, not a tombstone")
    }
}

fn json<T: Text + ?Sized>(s: &T) -> serde_json::Value {
    serde_json::from_str(s.text()).unwrap()
}

#[test]
fn field_ops_rewrite_before_and_after_but_not_source() {
    let p = fields_plan(vec![with_fields(
        "people",
        "public.people",
        FieldSpec {
            exclude: Some(strs(&["note"])),
            mask: strs(&["card", "age"]),
            hash: strs(&["email"]),
            rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
            ..Default::default()
        },
    )]);
    let mut e = event("public", "people", Op::Update, None, Position::Lsn(7));
    e.before = raw(r#"{"id":1,"email":"old@x.y","card":"4111","age":40,"note":"n","cust_id":3}"#);
    e.after = raw(r#"{"id":1,"email":"new@x.y","card":null,"age":41,"note":"n","cust_id":3}"#);
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(r.topic, "people.topic");
    let v = json(&r.value);
    assert_eq!(
        v["before"],
        serde_json::json!({"id": 1, "email": hmac_hex("old@x.y"), "card": "***", "age": null, "customer_id": 3})
    );
    assert_eq!(
        v["after"],
        serde_json::json!({"id": 1, "email": hmac_hex("new@x.y"), "card": null, "age": null, "customer_id": 3})
    );
    // `source`, `op` and the times are the envelope's, untouched.
    let plain = json(&encode(&e, &pg(), NOW).unwrap());
    for k in ["source", "op", "ts_ms", "ts_us"] {
        assert_eq!(v[k], plain[k], "{k}");
    }
    assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
    assert!(!r.value.text().contains("old@x.y") && !r.value.text().contains("4111"));
}

#[test]
fn the_key_object_gets_the_same_field_ops_and_decides_the_partition() {
    let route = |f: FieldSpec| with_fields("t", "public.t", f);
    let e = |email: &str| {
        let mut e = event("public", "t", Op::Insert, None, Position::Lsn(1));
        e.after = raw(&format!(r#"{{"email":"{email}","id":1,"n":2}}"#));
        e.key = Some(format!(r#"{{"email":"{email}","id":1}}"#));
        e
    };
    let k = |key: &str| format!(r#"{{"db":"app","schema":"public","table":"t","key":{key}}}"#);
    let cases: Vec<(FieldSpec, String)> = vec![
        (
            FieldSpec {
                hash: strs(&["email"]),
                ..Default::default()
            },
            k(&format!(r#"{{"email":"{}","id":1}}"#, hmac_hex("a@b.c"))),
        ),
        (
            FieldSpec {
                mask: strs(&["email"]),
                rename: [("id".to_string(), "user_id".to_string())].into(),
                ..Default::default()
            },
            k(r#"{"email":"***","user_id":1}"#),
        ),
        (
            FieldSpec {
                exclude: Some(strs(&["email"])),
                ..Default::default()
            },
            k(r#"{"id":1}"#),
        ),
        // Every key column removed: `"key": null`, so the table's partition.
        (
            FieldSpec {
                include: Some(strs(&["n"])),
                ..Default::default()
            },
            k("null"),
        ),
    ];
    for (spec, want) in cases {
        for by in [PartitionBy::Key, PartitionBy::Table] {
            let mut p = fields_plan(vec![route(spec.clone())]);
            p.set_partition_by(by);
            let r = p.render(&e("a@b.c"), NOW).unwrap().remove(0);
            assert_eq!(r.key, want, "{spec:?}");
            assert!(!r.key.contains("a@b.c"), "{}", r.key);
            let part = match by {
                PartitionBy::Key => want.clone(),
                PartitionBy::Table => "public.t".to_string(),
            };
            assert_eq!(r.partition_input(), part, "{spec:?} {by:?}");
        }
    }
    // A hashed key column keeps rows apart and the same row together: per-row order holds.
    let p = fields_plan(vec![route(FieldSpec {
        hash: strs(&["email"]),
        ..Default::default()
    })]);
    let pk = |email: &str| {
        let r = p.render(&e(email), NOW).unwrap().remove(0);
        r.partition_input().to_string()
    };
    assert_eq!(pk("a@b.c"), pk("a@b.c"));
    assert_ne!(pk("a@b.c"), pk("x@y.z"));
    // A MongoDB `_id` object is hashed from its compact JSON, in the key as in the value.
    let mut p = fields_plan(vec![with_fields(
        "m",
        "shop.users",
        FieldSpec {
            hash: strs(&["_id"]),
            ..Default::default()
        },
    )]);
    p.set_source(SourceInfo::Mongodb);
    let mut m = event(
        "shop",
        "users",
        Op::Insert,
        None,
        Position::Text("t".into()),
    );
    m.after = raw(r#"{"_id":{"$oid":"6530"},"n":1}"#);
    m.key = Some(r#"{"_id": {"$oid": "6530"}}"#.into());
    let r = p.render(&m, NOW).unwrap().remove(0);
    let digest = hmac_hex(r#"{"$oid":"6530"}"#);
    assert_eq!(json(&r.key)["key"]["_id"], digest.as_str());
    assert_eq!(json(&r.value)["after"]["_id"], digest.as_str());
}

#[test]
fn dead_letter_values_have_the_field_ops_applied() {
    let p = fields_plan(vec![KafkaRoute {
        filter: Some("row.amount > 10".into()),
        fields: FieldSpec {
            mask: strs(&["card"]),
            hash: strs(&["email"]),
            exclude: Some(strs(&["note"])),
            ..Default::default()
        },
        ..named("o", "public.orders", "orders")
    }]);
    let e = row(
        "orders",
        Op::Insert,
        r#"{"amount":"abc","card":"4111","email":"a@b.c","note":"secret"}"#,
    );
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(r.topic, DLQ);
    assert_eq!(
        json(&r.value)["after"],
        serde_json::json!({"amount": "abc", "card": "***", "email": hmac_hex("a@b.c")})
    );
    assert!(!r.value.text().contains("4111") && !r.value.text().contains("secret"));
    assert_eq!(r.headers[0].0, "aqueduct-error");
    assert_eq!(r.headers[1], (HASH_KEY_ID_HEADER, "k1".to_string()));
    assert_eq!(json(&r.key)["key"], serde_json::Value::Null);
}

#[test]
fn an_unhashable_string_goes_to_the_dead_letter_topic_without_the_row() {
    let p = fields_plan(vec![with_fields(
        "h",
        "public.users",
        FieldSpec {
            hash: strs(&["email"]),
            ..Default::default()
        },
    )]);
    let e = row(
        "users",
        Op::Insert,
        r#"{"email":"\ud800secret","card":"4111"}"#,
    );
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(r.topic, DLQ);
    assert_eq!(
        r.headers[0],
        (
            "aqueduct-error",
            "route `h`: hash `email`: the value is a string that is not valid Unicode".to_string()
        )
    );
    let v = json(&r.value);
    assert_eq!(v["after"], serde_json::Value::Null);
    assert!(!r.value.text().contains("secret") && !r.value.text().contains("4111"));
}

#[test]
fn a_rename_collision_goes_to_the_dead_letter_topic_with_original_names() {
    let p = fields_plan(vec![with_fields(
        "r",
        "public.users",
        FieldSpec {
            mask: strs(&["card"]),
            rename: [("cust".to_string(), "customer".to_string())].into(),
            ..Default::default()
        },
    )]);
    let ok = row("users", Op::Insert, r#"{"cust":1,"card":"4111"}"#);
    assert_eq!(
        json(&p.render(&ok, NOW).unwrap()[0].value)["after"],
        serde_json::json!({"customer": 1, "card": "***"})
    );
    let clash = row(
        "users",
        Op::Insert,
        r#"{"cust":1,"customer":2,"card":"4111"}"#,
    );
    let r = p.render(&clash, NOW).unwrap().remove(0);
    assert_eq!(r.topic, DLQ);
    assert_eq!(
        r.headers,
        vec![(
            "aqueduct-error",
            "route `r`: rename `cust` to `customer`: the row already has a column `customer`"
                .to_string()
        )]
    );
    // Masked even though the rename could not apply.
    assert_eq!(
        json(&r.value)["after"],
        serde_json::json!({"cust": 1, "customer": 2, "card": "***"})
    );
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.errors), (2, 1, 1));
}

#[test]
fn field_ops_are_validated_at_startup() {
    let build = |f: FieldSpec, dlq: Option<&str>, key: Option<HashKey>| {
        RoutePlan::build(
            vec![with_fields("r", "public.*", f)],
            None,
            dlq.map(Into::into),
            key,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    };
    let hash = FieldSpec {
        hash: strs(&["email"]),
        ..Default::default()
    };
    let e = build(hash.clone(), None, None).unwrap_err();
    assert!(
        e.contains("sink route `r` uses `hash`") && e.contains("AQUEDUCT_HASH_KEY"),
        "{e}"
    );
    assert!(build(hash, None, Some(hash_key())).is_ok());
    let rename = FieldSpec {
        rename: [("a".to_string(), "b".to_string())].into(),
        ..Default::default()
    };
    // A collision goes to the route's dead-letter topic, `<topic>.dlq` by default.
    assert!(build(rename.clone(), None, None).is_ok());
    assert!(build(rename, Some(DLQ), None).is_ok());
    // With `include`, collisions are ruled out at startup.
    let included = FieldSpec {
        include: Some(strs(&["a"])),
        rename: [("a".to_string(), "b".to_string())].into(),
        ..Default::default()
    };
    assert!(build(included, None, None).is_ok());
    // Spec errors name the route.
    let both = FieldSpec {
        include: Some(strs(&["a"])),
        exclude: Some(strs(&["b"])),
        ..Default::default()
    };
    let e = build(both, None, None).unwrap_err();
    assert!(e.contains("sink route `r`: `include` and `exclude`"), "{e}");
    let dotted = FieldSpec {
        mask: strs(&["address.city"]),
        ..Default::default()
    };
    assert!(build(dotted, None, None).unwrap_err().contains("dotted"));
}

#[test]
fn unseen_field_op_columns_are_reported_without_a_where() {
    let p = fields_plan(vec![with_fields(
        "u",
        "public.users",
        FieldSpec {
            mask: strs(&["card_last_4"]),
            hash: strs(&["email"]),
            ..Default::default()
        },
    )]);
    let ev = row(
        "users",
        Op::Insert,
        r#"{"email":"a@b.c","card_last4":"1234"}"#,
    );
    for _ in 0..UNSEEN_FIELD_ROWS {
        p.render(&ev, NOW).unwrap();
    }
    assert_eq!(
        p.stats().snapshot()[0].unseen_fields,
        vec!["row.card_last_4"]
    );
}

#[test]
fn describe_lists_field_ops_and_the_key_id_but_never_the_key() {
    let p = fields_plan(vec![
        with_fields(
            "u",
            "public.users",
            FieldSpec {
                exclude: Some(strs(&["note"])),
                mask: strs(&["card"]),
                hash: strs(&["email"]),
                rename: [("cust".to_string(), "customer".to_string())].into(),
                ..Default::default()
            },
        ),
        with_fields(
            "i",
            "public.items",
            FieldSpec {
                include: Some(strs(&["id"])),
                ..Default::default()
            },
        ),
    ]);
    let v = serde_json::to_value(p.describe()).unwrap();
    assert_eq!(v["hash_key_id"], "k1");
    let u = &v["routes"][0];
    assert_eq!(u["exclude"], serde_json::json!(["note"]));
    assert_eq!(u["mask"], serde_json::json!(["card"]));
    assert_eq!(u["hash"], serde_json::json!(["email"]));
    assert_eq!(u["rename"], serde_json::json!({"cust": "customer"}));
    assert!(u.get("include").is_none());
    assert_eq!(v["routes"][1]["include"], serde_json::json!(["id"]));
    assert!(v["routes"][1].get("hash").is_none());
    let text = v.to_string();
    assert!(!text.contains("0123456789abcdef"), "{text}");
    assert!(!format!("{p:?}").contains("0123456789abcdef"));
}

#[test]
fn a_route_key_may_not_name_a_protected_column() {
    let build = |key: &[&str], f: FieldSpec| {
        RoutePlan::build(
            vec![KafkaRoute {
                key: Some(strs(key)),
                fields: f,
                ..named("r", "public.users", "users")
            }],
            None,
            Some(DLQ.into()),
            Some(hash_key()),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    };
    let cases: Vec<(FieldSpec, &str)> = vec![
        (
            FieldSpec {
                hash: strs(&["email"]),
                ..Default::default()
            },
            "hashed",
        ),
        (
            FieldSpec {
                mask: strs(&["email"]),
                ..Default::default()
            },
            "masked",
        ),
        (
            FieldSpec {
                exclude: Some(strs(&["email"])),
                ..Default::default()
            },
            "excluded",
        ),
        (
            FieldSpec {
                include: Some(strs(&["id"])),
                ..Default::default()
            },
            "not included",
        ),
    ];
    for (f, how) in cases {
        let e = build(&["id", "email"], f.clone()).unwrap_err();
        assert!(
            e.contains(&format!("sink route `r`: `key` column `email` is {how}")),
            "{e}"
        );
        // Keying by an unprotected column is fine.
        assert_eq!(build(&["id"], f), Ok(()));
    }
    // A renamed column may key the route: it is not protected.
    let renamed = FieldSpec {
        rename: [("id".to_string(), "user_id".to_string())].into(),
        ..Default::default()
    };
    assert_eq!(build(&["id"], renamed), Ok(()));
}

#[test]
fn a_route_key_is_kept_and_the_value_rewritten() {
    let p = fields_plan(vec![KafkaRoute {
        key: Some(strs(&["id"])),
        fields: FieldSpec {
            hash: strs(&["email"]),
            ..Default::default()
        },
        ..named("r", "public.users", "users")
    }]);
    let mut e = row("users", Op::Insert, r#"{"id":5,"email":"a@b.c"}"#);
    e.key = Some(r#"{"email":"a@b.c"}"#.into());
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"users","key":{"id":5}}"#
    );
    assert_eq!(r.partition_input(), r.key);
    assert_eq!(json(&r.value)["after"]["email"], hmac_hex("a@b.c").as_str());
    assert!(!r.key.contains("a@b.c"));
}

#[test]
fn a_content_routed_topic_may_not_read_a_protected_column() {
    let build = |topic: &str, values: Option<&[&str]>, f: FieldSpec| {
        RoutePlan::build(
            vec![KafkaRoute {
                fields: f,
                ..content("r", topic, Some("orders.unknown"), values)
            }],
            None,
            Some(DLQ.into()),
            Some(hash_key()),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    };
    let spec = |how: &str| match how {
        "hashed" => FieldSpec {
            hash: strs(&["region"]),
            ..Default::default()
        },
        "masked" => FieldSpec {
            mask: strs(&["region"]),
            ..Default::default()
        },
        "excluded" => FieldSpec {
            exclude: Some(strs(&["region"])),
            ..Default::default()
        },
        _ => FieldSpec {
            include: Some(strs(&["id"])),
            ..Default::default()
        },
    };
    for how in ["hashed", "masked", "excluded", "not included"] {
        // Any row path into the column counts: `row.`, `after.`, `before.`, nested too.
        for path in ["row.region", "after.region", "before.region.code"] {
            let e = build(&format!("orders.{{{path}}}"), None, spec(how)).unwrap_err();
            assert!(
                e.contains(&format!(
                    "sink route `r`: `topic` reads `{path}`, but column `region` is {how}"
                )),
                "{e}"
            );
        }
        // With an allow-list the message says `topic_values` would carry it too.
        let e = build("orders.{row.region}", Some(&["eu", "us"]), spec(how)).unwrap_err();
        assert!(e.contains("the topic name (and `topic_values`)"), "{e}");
        // Routing by an unprotected column is fine.
        let ok = build("orders.{row.tenant}", None, spec(how));
        assert_eq!(ok.is_ok(), how != "not included", "{ok:?}");
    }
    // `not included` also covers the tenant column in that last case.
    let e = build("orders.{row.tenant}", None, spec("not included")).unwrap_err();
    assert!(e.contains("column `tenant` is not included"), "{e}");
    // A renamed column may route: the template reads the original row.
    let renamed = FieldSpec {
        rename: [("region".to_string(), "area".to_string())].into(),
        ..Default::default()
    };
    assert_eq!(build("orders.{row.region}", None, renamed), Ok(()));
}

#[test]
fn fallback_records_carry_the_rewritten_value_key_and_key_id() {
    let mut p = RoutePlan::build(
        vec![KafkaRoute {
            fields: FieldSpec {
                hash: strs(&["email"]),
                mask: strs(&["card"]),
                ..Default::default()
            },
            ..content("o", "orders.{row.region}", Some("orders.unknown"), None)
        }],
        None,
        None,
        Some(hash_key()),
    )
    .unwrap();
    p.set_source(pg());
    let ev = |region: &str| {
        let mut e = row(
            "orders",
            Op::Insert,
            &format!(r#"{{"email":"a@b.c","card":"4111","region":{region}}}"#),
        );
        e.key = Some(r#"{"email":"a@b.c"}"#.into());
        e
    };
    let check = |r: &OutRecord, topic: &str| {
        assert_eq!(r.topic, topic);
        let v = json(&r.value);
        assert_eq!(v["after"]["email"], hmac_hex("a@b.c").as_str());
        assert_eq!(v["after"]["card"], "***");
        assert_eq!(json(&r.key)["key"]["email"], hmac_hex("a@b.c").as_str());
        assert_eq!(r.partition_input(), r.key);
        assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
        assert!(!r.value.text().contains("a@b.c") && !r.key.contains("a@b.c"));
        assert!(!r.value.text().contains("4111"));
    };
    // Normal content-routed record.
    let normal = p.render(&ev(r#""eu""#), NOW).unwrap().remove(0);
    check(&normal, "orders.eu");
    // Render-time fallback: a null routing value.
    let fallback = p.render(&ev("null"), NOW).unwrap().remove(0);
    check(&fallback, "orders.unknown");
    // The sink's refusal re-route changes only the topic.
    let mut refused = p.render(&ev(r#""us""#), NOW).unwrap().remove(0);
    p.topic_refused(&mut refused, "TopicAuthorizationFailed");
    check(&refused, "orders.unknown");
    // Apart from the topic, all three records are the same.
    for r in [&fallback, &refused] {
        assert_eq!(
            (&r.key, &r.headers, r.op),
            (&normal.key, &normal.headers, normal.op)
        );
    }
    p.commit_counts();
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (3, 1, 2));
}

// --- golden: byte-identical to the router and record builders this plan replaced ----------

const NOW: i64 = 1_792_502_627_093_140;

fn pg() -> SourceInfo {
    SourceInfo::Postgres { db: "app".into() }
}

/// The pre-plan `TopicRouter::topic_for`, verbatim.
fn legacy_topic(
    routes: &[KafkaRoute],
    default: Option<&str>,
    schema: &str,
    table: &str,
) -> Option<String> {
    let name = format!("{schema}.{table}");
    let template = routes
        .iter()
        .find(|r| glob_match(&r.pattern, &name))
        .map(|r| r.topic.as_str())
        .or(default)?;
    Some(
        template
            .replace("{schema}", schema)
            .replace("{table}", table),
    )
}

fn raw(s: &str) -> Option<Box<serde_json::value::RawValue>> {
    serde_json::value::RawValue::from_string(s.into()).ok()
}

fn event(schema: &str, table: &str, op: Op, key: Option<&str>, pos: Position) -> ChangeEvent {
    ChangeEvent {
        position: pos,
        xid: 9,
        commit_ts_micros: 1_700_000_123_456_789,
        schema: schema.into(),
        table: table.into(),
        op,
        before: matches!(op, Op::Update | Op::Delete)
            .then(|| raw(r#"{"id":1,"n":"a"}"#))
            .flatten(),
        after: matches!(op, Op::Insert | Op::Update)
            .then(|| raw(r#"{"id":1,"n":"é\"x"}"#))
            .flatten(),
        key: key.map(Into::into),
        checkpoint: None,
        unchanged: Vec::new(),
        after_is_key: false,
    }
}

#[test]
fn output_is_byte_identical_to_the_topic_router_it_replaced() {
    let configs: Vec<(Vec<KafkaRoute>, Option<&str>)> = vec![
        (vec![], Some("cdc")),
        (vec![], Some("cdc.{schema}.{table}")),
        (
            vec![
                route("public.users", "users"),
                route("public.*", "cdc.{table}"),
                route("*.audit_*", "audit.{schema}"),
                route("*", "everything"),
            ],
            None,
        ),
        (
            vec![route("sales.*", "s.{schema}.{table}")],
            Some("fallback.{table}"),
        ),
    ];
    let events = [
        event(
            "public",
            "users",
            Op::Insert,
            Some("1"),
            Position::Lsn(0x16_B374_D848),
        ),
        event(
            "public",
            "orders",
            Op::Update,
            Some("7\u{1f}x"),
            Position::Lsn(42),
        ),
        event(
            "sales",
            "items",
            Op::Delete,
            None,
            Position::Text("resume-token".into()),
        ),
        event("hr", "audit_log", Op::Truncate, None, Position::Lsn(0)),
        event(
            "a.b",
            "c{d}",
            Op::Insert,
            Some("k"),
            Position::Lsn(u64::MAX),
        ),
    ];
    for (routes, default) in &configs {
        for (by, info) in [PartitionBy::Table, PartitionBy::Key]
            .into_iter()
            .flat_map(|by| [(by, pg()), (by, SourceInfo::Mongodb)])
        {
            let mut plan = RoutePlan::new(routes.clone(), default.map(Into::into)).unwrap();
            plan.set_partition_by(by);
            plan.set_source(info.clone());
            for e in &events {
                let want_topic = legacy_topic(routes, *default, &e.schema, &e.table);
                let got = plan.render(e, NOW);
                let Some(topic) = want_topic else {
                    assert!(got.is_err());
                    continue;
                };
                let got = got.unwrap();
                assert_eq!(got.len(), 1);
                let r = &got[0];
                assert_eq!(r.topic, topic);
                let key = record_key(e, &info);
                assert_eq!(
                    r.partition_input(),
                    crate::kafka::partition_input(e, &key, by)
                );
                assert_eq!(r.key, key);
                assert_eq!(r.value.text(), encode(e, &info, NOW).unwrap());
                assert_eq!(r.op, op_code(e.op));
                assert_eq!(r.lsn, e.position.to_checkpoint());
                assert_eq!(r.commit_ts_micros, e.commit_ts_micros);
            }
        }
    }
}

#[test]
fn pinned_output_for_the_sample_event() {
    // Literal expectations, so a change to key, value or header encoding is a visible diff.
    let mut p = RoutePlan::new(vec![], Some("cdc.{schema}.{table}".into())).unwrap();
    p.set_source(pg());
    let r = p.render(&crate::sample(), NOW).unwrap().remove(0);
    assert_eq!(r.topic, "cdc.public.users");
    assert_eq!(
        r.key,
        r#"{"db":"app","schema":"public","table":"users","key":{"id":1}}"#
    );
    assert_eq!(r.partition_input(), r.key);
    assert_eq!(r.op, "c");
    assert_eq!(r.lsn, "97500059720");
    assert_eq!(
        r.value.text(),
        concat!(
            r#"{"before":null,"after":{"id":1,"name":"ada"},"source":{"version":""#,
            env!("CARGO_PKG_VERSION"),
            r#"","connector":"postgresql","ts_ms":1700000000000,"ts_us":1700000000000000,"#,
            r#""snapshot":"false","db":"app","schema":"public","table":"users","txId":7,"#,
            r#""lsn":97500059720},"op":"c","ts_ms":1792502627093,"ts_us":1792502627093140}"#
        )
    );
}

/// `cargo test --release -p aqueduct-sinks -- --ignored --nocapture render_cost`
#[test]
#[ignore = "timing, run by hand in release mode"]
fn render_cost_against_the_legacy_path() {
    use std::time::Instant;
    let routes = vec![
        route("public.users", "users"),
        route("public.*", "cdc.{table}"),
    ];
    let mut plan = RoutePlan::new(routes.clone(), Some("rest".into())).unwrap();
    plan.set_source(pg());
    let e = event("public", "orders", Op::Update, Some("7"), Position::Lsn(42));
    let n = 2_000_000;
    let mut sink = 0usize;
    let t = Instant::now();
    for _ in 0..n {
        let t = legacy_topic(&routes, Some("rest"), &e.schema, &e.table).unwrap();
        let k = record_key(&e, &pg());
        let pk = crate::kafka::partition_input(&e, &k, PartitionBy::Key);
        let v = encode(&e, &pg(), NOW).unwrap();
        // The old `record()` also built both header values.
        let op = op_code(e.op);
        let lsn = e.position.to_checkpoint();
        sink += t.len() + k.len() + pk.len() + v.len() + op.len() + lsn.len();
    }
    let legacy = t.elapsed();
    let mut out = Vec::new();
    let t = Instant::now();
    for _ in 0..n {
        out.clear();
        plan.render_into(&e, NOW, &mut out).unwrap();
        sink += out[0].topic.len() + out[0].key.len() + out[0].value.text().len();
    }
    let new = t.elapsed();
    println!("legacy {legacy:?}, plan {new:?} for {n} events (sink {sink})");

    // Field operations, one class at a time, on a 10-column row with a 2-column key.
    let mut e = event("public", "users", Op::Update, None, Position::Lsn(42));
    let row = r#"{"id":7,"tenant":3,"email":"ada@example.com","name":"Ada Lovelace","card":"4111111111111111","phone":"+32 470 00 00 00","note":"likes \"engines\"","cust_id":99,"active":true,"created_at":1792502627093140}"#;
    e.before = raw(row);
    e.after = raw(row);
    e.key = Some(r#"{"id":7,"tenant":3}"#.into());
    let s = strs;
    let classes: Vec<(&str, FieldSpec)> = vec![
        ("none", FieldSpec::default()),
        (
            "include",
            FieldSpec {
                include: Some(s(&["id", "tenant", "name", "active"])),
                ..Default::default()
            },
        ),
        (
            "exclude",
            FieldSpec {
                exclude: Some(s(&["note", "phone"])),
                ..Default::default()
            },
        ),
        (
            "mask",
            FieldSpec {
                mask: s(&["card", "phone"]),
                ..Default::default()
            },
        ),
        (
            "hash",
            FieldSpec {
                hash: s(&["email", "phone"]),
                ..Default::default()
            },
        ),
        (
            "rename",
            FieldSpec {
                rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
                ..Default::default()
            },
        ),
        (
            "all",
            FieldSpec {
                exclude: Some(s(&["note"])),
                mask: s(&["card"]),
                hash: s(&["email", "phone"]),
                rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
                ..Default::default()
            },
        ),
    ];
    let n = 500_000;
    for (class, spec) in classes {
        let plan = fields_plan(vec![with_fields("u", "public.users", spec)]);
        let t = Instant::now();
        for _ in 0..n {
            out.clear();
            plan.render_into(&e, NOW, &mut out).unwrap();
            sink += out[0].value.text().len();
        }
        let per = t.elapsed().as_nanos() / n as u128;
        println!("fields {class:>8}: {per} ns/event (sink {sink})");
    }

    // An op filter in `where` reads no row: an update that passes, a delete it filters.
    let mut del = e.clone();
    (del.op, del.after) = (Op::Delete, None);
    for (class, filter) in [
        ("none", None),
        ("op where", Some("op in ['insert', 'update']")),
    ] {
        let mut plan = RoutePlan::new(
            vec![KafkaRoute {
                filter: filter.map(Into::into),
                ..route("public.users", "users")
            }],
            None,
        )
        .unwrap();
        plan.set_source(pg());
        for ev in [&e, &del] {
            let t = Instant::now();
            for _ in 0..n {
                out.clear();
                plan.render_into(ev, NOW, &mut out).unwrap();
                sink += out.len();
            }
            let per = t.elapsed().as_nanos() / n as u128;
            println!(
                "filter {class:>8} {:?}: {per} ns/event (sink {sink})",
                ev.op
            );
        }
    }
}

// --- shapes (R7) ------------------------------------------------------------------------------

fn shaped(name: &str, format: Option<&str>, deletes: Option<&str>) -> KafkaRoute {
    KafkaRoute {
        format: format.map(Into::into),
        deletes: deletes.map(Into::into),
        ..named(name, "public.*", "t")
    }
}

fn tombstoned(name: &str) -> KafkaRoute {
    KafkaRoute {
        tombstones: true,
        ..named(name, "public.*", "t")
    }
}

const PG_KEY: &str = r#"{"db":"app","schema":"public","table":"orders","key":{"id":1}}"#;

#[test]
fn row_format_values_are_the_row_after_field_ops_are_none() {
    let p = plan_with(vec![shaped("rows", Some("row"), None)]);
    for op in [Op::Insert, Op::Update, Op::Read] {
        let mut e = row("orders", op, r#"{"id":1,"n":"a"}"#);
        if op == Op::Update {
            e.before = raw(r#"{"id":1,"n":"old"}"#);
        }
        let out = p.render(&e, NOW).unwrap();
        assert_eq!(out.len(), 1, "{op:?}");
        let r = &out[0];
        assert_eq!(
            r.value.text(),
            r#"{"id":1,"n":"a"}"#,
            "{op:?}: the row as is"
        );
        assert_eq!(r.key, PG_KEY);
        assert_eq!((r.op, r.lsn.as_str()), (op_code(op), "7"), "headers stay");
        assert!(r.headers.is_empty());
    }
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced), (3, 3));
}

#[test]
fn each_delete_mode_and_truncates_in_row_format() {
    let del = row("orders", Op::Delete, r#"{"id":1,"n":"a"}"#);
    let trunc = row("orders", Op::Truncate, "{}");
    // Default and explicit `tombstone`: the delete itself is a null value under the row's key.
    for deletes in [None, Some("tombstone")] {
        let p = plan_with(vec![shaped("rows", Some("row"), deletes)]);
        let out = p.render(&del, NOW).unwrap();
        assert_eq!(out.len(), 1, "no extra record: the delete is the tombstone");
        assert_eq!(out[0].value, None);
        assert_eq!((out[0].key.as_str(), out[0].op), (PG_KEY, "d"));
        assert_eq!(out[0].partition_input(), PG_KEY);
        assert!(p.render(&trunc, NOW).unwrap().is_empty());
        let c = p.stats().snapshot()[0].counts;
        assert_eq!((c.matched, c.produced, c.filtered), (2, 1, 1));
    }
    // `drop`: no record, counted as filtered, like the truncate.
    let p = plan_with(vec![shaped("rows", Some("row"), Some("drop"))]);
    assert!(p.render(&del, NOW).unwrap().is_empty());
    assert!(p.render(&trunc, NOW).unwrap().is_empty());
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.filtered), (2, 0, 2));
    // `row`: the before-image with `"__deleted": true` written into the object.
    let p = plan_with(vec![shaped("rows", Some("row"), Some("row"))]);
    let out = p.render(&del, NOW).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].value.text(), r#"{"id":1,"n":"a","__deleted":true}"#);
    assert_eq!((out[0].key.as_str(), out[0].op), (PG_KEY, "d"));
    // A change without a row (a MongoDB update whose document is gone) is no record.
    let mut gone = row("orders", Op::Update, "{}");
    gone.after = None;
    assert!(p.render(&gone, NOW).unwrap().is_empty());
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.filtered), (2, 1, 1));
}

#[test]
fn deleted_rows_replace_a_deleted_column_and_keep_order() {
    let d = |s: Option<&str>| deleted_row(s.and_then(raw).as_deref());
    assert_eq!(d(None), r#"{"__deleted":true}"#);
    assert_eq!(d(Some("{}")), r#"{"__deleted":true}"#);
    assert_eq!(d(Some(" { } ")), r#"{"__deleted":true}"#);
    assert_eq!(d(Some(r#"{"a":1}"#)), r#"{"a":1,"__deleted":true}"#);
    assert_eq!(
        d(Some(r#"{"a":1,"__deleted":false,"b":"x"}"#)),
        r#"{"a":1,"b":"x","__deleted":true}"#
    );
    // A string value "__deleted" takes the slow path and is kept.
    assert_eq!(
        d(Some(r#"{"a":"__deleted"}"#)),
        r#"{"a":"__deleted","__deleted":true}"#
    );
    // An escaped quote inside a string never looks like the column.
    assert_eq!(
        d(Some(r#"{"a":"x\"__deleted\""}"#)),
        r#"{"a":"x\"__deleted\"","__deleted":true}"#
    );
    assert_eq!(d(Some("[1]")), r#"{"__deleted":true}"#);
    for s in [r#"{"a":1,"__deleted":false}"#, r#"{"a":{"b":[1,2]}}"#] {
        let v: serde_json::Value = serde_json::from_str(&d(Some(s))).unwrap();
        assert_eq!(v["__deleted"], true, "{s}");
    }
}

#[test]
fn field_ops_apply_to_row_values_deleted_rows_and_tombstone_keys() {
    let spec = || FieldSpec {
        exclude: Some(strs(&["note"])),
        mask: strs(&["card"]),
        hash: strs(&["email"]),
        rename: [("cust_id".to_string(), "customer_id".to_string())].into(),
        ..Default::default()
    };
    let users = |op: Op| {
        let mut e = row(
            "users",
            op,
            r#"{"email":"a@b.c","card":"4111","note":"secret","cust_id":9}"#,
        );
        e.key = Some(r#"{"email":"a@b.c"}"#.into());
        e
    };
    let check = |v: &serde_json::Value| {
        assert_eq!(v["email"], hmac_hex("a@b.c").as_str());
        assert_eq!(v["card"], "***");
        assert_eq!(v["customer_id"], 9);
        assert!(v.get("note").is_none() && v.get("cust_id").is_none(), "{v}");
    };
    // Row format: the value and a `deletes = "row"` before-image are rewritten.
    let p = fields_plan(vec![KafkaRoute {
        format: Some("row".into()),
        deletes: Some("row".into()),
        ..with_fields("u", "public.users", spec())
    }]);
    let r = p.render(&users(Op::Insert), NOW).unwrap().remove(0);
    check(&json(&r.value));
    assert_eq!(r.headers, vec![(HASH_KEY_ID_HEADER, "k1".to_string())]);
    let r = p.render(&users(Op::Delete), NOW).unwrap().remove(0);
    let v = json(&r.value);
    check(&v);
    assert_eq!(v["__deleted"], true);
    assert!(!r.value.text().contains("a@b.c") && !r.value.text().contains("4111"));
    assert!(!r.key.contains("a@b.c"), "the key is rewritten too");
    assert_eq!(json(&r.key)["key"]["email"], hmac_hex("a@b.c").as_str());

    // Row-format tombstone and envelope tombstone: the rewritten key, never the original.
    for route in [
        KafkaRoute {
            format: Some("row".into()),
            ..with_fields("u", "public.users", spec())
        },
        KafkaRoute {
            tombstones: true,
            ..with_fields("u", "public.users", spec())
        },
    ] {
        let p = fields_plan(vec![route]);
        let out = p.render(&users(Op::Delete), NOW).unwrap();
        let t = out.last().unwrap();
        assert_eq!(t.value, None);
        assert!(!t.key.contains("a@b.c"), "{}", t.key);
        assert_eq!(json(&t.key)["key"]["email"], hmac_hex("a@b.c").as_str());
        assert_eq!(t.partition_input(), t.key);
    }
}

#[test]
fn a_tombstone_follows_its_delete_with_the_same_key_and_partition() {
    let p = plan_with(vec![tombstoned("orders")]);
    let ins = row("orders", Op::Insert, r#"{"id":1}"#);
    let del = row("orders", Op::Delete, r#"{"id":1}"#);
    assert_eq!(p.render(&ins, NOW).unwrap().len(), 1, "only deletes");
    let out = p.render(&del, NOW).unwrap();
    assert_eq!(out.len(), 2);
    let (d, t) = (&out[0], &out[1]);
    assert_eq!(
        json(&d.value)["op"],
        "d",
        "the delete first, as an envelope"
    );
    assert_eq!(t.value, None, "then the tombstone");
    assert_eq!(
        (&t.topic, &t.key, &t.partition_key, t.op, &t.lsn),
        (&d.topic, &d.key, &d.partition_key, d.op, &d.lsn)
    );
    assert_eq!(t.key, PG_KEY);
    // Counted once, as the delete.
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.tombstones_skipped), (2, 2, 0));

    // A per-route key: the tombstone carries it too.
    let p = plan_with(vec![KafkaRoute {
        key: Some(strs(&["id"])),
        ..tombstoned("orders")
    }]);
    let out = p.render(&del, NOW).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[1].key, out[0].key);
    assert_eq!(json(&out[1].key)["key"], serde_json::json!({"id": 1}));
}

#[test]
fn a_delete_with_a_null_key_gets_no_tombstone() {
    // No primary key: the record key's `key` is null.
    let p = plan_with(vec![tombstoned("orders")]);
    let mut del = row("orders", Op::Delete, r#"{"id":1}"#);
    del.key = None;
    let out = p.render(&del, NOW).unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].value.is_some(), "the delete itself is produced");
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.produced, c.tombstones_skipped), (1, 1));
    // A per-route key whose columns the before-image lacks is null too.
    let p = plan_with(vec![KafkaRoute {
        key: Some(strs(&["email"])),
        ..tombstoned("orders")
    }]);
    let del = row("orders", Op::Delete, r#"{"id":1}"#);
    assert_eq!(p.render(&del, NOW).unwrap().len(), 1);
    let mut with_email = row("orders", Op::Delete, r#"{"id":1,"email":"x"}"#);
    with_email.key = None;
    assert_eq!(p.render(&with_email, NOW).unwrap().len(), 2);
    assert_eq!(p.stats().snapshot()[0].counts.tombstones_skipped, 1);
    // Field operations that remove every key column leave a null key as well.
    let p = fields_plan(vec![KafkaRoute {
        tombstones: true,
        ..with_fields(
            "u",
            "public.orders",
            FieldSpec {
                exclude: Some(strs(&["id"])),
                ..Default::default()
            },
        )
    }]);
    assert_eq!(p.render(&del, NOW).unwrap().len(), 1);
    assert_eq!(p.stats().snapshot()[0].counts.tombstones_skipped, 1);
}

#[test]
fn a_tombstone_follows_its_delete_to_the_fallback_and_is_not_counted_twice() {
    let mut p = RoutePlan::new(
        vec![KafkaRoute {
            tombstones: true,
            ..content("o", "orders.{row.region}", Some("orders.unknown"), None)
        }],
        None,
    )
    .unwrap();
    p.set_source(pg());
    let del = row("orders", Op::Delete, r#"{"id":1,"region":"eu"}"#);
    let mut out = p.render(&del, NOW).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].topic, "orders.eu");
    assert!(!out[0].content.as_ref().unwrap().tombstone);
    assert!(out[1].content.as_ref().unwrap().tombstone);
    // The broker refuses the topic: both go to the fallback, the event is counted once.
    for r in &mut out {
        p.topic_refused(r, "PolicyViolation");
    }
    assert_eq!(out[0].topic, out[1].topic);
    assert_eq!(out[1].value, None);
    p.commit_counts();
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (1, 0, 1));
}

#[test]
fn unchanged_dead_letter_sends_rows_with_omitted_toast_columns_to_the_dead_letter_topic() {
    let route = |unchanged: Option<&str>| KafkaRoute {
        unchanged: unchanged.map(Into::into),
        ..shaped("docs", Some("row"), None)
    };
    // The decoder's signal: `doc` was an unchanged TOAST value, so `after` lacks it.
    let mut upd = row("docs", Op::Update, r#"{"id":1,"title":"t"}"#);
    upd.unchanged = vec!["doc".into()];
    let p = plan_with(vec![route(Some("dead_letter"))]);
    let out = p.render(&upd, NOW).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].topic, DLQ);
    assert_eq!(
        out[0].headers,
        vec![(
            "aqueduct-error",
            "route `docs`: unchanged = \"dead_letter\": the source left out unchanged column(s) `doc`"
                .to_string()
        )]
    );
    assert_eq!(
        json(&out[0].value)["op"],
        "u",
        "the dead-letter value is the envelope"
    );
    // A complete row passes.
    let mut full = upd.clone();
    full.unchanged.clear();
    assert_eq!(p.render(&full, NOW).unwrap()[0].topic, "t");
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.errors), (2, 1, 1));
    // `omit` (the default): the incomplete row is produced as it is.
    for u in [None, Some("omit")] {
        let p = plan_with(vec![route(u)]);
        let out = p.render(&upd, NOW).unwrap();
        assert_eq!(out[0].topic, "t");
        assert_eq!(out[0].value.text(), r#"{"id":1,"title":"t"}"#);
    }
}

#[test]
fn shapes_are_validated_at_startup() {
    let err = |r: KafkaRoute, dlq: Option<&str>| {
        RoutePlan::with_dead_letter(vec![r], None, dlq.map(Into::into))
            .unwrap_err()
            .to_string()
    };
    let e = err(shaped("r", None, Some("row")), None);
    assert!(
        e.contains("`deletes` applies to `format = \"row\"` only"),
        "{e}"
    );
    let e = err(shaped("r", Some("flat"), None), None);
    assert!(
        e.contains("route `r`: unknown format `flat` (expected `envelope` or `row`)"),
        "{e}"
    );
    let e = err(shaped("r", Some("row"), Some("delete")), None);
    assert!(e.contains("unknown deletes `delete`"), "{e}");
    let e = err(
        KafkaRoute {
            tombstones: true,
            ..shaped("r", Some("row"), None)
        },
        None,
    );
    assert!(e.contains("`tombstones` is for the envelope format"), "{e}");
    let unchanged = |u: &str| KafkaRoute {
        unchanged: Some(u.into()),
        ..named("r", "public.*", "t")
    };
    assert!(RoutePlan::new(vec![unchanged("dead_letter")], None).is_ok());
    let e = err(unchanged("fill"), Some(DLQ));
    assert!(e.contains("unknown unchanged `fill`"), "{e}");
    assert!(
        RoutePlan::with_dead_letter(vec![unchanged("dead_letter")], None, Some(DLQ.into())).is_ok()
    );
    // Case does not matter.
    assert!(RoutePlan::new(vec![shaped("r", Some("ROW"), Some("Drop"))], None).is_ok());
    // Outbox routes take none of the shape keys.
    let e = err(
        KafkaRoute {
            tombstones: true,
            unchanged: Some("omit".into()),
            outbox: Some(OutboxSpec::default()),
            ..named("o", "public.outbox", "")
        },
        Some(DLQ),
    );
    assert!(
        e.contains("`outbox` and `tombstones`, `unchanged` are mutually exclusive"),
        "{e}"
    );

    // Tombstones need a per-row key: `partition_by = "key"`, or the route's own `key`.
    let p = RoutePlan::new(vec![tombstoned("r")], None).unwrap();
    assert!(p.check_partition_by(PartitionBy::Key).is_ok());
    let e = p
        .check_partition_by(PartitionBy::Table)
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("route `r` uses `tombstones`, which needs a per-row key"),
        "{e}"
    );
    let keyed = RoutePlan::new(
        vec![KafkaRoute {
            key: Some(strs(&["id"])),
            ..tombstoned("r")
        }],
        None,
    )
    .unwrap();
    assert!(keyed.check_partition_by(PartitionBy::Table).is_ok());
    assert!(
        RoutePlan::new(vec![route("public.*", "t")], None)
            .unwrap()
            .check_partition_by(PartitionBy::Table)
            .is_ok()
    );
}

#[test]
fn shapes_are_described() {
    let p = plan_with(vec![
        KafkaRoute {
            unchanged: Some("dead_letter".into()),
            ..KafkaRoute {
                pattern: "public.a".into(),
                ..shaped("rows", Some("row"), None)
            }
        },
        KafkaRoute {
            pattern: "public.b".into(),
            ..tombstoned("events")
        },
        named("plain", "public.*", "t"),
    ]);
    let d = p.describe();
    let r = &d.routes;
    assert_eq!(
        (r[0].format, r[0].deletes, r[0].tombstones, r[0].unchanged),
        (Some("row"), Some("tombstone"), false, Some("dead_letter"))
    );
    assert_eq!(
        (r[1].format, r[1].deletes, r[1].tombstones, r[1].unchanged),
        (None, None, true, None)
    );
    // Unset keys are left out of the JSON, so plain routes describe as before.
    let v = serde_json::to_value(&r[2]).unwrap();
    for k in ["format", "deletes", "tombstones", "unchanged"] {
        assert!(v.get(k).is_none(), "{k}: {v}");
    }
    let v = serde_json::to_value(&r[1]).unwrap();
    assert_eq!(v["tombstones"], true);
}

/// What one delete on `route` produced: the number of records and the route's counts
/// (produced, filtered, tombstones_skipped).
fn delete_outcome(route: KafkaRoute, del: &ChangeEvent) -> (usize, (u64, u64, u64)) {
    let p = fields_plan(vec![route]);
    let n = p.render(del, NOW).unwrap().len();
    let c = p.stats().snapshot()[0].counts;
    (n, (c.produced, c.filtered, c.tombstones_skipped))
}

#[test]
fn no_null_value_record_is_produced_under_a_key_shared_by_other_rows() {
    let row_route = |r: KafkaRoute| KafkaRoute {
        format: Some("row".into()),
        pattern: "public.*".into(),
        ..r
    };
    let env_route = |r: KafkaRoute| KafkaRoute {
        tombstones: true,
        pattern: "public.*".into(),
        ..r
    };
    let plain = || named("o", "public.*", "t");
    let ops = |f: FieldSpec| with_fields("o", "public.*", f);
    let mut no_pk = row("orders", Op::Delete, r#"{"id":1,"email":"a@b.c"}"#);
    no_pk.key = None;
    let composite = {
        let mut e = row(
            "orders",
            Op::Delete,
            r#"{"id":1,"tenant":2,"email":"a@b.c"}"#,
        );
        e.key = Some(r#"{"id":1,"tenant":2}"#.into());
        e
    };
    let one_col = |c: &str| FieldSpec {
        exclude: Some(strs(&[c])),
        ..Default::default()
    };
    // (case, route base, event, per-row?)
    let cases: Vec<(&str, KafkaRoute, ChangeEvent, bool)> = vec![
        ("primary key", plain(), composite.clone(), true),
        (
            "no primary key (FULL or none)",
            plain(),
            no_pk.clone(),
            false,
        ),
        (
            "route key present",
            KafkaRoute {
                key: Some(strs(&["email"])),
                ..plain()
            },
            no_pk.clone(),
            true,
        ),
        (
            "route key absent from the before-image",
            KafkaRoute {
                key: Some(strs(&["region"])),
                ..plain()
            },
            composite.clone(),
            false,
        ),
        (
            "one route key column null",
            KafkaRoute {
                key: Some(strs(&["id", "region"])),
                ..plain()
            },
            row("orders", Op::Delete, r#"{"id":1,"region":null}"#),
            false,
        ),
        (
            "every key column excluded",
            ops(one_col("id")),
            row("orders", Op::Delete, r#"{"id":1}"#),
            false,
        ),
        (
            "one column of a composite key excluded",
            ops(one_col("tenant")),
            composite.clone(),
            false,
        ),
        (
            "a key column not included",
            ops(FieldSpec {
                include: Some(strs(&["id", "email"])),
                ..Default::default()
            }),
            composite.clone(),
            false,
        ),
        (
            "a key column masked",
            ops(FieldSpec {
                mask: strs(&["id"]),
                ..Default::default()
            }),
            composite.clone(),
            false,
        ),
        (
            "a key column hashed (HMAC keeps it unique)",
            ops(FieldSpec {
                hash: strs(&["id"]),
                ..Default::default()
            }),
            composite.clone(),
            true,
        ),
        (
            "a key column renamed",
            ops(FieldSpec {
                rename: [("tenant".to_string(), "org".to_string())].into(),
                ..Default::default()
            }),
            composite.clone(),
            true,
        ),
        (
            "another column excluded",
            ops(one_col("email")),
            composite.clone(),
            true,
        ),
    ];
    for (case, base, del, per_row) in cases {
        // Row format, `deletes = "tombstone"`: the delete is the null-value record, so a
        // shared key drops it (filtered) and counts the skipped tombstone.
        let got = delete_outcome(row_route(base.clone()), &del);
        let want = if per_row {
            (1, (1, 0, 0))
        } else {
            (0, (0, 1, 1))
        };
        assert_eq!(got, want, "row format: {case}");
        // Envelope: the delete is always produced, its tombstone only under a per-row key.
        let got = delete_outcome(env_route(base), &del);
        let want = if per_row {
            (2, (1, 0, 0))
        } else {
            (1, (1, 0, 1))
        };
        assert_eq!(got, want, "envelope: {case}");
    }
    // `deletes = "row"` and `"drop"` produce no null value: no check.
    let r = KafkaRoute {
        deletes: Some("row".into()),
        ..row_route(plain())
    };
    assert_eq!(delete_outcome(r, &no_pk), (1, (1, 0, 0)));
}

#[test]
fn row_format_tombstones_need_a_per_row_key_at_startup() {
    let p = RoutePlan::new(vec![shaped("r", Some("row"), None)], None).unwrap();
    let e = p
        .check_partition_by(PartitionBy::Table)
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("`format = \"row\"` with `deletes = \"tombstone\"`")
            && e.contains("per-row key"),
        "{e}"
    );
    for ok in [
        shaped("r", Some("row"), Some("drop")),
        shaped("r", Some("row"), Some("row")),
        KafkaRoute {
            key: Some(strs(&["id"])),
            ..shaped("r", Some("row"), None)
        },
    ] {
        let p = RoutePlan::new(vec![ok], None).unwrap();
        assert!(p.check_partition_by(PartitionBy::Table).is_ok());
    }
}

#[test]
fn a_change_whose_after_is_only_the_key_is_no_row() {
    // As the MongoDB mapping builds it for an update without a fullDocument: `after` is the
    // documentKey and the event says so.
    let mut e = row("orders", Op::Update, r#"{"_id":1}"#);
    e.after_is_key = true;
    let p = plan_with(vec![shaped("rows", Some("row"), None)]);
    assert!(p.render(&e, NOW).unwrap().is_empty(), "not a row");
    let c = p.stats().snapshot()[0].counts;
    assert_eq!((c.matched, c.filtered), (1, 1));
    // The envelope is unchanged: `after` is the key, as before.
    let p = plan_with(vec![named("env", "public.*", "t")]);
    let r = p.render(&e, NOW).unwrap().remove(0);
    assert_eq!(json(&r.value)["after"], serde_json::json!({"_id": 1}));
}

#[test]
fn unchanged_dead_letter_ignores_dropped_columns_and_escapes_names() {
    let route = |f: FieldSpec| KafkaRoute {
        unchanged: Some("dead_letter".into()),
        ..with_fields("docs", "public.*", f)
    };
    let mut upd = row("docs", Op::Update, r#"{"id":1}"#);
    upd.unchanged = vec!["blob".into()];
    // `blob` is excluded (or not included): its absence changes nothing the route sends.
    for f in [
        FieldSpec {
            exclude: Some(strs(&["blob"])),
            ..Default::default()
        },
        FieldSpec {
            include: Some(strs(&["id"])),
            ..Default::default()
        },
    ] {
        let p = fields_plan(vec![route(f)]);
        assert_eq!(p.render(&upd, NOW).unwrap()[0].topic, "docs.topic");
    }
    // A masked column is still sent, so its absence counts.
    let p = fields_plan(vec![route(FieldSpec {
        mask: strs(&["blob"]),
        ..Default::default()
    })]);
    assert_eq!(p.render(&upd, NOW).unwrap()[0].topic, DLQ);
    // Control characters and backticks in a name cannot forge the header or the log.
    upd.unchanged = vec!["a`b\nc\u{1b}".into()];
    let p = plan_with(vec![KafkaRoute {
        unchanged: Some("dead_letter".into()),
        ..named("docs", "public.*", "t")
    }]);
    let r = p.render(&upd, NOW).unwrap().remove(0);
    assert!(
        r.headers[0].1.ends_with("column(s) `a?b?c?`"),
        "{}",
        r.headers[0].1
    );
}

// --- dead-letter topics per route: `<topic>.dlq` by default (2026-10-21) --------------------

/// Every route's dead-letter topic as described, routes first, `default` last.
fn dlqs(p: &RoutePlan) -> Vec<Option<String>> {
    p.describe()
        .routes
        .into_iter()
        .map(|r| r.dead_letter_topic)
        .collect()
}

fn some(v: &[&str]) -> Vec<Option<String>> {
    v.iter().map(|s| Some(s.to_string())).collect()
}

/// A route on `public.<name>` whose `where` reads row data, so it dead-letters `{"n":"x"}`.
fn failing(name: &str, topic: &str) -> KafkaRoute {
    KafkaRoute {
        filter: Some("row.n > 1".into()),
        ..named(name, &format!("public.{name}"), topic)
    }
}

#[test]
fn each_route_dead_letters_to_its_own_topic_plus_dlq_by_default() {
    let p = RoutePlan::new(
        vec![
            // A static topic.
            named("assets", "sim.assets", "sim.assets.active"),
            // A `{schema}`/`{table}` template: rendered per event, then suffixed.
            named("cdc", "public.*", "cdc.{schema}.{table}"),
            // `{op}` is dropped with one separator: no per-operation dead-letter topic.
            named("ops", "hr.*", "hr.{table}.{op}"),
            // A content-routed template: the row is the problem, so `<topic_fallback>.dlq`.
            content(
                "orders",
                "orders.{row.region}",
                Some("orders.unknown"),
                None,
            ),
            // An outbox: `<outbox topic_fallback>.dlq`, or its static topic plus `.dlq`.
            outbox_route(outbox_spec()),
            KafkaRoute {
                name: Some("ob2".into()),
                pattern: "public.outbox2".into(),
                outbox: Some(OutboxSpec {
                    topic: "events".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        // The `default` route follows the same rules.
        Some("all.{schema}.{table}".into()),
    )
    .unwrap();
    assert_eq!(
        dlqs(&p),
        some(&[
            "sim.assets.active.dlq",
            "cdc.{schema}.{table}.dlq",
            "hr.{table}.dlq",
            "orders.unknown.dlq",
            "outbox.unknown.dlq",
            "events.dlq",
            "all.{schema}.{table}.dlq",
        ])
    );
    // No global override.
    assert_eq!(p.describe().dead_letter_topic, None);
    // A template is rendered for the event, like the route's topic, `{op}` left out.
    let p = RoutePlan::new(vec![failing("orders", "cdc.{schema}.{table}.{op}")], None).unwrap();
    for op in [Op::Insert, Op::Update] {
        let out = p.render(&row("orders", op, r#"{"n":"x"}"#), NOW).unwrap();
        assert_eq!(out[0].topic, "cdc.public.orders.dlq");
    }
    // A bad row of a content route goes to its fallback's dead-letter topic.
    let q = RoutePlan::new(
        vec![KafkaRoute {
            filter: Some("row.n > 1".into()),
            ..content(
                "orders",
                "orders.{row.region}",
                Some("orders.unknown"),
                None,
            )
        }],
        None,
    )
    .unwrap();
    let out = q
        .render(
            &row("orders", Op::Insert, r#"{"n":"x","region":"eu"}"#),
            NOW,
        )
        .unwrap();
    assert_eq!(out[0].topic, "orders.unknown.dlq");
    assert_eq!(out[0].headers[0].0, "aqueduct-error");
    assert_eq!(q.stats().snapshot()[0].counts.errors, 1);
}

#[test]
fn dead_letter_topic_precedence_is_route_then_sink_then_suffix() {
    let own = |name: &str, dlq: &str| KafkaRoute {
        dead_letter_topic: Some(dlq.into()),
        ..failing(name, &format!("{name}.t"))
    };
    let routes = || vec![own("a", "a.errors"), failing("b", "b.t")];
    let p = RoutePlan::new(routes(), Some("rest".into())).unwrap();
    assert_eq!(dlqs(&p), some(&["a.errors", "b.t.dlq", "rest.dlq"]));
    let p =
        RoutePlan::with_dead_letter(routes(), Some("rest".into()), Some("all.dlq".into())).unwrap();
    assert_eq!(dlqs(&p), some(&["a.errors", "all.dlq", "all.dlq"]));
    assert_eq!(p.describe().dead_letter_topic.as_deref(), Some("all.dlq"));
    // Records follow it.
    for (table, want) in [("a", "a.errors"), ("b", "all.dlq")] {
        let out = p
            .render(&row(table, Op::Insert, r#"{"n":"x"}"#), NOW)
            .unwrap();
        assert_eq!(out[0].topic, want);
    }
    // The fixed ones of routes built to dead-letter are opened at startup, once each.
    assert_eq!(p.dead_letter_topics(), ["a.errors", "all.dlq"]);
    let p = RoutePlan::new(
        vec![
            failing("a", "a.t"),
            own("b", "a.t.dlq"),
            named("plain", "x.*", "plain"),
            failing("tpl", "y.{table}"),
        ],
        None,
    )
    .unwrap();
    // `plain` cannot dead-letter, and `y.{table}.dlq` is created on first use.
    assert_eq!(p.dead_letter_topics(), ["a.t.dlq"]);
    // The inspector reads every one, templates as globs.
    assert_eq!(
        p.topic_globs(),
        [
            "a.t",
            "b.t",
            "plain",
            "y.*",
            "a.t.dlq",
            "plain.dlq",
            "y.*.dlq"
        ]
    );
}

#[test]
fn dead_letter_topics_cannot_be_reached_by_row_values() {
    let build = |routes: Vec<KafkaRoute>, default: Option<&str>, dlq: Option<&str>| {
        RoutePlan::with_dead_letter(routes, default.map(Into::into), dlq.map(Into::into))
            .map(|p| dlqs(&p))
            .map_err(|e| e.to_string())
    };
    let ok = || content("o", "orders.{row.r}", Some("orders.unknown"), None);
    // `orders.{row.r}` reads as the glob `orders.*`, which matches `orders.unknown.dlq` and
    // `orders.dlq`; but a row value is one segment without dots and never `dlq`, so it can
    // render neither: the content route and a static `orders` route keep their defaults.
    assert_eq!(
        build(vec![ok(), failing("s", "orders")], None, None),
        Ok(some(&["orders.unknown.dlq", "orders.dlq"]))
    );
    assert_eq!(
        build(vec![ok()], None, Some("orders.dlq")),
        Ok(some(&["orders.dlq"]))
    );
    // A name a row value, a table or an operation can render is refused, and so are names
    // that break the other rules: as a route's override, the global one, or a default.
    type Case<'a> = (Vec<KafkaRoute>, Option<&'a str>, Option<&'a str>, &'a str);
    let cases: Vec<Case> = vec![
        (
            vec![
                ok(),
                KafkaRoute {
                    dead_letter_topic: Some("orders.eu".into()),
                    ..failing("s", "s.t")
                },
            ],
            None,
            None,
            "sink route `s`: `dead_letter_topic` `orders.eu` is also a topic events are routed to (`orders.{row.r}`)",
        ),
        (
            vec![ok()],
            None,
            Some("orders.eu"),
            "sink.dead_letter_topic `orders.eu` is also a topic events are routed to",
        ),
        // A content template with a fixed `.dlq` suffix renders `orders.x.dlq`, the default
        // of a static `orders.x` route, and `orders.unknown.dlq`, its own fallback's.
        (
            vec![
                content("o", "orders.{row.r}.dlq", Some("fb"), None),
                failing("s", "orders.x"),
            ],
            None,
            None,
            "sink route `s`: the dead-letter topic (its `topic` plus `.dlq`) `orders.x.dlq` is also a topic events are routed to (`orders.{row.r}.dlq`); set `dead_letter_topic` on the route or `sink.dead_letter_topic`",
        ),
        (
            vec![KafkaRoute {
                filter: Some("row.n > 1".into()),
                ..content("o", "orders.{row.r}.dlq", Some("orders.unknown"), None)
            }],
            None,
            None,
            "the dead-letter topic (its `topic_fallback` plus `.dlq`) `orders.unknown.dlq` is also a topic events are routed to",
        ),
        // `{table}` stands for one name: a table named `dlq` would write into `cdc.dlq`.
        (
            vec![failing("s", "s.t")],
            Some("cdc.{table}"),
            Some("cdc.dlq"),
            "sink.dead_letter_topic `cdc.dlq` is also a topic events are routed to (`cdc.{table}`)",
        ),
        (
            vec![failing("s", "s.t"), named("ins", "i.*", "s.{op}")],
            None,
            Some("s.insert"),
            "is also a topic events are routed to (`s.{op}`)",
        ),
        // A rendered dead-letter template must not overlap a content-routed template.
        (
            vec![
                content("o", "p.{row.r}.x.dlq", Some("fb"), None),
                failing("s", "p.{table}.x"),
            ],
            None,
            None,
            "`p.{table}.x.dlq` (as the glob `p.*.x.dlq`) overlaps the content-routed `topic` `p.{row.r}.x.dlq`",
        ),
        (
            vec![KafkaRoute {
                dead_letter_topic: Some("bad topic".into()),
                ..failing("s", "s.t")
            }],
            None,
            None,
            "sink route `s`: `dead_letter_topic` `bad topic` is not a valid Kafka topic name",
        ),
        (
            vec![KafkaRoute {
                dead_letter_topic: Some("{table}.dlq".into()),
                ..failing("s", "s.t")
            }],
            None,
            None,
            "is not a valid Kafka topic name",
        ),
        (
            vec![failing("s", "_s.t")],
            None,
            None,
            "sink route `s`: the dead-letter topic (its `topic` plus `.dlq`) `_s.t.dlq` must not start with `_`",
        ),
        (
            vec![KafkaRoute {
                dead_letter_topic: Some("fb".into()),
                ..content("o", "o.{row.r}", Some("fb"), None)
            }],
            None,
            None,
            "`dead_letter_topic` `fb` is also a route's `topic_fallback`",
        ),
        (
            vec![KafkaRoute {
                dead_letter_topic: Some("t".into()),
                ..failing("s", "s.t")
            }],
            Some("t"),
            None,
            "`dead_letter_topic` `t` is also a topic events are routed to (`t`)",
        ),
        (
            vec![failing("s", &format!("s.{}", "x".repeat(244)))],
            None,
            None,
            "is not a valid Kafka topic name",
        ),
    ];
    for (routes, default, dlq, want) in cases {
        let err = build(routes, default, dlq).expect_err(want);
        assert!(err.contains(want), "{err}\nshould contain: {want}");
    }
    // A route that cannot dead-letter gets no dead-letter topic rather than a startup error.
    assert_eq!(
        build(vec![named("p", "p.*", "_p")], None, None),
        Ok(vec![None])
    );
}

#[test]
fn dlq_is_a_reserved_row_value() {
    // `a.{row.x}.{row.y}` would render `a.foo.dlq` for y = "dlq": such rows go to the fallback.
    let p = RoutePlan::new(
        vec![
            content("o", "a.{row.x}.{row.y}", Some("a.unknown"), None),
            failing("s", "a.foo"),
        ],
        None,
    )
    .unwrap();
    assert_eq!(dlqs(&p), some(&["a.unknown.dlq", "a.foo.dlq"]));
    let topic = |json: &str| {
        p.render(&row("orders", Op::Insert, json), NOW).unwrap()[0]
            .topic
            .clone()
    };
    assert_eq!(topic(r#"{"x":"foo","y":"dlq"}"#), "a.unknown");
    assert_eq!(topic(r#"{"x":"dlq","y":"foo"}"#), "a.unknown");
    // Topic names are case-sensitive, so only `dlq` itself is reserved.
    assert_eq!(topic(r#"{"x":"foo","y":"DLQ"}"#), "a.foo.DLQ");
    assert_eq!(p.stats().snapshot()[0].counts.fallback, 2);
    // Listing it in `topic_values` is refused.
    let e = RoutePlan::new(
        vec![content(
            "o",
            "a.{row.x}",
            Some("a.unknown"),
            Some(&["eu", "dlq"]),
        )],
        None,
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    assert!(
        e.contains("`topic_values` entry `dlq` can never be used: it is reserved"),
        "{e}"
    );
    // Dead-letter topics already on the broker are not counted as content-routed.
    let q = content_plan(None, 100);
    assert_eq!(
        q.add_existing_topics(["orders.eu", "orders.unknown.dlq", "orders.dlq", "rest.dlq"]),
        1
    );
}

#[test]
fn a_rendered_dead_letter_name_kafka_would_refuse_fails_the_batch() {
    let p = RoutePlan::new(
        vec![KafkaRoute {
            filter: Some("row.n > 1".into()),
            ..named("r", "*", "{schema}.{table}")
        }],
        None,
    )
    .unwrap();
    let mut e = row("orders", Op::Insert, r#"{"n":"x"}"#);
    e.schema = "_internal".into();
    let err = p.render(&e, NOW).unwrap_err().to_string();
    assert!(
        err.contains("its dead-letter topic `_internal.orders.dlq` is not a usable topic name"),
        "{err}"
    );
}

#[test]
fn without_op_drops_the_placeholder_and_one_separator() {
    for (raw, want) in [
        ("cdc.{table}.{op}", "cdc.{table}"),
        ("{op}.cdc.{table}", "cdc.{table}"),
        ("cdc.{op}.{table}", "cdc.{table}"),
        ("cdc_{op}_{table}", "cdc_{table}"),
        ("cdc{op}{table}", "cdc{table}"),
        ("a.{op}.{op}.b", "a.b"),
        ("{op}", "{op}"),
        ("cdc.{table}", "cdc.{table}"),
    ] {
        assert_eq!(without_op(raw), want, "{raw}");
    }
}
