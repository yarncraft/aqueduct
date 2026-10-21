//! Synthetic "Industrial Assets" CDC workload: a wide `industrial.assets`
//! table (asset registry + latest telemetry, ~90 columns, nested JSON,
//! free-text notes) with REPLICA IDENTITY FULL, so updates carry both the
//! before and after row.

use aqueduct_core::{ChangeEvent, Op, Position};
use serde_json::value::{to_raw_value, RawValue};
use serde_json::{json, Map, Value};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn f(&mut self, lo: f64, hi: f64) -> f64 {
        let u = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        ((lo + u * (hi - lo)) * 1000.0).round() / 1000.0
    }
    fn pick<'a>(&mut self, v: &[&'a str]) -> &'a str {
        v[(self.next() as usize) % v.len()]
    }
}

const METRICS: [&str; 40] = [
    "temp_c",
    "vibration_mm_s",
    "pressure_bar",
    "flow_m3_h",
    "rpm",
    "current_a",
    "voltage_v",
    "power_kw",
    "energy_kwh",
    "oil_temp_c",
    "bearing_temp_de_c",
    "bearing_temp_nde_c",
    "winding_temp_u_c",
    "winding_temp_v_c",
    "winding_temp_w_c",
    "humidity_pct",
    "torque_nm",
    "efficiency_pct",
    "runtime_h",
    "starts_count",
    "load_pct",
    "speed_setpoint",
    "valve_pos_pct",
    "level_pct",
    "ph",
    "conductivity_us",
    "turbidity_ntu",
    "noise_db",
    "axial_disp_um",
    "radial_disp_x_um",
    "radial_disp_y_um",
    "phase_angle_deg",
    "thd_pct",
    "power_factor",
    "frequency_hz",
    "ambient_temp_c",
    "ambient_humidity_pct",
    "dp_filter_bar",
    "lube_level_pct",
    "health_score",
];

const WORDS: [&str; 17] = [
    "bearing",
    "inspected",
    "greased",
    "seal",
    "replaced",
    "vibration",
    "within",
    "limits",
    "alignment",
    "checked",
    "coupling",
    "worn",
    "scheduled",
    "follow-up",
    "operator",
    "reported",
    "noise",
];

fn asset_row(r: &mut Rng, id: u64) -> Map<String, Value> {
    let mut m = Map::new();
    let site = r.pick(&["ROT-01", "AMS-02", "ANT-03", "GRO-04", "EIN-05"]);
    let kind = r.pick(&["PMP", "MTR", "CMP", "FAN", "VLV", "GBX", "HX"]);
    m.insert(
        "asset_id".into(),
        json!(format!("a7c1e5d0-{:04x}-4b2a-9f3e-{:012x}", id % 65536, id)),
    );
    m.insert(
        "asset_tag".into(),
        json!(format!("{site}-{kind}-{:05}", id % 100000)),
    );
    m.insert("site_code".into(), json!(site));
    m.insert(
        "plant".into(),
        json!(r.pick(&["Chemical", "Utilities", "Packaging", "Wastewater"])),
    );
    m.insert(
        "production_line".into(),
        json!(format!("L{}", r.next() % 24)),
    );
    m.insert(
        "asset_type".into(),
        json!(r.pick(&[
            "centrifugal_pump",
            "induction_motor",
            "screw_compressor",
            "gearbox",
            "heat_exchanger",
            "control_valve"
        ])),
    );
    m.insert(
        "manufacturer".into(),
        json!(r.pick(&[
            "Siemens",
            "ABB",
            "Grundfos",
            "Atlas Copco",
            "Flowserve",
            "Emerson"
        ])),
    );
    m.insert(
        "model".into(),
        json!(format!(
            "{}-{}",
            r.pick(&["XT", "HV", "GA", "NK", "CR"]),
            100 + r.next() % 900
        )),
    );
    m.insert(
        "serial_number".into(),
        json!(format!("SN{:010}", r.next() % 10_000_000_000)),
    );
    m.insert(
        "firmware".into(),
        json!(format!(
            "{}.{}.{}",
            r.next() % 5,
            r.next() % 20,
            r.next() % 100
        )),
    );
    m.insert(
        "status".into(),
        json!(r.pick(&[
            "running",
            "running",
            "running",
            "standby",
            "maintenance",
            "fault"
        ])),
    );
    m.insert("criticality".into(), json!(r.pick(&["A", "B", "C"])));
    m.insert(
        "owner_team".into(),
        json!(r.pick(&["rotating-eq", "e-and-i", "process", "reliability"])),
    );
    m.insert(
        "install_date".into(),
        json!(format!(
            "20{:02}-{:02}-{:02}",
            8 + r.next() % 17,
            1 + r.next() % 12,
            1 + r.next() % 28
        )),
    );
    m.insert(
        "last_maintenance_ts".into(),
        json!(1_690_000_000_000_000i64 + (r.next() % 90_000_000_000_000) as i64),
    );
    m.insert(
        "next_maintenance_due".into(),
        json!(1_700_000_000_000_000i64 + (r.next() % 90_000_000_000_000) as i64),
    );
    let (lat, lon) = (r.f(51.8, 52.2), r.f(4.2, 4.8));
    m.insert(
        "location".into(),
        json!({
            "building": format!("B{}", r.next() % 12),
            "floor": r.next() % 4,
            "zone": r.pick(&["north", "south", "pump-hall", "boiler"]),
            "gps": { "lat": lat, "lon": lon }
        }),
    );
    m.insert(
        "tags".into(),
        json!([
            "critical-path",
            r.pick(&["atex", "outdoor", "indoor", "hygienic"]),
            r.pick(&["iso10816", "api610", "iec60034"])
        ]),
    );
    for name in METRICS {
        let v = match name {
            "temp_c" | "oil_temp_c" | "ambient_temp_c" => r.f(15.0, 95.0),
            "health_score" | "efficiency_pct" | "load_pct" => r.f(0.0, 100.0),
            _ => r.f(0.0, 5000.0),
        };
        m.insert(name.into(), json!(v));
    }
    for a in [
        "alarm_high_temp",
        "alarm_high_vibration",
        "alarm_low_pressure",
        "alarm_overcurrent",
        "alarm_comm_loss",
        "maintenance_overdue",
    ] {
        m.insert(a.into(), Value::Bool(r.next().is_multiple_of(20)));
    }
    let n = 15 + (r.next() % 70) as usize;
    let notes: Vec<&str> = (0..n)
        .map(|_| WORDS[(r.next() as usize) % WORDS.len()])
        .collect();
    m.insert("maintenance_notes".into(), json!(notes.join(" ")));
    let mode = r.pick(&["continuous", "burst"]);
    let unit = r.next() % 247;
    m.insert(
        "config".into(),
        json!({
            "limits": { "temp_c": { "warn": 80, "trip": 90 }, "vibration_mm_s": { "warn": 4.5, "trip": 7.1 } },
            "sampling": { "interval_ms": 1000, "mode": mode },
            "modbus": { "unit": unit, "baud": 19200, "registers": (0..8).map(|i| 40001 + i * 2).collect::<Vec<_>>() }
        }),
    );
    m.insert(
        "updated_at".into(),
        json!(1_700_000_000_000_000i64 + (r.next() % 1_000_000_000) as i64),
    );
    m
}

fn raw(row: Map<String, Value>) -> Box<RawValue> {
    to_raw_value(&row).expect("row serialises")
}

/// Mix: 30% insert, 68% update (full before + after, a few columns changed),
/// 2% delete. Rows are unique so the pool does not compress unrealistically
/// within itself.
pub fn pool(n: usize) -> Vec<ChangeEvent> {
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    (0..n)
        .map(|i| {
            let id = r.next() % 2_000_000;
            let row = asset_row(&mut r, id);
            let roll = r.next() % 100;
            let (op, before, after) = if roll < 30 {
                (Op::Insert, None, Some(row))
            } else if roll < 98 {
                let mut after = row.clone();
                after.insert(
                    "status".into(),
                    json!(r.pick(&["running", "fault", "maintenance"])),
                );
                after.insert("health_score".into(), json!(r.f(0.0, 100.0)));
                after.insert("temp_c".into(), json!(r.f(15.0, 95.0)));
                after.insert(
                    "updated_at".into(),
                    json!(1_700_000_000_000_000i64 + i as i64),
                );
                (Op::Update, Some(row), Some(after))
            } else {
                (Op::Delete, Some(row), None)
            };
            ChangeEvent {
                position: Position::Lsn(0),
                xid: (i / 20) as u32 + 1,
                commit_ts_micros: 1_700_000_000_000_000 + i as i64 * 100,
                schema: "industrial".into(),
                table: "assets".into(),
                op,
                before: before.map(raw),
                after: after.map(raw),
                key: None,
                checkpoint: None,
                unchanged: Vec::new(),
            }
        })
        .collect()
}
