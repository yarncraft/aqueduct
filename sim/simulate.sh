#!/bin/bash
# Endless industrial workload: asset events, asset status changes, work orders opened/closed, assets moving
# between organizations, assets commissioned/retired/removed. RATE = loop iterations per second (default 15);
# each iteration is one short transaction. Row counts are bounded so the sim can run for days.
export PGHOST=postgres PGUSER=aqueduct PGPASSWORD=aqueduct PGDATABASE=app
RATE="${RATE:-15}"
until pg_isready -q; do sleep 1; done
echo "simulator: running at ~${RATE} tx/s"
while true; do
  psql -q -v ON_ERROR_STOP=0 <<'SQL'
BEGIN;
-- asset and system events from two live assets (insert-only, capped below); maintenance events come with the work
-- orders below
INSERT INTO events(asset_id, type, kind, severity, detail)
  SELECT a.id, t.type, k.kind,
         CASE k.kind WHEN 'alarm' THEN 'critical' WHEN 'threshold_exceeded' THEN 'warning' ELSE 'info' END,
         jsonb_build_object('asset_type', a.asset_type, 'value', round((random() * 100)::numeric, 2))
  FROM assets a
  CROSS JOIN LATERAL (SELECT CASE WHEN random() + 0 * a.id < 0.7 THEN 'asset' ELSE 'system' END AS type) t
  CROSS JOIN LATERAL (SELECT CASE t.type
                        WHEN 'asset' THEN (ARRAY['heartbeat','heartbeat','heartbeat','threshold_exceeded','alarm'])[1 + floor(random()*5)::int]
                        ELSE (ARRAY['firmware_update','door_opened','config_synced'])[1 + floor(random()*3)::int] END AS kind) k
  WHERE a.status IN ('running','fault') ORDER BY random() LIMIT 2;
-- status change on a random live asset (fault is the least likely outcome)
UPDATE assets SET status = (ARRAY['running','running','running','idle','idle','fault'])[1 + floor(random()*6)::int],
                  updated_at = now()
  WHERE id = (SELECT id FROM assets WHERE status <> 'retired' ORDER BY random() LIMIT 1) AND random() < 0.3;
-- a faulted asset without an open work order gets a corrective one
WITH target AS (
  SELECT a.id FROM assets a
  WHERE a.status = 'fault' AND NOT EXISTS (SELECT 1 FROM work_orders w WHERE w.asset_id = a.id AND w.status <> 'closed')
  ORDER BY random() LIMIT 1
), wo AS (
  INSERT INTO work_orders(asset_id, kind, priority)
    SELECT id, 'corrective', (ARRAY['normal','high','high'])[1 + floor(random()*3)::int] FROM target
    RETURNING id, asset_id
)
INSERT INTO events(work_order_id, asset_id, type, kind, severity, detail)
  SELECT id, asset_id, 'maintenance', 'opened', 'info', jsonb_build_object('note', 'fault detected') FROM wo;
-- occasional scheduled work on a healthy asset
WITH wo AS (
  INSERT INTO work_orders(asset_id, kind, priority)
    SELECT a.id, (ARRAY['preventive','inspection'])[1 + floor(random()*2)::int], 'low'
    FROM assets a WHERE a.status IN ('running','idle') AND random() < 0.5
      AND NOT EXISTS (SELECT 1 FROM work_orders w WHERE w.asset_id = a.id AND w.status <> 'closed')
      AND (SELECT count(*) FROM work_orders WHERE status <> 'closed') < 25
      AND (SELECT random()) < 0.05
    ORDER BY random() LIMIT 1
    RETURNING id, asset_id
)
INSERT INTO events(work_order_id, asset_id, type, kind, severity, detail)
  SELECT id, asset_id, 'maintenance', 'opened', 'info', jsonb_build_object('note', 'scheduled') FROM wo;
-- work starts, parts get replaced
WITH wo AS (
  UPDATE work_orders SET status = 'in_progress'
    WHERE id = (SELECT id FROM work_orders WHERE status = 'open' ORDER BY random() LIMIT 1) AND random() < 0.5
    RETURNING id, asset_id
)
INSERT INTO events(work_order_id, asset_id, type, kind, severity, detail)
  SELECT id, asset_id, 'maintenance', 'started', 'info', jsonb_build_object('note', 'technician on site') FROM wo;
INSERT INTO events(work_order_id, asset_id, type, kind, severity, detail)
  SELECT id, asset_id, 'maintenance', 'part_replaced', 'info', jsonb_build_object('note', (ARRAY['seal','bearing','filter','valve'])[1 + floor(random()*4)::int])
  FROM work_orders WHERE status = 'in_progress' AND random() < 0.3 ORDER BY random() LIMIT 1;
-- work orders close; a corrective close brings a faulted asset back to running
WITH closed AS (
  UPDATE work_orders SET status = 'closed', closed_at = now()
    WHERE id = (SELECT id FROM work_orders WHERE status = 'in_progress' ORDER BY random() LIMIT 1) AND random() < 0.4
    RETURNING id, asset_id, kind
), fixed AS (
  UPDATE assets SET status = 'running', updated_at = now()
    WHERE id IN (SELECT asset_id FROM closed WHERE kind = 'corrective') AND status = 'fault'
)
INSERT INTO events(work_order_id, asset_id, type, kind, severity, detail)
  SELECT id, asset_id, 'maintenance', 'closed', 'info', jsonb_build_object('note', 'verified and released') FROM closed;
-- an asset moves to another organization (and one of its sites)
UPDATE assets a SET organization_id = s.organization_id, site_id = s.id, updated_at = now()
  FROM (SELECT id, organization_id FROM sites ORDER BY random() LIMIT 1) s
  WHERE a.id = (SELECT id FROM assets WHERE status <> 'retired' ORDER BY random() LIMIT 1)
    AND a.organization_id <> s.organization_id AND random() < 0.1;
-- lifecycle: retire, remove retired, commission replacements (keeps ~60 live assets)
UPDATE assets SET status = 'retired', updated_at = now()
  WHERE id = (SELECT id FROM assets WHERE status <> 'retired' ORDER BY random() LIMIT 1) AND random() < 0.03;
DELETE FROM assets WHERE id IN (SELECT id FROM assets WHERE status = 'retired' ORDER BY random() LIMIT 1) AND random() < 0.3;
INSERT INTO assets(serial_number, asset_type, model, status, organization_id, site_id)
  SELECT (ARRAY['PMP','CMP','TLS'])[t.i] || '-' || lpad((100000 + floor(random()*900000)::int)::text, 6, '0'),
         (ARRAY['pump','compressor','tool'])[t.i],
         (ARRAY['P-300','C-75','T-12'])[t.i] || (ARRAY['A','B','X'])[1 + floor(random()*3)::int],
         'idle', s.organization_id, s.id
  FROM (SELECT 1 + floor(random()*3)::int AS i) t,
       (SELECT id, organization_id FROM sites ORDER BY random() LIMIT 1) s
  WHERE (SELECT count(*) FROM assets WHERE status <> 'retired') < 60 AND random() < 0.3;
-- keep the tables bounded so the sim can run for days
DELETE FROM events WHERE id < (SELECT max(id) - 3000 FROM events);
DELETE FROM work_orders WHERE status = 'closed' AND id < (SELECT max(id) - 500 FROM work_orders);
COMMIT;
SQL
  sleep "$(awk "BEGIN{print 1/$RATE}")"
done
