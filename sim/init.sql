-- Industrial demo schema: organizations own sites and assets (pumps, compressors, tools); assets emit readings
-- and get work orders and maintenance events. No foreign keys on purpose: the simulator deletes freely to stay bounded.
CREATE TABLE organizations (
  id serial PRIMARY KEY,
  name text NOT NULL,
  kind text NOT NULL,            -- operator | service_partner | manufacturer
  country text NOT NULL
);
CREATE TABLE sites (
  id serial PRIMARY KEY,
  organization_id int NOT NULL,
  name text NOT NULL,
  city text NOT NULL
);
CREATE TABLE assets (
  id serial PRIMARY KEY,
  serial_number text NOT NULL,
  asset_type text NOT NULL,      -- pump | compressor | tool
  model text NOT NULL,
  status text NOT NULL DEFAULT 'idle',   -- running | idle | fault | retired
  organization_id int NOT NULL,
  site_id int NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE asset_readings (
  id bigserial PRIMARY KEY,
  asset_id int NOT NULL,
  metric text NOT NULL,          -- pressure_bar | flow_lpm | temperature_c | vibration_mms | torque_nm
  value double precision NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE work_orders (
  id serial PRIMARY KEY,
  asset_id int NOT NULL,
  kind text NOT NULL,            -- corrective | preventive | inspection
  priority text NOT NULL,        -- low | normal | high
  status text NOT NULL DEFAULT 'open',   -- open | in_progress | closed
  opened_at timestamptz NOT NULL DEFAULT now(),
  closed_at timestamptz
);
CREATE TABLE maintenance_events (
  id bigserial PRIMARY KEY,
  work_order_id int NOT NULL,
  asset_id int NOT NULL,
  event_type text NOT NULL,      -- opened | started | part_replaced | closed
  note text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Seed: 6 organizations, 2 sites each, 60 assets spread across them.
INSERT INTO organizations(name, kind, country) VALUES
  ('Nordwind Water Utilities', 'operator',        'DE'),
  ('Kestrel Process Plants',   'operator',        'NL'),
  ('Harbor Steelworks',        'operator',        'SE'),
  ('Tecnoservice SpA',         'service_partner', 'IT'),
  ('Pioneer Field Services',   'service_partner', 'US'),
  ('Atlas Industrial OEM',     'manufacturer',    'BE');
INSERT INTO sites(organization_id, name, city)
  SELECT o.id, o.name || ' - ' || s.label, s.city
  FROM organizations o
  CROSS JOIN (VALUES ('Plant A', 'Hamburg'), ('Depot B', 'Rotterdam')) AS s(label, city);
INSERT INTO assets(serial_number, asset_type, model, status, organization_id, site_id)
  SELECT (ARRAY['PMP','CMP','TLS'])[1 + n % 3] || '-' || lpad(n::text, 5, '0'),
         (ARRAY['pump','compressor','tool'])[1 + n % 3],
         (ARRAY['P-300','C-75','T-12'])[1 + n % 3] || (ARRAY['A','B','X'])[1 + (n / 3) % 3],
         (ARRAY['running','running','idle','fault'])[1 + n % 4],
         s.organization_id, s.id
  FROM generate_series(1, 60) n
  JOIN (SELECT id, organization_id, row_number() OVER (ORDER BY id) - 1 AS rn FROM sites) s ON s.rn = n % 12;

ALTER TABLE organizations REPLICA IDENTITY FULL;
ALTER TABLE sites REPLICA IDENTITY FULL;
ALTER TABLE assets REPLICA IDENTITY FULL;
ALTER TABLE asset_readings REPLICA IDENTITY FULL;
ALTER TABLE work_orders REPLICA IDENTITY FULL;
ALTER TABLE maintenance_events REPLICA IDENTITY FULL;
CREATE PUBLICATION aqueduct FOR TABLE organizations, sites, assets, asset_readings, work_orders, maintenance_events;
