#!/bin/bash
# Endless workload: a few random inserts/updates/deletes per second across users, orders, payments.
# RATE = loop iterations per second (default 2). Each iteration is one short transaction.
export PGHOST=postgres PGUSER=aqueduct PGPASSWORD=aqueduct PGDATABASE=app
RATE="${RATE:-2}"
until pg_isready -q; do sleep 1; done
echo "simulator: running at ~${RATE} tx/s"
while true; do
  psql -q -v ON_ERROR_STOP=0 <<'SQL'
BEGIN;
INSERT INTO users(name, email, plan)
  SELECT 'user' || n, 'user' || n || '@example.com', (ARRAY['free','pro','team'])[1 + floor(random()*3)::int]
  FROM (SELECT floor(random()*100000)::int AS n) s WHERE random() < 0.2;
INSERT INTO orders(user_id, total_cents)
  SELECT id, 500 + floor(random()*20000)::int FROM users ORDER BY random() LIMIT 1;
UPDATE orders SET status = (ARRAY['paid','shipped','delivered','cancelled'])[1 + floor(random()*4)::int]
  WHERE id = (SELECT id FROM orders WHERE status IN ('new','paid','shipped') ORDER BY random() LIMIT 1);
INSERT INTO payments(order_id, amount_cents, method)
  SELECT id, total_cents, (ARRAY['card','paypal','sepa'])[1 + floor(random()*3)::int]
  FROM orders WHERE status = 'paid' ORDER BY random() LIMIT 1;
UPDATE users SET plan = (ARRAY['free','pro','team'])[1 + floor(random()*3)::int]
  WHERE id = (SELECT id FROM users ORDER BY random() LIMIT 1) AND random() < 0.3;
DELETE FROM payments WHERE id IN (SELECT id FROM payments ORDER BY random() LIMIT 1) AND random() < 0.1;
-- keep the tables bounded so the sim can run for days
DELETE FROM orders WHERE id < (SELECT max(id) - 2000 FROM orders);
DELETE FROM payments WHERE id < (SELECT max(id) - 2000 FROM payments);
COMMIT;
SQL
  sleep "$(awk "BEGIN{print 1/$RATE}")"
done
