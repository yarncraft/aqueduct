CREATE TABLE users    (id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL, plan text NOT NULL DEFAULT 'free');
CREATE TABLE orders   (id serial PRIMARY KEY, user_id int NOT NULL, status text NOT NULL DEFAULT 'new', total_cents int NOT NULL);
CREATE TABLE payments (id serial PRIMARY KEY, order_id int NOT NULL, amount_cents int NOT NULL, method text NOT NULL);
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
CREATE PUBLICATION aqueduct FOR TABLE users, orders, payments;
