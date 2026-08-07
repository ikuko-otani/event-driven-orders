-- Runs once, on first initialisation of the postgres data volume.
-- Design §3.1: one PostgreSQL instance, two schemas, no cross-schema access.
CREATE SCHEMA IF NOT EXISTS orders;
CREATE SCHEMA IF NOT EXISTS inventory;
