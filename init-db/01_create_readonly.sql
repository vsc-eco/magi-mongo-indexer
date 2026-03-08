-- ===============================================
-- Create a dedicated read-only user for Hasura
-- ===============================================
CREATE USER gql_reader WITH PASSWORD 'readonlypass';

-- ===============================================
-- Grant minimal privileges (principle of least privilege)
-- ===============================================

-- Allow connection to the indexer database
GRANT CONNECT ON DATABASE indexerdb TO gql_reader;

-- Allow usage of the public schema (but no write privileges)
GRANT USAGE ON SCHEMA public TO gql_reader;

-- Allow read access (SELECT) on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gql_reader;

-- Ensure future tables created in public schema automatically
-- grant SELECT permission to this user
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO gql_reader;
