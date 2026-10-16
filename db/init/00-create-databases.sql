CREATE ROLE loan_app LOGIN PASSWORD 'local-app-password';
CREATE ROLE n8n LOGIN PASSWORD 'local-n8n-password';

CREATE DATABASE loan_engine OWNER loan_app;
CREATE DATABASE n8n OWNER n8n;

\connect loan_engine
\ir /opt/loan-engine/schema.sql

GRANT USAGE ON SCHEMA public TO loan_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO loan_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO loan_app;

