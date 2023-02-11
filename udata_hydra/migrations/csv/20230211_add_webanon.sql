-- add a webanon role for postgrest integration (not idempotent)
-- TODO: we might want a dedicated schema for CSVs

DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'web_anon') THEN
        CREATE ROLE web_anon nologin;
        GRANT usage ON SCHEMA public TO web_anon;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO web_anon;
   END IF;
END
$do$;
