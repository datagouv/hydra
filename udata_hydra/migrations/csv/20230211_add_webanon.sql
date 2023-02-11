-- add a webanon role for postgrest integration
-- TODO: we might want a dedicated schema for CSVs

CREATE ROLE web_anon nologin;
GRANT usage ON SCHEMA public TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO web_anon;
