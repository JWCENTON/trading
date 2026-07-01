CREATE OR REPLACE VIEW v_orc_kv_compact AS
SELECT
  key,
  updated_at,
  CASE
    WHEN value IS NULL THEN NULL
    WHEN left(trim(value), 1) = '{' THEN 'json'
    ELSE 'text'
  END AS value_type,
  value::jsonb->>'orc_version' AS orc_version,
  value::jsonb->>'orc_mode' AS orc_mode,
  value::jsonb->>'picks_view' AS picks_view,
  NULLIF(value::jsonb->>'core_picks_n','')::int AS core_picks_n,
  NULLIF(value::jsonb->>'explore_picks_n','')::int AS explore_picks_n,
  NULLIF(value::jsonb->>'want_on_n','')::int AS want_on_n,
  NULLIF(value::jsonb->>'touched_on','')::int AS touched_on,
  NULLIF(value::jsonb->>'touched_off','')::int AS touched_off,
  NULLIF(value::jsonb->>'universe_n','')::int AS universe_n,
  CASE
    WHEN length(value) <= 220 THEN value
    ELSE left(value, 220) || ' ...'
  END AS value_short
FROM automation_kv
WHERE key LIKE 'orc%';
