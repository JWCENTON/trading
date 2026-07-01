CREATE OR REPLACE VIEW v_orc_kv_compact AS
WITH src AS (
  SELECT
    key,
    value,
    updated_at,
    CASE
      WHEN value IS NOT NULL AND left(trim(value), 1) = '{'
        THEN value::jsonb
      ELSE NULL::jsonb
    END AS j
  FROM automation_kv
  WHERE key LIKE 'orc%'
)
SELECT
  key,
  updated_at,
  CASE
    WHEN value IS NULL THEN NULL
    WHEN j IS NOT NULL THEN 'json'
    ELSE 'text'
  END AS value_type,
  j->>'orc_version' AS orc_version,
  j->>'orc_mode' AS orc_mode,
  j->>'picks_view' AS picks_view,
  NULLIF(j->>'core_picks_n','')::int AS core_picks_n,
  NULLIF(j->>'explore_picks_n','')::int AS explore_picks_n,
  NULLIF(j->>'want_on_n','')::int AS want_on_n,
  NULLIF(j->>'touched_on','')::int AS touched_on,
  NULLIF(j->>'touched_off','')::int AS touched_off,
  NULLIF(j->>'universe_n','')::int AS universe_n,
  CASE
    WHEN value IS NULL THEN NULL
    WHEN length(value) <= 220 THEN value
    ELSE left(value, 220) || ' ...'
  END AS value_short
FROM src;
