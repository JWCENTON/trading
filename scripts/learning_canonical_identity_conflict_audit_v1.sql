\set ON_ERROR_STOP on

\if :{?audit_environment}
\else
  \set audit_environment trading_live
\endif
\if :{?window_days}
\else
  \set window_days 30
\endif

-- Read-only, one row per canonical EXCLUDED_CONFLICTING_IDENTITY key.
WITH conflicts AS (
    SELECT *
    FROM learning_canonical_evidence_universe_v1(
        :'audit_environment',
        now() - make_interval(days => :window_days),
        now(),
        now()
    )
    WHERE eligibility_reason = 'EXCLUDED_CONFLICTING_IDENTITY'
), registry AS (
    SELECT
        c.decision_key,
        count(r.*) AS registry_rows,
        count(DISTINCT r.decision_id) AS registry_id_count,
        count(DISTINCT r.deployment_id) AS deployment_count,
        count(DISTINCT r.environment) AS registry_environment_count,
        array_agg(DISTINCT r.decision_id ORDER BY r.decision_id)
            FILTER (WHERE r.decision_id IS NOT NULL) AS registry_decision_ids,
        array_agg(DISTINCT r.deployment_id ORDER BY r.deployment_id)
            FILTER (WHERE r.deployment_id IS NOT NULL) AS registry_deployments,
        array_agg(DISTINCT r.environment ORDER BY r.environment)
            FILTER (WHERE r.environment IS NOT NULL) AS registry_environments,
        array_agg(DISTINCT r.source_table ORDER BY r.source_table)
            FILTER (WHERE r.source_table IS NOT NULL) AS source_tables,
        array_agg(DISTINCT r.position_id ORDER BY r.position_id)
            FILTER (WHERE r.position_id IS NOT NULL) AS registry_position_ids,
        array_agg(DISTINCT r.source_natural_key ORDER BY r.source_natural_key)
            FILTER (WHERE r.source_natural_key IS NOT NULL)
            AS source_natural_keys,
        min(r.created_at) AS registry_created_at,
        max(r.ingested_at) AS registry_ingested_at
    FROM conflicts c
    LEFT JOIN decision_registry_v1 r
      ON r.legacy_decision_key = c.decision_key
    GROUP BY c.decision_key
), outcomes AS (
    SELECT
        c.decision_key,
        count(o.*) AS outcome_rows,
        count(DISTINCT o.outcome_id) AS outcome_id_count,
        array_agg(DISTINCT o.decision_id ORDER BY o.decision_id)
            FILTER (WHERE o.decision_id IS NOT NULL) AS outcome_decision_ids,
        array_agg(DISTINCT o.position_id ORDER BY o.position_id)
            FILTER (WHERE o.position_id IS NOT NULL) AS outcome_position_ids,
        bool_and(o.outcome_status = 'COMPLETE')
            FILTER (WHERE o.outcome_id IS NOT NULL) AS outcomes_complete,
        min(o.created_at) AS outcome_created_at,
        max(o.calculated_at) AS outcome_calculated_at
    FROM conflicts c
    LEFT JOIN decision_registry_v1 r
      ON r.legacy_decision_key = c.decision_key
    LEFT JOIN decision_outcomes_v1 o
      ON o.decision_id = r.decision_id
     AND o.outcome_type = 'ACTUAL_TRADE'
    GROUP BY c.decision_key
), warehouse AS (
    SELECT
        c.decision_key,
        count(w.*) AS warehouse_rows,
        array_agg(DISTINCT w.position_id ORDER BY w.position_id)
            FILTER (WHERE w.position_id IS NOT NULL) AS warehouse_position_ids,
        min(w.created_at) AS warehouse_created_at,
        max(w.refreshed_at) AS warehouse_refreshed_at
    FROM conflicts c
    LEFT JOIN learning_feature_warehouse_v1 w
      ON w.environment = c.environment
     AND w.decision_key = c.decision_key
    GROUP BY c.decision_key
), position_linkage AS (
    SELECT
        c.decision_key,
        bool_and(p.status = 'CLOSED') FILTER (WHERE p.id IS NOT NULL)
            AS positions_closed,
        jsonb_agg(DISTINCT jsonb_build_object(
            'position_id', p.id,
            'entry_order_id', p.entry_order_id,
            'entry_client_order_id', p.entry_client_order_id,
            'exit_order_id', p.exit_order_id,
            'exit_client_order_id', p.exit_client_order_id
        )) FILTER (WHERE p.id IS NOT NULL) AS order_client_order_linkage
    FROM conflicts c
    LEFT JOIN positions p ON p.id = c.position_id
    GROUP BY c.decision_key
)
SELECT
    c.decision_key,
    r.registry_decision_ids,
    o.outcome_decision_ids,
    lower(r.registry_deployments[1]) AS deployment_instance_id,
    c.environment,
    r.registry_deployments AS deployment_id,
    r.registry_environments,
    r.source_tables AS source_table,
    c.strategy,
    c.symbol,
    c.interval AS timeframe,
    c.position_id,
    p.order_client_order_linkage,
    r.registry_created_at,
    o.outcome_created_at,
    o.outcome_calculated_at,
    r.registry_ingested_at,
    CASE
        WHEN r.registry_rows = 0
            THEN 'FALSE_CONFLICT_MISSING_REGISTRY_COUNT_STAR'
        WHEN r.registry_id_count <> 1
            THEN 'REGISTRY_DECISION_ID_CARDINALITY'
        WHEN NOT (ARRAY[c.position_id]::BIGINT[] <@
                  COALESCE(r.registry_position_ids, ARRAY[]::BIGINT[]))
            THEN 'WAREHOUSE_REGISTRY_POSITION_MISMATCH'
        WHEN NOT (ARRAY[c.position_id]::BIGINT[] <@
                  COALESCE(o.outcome_position_ids, ARRAY[]::BIGINT[]))
            THEN 'WAREHOUSE_OUTCOME_POSITION_MISMATCH'
        ELSE 'UNKNOWN'
    END AS exact_conflict_reason,
    greatest(r.registry_rows, o.outcome_rows, w.warehouse_rows)
        AS conflict_record_count,
    (
        COALESCE(p.positions_closed, false)
        AND COALESCE(o.outcomes_complete, false)
        AND cardinality(r.source_natural_keys) > 0
    ) AS complete_lifecycle_provenance,
    (
        r.registry_id_count = 1
        AND r.deployment_count = 1
        AND r.registry_environment_count = 1
        AND r.registry_deployments[1] IN ('LOCAL', 'VPS')
        AND r.registry_environments[1] = c.environment
        AND ARRAY[c.position_id]::BIGINT[] <@
            COALESCE(r.registry_position_ids, ARRAY[]::BIGINT[])
        AND ARRAY[c.position_id]::BIGINT[] <@
            COALESCE(o.outcome_position_ids, ARRAY[]::BIGINT[])
    ) AS legacy_identity_only,
    true AS present_in_feedback_source,
    false AS member_of_manifest_universe,
    r.registry_rows,
    o.outcome_rows,
    w.warehouse_rows,
    r.registry_id_count,
    r.deployment_count,
    r.registry_environment_count,
    r.registry_position_ids,
    o.outcome_position_ids,
    w.warehouse_position_ids
FROM conflicts c
JOIN registry r USING (decision_key)
JOIN outcomes o USING (decision_key)
JOIN warehouse w USING (decision_key)
JOIN position_linkage p USING (decision_key)
ORDER BY c.decision_key;
