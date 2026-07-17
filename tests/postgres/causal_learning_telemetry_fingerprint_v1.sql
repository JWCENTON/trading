\set ON_ERROR_STOP on
SET client_encoding = 'UTF8';

-- Machine interface for the canonical runner. Every text field is hex encoded,
-- so psql display settings, delimiters, newlines and NULL formatting are inert.
WITH
new_tables(name) AS (VALUES
  ('learning_recommendation_snapshots_v1'),
  ('learning_recommendation_activations_v1'),
  ('learning_would_trade_decisions_v1'),
  ('learning_counterfactual_outcomes_v1')
),
manifest_columns(table_name, column_name) AS (VALUES
  ('learning_recommendation_snapshots_v1','recommendation_id'),
  ('learning_recommendation_snapshots_v1','recommendation_version'),
  ('learning_recommendation_snapshots_v1','environment'),
  ('learning_recommendation_snapshots_v1','slot_key'),
  ('learning_recommendation_snapshots_v1','strategy'),
  ('learning_recommendation_snapshots_v1','symbol'),
  ('learning_recommendation_snapshots_v1','interval'),
  ('learning_recommendation_snapshots_v1','market_regime'),
  ('learning_recommendation_snapshots_v1','recommendation_action'),
  ('learning_recommendation_snapshots_v1','recommendation_type'),
  ('learning_recommendation_snapshots_v1','confidence'),
  ('learning_recommendation_snapshots_v1','evidence_decisions'),
  ('learning_recommendation_snapshots_v1','evidence_start_at'),
  ('learning_recommendation_snapshots_v1','evidence_cutoff_at'),
  ('learning_recommendation_snapshots_v1','created_at'),
  ('learning_recommendation_snapshots_v1','valid_from'),
  ('learning_recommendation_snapshots_v1','expires_at'),
  ('learning_recommendation_snapshots_v1','reset_at'),
  ('learning_recommendation_snapshots_v1','policy_version'),
  ('learning_recommendation_snapshots_v1','payload_hash'),
  ('learning_recommendation_snapshots_v1','status'),
  ('learning_recommendation_snapshots_v1','payload'),
  ('learning_recommendation_snapshots_v1','schema_version'),
  ('learning_recommendation_activations_v1','activation_id'),
  ('learning_recommendation_activations_v1','recommendation_id'),
  ('learning_recommendation_activations_v1','experiment_id'),
  ('learning_recommendation_activations_v1','environment'),
  ('learning_recommendation_activations_v1','slot_key'),
  ('learning_recommendation_activations_v1','experiment_arm'),
  ('learning_recommendation_activations_v1','baseline_policy_version'),
  ('learning_recommendation_activations_v1','candidate_policy_version'),
  ('learning_recommendation_activations_v1','promotion_event_id'),
  ('learning_recommendation_activations_v1','promotion_payload_hash'),
  ('learning_recommendation_activations_v1','promotion_policy_version'),
  ('learning_recommendation_activations_v1','promotion_candidate_id'),
  ('learning_recommendation_activations_v1','activated_at'),
  ('learning_recommendation_activations_v1','effective_from'),
  ('learning_recommendation_activations_v1','expires_at'),
  ('learning_recommendation_activations_v1','deactivated_at'),
  ('learning_recommendation_activations_v1','deactivation_reason'),
  ('learning_recommendation_activations_v1','apply_mode'),
  ('learning_recommendation_activations_v1','created_at'),
  ('learning_would_trade_decisions_v1','decision_key'),
  ('learning_would_trade_decisions_v1','decision_id'),
  ('learning_would_trade_decisions_v1','recommendation_id'),
  ('learning_would_trade_decisions_v1','recommendation_version'),
  ('learning_would_trade_decisions_v1','activation_id'),
  ('learning_would_trade_decisions_v1','experiment_id'),
  ('learning_would_trade_decisions_v1','experiment_arm'),
  ('learning_would_trade_decisions_v1','environment'),
  ('learning_would_trade_decisions_v1','slot_key'),
  ('learning_would_trade_decisions_v1','strategy'),
  ('learning_would_trade_decisions_v1','symbol'),
  ('learning_would_trade_decisions_v1','interval'),
  ('learning_would_trade_decisions_v1','market_regime'),
  ('learning_would_trade_decisions_v1','would_trade'),
  ('learning_would_trade_decisions_v1','would_side'),
  ('learning_would_trade_decisions_v1','would_entry_price'),
  ('learning_would_trade_decisions_v1','would_qty'),
  ('learning_would_trade_decisions_v1','would_notional'),
  ('learning_would_trade_decisions_v1','would_stop'),
  ('learning_would_trade_decisions_v1','would_take_profit'),
  ('learning_would_trade_decisions_v1','would_signal_at'),
  ('learning_would_trade_decisions_v1','would_reason'),
  ('learning_would_trade_decisions_v1','baseline_policy_version'),
  ('learning_would_trade_decisions_v1','candidate_policy_version'),
  ('learning_would_trade_decisions_v1','payload_hash'),
  ('learning_would_trade_decisions_v1','created_at'),
  ('learning_counterfactual_outcomes_v1','decision_key'),
  ('learning_counterfactual_outcomes_v1','recommendation_id'),
  ('learning_counterfactual_outcomes_v1','activation_id'),
  ('learning_counterfactual_outcomes_v1','experiment_id'),
  ('learning_counterfactual_outcomes_v1','experiment_arm'),
  ('learning_counterfactual_outcomes_v1','evaluation_status'),
  ('learning_counterfactual_outcomes_v1','evaluation_horizon_minutes'),
  ('learning_counterfactual_outcomes_v1','entry_reference_price'),
  ('learning_counterfactual_outcomes_v1','max_favorable_excursion'),
  ('learning_counterfactual_outcomes_v1','max_adverse_excursion'),
  ('learning_counterfactual_outcomes_v1','fixed_horizon_exit_price'),
  ('learning_counterfactual_outcomes_v1','gross_pnl'),
  ('learning_counterfactual_outcomes_v1','estimated_fees'),
  ('learning_counterfactual_outcomes_v1','net_pnl'),
  ('learning_counterfactual_outcomes_v1','outcome_at'),
  ('learning_counterfactual_outcomes_v1','method_version'),
  ('learning_counterfactual_outcomes_v1','evidence'),
  ('learning_counterfactual_outcomes_v1','created_at'),
  ('learning_counterfactual_outcomes_v1','refreshed_at'),
  ('decision_registry_v1','recommendation_version'),
  ('decision_registry_v1','activation_id'),
  ('decision_registry_v1','experiment_id'),
  ('decision_registry_v1','experiment_arm'),
  ('decision_registry_v1','baseline_policy_version'),
  ('decision_registry_v1','candidate_policy_version'),
  ('decision_registry_v1','promotion_event_id'),
  ('decision_registry_v1','promotion_candidate_id'),
  ('decision_registry_v1','consumed_promotion_hash'),
  ('decision_registry_v1','consumed_promotion_version'),
  ('decision_registry_v1','causal_linkage_status'),
  ('decision_registry_v1','causal_attributed_at'),
  ('decision_replay_v1','recommendation_id'),
  ('decision_replay_v1','recommendation_version'),
  ('decision_replay_v1','activation_id'),
  ('decision_replay_v1','experiment_id'),
  ('decision_replay_v1','experiment_arm'),
  ('decision_replay_v1','baseline_policy_version'),
  ('decision_replay_v1','candidate_policy_version'),
  ('decision_replay_v1','causal_linkage_status'),
  ('decision_replay_v1','counterfactual_status'),
  ('learning_feature_warehouse_v1','recommendation_id'),
  ('learning_feature_warehouse_v1','recommendation_version'),
  ('learning_feature_warehouse_v1','activation_id'),
  ('learning_feature_warehouse_v1','experiment_id'),
  ('learning_feature_warehouse_v1','experiment_arm'),
  ('learning_feature_warehouse_v1','baseline_policy_version'),
  ('learning_feature_warehouse_v1','candidate_policy_version'),
  ('learning_feature_warehouse_v1','causal_linkage_status'),
  ('learning_feature_warehouse_v1','counterfactual_status')
),
manifest_constraints(table_name, constraint_name) AS (VALUES
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_pkey'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_environment_check'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v_recommendation_action_check'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_evidence_decisions_check'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_status_check'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_check'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_check1'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_check2'),
    ('learning_recommendation_snapshots_v1','learning_recommendation_snapshots_v1_check3'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_pkey'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_recommendation_id_fkey'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_environment_check'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_experiment_arm_check'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_apply_mode_check'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_check'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_check1'),
    ('learning_recommendation_activations_v1','learning_recommendation_activations_v1_check2'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_pkey'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_experiment_arm_check'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_environment_check'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_check'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_would_qty_check'),
    ('learning_would_trade_decisions_v1','learning_would_trade_decisions_v1_would_notional_check'),
    ('learning_counterfactual_outcomes_v1','learning_counterfactual_outcomes_v1_pkey'),
    ('learning_counterfactual_outcomes_v1','learning_counterfactual_outcomes_v1_decision_key_fkey'),
    ('learning_counterfactual_outcomes_v1','learning_counterfactual_outcomes_v1_experiment_arm_check'),
    ('learning_counterfactual_outcomes_v1','learning_counterfactual_outcomes_v1_evaluation_status_check'),
    ('learning_counterfactual_outcomes_v1','learning_counterfactual_outcom_evaluation_horizon_minutes_check'),
    ('decision_registry_v1','ck_decision_registry_experiment_arm_v1'),
    ('decision_registry_v1','ck_decision_registry_causal_status_v1')
),
manifest_rows(record_type, schema_name, object_name, subobject_name, definition) AS (
  SELECT 'table','public',m.name,NULL::text,'table' FROM new_tables m
  JOIN pg_class c ON c.relname=m.name JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND c.relkind='r'
  UNION ALL
  SELECT 'column','public',m.table_name,m.column_name,
         format_type(a.atttypid,a.atttypmod)||'|nullable='||CASE WHEN a.attnotnull THEN 'no' ELSE 'yes' END||
         '|default='||COALESCE(pg_get_expr(d.adbin,d.adrelid),'\x00NULL')
  FROM manifest_columns m JOIN pg_class c ON c.relname=m.table_name
  JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='public'
  JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=m.column_name AND a.attnum>0 AND NOT a.attisdropped
  LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
  UNION ALL
  SELECT 'constraint','public',m.table_name,m.constraint_name,pg_get_constraintdef(con.oid,false)
  FROM manifest_constraints m JOIN pg_class c ON c.relname=m.table_name
  JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='public'
  JOIN pg_constraint con ON con.conrelid=c.oid AND con.conname=m.constraint_name
  UNION ALL
  SELECT 'index','public',i.tablename,i.indexname,i.indexdef FROM pg_indexes i
  WHERE i.schemaname='public' AND i.indexname IN ('ux_learning_recommendation_identity_v1','ix_learning_activation_lookup_v1')
  UNION ALL
  SELECT 'trigger','public',c.relname,t.tgname,pg_get_triggerdef(t.oid,false)
  FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname IN
    ('learning_snapshot_immutable_v1','learning_activation_append_only_v1','decision_registry_causal_attribution_v1','decision_registry_causal_immutable_v1','decision_registry_causal_propagation_v1')
  UNION ALL
  SELECT 'function','public',p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid)
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN
    ('prevent_causal_snapshot_mutation_v1','prevent_causal_activation_mutation_v1','attribute_decision_causally_v1','prevent_decision_causal_rewrite_v1','record_learning_would_trade_v1','record_learning_counterfactual_outcome_v1','propagate_decision_causal_linkage_v1')
  UNION ALL
  SELECT 'view','public',c.relname,NULL::text,pg_get_viewdef(c.oid,false)
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='v' AND c.relname IN
    ('v_learning_causal_attribution_v1','v_learning_causal_coverage_v1','v_learning_counterfactual_outcomes_v1','v_learning_experiment_readiness_v1')
  UNION ALL
  SELECT 'flag','public','automation_kv',k.key,k.value FROM automation_kv k WHERE k.key IN
    ('causal_learning_telemetry_v1_enabled','causal_learning_auto_apply_enabled','causal_learning_kill_switch_available')
)
SELECT encode(convert_to(record_type,'UTF8'),'hex')||E'\t'||
       encode(convert_to(schema_name,'UTF8'),'hex')||E'\t'||
       encode(convert_to(object_name,'UTF8'),'hex')||E'\t'||
       CASE WHEN subobject_name IS NULL THEN '-' ELSE encode(convert_to(subobject_name,'UTF8'),'hex') END||E'\t'||
       CASE WHEN definition IS NULL THEN '-' ELSE encode(convert_to(definition,'UTF8'),'hex') END
FROM manifest_rows;
