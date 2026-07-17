\set ON_ERROR_STOP on
SET client_encoding='UTF8';
WITH manifest_columns(table_name,column_name) AS (VALUES
 ('learning_recommendation_snapshots_v1','deployment_id'),
 ('learning_recommendation_activations_v1','deployment_id'),
 ('learning_would_trade_decisions_v1','deployment_id'),
 ('learning_would_trade_decisions_v1','actual_action'),
 ('learning_would_trade_decisions_v1','actual_execution_eligible'),
 ('learning_would_trade_decisions_v1','recommended_shadow_action'),
 ('learning_would_trade_decisions_v1','would_trade_without_recommendation'),
 ('learning_would_trade_decisions_v1','would_trade_with_recommendation'),
 ('learning_would_trade_decisions_v1','recommendation_effect_applied'),
 ('learning_counterfactual_outcomes_v1','deployment_id'),
 ('learning_counterfactual_outcomes_v1','outcome_status'),
 ('learning_counterfactual_outcomes_v1','evaluation_type'),
 ('decision_replay_v1','deployment_id'),('decision_replay_v1','observation_decision_key'),
 ('decision_replay_v1','activation_mode'),('decision_replay_v1','promotion_consumption_event_id'),
 ('learning_feature_warehouse_v1','deployment_id'),('learning_feature_warehouse_v1','observation_decision_key'),
 ('learning_feature_warehouse_v1','activation_mode'),('learning_feature_warehouse_v1','promotion_consumption_event_id'),
 ('causal_decision_observation_v1','event_id'),('causal_decision_observation_v1','decision_key'),
 ('causal_decision_observation_v1','decision_created_at'),('causal_decision_observation_v1','environment'),
 ('causal_decision_observation_v1','deployment_id'),('causal_decision_observation_v1','strategy'),
 ('causal_decision_observation_v1','symbol'),('causal_decision_observation_v1','interval'),
 ('causal_decision_observation_v1','slot_key'),('causal_decision_observation_v1','regime'),
 ('causal_decision_observation_v1','regime_confidence'),('causal_decision_observation_v1','action'),
 ('causal_decision_observation_v1','direction'),('causal_decision_observation_v1','confidence'),
 ('causal_decision_observation_v1','quantity_intent'),('causal_decision_observation_v1','entry_intent'),
 ('causal_decision_observation_v1','stop_loss_intent'),('causal_decision_observation_v1','take_profit_intent'),
 ('causal_decision_observation_v1','exit_intent'),('causal_decision_observation_v1','execution_eligible'),
 ('causal_decision_observation_v1','decision_reason'),('causal_decision_observation_v1','decision_payload_hash'),
 ('causal_decision_observation_v1','semantic_digest'),('causal_decision_observation_v1','event_digest'),
 ('causal_decision_observation_v1','source_service'),('causal_decision_observation_v1','source_instance'),
 ('causal_decision_observation_v1','decision_kind'),('causal_decision_observation_v1','schema_version'),
 ('causal_decision_observation_v1','created_at'),('causal_decision_observation_v1','inserted_at'),
 ('causal_promotion_consumption_v1','promotion_consumption_event_id'),
 ('causal_promotion_consumption_v1','deployment_id'),('causal_promotion_consumption_v1','decision_key'),
 ('causal_promotion_consumption_v1','promotion_event_id'),('causal_promotion_consumption_v1','promotion_hash'),
 ('causal_promotion_consumption_v1','promotion_version'),('causal_promotion_consumption_v1','consumer'),
 ('causal_promotion_consumption_v1','consumed_at'),('causal_promotion_consumption_v1','activation_id'),
 ('causal_promotion_consumption_v1','recommendation_id')
), manifest_rows(record_type,schema_name,object_name,subobject_name,definition) AS (
 SELECT 'table','public',c.relname,NULL::text,'table' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relkind='r' AND c.relname IN ('causal_decision_observation_v1','causal_promotion_consumption_v1')
 UNION ALL SELECT 'column','public',m.table_name,m.column_name,
  format_type(a.atttypid,a.atttypmod)||'|nullable='||CASE WHEN a.attnotnull THEN 'no' ELSE 'yes' END||'|default='||COALESCE(pg_get_expr(d.adbin,d.adrelid),'NULL')
 FROM manifest_columns m JOIN pg_class c ON c.relname=m.table_name JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='public'
 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=m.column_name AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 UNION ALL SELECT 'constraint','public',c.relname,con.conname,pg_get_constraintdef(con.oid,false)
 FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND con.conname IN ('ck_would_trade_effect_not_applied_v1_1','ck_counterfactual_outcome_status_v1_1',
 'ck_counterfactual_evaluation_type_v1_1','ck_decision_registry_causal_status_v1_1','causal_decision_observation_v1_pkey',
 'causal_decision_observation_v1_deployment_id_decision_key_key','causal_decision_observation_v1_decision_kind_check',
 'causal_promotion_consumption_v1_pkey','causal_promotion_consumption__deployment_id_decision_key_pr_key')
 UNION ALL SELECT 'index','public',tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND indexname IN
 ('ux_learning_recommendation_identity_v1_1','ix_learning_activation_lookup_v1_1','ux_learning_activation_deployment_v1_1',
 'ux_would_trade_deployment_decision_v1_1','ux_counterfactual_deployment_decision_v1_1',
 'ux_decision_replay_causal_observation_v1_1','ux_warehouse_causal_observation_v1_1')
 UNION ALL SELECT 'trigger','public',c.relname,t.tgname,pg_get_triggerdef(t.oid,false) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname IN
 ('causal_decision_observation_immutable_v1','causal_promotion_consumption_immutable_v1')
 UNION ALL SELECT 'function','public',p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid)
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname IN
 ('prevent_causal_v1_1_mutation','attribute_decision_causally_v1','propagate_decision_causal_linkage_v1')
 UNION ALL SELECT 'view','public',c.relname,NULL::text,pg_get_viewdef(c.oid,false) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relkind='v' AND c.relname IN ('v_learning_causal_attribution_v1_1','v_learning_causal_coverage_v1_1',
 'v_learning_counterfactual_outcomes_v1_1','v_learning_experiment_readiness_v1_1')
 UNION ALL SELECT 'flag','public','automation_kv',key,value FROM automation_kv WHERE key IN
 ('causal_shadow_observation_enabled','causal_learning_auto_apply_enabled')
)
SELECT encode(convert_to(record_type,'UTF8'),'hex')||E'\t'||encode(convert_to(schema_name,'UTF8'),'hex')||E'\t'||
 encode(convert_to(object_name,'UTF8'),'hex')||E'\t'||CASE WHEN subobject_name IS NULL THEN '-' ELSE encode(convert_to(subobject_name,'UTF8'),'hex') END||E'\t'||
 CASE WHEN definition IS NULL THEN '-' ELSE encode(convert_to(definition,'UTF8'),'hex') END FROM manifest_rows;
