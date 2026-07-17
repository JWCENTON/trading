\set ON_ERROR_STOP on
SET client_encoding='UTF8';
WITH manifest_columns(column_name) AS (VALUES
 ('event_id'),('deployment_id'),('decision_key'),('event_schema_version'),('event_payload'),
 ('event_payload_hash'),('semantic_digest'),('source_service'),('source_instance'),
 ('decision_created_at'),('inserted_at'),('processing_status'),('attempt_count'),
 ('next_attempt_at'),('claimed_at'),('claimed_by'),('processed_at'),('last_error_code'),('last_error_at')
), manifest_rows(record_type,schema_name,object_name,subobject_name,definition) AS (
 SELECT 'table','public',c.relname,NULL::text,'table' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relkind='r' AND c.relname='causal_decision_observation_outbox_v1'
 UNION ALL SELECT 'column','public','causal_decision_observation_outbox_v1',m.column_name,
  format_type(a.atttypid,a.atttypmod)||'|nullable='||CASE WHEN a.attnotnull THEN 'no' ELSE 'yes' END||'|default='||COALESCE(pg_get_expr(d.adbin,d.adrelid),'NULL')
 FROM manifest_columns m JOIN pg_class c ON c.relname='causal_decision_observation_outbox_v1'
 JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='public'
 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=m.column_name AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 UNION ALL SELECT 'constraint','public',c.relname,con.conname,pg_get_constraintdef(con.oid,false)
 FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname='causal_decision_observation_outbox_v1'
 UNION ALL SELECT 'index','public',tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public'
 AND indexname='ix_causal_observation_outbox_claim_v1'
 UNION ALL SELECT 'trigger','public',c.relname,t.tgname,pg_get_triggerdef(t.oid,false) FROM pg_trigger t
 JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname='causal_observation_outbox_event_immutable_v1'
 UNION ALL SELECT 'function','public',p.proname,pg_get_function_identity_arguments(p.oid),pg_get_functiondef(p.oid)
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
 AND p.proname='protect_causal_observation_outbox_event_v1'
 UNION ALL SELECT 'flag','public','automation_kv',key,value FROM automation_kv WHERE key IN
 ('causal_decision_observation_enabled','causal_observation_consumer_last_poll',
  'causal_observation_consumer_last_batch_success','causal_observation_consumer_current_batch')
)
SELECT encode(convert_to(record_type,'UTF8'),'hex')||E'\t'||encode(convert_to(schema_name,'UTF8'),'hex')||E'\t'||
 encode(convert_to(object_name,'UTF8'),'hex')||E'\t'||CASE WHEN subobject_name IS NULL THEN '-' ELSE encode(convert_to(subobject_name,'UTF8'),'hex') END||E'\t'||
 CASE WHEN definition IS NULL THEN '-' ELSE encode(convert_to(definition,'UTF8'),'hex') END FROM manifest_rows;
