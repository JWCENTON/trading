-- Runtime DDL parity V1. Mirrors existing ensure_* shapes; no runtime fallback is removed.
-- IF NOT EXISTS makes this a shape-preserving bridge for already-provisioned databases.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.ui_audit_log (
  id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor TEXT NOT NULL, actor_role TEXT, action TEXT NOT NULL,
  target_type TEXT NOT NULL, target_key TEXT NOT NULL, before_json JSONB,
  after_json JSONB, source TEXT, note TEXT
);
CREATE INDEX IF NOT EXISTS ix_ui_audit_log_created_at
  ON public.ui_audit_log(created_at DESC);

CREATE TABLE IF NOT EXISTS public.api_key_safety_confirmations (
  id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), user_id BIGINT,
  username TEXT NOT NULL, reading_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  spot_trading_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  withdrawals_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  margin_loan_repay_transfer_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  internal_transfer_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  universal_transfer_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  ip_whitelist_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  risk_accepted BOOLEAN NOT NULL DEFAULT FALSE,
  no_investment_advice_ack BOOLEAN NOT NULL DEFAULT FALSE,
  client_controls_binance_account_ack BOOLEAN NOT NULL DEFAULT FALSE,
  all_confirmed BOOLEAN NOT NULL DEFAULT FALSE, ip TEXT, user_agent TEXT
);
CREATE INDEX IF NOT EXISTS ix_api_key_safety_confirmations_created_at
  ON public.api_key_safety_confirmations(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_api_key_safety_confirmations_user_id
  ON public.api_key_safety_confirmations(user_id);

CREATE TABLE IF NOT EXISTS public.api_key_validation_events (
  id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  result TEXT NOT NULL, account_read_check TEXT NOT NULL,
  spot_trading_check TEXT NOT NULL,
  can_read_account BOOLEAN NOT NULL DEFAULT FALSE, binance_can_trade BOOLEAN,
  account_can_withdraw_reported BOOLEAN, error_type TEXT, error_message TEXT,
  source TEXT NOT NULL DEFAULT 'api_key_status', environment TEXT, trading_mode TEXT
);
CREATE INDEX IF NOT EXISTS ix_api_key_validation_events_created_at
  ON public.api_key_validation_events(created_at DESC);

CREATE TABLE IF NOT EXISTS public.user_totp (
  user_id INTEGER PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,
  totp_secret TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), enabled_at TIMESTAMPTZ,
  disabled_at TIMESTAMPTZ, last_used_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS public.user_recovery_codes (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  code_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_user_recovery_codes_user_active
  ON public.user_recovery_codes(user_id, used_at);

CREATE TABLE IF NOT EXISTS public.panic_state (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
  panic_enabled BOOLEAN NOT NULL DEFAULT FALSE, reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.worker_heartbeats (
  service_name TEXT NOT NULL, environment TEXT NOT NULL DEFAULT 'UNKNOWN',
  status TEXT NOT NULL DEFAULT 'unknown', last_tick TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_ok TIMESTAMPTZ, last_error TEXT, loop_duration_ms INTEGER,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (service_name, environment)
);
CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_status_updated
  ON public.worker_heartbeats(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_last_tick
  ON public.worker_heartbeats(last_tick DESC);

CREATE TABLE IF NOT EXISTS public.ui_notification_preferences (
  category TEXT PRIMARY KEY, enabled BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.ui_notifications (
  id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'CRITICAL',
  severity TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL,
  message TEXT NOT NULL, source TEXT, read_at TIMESTAMPTZ,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_ui_notifications_created_at
  ON public.ui_notifications(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_ui_notifications_read_at
  ON public.ui_notifications(read_at);

DO $shape_assertions$
DECLARE
  expected_relation_name text;
  expected_column_name text;
  default_pattern text;
  expected_index_name text;
  index_pattern text;
  expected_columns text[];
  actual_columns text[];
BEGIN
  FOR expected_relation_name, expected_columns IN
    SELECT * FROM (VALUES
      ('ui_audit_log', ARRAY['id:bigint:NO','created_at:timestamp with time zone:NO','actor:text:NO','actor_role:text:YES','action:text:NO','target_type:text:NO','target_key:text:NO','before_json:jsonb:YES','after_json:jsonb:YES','source:text:YES','note:text:YES']),
      ('api_key_safety_confirmations', ARRAY['id:bigint:NO','created_at:timestamp with time zone:NO','updated_at:timestamp with time zone:NO','user_id:bigint:YES','username:text:NO','reading_enabled:boolean:NO','spot_trading_enabled:boolean:NO','withdrawals_disabled:boolean:NO','margin_loan_repay_transfer_disabled:boolean:NO','internal_transfer_disabled:boolean:NO','universal_transfer_disabled:boolean:NO','ip_whitelist_enabled:boolean:NO','risk_accepted:boolean:NO','no_investment_advice_ack:boolean:NO','client_controls_binance_account_ack:boolean:NO','all_confirmed:boolean:NO','ip:text:YES','user_agent:text:YES']),
      ('api_key_validation_events', ARRAY['id:bigint:NO','created_at:timestamp with time zone:NO','result:text:NO','account_read_check:text:NO','spot_trading_check:text:NO','can_read_account:boolean:NO','binance_can_trade:boolean:YES','account_can_withdraw_reported:boolean:YES','error_type:text:YES','error_message:text:YES','source:text:NO','environment:text:YES','trading_mode:text:YES']),
      ('user_totp', ARRAY['user_id:integer:NO','totp_secret:text:NO','enabled:boolean:NO','created_at:timestamp with time zone:NO','enabled_at:timestamp with time zone:YES','disabled_at:timestamp with time zone:YES','last_used_at:timestamp with time zone:YES']),
      ('user_recovery_codes', ARRAY['id:bigint:NO','user_id:integer:NO','code_hash:text:NO','created_at:timestamp with time zone:NO','used_at:timestamp with time zone:YES']),
      ('panic_state', ARRAY['id:boolean:NO','panic_enabled:boolean:NO','reason:text:YES','updated_at:timestamp with time zone:NO']),
      ('worker_heartbeats', ARRAY['service_name:text:NO','environment:text:NO','status:text:NO','last_tick:timestamp with time zone:NO','last_ok:timestamp with time zone:YES','last_error:text:YES','loop_duration_ms:integer:YES','meta:jsonb:NO','updated_at:timestamp with time zone:NO']),
      ('ui_notification_preferences', ARRAY['category:text:NO','enabled:boolean:NO','updated_at:timestamp with time zone:NO']),
      ('ui_notifications', ARRAY['id:bigint:NO','created_at:timestamp with time zone:NO','event_type:text:NO','category:text:NO','severity:text:NO','title:text:NO','message:text:NO','source:text:YES','read_at:timestamp with time zone:YES','meta:jsonb:NO'])
    ) AS expected(relation_name, expected_columns)
  LOOP
    SELECT array_agg(
             cols.column_name || ':' || cols.data_type || ':' || cols.is_nullable
             ORDER BY cols.ordinal_position
           )
      INTO actual_columns
      FROM information_schema.columns cols
     WHERE cols.table_schema='public'
       AND cols.table_name=expected_relation_name;
    IF actual_columns IS DISTINCT FROM expected_columns THEN
      RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: public.%, expected columns %, actual %',
        expected_relation_name, expected_columns, actual_columns;
    END IF;
  END LOOP;

  FOR expected_relation_name, expected_column_name, default_pattern IN
    SELECT * FROM (VALUES
      ('ui_audit_log','id','nextval(%'), ('ui_audit_log','created_at','now()'),
      ('api_key_safety_confirmations','id','nextval(%'), ('api_key_safety_confirmations','created_at','now()'), ('api_key_safety_confirmations','updated_at','now()'),
      ('api_key_safety_confirmations','reading_enabled','false'), ('api_key_safety_confirmations','spot_trading_enabled','false'), ('api_key_safety_confirmations','withdrawals_disabled','false'), ('api_key_safety_confirmations','margin_loan_repay_transfer_disabled','false'), ('api_key_safety_confirmations','internal_transfer_disabled','false'), ('api_key_safety_confirmations','universal_transfer_disabled','false'), ('api_key_safety_confirmations','ip_whitelist_enabled','false'), ('api_key_safety_confirmations','risk_accepted','false'), ('api_key_safety_confirmations','no_investment_advice_ack','false'), ('api_key_safety_confirmations','client_controls_binance_account_ack','false'), ('api_key_safety_confirmations','all_confirmed','false'),
      ('api_key_validation_events','id','nextval(%'), ('api_key_validation_events','created_at','now()'), ('api_key_validation_events','can_read_account','false'), ('api_key_validation_events','source','''api_key_status''%'),
      ('user_totp','enabled','false'), ('user_totp','created_at','now()'),
      ('user_recovery_codes','id','nextval(%'), ('user_recovery_codes','created_at','now()'),
      ('panic_state','id','true'), ('panic_state','panic_enabled','false'), ('panic_state','updated_at','now()'),
      ('worker_heartbeats','environment','''UNKNOWN''%'), ('worker_heartbeats','status','''unknown''%'), ('worker_heartbeats','last_tick','now()'), ('worker_heartbeats','meta','''{}''%'), ('worker_heartbeats','updated_at','now()'),
      ('ui_notification_preferences','enabled','false'), ('ui_notification_preferences','updated_at','now()'),
      ('ui_notifications','id','nextval(%'), ('ui_notifications','created_at','now()'), ('ui_notifications','category','''CRITICAL''%'), ('ui_notifications','severity','''info''%'), ('ui_notifications','meta','''{}''%')
    ) AS expected(relation_name, column_name, default_pattern)
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns cols
       WHERE cols.table_schema='public'
         AND cols.table_name=expected_relation_name
         AND cols.column_name=expected_column_name
         AND cols.column_default LIKE default_pattern
    ) THEN
      RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: public.%.% default',
        expected_relation_name, expected_column_name;
    END IF;
  END LOOP;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name='ui_notifications'
       AND column_name='category' AND data_type='text' AND is_nullable='NO'
       AND column_default LIKE '%CRITICAL%'
  ) THEN
    RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: ui_notifications.category';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid='public.user_totp'::regclass AND contype='f'
       AND confrelid='public.users'::regclass
  ) OR NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid='public.user_recovery_codes'::regclass AND contype='f'
       AND confrelid='public.users'::regclass
  ) THEN
    RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: required users foreign key';
  END IF;

  IF (
    SELECT count(*) FROM pg_constraint
     WHERE conrelid IN (
       'public.ui_audit_log'::regclass,
       'public.api_key_safety_confirmations'::regclass,
       'public.api_key_validation_events'::regclass,
       'public.user_totp'::regclass,
       'public.user_recovery_codes'::regclass,
       'public.panic_state'::regclass,
       'public.worker_heartbeats'::regclass,
       'public.ui_notification_preferences'::regclass,
       'public.ui_notifications'::regclass
     ) AND contype='p'
  ) <> 9 THEN
    RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: primary keys';
  END IF;

  FOR expected_index_name, index_pattern IN
    SELECT * FROM (VALUES
      ('ix_ui_audit_log_created_at','%(created_at DESC)'),
      ('ix_api_key_safety_confirmations_created_at','%(created_at DESC)'),
      ('ix_api_key_safety_confirmations_user_id','%(user_id)'),
      ('ix_api_key_validation_events_created_at','%(created_at DESC)'),
      ('ix_user_recovery_codes_user_active','%(user_id, used_at)'),
      ('ix_worker_heartbeats_status_updated','%(status, updated_at DESC)'),
      ('ix_worker_heartbeats_last_tick','%(last_tick DESC)'),
      ('ix_ui_notifications_created_at','%(created_at DESC)'),
      ('ix_ui_notifications_read_at','%(read_at)')
    ) AS expected(index_name,index_pattern)
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_class idx JOIN pg_namespace n ON n.oid=idx.relnamespace
       WHERE n.nspname='public' AND idx.relname=expected_index_name
         AND pg_get_indexdef(idx.oid) LIKE index_pattern
    ) THEN
      RAISE EXCEPTION 'RUNTIME_DDL_SHAPE_MISMATCH: index %', expected_index_name;
    END IF;
  END LOOP;
END
$shape_assertions$;

COMMIT;
