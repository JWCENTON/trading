-- Evidence-preserving rollback. LOCAL PAPER only; never deletes L3 evidence.
BEGIN;
DO $guard$ BEGIN
 IF current_database()<>'trading_paper' THEN RAISE EXCEPTION 'L3_ROLLBACK_LOCAL_PAPER_ONLY'; END IF;
END $guard$;
UPDATE public.long_horizon_l3_contract_v1 SET status='TERMINATED'
 WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1' AND status='ACTIVE';
UPDATE public.bot_control SET regime_mode='ENFORCE',updated_at=clock_timestamp()
 WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE');
COMMIT;
