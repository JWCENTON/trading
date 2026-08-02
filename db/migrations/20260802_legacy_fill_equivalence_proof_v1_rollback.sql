BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DROP VIEW IF EXISTS public.v_legacy_fill_equivalence_proof_status_v1;
DROP TABLE IF EXISTS public.legacy_fill_equivalence_proof_v1;
DROP FUNCTION IF EXISTS public.prevent_legacy_fill_equivalence_mutation_v1();

COMMIT;
