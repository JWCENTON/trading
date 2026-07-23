BEGIN;

DO $canonical_identity_classification_fix$
DECLARE
    v_signature CONSTANT TEXT :=
        'learning_canonical_evidence_universe_v1(text,timestamp with time zone,timestamp with time zone,timestamp with time zone)';
    v_definition TEXT;
BEGIN
    IF to_regprocedure(v_signature) IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_V1_1_PREREQUISITE_MISSING: %',
            v_signature;
    END IF;

    v_definition := pg_get_functiondef(
        to_regprocedure(v_signature)
    );
    IF position('count(r.*) AS registry_rows' IN v_definition) > 0 THEN
        RAISE NOTICE
            'LEARNING_CANONICAL_V1_1_ALREADY_APPLIED';
        RETURN;
    END IF;
    IF position('count(*) AS registry_rows' IN v_definition) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_V1_1_UNEXPECTED_FUNCTION_DEFINITION';
    END IF;

    v_definition := replace(
        v_definition,
        'count(*) AS registry_rows',
        'count(r.*) AS registry_rows'
    );
    EXECUTE v_definition;

    IF position(
        'count(r.*) AS registry_rows'
        IN pg_get_functiondef(to_regprocedure(v_signature))
    ) = 0 THEN
        RAISE EXCEPTION
            'LEARNING_CANONICAL_V1_1_POSTCONDITION_FAILED';
    END IF;
END;
$canonical_identity_classification_fix$;

COMMIT;
