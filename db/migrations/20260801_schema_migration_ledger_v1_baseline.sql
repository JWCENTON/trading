-- WALTRADE CANONICAL SCHEMA MIGRATION LEDGER V1 BASELINE
-- Technical provenance only: no trading data or adoption state is changed.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE TABLE IF NOT EXISTS public.schema_migration_ledger_v1 (
    ledger_id BIGSERIAL PRIMARY KEY,
    migration_id TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL
        CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    environment TEXT NOT NULL CHECK (environment IN ('LIVE', 'PAPER')),
    deployment_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    applied_by TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'APPLIED', 'BASELINE_ADOPTED', 'FAILED',
        'SKIPPED_ALREADY_APPLIED', 'CHECKSUM_CONFLICT'
    )),
    success BOOLEAN NOT NULL,
    execution_duration_ms BIGINT NOT NULL CHECK (execution_duration_ms >= 0),
    git_sha TEXT NOT NULL,
    error_summary TEXT,
    schema_baseline_version TEXT NOT NULL,
    CONSTRAINT schema_migration_ledger_v1_status_success_ck CHECK (
        success = (
            status IN (
                'APPLIED', 'BASELINE_ADOPTED', 'SKIPPED_ALREADY_APPLIED'
            )
        )
    )
);

DO $ledger_column_contract$
DECLARE
    issues TEXT;
    serial_sequence TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'public'
          AND relation.relname = 'schema_migration_ledger_v1'
          AND relation.relkind = 'r'
          AND relation.relpersistence = 'p'
    ) THEN
        RAISE EXCEPTION 'SCHEMA_MIGRATION_LEDGER_RELATION_CONTRACT_MISMATCH';
    END IF;

    WITH expected(
        ordinal_position, column_name, data_type, not_null, default_rule
    ) AS (
        VALUES
          (1,  'ledger_id',               'bigint',                   TRUE,  'serial'),
          (2,  'migration_id',            'text',                     TRUE,  'none'),
          (3,  'checksum_sha256',         'text',                     TRUE,  'none'),
          (4,  'applied_at',              'timestamp with time zone', TRUE,  'clock'),
          (5,  'environment',             'text',                     TRUE,  'none'),
          (6,  'deployment_id',           'text',                     TRUE,  'none'),
          (7,  'database_name',           'text',                     TRUE,  'none'),
          (8,  'applied_by',              'text',                     TRUE,  'none'),
          (9,  'status',                  'text',                     TRUE,  'none'),
          (10, 'success',                 'boolean',                  TRUE,  'none'),
          (11, 'execution_duration_ms',   'bigint',                   TRUE,  'none'),
          (12, 'git_sha',                 'text',                     TRUE,  'none'),
          (13, 'error_summary',           'text',                     FALSE, 'none'),
          (14, 'schema_baseline_version', 'text',                     TRUE,  'none')
    ), actual AS (
        SELECT
            attribute.attnum AS ordinal_position,
            attribute.attname::TEXT AS column_name,
            format_type(attribute.atttypid, attribute.atttypmod) AS data_type,
            attribute.attnotnull AS not_null,
            pg_get_expr(definition.adbin, definition.adrelid) AS default_expression
        FROM pg_attribute attribute
        LEFT JOIN pg_attrdef definition
          ON definition.adrelid = attribute.attrelid
         AND definition.adnum = attribute.attnum
        WHERE attribute.attrelid =
              'public.schema_migration_ledger_v1'::regclass
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
    ), comparison AS (
        SELECT
            COALESCE(expected.column_name, actual.column_name) AS column_name,
            COALESCE(expected.ordinal_position, actual.ordinal_position) AS ordinal_position,
            CASE
              WHEN expected.column_name IS NULL THEN 'unexpected'
              WHEN actual.column_name IS NULL THEN 'missing'
              WHEN expected.ordinal_position <> actual.ordinal_position
                THEN 'ordinal'
              WHEN expected.data_type <> actual.data_type THEN 'type'
              WHEN expected.not_null <> actual.not_null THEN 'nullable'
              WHEN expected.default_rule = 'none'
                   AND actual.default_expression IS NOT NULL THEN 'default'
              WHEN expected.default_rule = 'clock'
                   AND COALESCE(
                       regexp_replace(
                           lower(actual.default_expression),
                           '[[:space:]]+', '', 'g'
                       ), ''
                   ) <> 'clock_timestamp()' THEN 'default'
              WHEN expected.default_rule = 'serial'
                   AND actual.default_expression IS NULL THEN 'default'
              ELSE NULL
            END AS issue
        FROM expected
        FULL JOIN actual USING (column_name)
    )
    SELECT string_agg(
        format('%s:%s', column_name, issue),
        ',' ORDER BY ordinal_position, column_name
    )
    INTO issues
    FROM comparison
    WHERE issue IS NOT NULL;

    IF issues IS NOT NULL THEN
        RAISE EXCEPTION
            'SCHEMA_MIGRATION_LEDGER_COLUMN_CONTRACT_MISMATCH:%', issues;
    END IF;

    SELECT pg_get_serial_sequence(
        'public.schema_migration_ledger_v1', 'ledger_id'
    ) INTO serial_sequence;
    IF serial_sequence IS DISTINCT FROM
       'public.schema_migration_ledger_v1_ledger_id_seq' THEN
        RAISE EXCEPTION
            'SCHEMA_MIGRATION_LEDGER_SEQUENCE_CONTRACT_MISMATCH:%',
            COALESCE(serial_sequence, '<missing>');
    END IF;
END;
$ledger_column_contract$;

DO $ledger_constraint_contract$
DECLARE
    issues TEXT;
BEGIN
    WITH expected(constraint_name, constraint_type, definition) AS (
        VALUES
          (
            'schema_migration_ledger_v1_checksum_sha256_check', 'c',
            'check(checksum_sha256~''^[0-9a-f]{64}$''::text)'
          ),
          (
            'schema_migration_ledger_v1_environment_check', 'c',
            'check(environment=any(array[''live''::text,''paper''::text]))'
          ),
          (
            'schema_migration_ledger_v1_execution_duration_ms_check', 'c',
            'check(execution_duration_ms>=0)'
          ),
          (
            'schema_migration_ledger_v1_pkey', 'p',
            'primarykey(ledger_id)'
          ),
          (
            'schema_migration_ledger_v1_status_check', 'c',
            'check(status=any(array[''applied''::text,''baseline_adopted''::text,''failed''::text,''skipped_already_applied''::text,''checksum_conflict''::text]))'
          ),
          (
            'schema_migration_ledger_v1_status_success_ck', 'c',
            'check(success=(status=any(array[''applied''::text,''baseline_adopted''::text,''skipped_already_applied''::text])))'
          )
    ), actual AS (
        SELECT
            constraint_row.conname::TEXT AS constraint_name,
            constraint_row.contype::TEXT AS constraint_type,
            regexp_replace(
                lower(pg_get_constraintdef(constraint_row.oid, TRUE)),
                '[[:space:]]+', '', 'g'
            ) AS definition,
            constraint_row.convalidated,
            constraint_row.condeferrable,
            constraint_row.condeferred
        FROM pg_constraint constraint_row
        WHERE constraint_row.conrelid =
              'public.schema_migration_ledger_v1'::regclass
          AND constraint_row.contype IN ('c', 'p', 'u', 'f')
    ), comparison AS (
        SELECT
            COALESCE(expected.constraint_name, actual.constraint_name)
                AS constraint_name,
            CASE
              WHEN expected.constraint_name IS NULL THEN 'unexpected'
              WHEN actual.constraint_name IS NULL THEN 'missing'
              WHEN expected.constraint_type <> actual.constraint_type THEN 'type'
              WHEN expected.definition <> actual.definition THEN 'definition'
              WHEN NOT actual.convalidated THEN 'not_validated'
              WHEN actual.condeferrable OR actual.condeferred THEN 'deferrability'
              ELSE NULL
            END AS issue
        FROM expected
        FULL JOIN actual USING (constraint_name)
    )
    SELECT string_agg(
        format('%s:%s', constraint_name, issue),
        ',' ORDER BY constraint_name
    )
    INTO issues
    FROM comparison
    WHERE issue IS NOT NULL;

    IF issues IS NOT NULL THEN
        RAISE EXCEPTION
            'SCHEMA_MIGRATION_LEDGER_CONSTRAINT_CONTRACT_MISMATCH:%', issues;
    END IF;
END;
$ledger_constraint_contract$;

CREATE INDEX IF NOT EXISTS ix_schema_migration_ledger_v1_lookup
    ON public.schema_migration_ledger_v1(
        environment, migration_id, applied_at DESC
    );

DO $ledger_index_contract$
DECLARE
    definition TEXT;
    conflicting_unique_indexes INTEGER;
BEGIN
    SELECT regexp_replace(
        lower(pg_get_indexdef(index_row.indexrelid)),
        '[[:space:]]+', '', 'g'
    )
    INTO definition
    FROM pg_index index_row
    JOIN pg_class index_relation
      ON index_relation.oid = index_row.indexrelid
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    WHERE index_namespace.nspname = 'public'
      AND index_relation.relname = 'ix_schema_migration_ledger_v1_lookup'
      AND index_row.indrelid = 'public.schema_migration_ledger_v1'::regclass
      AND index_row.indisvalid
      AND index_row.indisready
      AND NOT index_row.indisunique
      AND NOT index_row.indisprimary
      AND index_row.indpred IS NULL
      AND index_row.indexprs IS NULL;

    IF definition IS DISTINCT FROM
       'createindexix_schema_migration_ledger_v1_lookuponpublic.schema_migration_ledger_v1usingbtree(environment,migration_id,applied_atdesc)' THEN
        RAISE EXCEPTION
            'SCHEMA_MIGRATION_LEDGER_INDEX_CONTRACT_MISMATCH:%',
            COALESCE(definition, '<missing>');
    END IF;

    SELECT count(*)
    INTO conflicting_unique_indexes
    FROM pg_index index_row
    WHERE index_row.indrelid = 'public.schema_migration_ledger_v1'::regclass
      AND index_row.indisunique
      AND NOT index_row.indisprimary;

    IF conflicting_unique_indexes <> 0 THEN
        RAISE EXCEPTION
            'SCHEMA_MIGRATION_LEDGER_UNIQUE_CONTRACT_MISMATCH:%',
            conflicting_unique_indexes;
    END IF;
END;
$ledger_index_contract$;

COMMENT ON TABLE public.schema_migration_ledger_v1 IS
    'Technical SSOT for schema migration execution provenance; contains no trading data.';

COMMIT;
