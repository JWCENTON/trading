BEGIN;

CREATE OR REPLACE VIEW v_orc_integration_v2_picks AS
WITH base AS (
  SELECT
    c.*,

    CASE
      WHEN COALESCE(c.mme_orc_avoid, false) THEN -40
      WHEN COALESCE(c.mme_orc_priority, false) THEN 18
      WHEN COALESCE(c.mme_orc_watch, false) THEN 8
      ELSE 0
    END
    + CASE
        WHEN COALESCE(c.mme_sequence_type,'') ILIKE '%EARLY_REVERSAL%' THEN 12
        WHEN COALESCE(c.mme_sequence_type,'') ILIKE '%IMPULSE%' THEN 8
        ELSE 0
      END
    + CASE
        WHEN COALESCE(c.mme_sequence_stage,'') ILIKE '%LATE%' THEN -6
        WHEN COALESCE(c.mme_late_entry_risk,0) >= 70 THEN -8
        ELSE 0
      END
    + CASE
        WHEN COALESCE(c.mme_exhaustion_risk,0) >= 80 THEN -15
        WHEN COALESCE(c.mme_exhaustion_risk,0) >= 65 THEN -8
        ELSE 0
      END
    + CASE
        WHEN COALESCE(c.mme_remaining_score,100) < 25 THEN -20
        WHEN COALESCE(c.mme_remaining_score,100) < 40 THEN -8
        ELSE 0
      END AS mme_decision_delta,

    COALESCE(c.orc_context_score, COALESCE(c.v63_score,0))
    + (
      CASE
        WHEN COALESCE(c.mme_orc_avoid, false) THEN -40
        WHEN COALESCE(c.mme_orc_priority, false) THEN 18
        WHEN COALESCE(c.mme_orc_watch, false) THEN 8
        ELSE 0
      END
      + CASE
          WHEN COALESCE(c.mme_sequence_type,'') ILIKE '%EARLY_REVERSAL%' THEN 12
          WHEN COALESCE(c.mme_sequence_type,'') ILIKE '%IMPULSE%' THEN 8
          ELSE 0
        END
      + CASE
          WHEN COALESCE(c.mme_sequence_stage,'') ILIKE '%LATE%' THEN -6
          WHEN COALESCE(c.mme_late_entry_risk,0) >= 70 THEN -8
          ELSE 0
        END
      + CASE
          WHEN COALESCE(c.mme_exhaustion_risk,0) >= 80 THEN -15
          WHEN COALESCE(c.mme_exhaustion_risk,0) >= 65 THEN -8
          ELSE 0
        END
      + CASE
          WHEN COALESCE(c.mme_remaining_score,100) < 25 THEN -20
          WHEN COALESCE(c.mme_remaining_score,100) < 40 THEN -8
          ELSE 0
        END
    ) AS orc_final_score_v2,

    CASE
      WHEN COALESCE(c.orc_v7_ready,false) = true
       AND COALESCE(c.mme_orc_avoid,false) = false
       AND COALESCE(c.mme_remaining_score,100) >= 25
       AND COALESCE(c.mme_exhaustion_risk,0) < 80
        THEN true
      ELSE false
    END AS context_v2_ready_now,

    jsonb_build_object(
      'base_orc_score', COALESCE(c.orc_context_score, COALESCE(c.v63_score,0)),
      'mme_delta', (
        CASE
          WHEN COALESCE(c.mme_orc_avoid, false) THEN -40
          WHEN COALESCE(c.mme_orc_priority, false) THEN 18
          WHEN COALESCE(c.mme_orc_watch, false) THEN 8
          ELSE 0
        END
      ),
      'sequence_type', c.mme_sequence_type,
      'sequence_stage', c.mme_sequence_stage,
      'remaining_score', c.mme_remaining_score,
      'exhaustion_risk', c.mme_exhaustion_risk,
      'late_entry_risk', c.mme_late_entry_risk
    ) AS orc_integration_v2_payload

  FROM v_orc_candidate_context_v1 c
),
ranked AS (
  SELECT
    base.*,
    ROW_NUMBER() OVER (
      ORDER BY
        context_v2_ready_now DESC,
        orc_final_score_v2 DESC NULLS LAST,
        mme_orc_readiness_score DESC NULLS LAST,
        v7_rn ASC NULLS LAST
    ) AS context_v2_rn
  FROM base
)
SELECT *
FROM ranked;

CREATE OR REPLACE VIEW v_orc_integration_v2_best AS
SELECT *
FROM v_orc_integration_v2_picks
ORDER BY context_v2_rn
LIMIT 20;

COMMIT;
