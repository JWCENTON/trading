"""Frozen, evidence-only thesis semantic candidates.

This module has no trading imports or sinks.  It evaluates two prospective
shadow candidates from immutable THESIS_EVIDENCE_BUNDLE_V1 rows:

* NO_THESIS -> FORMING
* FORMING -> ACTIVE_CANDIDATE (observation-only, never authoritative)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from psycopg2.extras import Json

from common.thesis_evidence_bundle import canonical_json, fingerprint


CONTRACT_VERSION = "THESIS_SEMANTIC_CANDIDATE_OBSERVATION_V1"
FREEZE_CONTRACT_VERSION = "THESIS_SEMANTIC_CANDIDATE_FREEZE_V1"
FORMING_RULE_ID = "THESIS_FORMING_ALIGNMENT_V1"
ACTIVE_RULE_ID = "THESIS_ACTIVE_ADJACENT_COHERENCE_V1"
RULE_VERSION = "V1"

FORMING_RULE_SPEC = {
    "rule_id": FORMING_RULE_ID,
    "rule_version": RULE_VERSION,
    "from_state": "NO_THESIS",
    "candidate_to_state": "FORMING",
    "inputs": [
        "bundle.evidence_status",
        "structural.directional_return_pct:6h,24h,3d",
        "previous_complete.structural.directional_return_pct:6h",
        "mme.availability_state",
        "mme.direction",
    ],
    "predicate": [
        "bundle_complete",
        "sign_24h_equals_sign_3d",
        "shared_structural_sign_nonzero",
        "previous_complete_6h_sign_differs",
        "current_6h_sign_equals_shared_sign",
        "available_mme_contains_shared_direction",
        "available_mme_excludes_opposite_direction",
    ],
    "evaluation_order": [
        "evidence_complete",
        "from_state",
        "previous_complete_bundle",
        "long_horizon_agreement",
        "nonzero_direction",
        "previous_6h",
        "current_6h",
        "opposite_mme",
        "compatible_mme",
    ],
    "missing_data_behavior": "FREEZE_PREVIOUS_CANDIDATE_STATE_NO_TRANSITION",
    "tactical_role": "EVIDENCE_ONLY_NOT_AN_INPUT",
}

ACTIVE_RULE_SPEC = {
    "rule_id": ACTIVE_RULE_ID,
    "rule_version": RULE_VERSION,
    "from_state": "FORMING",
    "candidate_to_state": "ACTIVE_CANDIDATE",
    "authority": "SHADOW_VALIDATION_ONLY",
    "inputs": [
        "bundle.evidence_status",
        "previous_bundle.evidence_status",
        "stored_forming_direction",
        "current.structural.directional_return_pct:6h,24h,3d",
        "previous.structural.directional_return_pct:6h,24h,3d",
        "mme.availability_state",
        "mme.direction",
    ],
    "predicate": [
        "current_bundle_complete",
        "immediately_previous_semantic_evaluation_complete",
        "stored_forming_direction_unchanged",
        "current_all_horizons_equal_stored_direction",
        "previous_all_horizons_equal_stored_direction",
        "available_mme_contains_stored_direction",
        "available_mme_excludes_opposite_direction",
    ],
    "evaluation_order": [
        "current_evidence_complete",
        "from_state",
        "previous_bundle",
        "previous_evidence_complete",
        "previous_semantic_evaluation",
        "current_horizon_coherence",
        "previous_horizon_coherence",
        "opposite_mme",
        "compatible_mme",
    ],
    "missing_data_behavior": "FREEZE_PREVIOUS_CANDIDATE_STATE_NO_TRANSITION",
    "lifecycle_behavior": "EVENT_ONLY_NOT_LATCHED_AUTHORITY",
}

FORMING_RULE_FINGERPRINT = fingerprint(FORMING_RULE_SPEC)
ACTIVE_RULE_FINGERPRINT = fingerprint(ACTIVE_RULE_SPEC)


@dataclass(frozen=True)
class CandidateDecision:
    evaluation_result: str
    reason_code: str
    direction: str | None


def _sign(value: Any) -> int | None:
    if value is None:
        return None
    number = Decimal(str(value))
    if number > 0:
        return 1
    if number < 0:
        return -1
    return 0


def _direction(sign: int | None) -> str | None:
    return {1: "UP", -1: "DOWN"}.get(sign)


def _opposite(direction: str | None) -> str | None:
    return {"UP": "DOWN", "DOWN": "UP"}.get(direction)


def evaluate_forming_candidate(
    *,
    evidence_complete: bool,
    already_forming: bool,
    current_6h: Any,
    current_24h: Any,
    current_3d: Any,
    previous_complete_6h: Any,
    previous_complete_exists: bool,
    available_mme_directions: Sequence[str],
) -> CandidateDecision:
    shared_sign = _sign(current_24h)
    direction = _direction(shared_sign)
    if not evidence_complete:
        return CandidateDecision("EVIDENCE_INCOMPLETE", "EVIDENCE_INCOMPLETE", direction)
    if already_forming:
        return CandidateDecision("NO_MATCH", "FROM_STATE_NOT_NO_THESIS", direction)
    if not previous_complete_exists:
        return CandidateDecision("NO_MATCH", "PREVIOUS_COMPLETE_BUNDLE_NOT_FOUND", direction)
    if _sign(current_24h) != _sign(current_3d):
        return CandidateDecision("NO_MATCH", "STRUCTURAL_24H_3D_DISAGREE", None)
    if shared_sign in (None, 0):
        return CandidateDecision("NO_MATCH", "STRUCTURAL_DIRECTION_ZERO", None)
    if _sign(previous_complete_6h) == shared_sign:
        return CandidateDecision("NO_MATCH", "PREVIOUS_6H_ALREADY_ALIGNED", direction)
    if _sign(current_6h) != shared_sign:
        return CandidateDecision("NO_MATCH", "CURRENT_6H_NOT_ALIGNED", direction)
    available = set(available_mme_directions)
    if _opposite(direction) in available:
        return CandidateDecision("NO_MATCH", "MME_OPPOSITE_AVAILABLE", direction)
    if direction not in available:
        return CandidateDecision("NO_MATCH", "MME_COMPATIBLE_NOT_AVAILABLE", direction)
    return CandidateDecision("MATCH", "FORMING_ALIGNMENT_MATCH", direction)


def evaluate_active_candidate(
    *,
    evidence_complete: bool,
    forming_direction: str | None,
    previous_bundle_exists: bool,
    previous_evidence_complete: bool,
    previous_semantic_evaluation_complete: bool,
    current_horizons: Sequence[Any],
    previous_horizons: Sequence[Any],
    available_mme_directions: Sequence[str],
) -> CandidateDecision:
    if not evidence_complete:
        return CandidateDecision(
            "EVIDENCE_INCOMPLETE", "EVIDENCE_INCOMPLETE", forming_direction,
        )
    if forming_direction is None:
        return CandidateDecision("NO_MATCH", "FORMING_NOT_ESTABLISHED", None)
    if not previous_bundle_exists:
        return CandidateDecision("NO_MATCH", "PREVIOUS_BUNDLE_NOT_FOUND", forming_direction)
    if not previous_evidence_complete:
        return CandidateDecision(
            "NO_MATCH", "PREVIOUS_EVIDENCE_INCOMPLETE", forming_direction,
        )
    if not previous_semantic_evaluation_complete:
        return CandidateDecision(
            "NO_MATCH", "PREVIOUS_SEMANTIC_EVALUATION_MISSING", forming_direction,
        )
    expected_sign = {"UP": 1, "DOWN": -1}[forming_direction]
    if any(_sign(value) != expected_sign for value in current_horizons):
        return CandidateDecision(
            "NO_MATCH", "CURRENT_HORIZONS_NOT_COHERENT", forming_direction,
        )
    if any(_sign(value) != expected_sign for value in previous_horizons):
        return CandidateDecision(
            "NO_MATCH", "PREVIOUS_HORIZONS_NOT_COHERENT", forming_direction,
        )
    available = set(available_mme_directions)
    if _opposite(forming_direction) in available:
        return CandidateDecision("NO_MATCH", "MME_OPPOSITE_AVAILABLE", forming_direction)
    if forming_direction not in available:
        return CandidateDecision(
            "NO_MATCH", "MME_COMPATIBLE_NOT_AVAILABLE", forming_direction,
        )
    return CandidateDecision("MATCH", "ACTIVE_ADJACENT_COHERENCE_MATCH", forming_direction)


def candidate_enabled(environ: Mapping[str, str]) -> bool:
    return str(
        environ.get("THESIS_SEMANTIC_CANDIDATE_FREEZE_V1_ENABLED", "0")
    ).strip().lower() in {"1", "true", "yes", "on"}


def _insert_immutable(
    cur,
    *,
    table: str,
    id_column: str,
    identity: str,
    fingerprint_column: str,
    expected_fingerprint: str,
    values: Mapping[str, Any],
) -> bool:
    columns = tuple(values)
    rendered = [
        Json(values[column], dumps=canonical_json)
        if isinstance(values[column], (dict, list)) else values[column]
        for column in columns
    ]
    cur.execute(
        f"INSERT INTO public.{table}({','.join(columns)}) "
        f"VALUES({','.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ({id_column}) DO NOTHING",
        rendered,
    )
    if cur.rowcount == 1:
        return True
    cur.execute(
        f"SELECT {fingerprint_column} FROM public.{table} WHERE {id_column}=%s",
        (identity,),
    )
    row = cur.fetchone()
    if row is None or row[0] != expected_fingerprint:
        raise RuntimeError(
            f"THESIS_SEMANTIC_CANDIDATE_FINGERPRINT_CONFLICT:{table}:{identity}"
        )
    return False


def _ensure_freeze(cur, run: Mapping[str, Any]) -> tuple[str, Any, bool]:
    identity_source = {
        "contract_version": FREEZE_CONTRACT_VERSION,
        "environment": run["environment"],
        "deployment_id": run["deployment_id"],
    }
    freeze_id = fingerprint({"identity_kind": "THESIS_CANDIDATE_FREEZE", **identity_source})
    cur.execute(
        "SELECT effective_at,freeze_fingerprint FROM "
        "public.thesis_semantic_candidate_freeze_v1 WHERE freeze_id=%s",
        (freeze_id,),
    )
    row = cur.fetchone()
    if row is not None:
        return freeze_id, row[0], False
    values = {
        "freeze_id": freeze_id,
        **identity_source,
        "effective_at": run["evidence_cutoff"],
        "git_revision": run["git_revision"],
        "forming_rule_id": FORMING_RULE_ID,
        "forming_rule_version": RULE_VERSION,
        "forming_rule_fingerprint": FORMING_RULE_FINGERPRINT,
        "active_candidate_rule_id": ACTIVE_RULE_ID,
        "active_candidate_rule_version": RULE_VERSION,
        "active_candidate_rule_fingerprint": ACTIVE_RULE_FINGERPRINT,
    }
    values["freeze_fingerprint"] = fingerprint(values)
    inserted = _insert_immutable(
        cur,
        table="thesis_semantic_candidate_freeze_v1",
        id_column="freeze_id",
        identity=freeze_id,
        fingerprint_column="freeze_fingerprint",
        expected_fingerprint=values["freeze_fingerprint"],
        values=values,
    )
    return freeze_id, values["effective_at"], inserted


def _previous_bundle(cur, bundle: Mapping[str, Any], *, complete_only: bool) -> dict[str, Any] | None:
    complete_clause = "AND b.evidence_status='COMPLETE'" if complete_only else ""
    cur.execute(
        f"""
        SELECT b.bundle_id,b.evidence_status,b.evidence_cutoff,
               s6.directional_return_pct,s24.directional_return_pct,
               s3.directional_return_pct
          FROM public.thesis_evidence_bundle_v1 b
          JOIN public.thesis_structural_observation_v1 s6
            ON s6.observation_id=b.structural_6h_id
          JOIN public.thesis_structural_observation_v1 s24
            ON s24.observation_id=b.structural_24h_id
          JOIN public.thesis_structural_observation_v1 s3
            ON s3.observation_id=b.structural_3d_id
         WHERE b.environment=%s AND b.deployment_id=%s AND b.symbol=%s
           AND b.evidence_cutoff<%s {complete_clause}
         ORDER BY b.evidence_cutoff DESC,b.created_at DESC
         LIMIT 1
        """,
        (
            bundle["environment"], bundle["deployment_id"], bundle["symbol"],
            bundle["evidence_cutoff"],
        ),
    )
    row = cur.fetchone()
    if row is None:
        return None
    names = ("bundle_id", "evidence_status", "evidence_cutoff", "r6", "r24", "r3")
    return dict(zip(names, row))


def _forming_direction(cur, bundle: Mapping[str, Any]) -> str | None:
    cur.execute(
        """
        SELECT direction
          FROM public.thesis_semantic_candidate_observation_v1
         WHERE environment=%s AND deployment_id=%s AND symbol=%s
           AND candidate_rule_id=%s AND evaluation_result='MATCH'
           AND evidence_cutoff<%s
         ORDER BY evidence_cutoff ASC,created_at ASC
         LIMIT 1
        """,
        (
            bundle["environment"], bundle["deployment_id"], bundle["symbol"],
            FORMING_RULE_ID, bundle["evidence_cutoff"],
        ),
    )
    row = cur.fetchone()
    return None if row is None else row[0]


def _previous_semantic_complete(cur, previous_bundle_id: str | None) -> bool:
    if previous_bundle_id is None:
        return False
    cur.execute(
        """
        SELECT count(*),count(*) FILTER (
            WHERE evaluation_result<>'EVIDENCE_INCOMPLETE'
        )
          FROM public.thesis_semantic_candidate_observation_v1
         WHERE evidence_bundle_id=%s
        """,
        (previous_bundle_id,),
    )
    total, complete = cur.fetchone()
    return total == 2 and complete == 2


def _persist_evaluation(
    cur,
    *,
    freeze_id: str,
    rule_id: str,
    rule_fingerprint: str,
    from_state: str,
    candidate_to_state: str,
    decision: CandidateDecision,
    bundle: Mapping[str, Any],
    previous_bundle_id: str | None,
) -> bool:
    evaluation_id = fingerprint({
        "identity_kind": "THESIS_SEMANTIC_CANDIDATE_EVALUATION",
        "candidate_rule_id": rule_id,
        "candidate_rule_version": RULE_VERSION,
        "evidence_bundle_id": bundle["bundle_id"],
    })
    values = {
        "evaluation_id": evaluation_id,
        "freeze_id": freeze_id,
        "contract_version": CONTRACT_VERSION,
        "candidate_rule_id": rule_id,
        "candidate_rule_version": RULE_VERSION,
        "rule_fingerprint": rule_fingerprint,
        "symbol": bundle["symbol"],
        "direction": decision.direction,
        "from_state": from_state,
        "candidate_to_state": candidate_to_state,
        "evaluation_result": decision.evaluation_result,
        "reason_code": decision.reason_code,
        "evidence_bundle_id": bundle["bundle_id"],
        "previous_bundle_id": previous_bundle_id,
        "evidence_cutoff": bundle["evidence_cutoff"],
        "environment": bundle["environment"],
        "deployment_id": bundle["deployment_id"],
        "git_revision": bundle["git_revision"],
    }
    values["candidate_fingerprint"] = fingerprint(values)
    return _insert_immutable(
        cur,
        table="thesis_semantic_candidate_observation_v1",
        id_column="evaluation_id",
        identity=evaluation_id,
        fingerprint_column="candidate_fingerprint",
        expected_fingerprint=values["candidate_fingerprint"],
        values=values,
    )


def persist_candidate_evaluations(cur, cycle: Mapping[str, Any]) -> dict[str, int]:
    """Persist prospective candidate evaluations for one evidence cycle."""
    cur.execute("SELECT to_regclass('public.thesis_semantic_candidate_observation_v1')")
    if cur.fetchone()[0] is None:
        return {"candidate_freezes": 0, "candidate_evaluations": 0}
    run = cycle["run"]
    freeze_id, effective_at, freeze_inserted = _ensure_freeze(cur, run)
    counts = {
        "candidate_freezes": int(freeze_inserted),
        "candidate_evaluations": 0,
    }
    if run["evidence_cutoff"] < effective_at:
        return counts
    for item in cycle["bundles"]:
        bundle = item["bundle"]
        current = item["structural"]
        available_directions = sorted({
            observation["direction"]
            for observation in item["mme"]
            if observation["availability_state"] == "AVAILABLE"
            and observation["direction"] is not None
        })
        prior_complete = _previous_bundle(cur, bundle, complete_only=True)
        prior_adjacent = _previous_bundle(cur, bundle, complete_only=False)
        forming_direction = _forming_direction(cur, bundle)
        forming = evaluate_forming_candidate(
            evidence_complete=bundle["evidence_status"] == "COMPLETE",
            already_forming=forming_direction is not None,
            current_6h=current["6h"]["directional_return_pct"],
            current_24h=current["24h"]["directional_return_pct"],
            current_3d=current["3d"]["directional_return_pct"],
            previous_complete_6h=None if prior_complete is None else prior_complete["r6"],
            previous_complete_exists=prior_complete is not None,
            available_mme_directions=available_directions,
        )
        counts["candidate_evaluations"] += int(_persist_evaluation(
            cur,
            freeze_id=freeze_id,
            rule_id=FORMING_RULE_ID,
            rule_fingerprint=FORMING_RULE_FINGERPRINT,
            from_state="NO_THESIS",
            candidate_to_state="FORMING",
            decision=forming,
            bundle=bundle,
            previous_bundle_id=None if prior_complete is None else prior_complete["bundle_id"],
        ))
        active = evaluate_active_candidate(
            evidence_complete=bundle["evidence_status"] == "COMPLETE",
            forming_direction=forming_direction,
            previous_bundle_exists=prior_adjacent is not None,
            previous_evidence_complete=(
                prior_adjacent is not None
                and prior_adjacent["evidence_status"] == "COMPLETE"
            ),
            previous_semantic_evaluation_complete=_previous_semantic_complete(
                cur, None if prior_adjacent is None else prior_adjacent["bundle_id"],
            ),
            current_horizons=(
                current["6h"]["directional_return_pct"],
                current["24h"]["directional_return_pct"],
                current["3d"]["directional_return_pct"],
            ),
            previous_horizons=(
                None if prior_adjacent is None else prior_adjacent["r6"],
                None if prior_adjacent is None else prior_adjacent["r24"],
                None if prior_adjacent is None else prior_adjacent["r3"],
            ),
            available_mme_directions=available_directions,
        )
        counts["candidate_evaluations"] += int(_persist_evaluation(
            cur,
            freeze_id=freeze_id,
            rule_id=ACTIVE_RULE_ID,
            rule_fingerprint=ACTIVE_RULE_FINGERPRINT,
            from_state="FORMING",
            candidate_to_state="ACTIVE_CANDIDATE",
            decision=active,
            bundle=bundle,
            previous_bundle_id=None if prior_adjacent is None else prior_adjacent["bundle_id"],
        ))
    return counts
