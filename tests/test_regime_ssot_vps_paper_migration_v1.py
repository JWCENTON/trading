from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / "db/migrations/20260906_regime_ssot_direct_paper_enforcement_v1.sql"
VPS = ROOT / "db/migrations/20260906_regime_ssot_direct_vps_paper_enforcement_v1.sql"


def test_applied_local_migration_is_immutable():
    assert sha256(LOCAL.read_bytes()).hexdigest() == (
        "92bb110882177ce1cde10343ac3ecdd07bc7ab811f923ee1ddd21726b190658f"
    )


def test_vps_artifact_has_explicit_identity_and_safety_fences():
    sql = VPS.read_text()
    assert "v_environment IS DISTINCT FROM 'PAPER'" in sql
    assert "v_deployment_id IS DISTINCT FROM 'vps-paper'" in sql
    assert "v_runtime_deployment_id IS DISTINCT FROM 'vps-paper'" in sql
    assert "'PAPER', 'vps-paper'" in sql
    assert "deployment_id='vps-paper'" in sql
    assert "deployment_id='local-paper'" not in sql
    assert "585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e" in sql
    assert "('SUPERTREND','RANGE_LOWVOL', false" in sql
