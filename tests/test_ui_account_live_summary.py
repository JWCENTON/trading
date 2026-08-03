from fastapi.testclient import TestClient

import api.main as api


def test_live_account_empty_outcome_cohort_exposes_rollout_impact_counts(
    monkeypatch,
):
    monkeypatch.setattr(api, "TRADING_MODE", "LIVE")
    monkeypatch.setattr(api, "exchange_client", object())
    monkeypatch.setattr(
        api,
        "_load_account_summary",
        lambda: api.AccountSummary(total_usdt=0.0, balances=[]),
    )
    api.app.dependency_overrides[api.require_auth] = lambda: api.CurrentUser(
        id=1,
        username="targeted-test",
        is_active=True,
        is_admin=True,
        must_change_password=False,
    )
    try:
        response = TestClient(api.app).get("/ui/account")
    finally:
        api.app.dependency_overrides.pop(api.require_auth, None)

    assert response.status_code == 200
    assert response.json()["rollout_impact_counts"] == {}
