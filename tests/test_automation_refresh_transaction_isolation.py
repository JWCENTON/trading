import ast
import logging
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "automation_runner/main.py"


def _load_isolation_wrapper():
    tree = ast.parse(RUNNER.read_text())
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "run_independent_refresh_job"
    )
    namespace = {"logging": logging}
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), str(RUNNER), "exec"),
        namespace,
    )
    return namespace["run_independent_refresh_job"]


def test_failed_refresh_rolls_back_before_two_independent_jobs(caplog):
    run_job = _load_isolation_wrapper()

    class Connection:
        aborted = False
        rollbacks = 0

        def rollback(self):
            self.aborted = False
            self.rollbacks += 1

    conn = Connection()
    completed = []

    def job_a(connection):
        connection.aborted = True
        raise RuntimeError("numeric field overflow")

    def job_b(connection):
        assert connection.aborted is False
        completed.append("B")

    def job_c(connection):
        assert connection.aborted is False
        completed.append("C")

    with caplog.at_level(logging.ERROR):
        run_job(conn, job_a, "strategy_regime_stats_refresh failed")
        run_job(conn, job_b, "market_regime_confidence_refresh failed")
        run_job(conn, job_c, "market_memory_events_refresh failed")

    assert conn.rollbacks == 1
    assert completed == ["B", "C"]
    assert "strategy_regime_stats_refresh failed" in caplog.text
    assert "numeric field overflow" in caplog.text
