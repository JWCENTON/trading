"""Independent LOCAL PAPER observer. No runtime imports or trading authority.

Active PostgreSQL transport is SELECT-only in enforced read-only transactions.
All collector state, source evidence and outcomes live in an external SQLite DB.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import subprocess
import time

VERSION = "LOCAL_PAPER_MARKET_CONTEXT_SHADOW_V1"
L3_VERSION = "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4"
L3_FP = "da53f8e4f0477d23cc405215746bf8c6920d5bd0f4372eb11f06db8dc33078d5"
L3_REV = "47b6cbd4ff13f386ad4f1abfd558fea49bef2859"
SALT = "0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057"
THRESHOLDS = {
    "L3_REGIME_WOULD_ALLOW": int("21c7011d15cd6242162f54445f0c09cf558e20c9c172425fd3f0000000000000", 16),
    "L3_REGIME_WOULD_BLOCK_SAMPLE": int("1446de7b06f1b5b43244466635a7ba48e4b51d5979c7d8a07806000000000000", 16),
}
NA = "NOT_AVAILABLE"
ROOT = Path(__file__).resolve().parents[1]
STORE = Path("/home/jacek/waltrade-experiments/l3-market-context-shadow-v1")


def now():
    return datetime.now(timezone.utc).isoformat()


def dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def query(sql):
    # All call sites supply fixed SELECTs; never accept SQL from the CLI.
    if not sql.lstrip().upper().startswith("SELECT ") or ";" in sql:
        raise ValueError("SELECT_ONLY")
    cmd = ["docker", "compose", "--env-file", ".env.okx.paper", "-p", "trading-paper",
           "-f", "docker-compose.yaml", "-f", "docker-compose.paper.override.yaml",
           "exec", "-T", "db", "sh", "-c",
           'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=10000 '
           '-c lock_timeout=1000 -c application_name=l3_market_context_shadow_v1" '
           'psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d trading_paper']
    result = subprocess.run(cmd, cwd=ROOT, input="BEGIN READ ONLY;\n" + sql + ";\nROLLBACK;\n",
                            text=True, capture_output=True, timeout=25, check=True)
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def eligible(o, gate):
    return (o["deployment_id"] == "local-paper" and o["environment"] == "trading_paper"
            and o["raw_signal_state"] == "PRESENT" and o["base_decision"] == "BUY"
            and o["outcome_eligible"] and o["observation_type"] not in
            {"ALREADY_OPEN_BLOCK", "POSITION_HOLD"}
            and gate.get("decision") == "ENTRY_CHECK"
            and gate.get("why") in {"POLICY_ALLOW", "POLICY_WOULD_BLOCK"})


def assignment(o, gate):
    cohort = "L3_REGIME_WOULD_BLOCK_SAMPLE" if gate["would_block"] else "L3_REGIME_WOULD_ALLOW"
    identity = "|".join(("LOCAL_PAPER_REGIME_GATE_OPPORTUNITY_V1", "local-paper",
                         str(gate["id"]), o["symbol"], o["interval"], o["strategy"], "BUY",
                         dt(o["candle_open_time"]).astimezone(timezone.utc).isoformat()))
    digest = hashlib.sha256("|".join((identity, SALT, cohort, L3_VERSION)).encode()).hexdigest()
    return {"identity": identity, "cohort": cohort, "digest": digest,
            "selected": int(digest, 16) < THRESHOLDS[cohort],
            "authority": "DERIVED_FROM_FROZEN_L3_CONTRACT_NOT_EXECUTION_DECISION"}


def context(candles, at, minutes):
    end = dt(at)
    rows = [c for c in candles if end - timedelta(minutes=minutes) <= dt(c["open_time"])
            and dt(c["close_time"]) < end]
    rows.sort(key=lambda c: c["open_time"])
    # Strict complete trailing minute window; never impute missing bars.
    if len(rows) != minutes or any(dt(b["open_time"]) - dt(a["open_time"]) != timedelta(minutes=1)
                                  for a, b in zip(rows, rows[1:])):
        return {"status": NA, "reason": "INCOMPLETE_FINALIZED_1M_WINDOW", "count": len(rows)}
    closes = [float(c["close"]) for c in rows]
    returns = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    support = min(float(c["low"]) for c in rows)
    resistance = max(float(c["high"]) for c in rows)
    volume = [float(c["volume"]) for c in rows]
    return {"status": "AVAILABLE", "window_minutes": minutes,
            "momentum_pct": (closes[-1] / float(rows[0]["open"]) - 1) * 100,
            "realized_volatility_log_return_std": statistics.pstdev(returns),
            "volume_sum": sum(volume), "last_volume": volume[-1],
            "last_volume_over_window_mean": volume[-1] / statistics.mean(volume) if sum(volume) else NA,
            "support": support, "resistance": resistance,
            "distance_support_pct": (closes[-1] / support - 1) * 100,
            "distance_resistance_pct": (resistance / closes[-1] - 1) * 100,
            "source_ids": [c["id"] for c in rows], "source_max_close_time": rows[-1]["close_time"]}


def targets(price, entry_fee, exit_fee):
    p, fi, fo = map(lambda v: Decimal(str(v)), (price, entry_fee, exit_fee))
    return {str(n): str(p * (1 + fi + Decimal(n) / 100) / (1 - fo)) for n in (0, 1, 2, 3)}


def open_store(path):
    path.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path / "shadow.sqlite")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS contract (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS snapshots (identity TEXT PRIMARY KEY, opportunity_at TEXT, payload TEXT, hash TEXT);
      CREATE TABLE IF NOT EXISTS raw_candles (id INTEGER, hash TEXT, payload TEXT, observed_at TEXT,
                                              symbol TEXT, open_time TEXT, PRIMARY KEY(id,hash));
      CREATE INDEX IF NOT EXISTS raw_candle_symbol_time ON raw_candles(symbol,open_time);
      CREATE TABLE IF NOT EXISTS outcomes (identity TEXT, observed_at TEXT, payload TEXT, hash TEXT,
                                           PRIMARY KEY(identity,hash));
      CREATE TABLE IF NOT EXISTS polls (at TEXT PRIMARY KEY, payload TEXT);
    """)
    return db


def check_contract():
    rows = query("SELECT jsonb_build_object('database',current_database(),'read_only',"
                 "current_setting('transaction_read_only'),'contract',to_jsonb(c)) "
                 f"FROM long_horizon_l3_contract_v1 c WHERE contract_version={literal(L3_VERSION)}")
    if len(rows) != 1 or rows[0]["database"] != "trading_paper" or rows[0]["read_only"] != "on":
        raise RuntimeError("LOCAL_PAPER_READ_ONLY_CONTRACT_FAILED")
    c = rows[0]["contract"]
    if c["treatment_fingerprint"] != L3_FP or c["status"] != "ACTIVE" or c["source_revision"] != L3_REV:
        raise RuntimeError("L3_V4_PROVENANCE_CHANGED_STOP_COLLECTOR")
    return c


def initialize(db):
    row = db.execute("SELECT payload FROM contract WHERE id=1").fetchone()
    if row:
        return json.loads(row[0])
    source = check_contract()
    contract = {"version": VERSION, "start_utc": now(), "l3_fingerprint": L3_FP,
                "l3_source_revision": L3_REV, "l3_cutoff": source["start_cutoff"],
                "scope": "ACTUAL_CANONICAL_ELIGIBLE_OPPORTUNITIES_ONLY",
                "occupied_slot": "EXCLUDED_NOT_ELIGIBLE", "portfolio_reuse": "NOT_IDENTIFIABLE_IN_V1",
                "features": "TRAILING_FINALIZED_1M_WINDOWS_15_60_240_1440_MINUTES",
                "support_resistance": "TRAILING_15M_MIN_LOW_MAX_HIGH_NO_PIVOT_FUTURE_BARS",
                "regime": "EXISTING_MARKET_REGIME_TS_AND_CREATED_AT_BEFORE_OPPORTUNITY_ONLY",
                "source_availability": "CANDLE_FINAL_CLOSE_TIME_PROXY_NO_INGESTION_TIMESTAMP_IN_SCHEMA",
                "no_lookahead": "FEATURE_CLOSE_TIME_STRICTLY_BEFORE_OPPORTUNITY_OUTCOMES_STORED_SEPARATELY",
                "outcomes": "FINALIZED_1M_CLOSE_TOUCH_DIAGNOSTIC_NOT_FILL_PROOF",
                "horizon": "NO_FIXED_TIME_EXIT_OPEN_PATH_RIGHT_CENSORED_AT_LAST_OBSERVED_CLOSE",
                "external_sources": NA, "threshold_selection": "NONE", "trading_authority": "NONE"}
    contract["fingerprint"] = fingerprint(contract)
    db.execute("INSERT INTO contract VALUES(1,?)", (encode(contract),))
    db.commit()
    return contract


def candles_for(symbol, start, end):
    return query("SELECT to_jsonb(c) FROM candles c WHERE symbol=" + literal(symbol) +
                 " AND interval='1m' AND open_time >= " + literal(start) +
                 "::timestamptz AND close_time < " + literal(end) +
                 "::timestamptz AND close_time < clock_timestamp() ORDER BY open_time")


def save_raw(db, candles, observed):
    db.executemany("INSERT OR IGNORE INTO raw_candles VALUES(?,?,?,?,?,?)",
                   [(c["id"], fingerprint(c), encode(c), observed,c["symbol"],c["open_time"]) for c in candles])


def collect(db, contract):
    check_contract()
    observed = now()
    # Replay all post-start identities to recover projection lag without advancing past missing rows.
    rows = query("SELECT jsonb_build_object('opportunity',to_jsonb(o),'gate',to_jsonb(g),"
                 "'decision',to_jsonb(d)) FROM paper_opportunity_observation_v1 o "
                 "JOIN causal_decision_observation_v1 d ON d.event_id=o.causal_event_id "
                 "JOIN regime_gate_events g ON g.id=d.regime_gate_event_id "
                 "WHERE o.deployment_id='local-paper' AND o.environment='trading_paper' "
                 "AND o.raw_signal_state='PRESENT' AND o.base_decision='BUY' "
                 "AND o.outcome_eligible AND g.decision='ENTRY_CHECK' "
                 "AND g.why IN ('POLICY_ALLOW','POLICY_WOULD_BLOCK') "
                 "AND o.evaluation_started_at >= " + literal(contract["start_utc"]) + "::timestamptz "
                 "AND g.created_at >= " + literal(contract["start_utc"]) + "::timestamptz "
                 "ORDER BY o.evaluation_started_at")
    added = 0
    for item in rows:
        o, gate = item["opportunity"], item["gate"]
        if not eligible(o, gate):
            continue
        identity = o["observation_key"]
        if db.execute("SELECT 1 FROM snapshots WHERE identity=?", (identity,)).fetchone():
            continue
        at = o["evaluation_started_at"]
        if any(Decimal(str(o[k])) != Decimal("0.0035") for k in ("fee_rate_entry", "fee_rate_exit")):
            raise RuntimeError("FROZEN_FEE_V2_RATES_NOT_CONFIRMED")
        if dt(o["candle_open_time"]) + timedelta(minutes=int(o["interval"][:-1])) > dt(at):
            raise RuntimeError("OPPORTUNITY_SOURCE_CANDLE_NOT_FINAL")
        histories = {}
        for symbol in sorted({o["symbol"], "BTCUSDC"}):
            history = candles_for(symbol, (dt(at) - timedelta(days=1, minutes=1)).isoformat(), at)
            save_raw(db, history, observed)
            histories[symbol] = history
        regimes = query("SELECT to_jsonb(r) FROM market_regime r WHERE symbol IN ('BTCUSDC'," +
                        literal(o["symbol"]) + ") AND ts < " + literal(at) + "::timestamptz "
                        "AND created_at <= " + literal(at) + "::timestamptz "
                        "AND ts >= " + literal((dt(at)-timedelta(days=2)).isoformat()) +
                        "::timestamptz ORDER BY ts")
        latest = {}
        for r in regimes:
            latest[r["symbol"] + ":" + r["interval"]] = r
        own = context(histories[o["symbol"]], at, 15)
        price_targets = targets(o["reference_price"], o["fee_rate_entry"], o["fee_rate_exit"])
        room = {}
        for n in (1, 2, 3):
            target = float(price_targets[str(n)])
            resistance = own.get("resistance")
            room[str(n)] = {"target_price": price_targets[str(n)],
                            "resistance_minus_target": resistance-target if resistance else NA,
                            "resistance_before_target": resistance < target if resistance else NA}
        snapshot = {**item, "recorded_at": observed, "contract_fingerprint": contract["fingerprint"],
                    "sampling": assignment(o, gate), "symbol_context": own,
                    "symbol_regime": latest.get(o["symbol"]+":"+o["interval"], NA),
                    "btc_context": {tf: {"features": context(histories["BTCUSDC"], at, mins),
                         "regime": latest.get("BTCUSDC:"+tf, NA)}
                         for tf, mins in (("15m",15),("1h",60),("4h",240),("1d",1440))},
                    "fee_aware_target_prices": price_targets, "room_to_target": room,
                    "fee_basis": "CANONICAL_OPPORTUNITY_FROZEN_FEE_MODEL_REFERENCE_PRICE_NO_FILL_ASSUMPTION",
                    "external": {k: NA for k in ("funding", "open_interest", "etf_flows", "dxy",
                                                  "yields", "oil", "cpi_fomc_calendar", "news_sentiment")},
                    "lookahead_status": "PASS_EVENT_TIME_ONLY_INGESTION_TIME_NOT_AVAILABLE"}
        db.execute("INSERT INTO snapshots VALUES(?,?,?,?)", (identity, at, encode(snapshot), fingerprint(snapshot)))
        added += 1
        db.commit()
    update_outcomes(db, observed)
    status = {"observed_at": observed, "eligible_seen": len(rows), "added": added,
              "snapshots": db.execute("SELECT count(*) FROM snapshots").fetchone()[0],
              "active_db_writes": 0, "collector_status": "ACTIVE"}
    db.execute("INSERT INTO polls VALUES(?,?)", (observed, encode(status)))
    db.commit()
    print(encode(status), flush=True)
    return status


def path_outcome(snapshot, candles):
    o = snapshot["opportunity"]
    p = Decimal(str(o["reference_price"]))
    fi, fo = Decimal(str(o["fee_rate_entry"])), Decimal(str(o["fee_rate_exit"]))
    at = dt(o["evaluation_started_at"])
    path = sorted((c for c in candles if dt(c["open_time"]) >= at), key=lambda c: c["open_time"])
    values = [(c, Decimal(str(c["close"])) / p * (1-fo) - 1-fi) for c in path]
    touches = {}
    for level in (0, 1, 2, 3):
        hit = next(((c, net) for c, net in values if net >= Decimal(level)/100), None)
        touches[str(level)] = ({"source_id": hit[0]["id"], "at": hit[0]["close_time"],
                               "seconds": (dt(hit[0]["close_time"])-at).total_seconds()}
                              if hit else "PENDING_RIGHT_CENSORED")
    gaps = sum(dt(b["open_time"])-dt(a["open_time"]) != timedelta(minutes=1) for a,b in zip(path,path[1:]))
    return {"basis": "REFERENCE_PRICE_FINALIZED_CLOSE_COUNTERFACTUAL_PER_1_USDC_NOT_EXECUTABLE_FILL",
            "touches": touches, "MFE_net_per_usdc": str(max((v for _,v in values), default=Decimal(0))) if values else NA,
            "MAE_net_per_usdc": str(min((v for _,v in values), default=Decimal(0))) if values else NA,
            "path_count": len(path), "internal_gaps": gaps, "right_censored": True,
            "censored_at": path[-1]["close_time"] if path else NA,
            "hard_risk_without_actual_position": NA}


def update_outcomes(db, observed):
    snapshots = db.execute("SELECT identity,payload FROM snapshots").fetchall()
    if not snapshots:
        return
    parsed = [(identity,json.loads(payload)) for identity,payload in snapshots]
    starts = {}
    for _,snap in parsed:
        o = snap["opportunity"]
        starts[o["symbol"]] = min(starts.get(o["symbol"],o["evaluation_started_at"]),o["evaluation_started_at"])
    histories = {}
    for symbol,start in starts.items():
        latest = db.execute("SELECT max(open_time) FROM raw_candles WHERE symbol=?",(symbol,)).fetchone()[0]
        fetch_start = max(start,latest) if latest else start
        save_raw(db,candles_for(symbol,fetch_start,observed),observed)
        histories[symbol] = [json.loads(r[0]) for r in db.execute(
            "SELECT payload FROM raw_candles WHERE rowid IN (SELECT max(rowid) FROM raw_candles "
            "WHERE symbol=? AND open_time>=? GROUP BY id) ORDER BY open_time",(symbol,start))]
    earliest = min(starts.values())
    canonical_rows = query("SELECT jsonb_build_object('admission',to_jsonb(a),'position',to_jsonb(p),"
                          "'financial_truth',to_jsonb(f),'paired_l0',to_jsonb(l)) "
                          "FROM long_horizon_l3_admission_v1 a LEFT JOIN positions p ON p.id=a.position_id "
                          "LEFT JOIN canonical_financial_truth_v1 f ON f.position_id=p.id "
                          "LEFT JOIN long_horizon_l3_l0_comparator_v1 l ON l.position_id=p.id "
                          f"WHERE a.created_at >= {literal(earliest)}::timestamptz "
                          f"AND a.contract_version={literal(L3_VERSION)}")
    by_gate = {}
    for row in canonical_rows:
        by_gate.setdefault(row["admission"]["gate_event_id"],[]).append(row)
    for identity,snap in parsed:
        o = snap["opportunity"]
        candles = histories[o["symbol"]]
        gate_id = int(snap["gate"]["id"])
        canonical = by_gate.get(gate_id,[])
        result = {"diagnostic_path": path_outcome(snap, candles), "canonical_l3_and_paired_l0": canonical,
                  "unaccepted_outcome": "NO_ACTUAL_PNL_OR_HARD_RISK_FOR_UNEXECUTED_OPPORTUNITY"}
        digest = fingerprint(result)
        db.execute("INSERT OR IGNORE INTO outcomes VALUES(?,?,?,?)", (identity, observed, encode(result), digest))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    # One OS-level owner prevents overlapping collectors; lock is external only.
    import fcntl
    STORE.mkdir(parents=True, exist_ok=True)
    with (STORE / "collector.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = open_store(STORE)
        contract = initialize(db)
        while True:
            collect(db, contract)
            if args.once:
                break
            time.sleep(30)


if __name__ == "__main__":
    main()
