# common/reconcile_positions.py
import json
import logging
from binance.exceptions import BinanceAPIException

def _wd(conn, symbol, interval, strategy, severity, event, details: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO watchdog_events(symbol, interval, strategy, severity, event, details)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (symbol, interval, strategy, severity, event, json.dumps(details)),
        )


def _delete_orphan_open(conn, pos_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM positions
            WHERE id=%s
              AND status='OPEN'
              AND entry_order_id IS NULL
            """,
            (int(pos_id),),
        )


def reconcile_positions(conn, client, *, min_age_s: int = 60, limit: int = 200) -> None:
    """
    Deterministic reconcile:
      positions.*_client_order_id -> Binance get_order(origClientOrderId=...) -> positions.*_order_id
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, symbol, interval, strategy, entry_client_order_id
            FROM positions
            WHERE status='OPEN'
              AND entry_order_id IS NULL
              AND entry_client_order_id IS NOT NULL
              AND entry_time < now() - (%s || ' seconds')::interval
            ORDER BY entry_time ASC
            LIMIT %s
            """,
            (min_age_s, limit),
        )
        open_rows = cur.fetchall()

        cur.execute(
            """
            SELECT id, symbol, interval, strategy, exit_client_order_id
            FROM positions
            WHERE status='CLOSED'
              AND exit_time IS NOT NULL
              AND exit_order_id IS NULL
              AND exit_client_order_id IS NOT NULL
              AND exit_time < now() - (%s || ' seconds')::interval
            ORDER BY exit_time ASC
            LIMIT %s
            """,
            (min_age_s, limit),
        )
        closed_rows = cur.fetchall()

    # ENTRY
    for pos_id, symbol, interval, strategy, cid in open_rows:
        try:
            o = client.get_order(symbol=symbol, origClientOrderId=cid)
            oid = str(o.get("orderId") or "")
            if not oid:
                _wd(conn, symbol, interval, strategy, "WARN", "RECONCILE_ENTRY_NO_ORDER_ID", {"pos_id": pos_id, "cid": cid, "order": o})
                continue

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE positions SET entry_order_id=%s WHERE id=%s AND entry_order_id IS NULL",
                    (oid, int(pos_id)),
                )
            conn.commit()
            _wd(conn, symbol, interval, strategy, "INFO", "RECONCILE_ENTRY_OK", {"pos_id": pos_id, "cid": cid, "order_id": oid})
        except BinanceAPIException as e:
            code = getattr(e, "code", None)
            if code == -2013:
                logging.error(
                    "reconcile entry orphan phantom pos_id=%s symbol=%s cid=%s code=-2013",
                    pos_id, symbol, cid
                )
                try:
                    _delete_orphan_open(conn, int(pos_id))
                    conn.commit()
                    _wd(
                        conn, symbol, interval, strategy,
                        "CRIT", "RECONCILE_ENTRY_ORPHAN_PHANTOM_CLEANED",
                        {"pos_id": pos_id, "cid": cid, "err": str(e)}
                    )
                except Exception as cleanup_err:
                    conn.rollback()
                    logging.exception("orphan phantom cleanup failed pos_id=%s", pos_id)
                    _wd(
                        conn, symbol, interval, strategy,
                        "CRIT", "RECONCILE_ENTRY_ORPHAN_CLEANUP_FAILED",
                        {"pos_id": pos_id, "cid": cid, "err": str(cleanup_err)}
                    )
            else:
                logging.exception("reconcile entry failed pos_id=%s symbol=%s cid=%s", pos_id, symbol, cid)
                _wd(
                    conn, symbol, interval, strategy,
                    "CRIT", "RECONCILE_ENTRY_EXCEPTION",
                    {"pos_id": pos_id, "cid": cid, "err": str(e), "code": code}
                )
        except Exception as e:
            logging.exception("reconcile entry failed pos_id=%s symbol=%s cid=%s", pos_id, symbol, cid)
            _wd(
                conn, symbol, interval, strategy,
                "CRIT", "RECONCILE_ENTRY_EXCEPTION",
                {"pos_id": pos_id, "cid": cid, "err": str(e)}
            )

    # EXIT
    for pos_id, symbol, interval, strategy, cid in closed_rows:
        try:
            o = None
            oid = ""

            # try base + children (maker/market)
            for cid_try in (cid, f"{cid}-MKR", f"{cid}-MKT"):
                try:
                    o = client.get_order(symbol=symbol, origClientOrderId=cid_try)
                    oid = str((o or {}).get("orderId") or "")
                    if oid:
                        break
                except Exception:
                    continue

            if not oid:
                _wd(
                    conn, symbol, interval, strategy,
                    "WARN", "RECONCILE_EXIT_NO_ORDER_ID",
                    {"pos_id": pos_id, "cid": cid, "tried": [cid, f"{cid}-MKR", f"{cid}-MKT"], "order": o},
                )
                continue

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE positions SET exit_order_id=%s WHERE id=%s AND exit_order_id IS NULL",
                    (oid, int(pos_id)),
                )
            conn.commit()
            _wd(conn, symbol, interval, strategy, "INFO", "RECONCILE_EXIT_OK", {"pos_id": pos_id, "cid": cid, "order_id": oid})
        except Exception as e:
            logging.exception("reconcile exit failed pos_id=%s symbol=%s cid=%s", pos_id, symbol, cid)
            _wd(conn, symbol, interval, strategy, "CRIT", "RECONCILE_EXIT_EXCEPTION", {"pos_id": pos_id, "cid": cid, "err": str(e)})
