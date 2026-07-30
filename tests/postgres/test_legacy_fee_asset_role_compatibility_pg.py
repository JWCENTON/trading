from __future__ import annotations

from decimal import Decimal

from common.financial_truth_calculator import calculate_financial_truth
from common.financial_truth_repository import (
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
)


def test_legacy_exit_fee_resolves_from_canonical_symbol(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_legacy_fee_role"
    disposable_postgres_v16.create_database(database)

    def factory():
        return disposable_postgres_v16.connect(database)

    conn = factory()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE positions(
                  id BIGINT PRIMARY KEY,status TEXT,gross_pnl_usdc NUMERIC,
                  fees_usdc NUMERIC,net_pnl_usdc NUMERIC,symbol TEXT);
                CREATE TABLE simulated_orders(
                  id BIGINT PRIMARY KEY,symbol TEXT);
                CREATE TABLE financial_truth_account_identity_v1(
                  id BIGINT PRIMARY KEY,identity_fingerprint TEXT);
                CREATE TABLE financial_truth_instrument_snapshot_v1(
                  id BIGINT PRIMARY KEY,metadata_fingerprint TEXT,
                  step_size NUMERIC,base_asset TEXT,quote_asset TEXT);
                CREATE TABLE simulated_execution_fills_v1(
                  id BIGINT PRIMARY KEY,simulated_order_id BIGINT,
                  position_id BIGINT,order_purpose TEXT,side TEXT,symbol TEXT,
                  fill_qty NUMERIC,fill_price NUMERIC,fill_notional NUMERIC,
                  fee_qty NUMERIC,fee_asset TEXT,
                  authoritative_fee_usdc NUMERIC,estimated_fee_usdc NUMERIC,
                  account_identity_id BIGINT,instrument_snapshot_id BIGINT,
                  source_authority TEXT,environment TEXT,deployment_id TEXT,
                  simulation_model_version TEXT,execution_at TIMESTAMPTZ);
                INSERT INTO positions VALUES
                  (1,'CLOSED',0,0,0,'ETHUSDC'),
                  (2,'CLOSED',0,0,0,'SOLUSDC');
                INSERT INTO financial_truth_account_identity_v1
                  VALUES (1,'paper-account');
                INSERT INTO financial_truth_instrument_snapshot_v1
                  VALUES (1,'eth-snapshot',0.000001,'ETH','USDC'),
                         (2,'sol-snapshot',0.00001,'SOL','USDC');
                INSERT INTO simulated_orders VALUES
                  (11,'ETHUSDC'),(12,'ETHUSDC'),
                  (21,'SOLUSDC'),(22,'SOLUSDC');
                INSERT INTO simulated_execution_fills_v1 VALUES
                  (101,11,1,'ENTRY','BUY','ETHUSDC',10,2,20,.1,'USDC',
                   .1,NULL,1,1,'SIMULATED_EXECUTION','paper','local-paper',
                   'PAPER_SIMULATOR_FINANCIAL_MODEL_V1',now()-interval '2 min'),
                  (102,12,1,'EXIT','SELL','ETHUSDC',10,3,30,.1,'USDC',
                   .1,NULL,1,NULL,'SIMULATED_EXECUTION','paper','local-paper',
                   'PAPER_SIMULATOR_FINANCIAL_MODEL_V1',now()-interval '1 min'),
                  (201,21,2,'ENTRY','BUY','SOLUSDC',10,2,20,.1,'USDC',
                   .1,NULL,1,2,'SIMULATED_EXECUTION','paper','local-paper',
                   'PAPER_SIMULATOR_FINANCIAL_MODEL_V1',now()-interval '2 min'),
                  (202,22,2,'EXIT','SELL','SOLUSDC',10,3,30,.1,'USDC',
                   .1,NULL,1,NULL,'SIMULATED_EXECUTION','paper','local-paper',
                   'PAPER_SIMULATOR_FINANCIAL_MODEL_V1',now()-interval '1 min');
                """
            )
    conn.close()

    repository = FinancialTruthSourceRepository(factory)
    context = ExecutionEvidenceContext(
        environment="paper", exchange=None, deployment_id="local-paper"
    )
    for position_id, symbol in ((1, "ETHUSDC"), (2, "SOLUSDC")):
        position, fills, issue = repository.read_position_and_fills(
            position_id, context=context
        )
        assert issue is None
        assert fills[1].base_asset is None
        assert fills[1].quote_asset is None
        assert fills[1].instrument_metadata_fingerprint is None
        calculation = calculate_financial_truth(
            position_id=position[0], position_status=position[1],
            fills=fills, position_symbol=position[5],
        )
        assert position[5] == symbol
        assert calculation.financial_truth_status == "COMPLETE"
        assert calculation.authoritative_gross_pnl == Decimal("10")
        assert calculation.authoritative_fees_usdc == Decimal("0.2")
        assert calculation.authoritative_net_pnl == Decimal("9.8")
