# Long Horizon L3 power-calibrated 8 USDC sampling V1

STATUS=CORRECTED_PRE_ROLLOUT_AWAITING_PRODUCT_OWNER_REAPPROVAL

This contract corrects only the enrollment capacity and sampling surface of
`LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1`. Commit `7fc517bce531314f9123452bc4f49de0da5e46d3`
must not be rolled out in its original configuration. No migration or runtime
rollout has occurred.

## Scope and invariants

- `ENVIRONMENT=LOCAL_PAPER_ONLY`
- `L3_ENTRY_NOTIONAL=8_USDC`
- `L3_TARGET=+3.00_PERCENT_REALIZABLE_NET_AFTER_FEES`
- `ALLOW_SLEEVE=40_PERCENT_EQUITY`
- `BLOCK_SLEEVE=20_PERCENT_EQUITY`
- `GLOBAL_PORTFOLIO_HEAT=60_PERCENT`
- `MINIMUM_FREE_CASH=20_PERCENT`
- `SAME_THESIS_MAX_ACTIVE=1`
- Existing L3 exit, risk, linkage, mark-to-market, paired-L0 and evidence
  semantics are unchanged.
- LOCAL LIVE, VPS PAPER and VPS LIVE notionals and authority are unchanged.

Current public OKX instrument metadata was checked on 2026-09-07. The four
spot instruments expose minimum sizes of `BTC-USDC=0.0001`,
`ETH-USDC=0.001`, `SOL-USDC=0.01`, and `BNB-USDC=0.001`. At the observed asks
(`79244.2`, `2492.51`, `104.11`, and `740.2` USDC), their effective minimum
notionals were respectively `7.92442`, `2.49251`, `1.0411`, and `0.7402` USDC.
Thus 8 USDC was valid for all four instruments at verification time. Runtime
still fails closed if cached authoritative instrument limits make an 8 USDC
order invalid later.

## Frozen sampling contract

- `SAMPLING_VERSION=L3_POWER_CALIBRATED_SALTED_SHA256_THRESHOLD_V1`
- `ALLOW_SAMPLING_PROBABILITY=13.194281540_PERCENT`
- `BLOCK_SAMPLING_PROBABILITY=7.920637611_PERCENT`
- `ONE_SIDED_ALPHA=0.05`
- `POWER=0.80`
- `ALLOW_REQUIRED_COMPLETED_EPISODES=33`
- `BLOCK_REQUIRED_COMPLETED_EPISODES=53`
- `ENROLLMENT_TARGET=14_DAYS`
- `ALLOW_EXPECTED_CENSORED=41/232_DISCOVERY_RATIO`
- `BLOCK_EXPECTED_CENSORED=155/666_DISCOVERY_RATIO`
- `INDEPENDENCE_UNIT=CANONICAL_SAME_THESIS_EPISODE`
- `SAMPLING_SALT=0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057`
- `SAMPLING_FINGERPRINT=e6e5e35bdc8b4eb3a6ae19ff6f884857370d6c3384d9f0dbc238d18bec90d303`

Selection is deterministic:

`uint256(SHA256(canonical_opportunity_id|frozen_sampling_salt|cohort_name|contract_version)) < cohort_threshold`

The exact ALLOW threshold is
`15277934255019537564202551280000000000000000000000000000000000000000000000000`.
The exact BLOCK threshold is
`9171471770693549621713624198000000000000000000000000000000000000000000000000`.
The salt was generated once before enrollment and is immutable. It is included
in the fingerprint and may not change after outcomes are observed.

## Capacity contract

At canonical managed equity `635.430007829136 USDC`:

- `ALLOW_AVAILABLE_SLOTS_AT_8_USDC=31`
- `BLOCK_AVAILABLE_SLOTS_AT_8_USDC=15`
- `GLOBAL_AVAILABLE_SLOTS_AT_8_USDC=47`
- `BLOCK_EXPECTED_OCCUPANCY=12.278202639821414`
- `BLOCK_CAPACITY_PASS=YES`

Because capacity is discrete, expected occupancy 12.2782 requires 13 BLOCK
slots. At a 20% sleeve and 8 USDC per position, the minimum managed equity is
therefore exactly `520 USDC`. Below that boundary new L3 admissions pause as
`L3_CAPACITY_PAUSE`; existing positions are not closed and sampling
probabilities do not change.

Economic evidence must report absolute USDC, percent, and net per 1 USDC
allocated. The paired L0 path is normalized the same way. The 8 USDC amount is
an experiment-capacity setting, not an assertion that historical 20 USDC
outcomes have identical absolute economics.

## Rollout gate

`MIGRATION_APPLIED=NO`

`RUNTIME_CHANGED=NO`

`LOCAL_PAPER_CHANGED=NO`

Activation requires explicit Product Owner reapproval of the corrective commit.
