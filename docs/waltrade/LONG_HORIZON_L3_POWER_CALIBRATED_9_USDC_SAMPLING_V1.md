# Long Horizon L3 power-calibrated 9 USDC sampling V1

STATUS=TERMINATED_AFTER_PREMATURE_MIGRATION_ROLLBACK

This contract corrects only the enrollment capacity and sampling surface of
`LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1`. Commit `7fc517bce531314f9123452bc4f49de0da5e46d3`
must not be rolled out in its original configuration. The V1 migration was
prematurely applied, then rolled back with audit history preserved. L3 runtime
was never rebuilt, recreated, or activated. V1 is immutable historical
evidence and must never be reactivated; any future rollout uses the separate
V2 contract.

## Scope and invariants

- `ENVIRONMENT=LOCAL_PAPER_ONLY`
- `L3_ENTRY_NOTIONAL=9_USDC`
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
The current BTC effective minimum was `7.92442 USDC`; 9 USDC therefore has a
`1.07558 USDC` (`~13.57%`) buffer. Runtime checks authoritative cached
instrument limits before admission. If 9 USDC is no longer feasible, it emits
and persists `MIN_NOTIONAL_NOT_MET`, does not increase notional, does not open
a position, and does not count the opportunity as an admitted L3 outcome.

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
- `SAMPLING_FINGERPRINT=0f67b3c42d19ab5447dc4b1e9a9f0e553f62c3b72d88bdc8d9dc64b36f0312d1`

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

- `ALLOW_AVAILABLE_SLOTS_AT_9_USDC=28`
- `BLOCK_AVAILABLE_SLOTS_AT_9_USDC=14`
- `GLOBAL_AVAILABLE_SLOTS_AT_9_USDC=42`
- `BLOCK_EXPECTED_OCCUPANCY=12.278202639821414`
- `BLOCK_CAPACITY_PASS=YES`

Because capacity is discrete, expected occupancy 12.2782 requires 13 BLOCK
slots. At a 20% sleeve and 9 USDC per position, the minimum managed equity is
therefore exactly `585 USDC`. Below that boundary new L3 admissions pause as
`L3_CAPACITY_PAUSE`; existing positions are not closed and sampling
probabilities do not change.

Economic evidence must report absolute USDC, percent, and net per 1 USDC
allocated. The paired L0 path is normalized the same way. The 9 USDC amount is
an experiment-capacity setting, not an assertion that historical 20 USDC
outcomes have identical absolute economics.

## Rollout gate

`MIGRATION_APPLIED_AND_ROLLED_BACK=YES`

`RUNTIME_CHANGED=NO`

`LOCAL_PAPER_CHANGED=NO`

V1 activation is permanently closed.
