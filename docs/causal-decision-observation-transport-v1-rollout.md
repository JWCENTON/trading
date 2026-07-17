# Causal Decision Observation Transport V1 rollout plan

No rollout is part of this change.

Later, apply the additive migration twice and verify the canonical manifest/fingerprint first on
LOCAL LIVE, LOCAL PAPER, VPS LIVE and VPS PAPER, in that order. Require 4/4 parity and zero runtime
events. Roll out the runtime image to LOCAL PAPER only, set `DEPLOYMENT_ID=local-paper`, disable the
kill switch only for causal transport, enable decision observation, and keep shadow and auto-apply
off. Validate outbox, consumer, observation and independent Replay/Warehouse rows without orders.
Then repeat for VPS PAPER with `vps-paper`. LIVE runtime remains disabled. Shadow activation is a
separate reviewed change.

Disable by activating `CAUSAL_LEARNING_KILL_SWITCH=1` or setting observation enabled to 0. This
stops append and claim while retaining pending events. Runtime rollback is safe because the schema
is additive; do not drop the table or delete poison events. Re-enable only after deployment identity,
pending age, dead letters, fingerprint parity and single-consumer topology are verified.
