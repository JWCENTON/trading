# Causal Shadow Observation V1 rollout plan

This is a plan only; nothing is rolled out by the implementation task.

1. Review and commit the schema foundation and adapter separately.
2. Apply V1.1 to LOCAL PAPER and validate flags remain OFF.
3. Deploy runtime to LOCAL PAPER, then create its independent activation.
4. Apply and deploy to VPS PAPER, then create a different activation.
5. Use one shared observation protocol identifier but distinct activation IDs.
6. Confirm `local-paper` and `vps-paper` never mix in audit groups.
7. Collect both cohorts in parallel and apply the documented sample policy.
8. Leave LOCAL LIVE and VPS LIVE untouched and without activations.

Disable/rollback starts by setting the causal kill switch and shadow flag OFF;
trading continues. Preserve append-only evidence. Schema removal, experiments
and auto-apply require separate reviewed changes.
