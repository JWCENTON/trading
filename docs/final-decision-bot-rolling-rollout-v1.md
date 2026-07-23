# FinalDecision bot image rolling rollout contract V1 — superseded

This profiled-service contract is retained as historical design evidence only.
It MUST NOT be used while the production topology is the consolidated
`bot-runner`: starting any of the four profiled services beside that runner
would duplicate strategy workers and could duplicate decisions or orders.

The authoritative production procedure is
`final-decision-consolidated-bot-runner-rollout-v2.md`. It treats the single
container and all of its child processes as one atomic version boundary.
