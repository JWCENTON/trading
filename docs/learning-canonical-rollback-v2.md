# Canonical Learning rollback contracts V2

`20260724_learning_canonical_shared_rollback_v2.sql` is the preferred shared
rollback for an already-upgraded database. It removes only Manifest V1,
canonical source-universe objects and the Feedback canonical function upgrade.
It restores the preserved pre-canonical Feedback function and leaves
Feedback/Validation history and every exact repair object untouched.

The caller must set transaction/session context explicitly:

```text
waltrade.deployment_instance_id = local | vps | future strict instance
waltrade.environment = live | paper
```

The migration derives `<instance>-<environment>`, validates strict scalable
identity syntax, and requires `trading_live` for `live` or `trading_paper` for
`paper`. It supports local/live, local/paper, vps/live and vps/paper without
hard-coding those deployment IDs, and therefore also supports a future
strictly named instance such as `vps2`. A second identical run is an
identity-validated no-op; partial state fails closed.

`20260724_learning_decision_98b4_rollback_v1.sql` is a separate LOCAL LIVE
artifact. It can undo only the exact audited 98b4 registry/outcome repair. The
shared rollback never invokes it.

The previously published general rollback remains immutable for historical
compatibility but is superseded because it couples shared rollback with the
LOCAL-only repair. The PAPER-specific V1 rollback remains valid and retained;
new rollback plans should use shared V2 so the same reviewed contract covers
LOCAL and VPS identities.

Neither new rollback uses `CASCADE`, changes `bot_control`, enables Learning,
or rewrites historical Feedback/Validation rows.
