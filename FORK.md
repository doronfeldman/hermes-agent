# Custom Hermes runtime

This fork's runnable branch is `custom/jev-runtime`. Core changes are committed directly
to the source; installing this branch does not require applying external patch files.
`main` remains available as an upstream tracking branch.

The initial base deliberately matches the existing installed runtime:
`34f8ec3b407e50bad3ae27e4cd79d65212061356` (Hermes 0.21.5). It is not a claim to include
every newer upstream commit.

## Carried changes

1. **Optional ingress policies**, commit `abf9fb5427991bc7123ae7597c34a4f4b0f55bba`.
   This preserves the exact core change previously distributed by
   [hermes-jev-reply-gate](https://github.com/doronfeldman/hermes-jev-reply-gate).
   The host API is generic; the reply-gate plugin remains a separately installed plugin.
   See [ingress policy documentation](website/docs/developer-guide/plugins/ingress-policies.md).
2. **Opt-in cron Smart approvals**. `approvals.cron_mode: smart` reuses the existing
   approval-model provider and policy, including the separate `jev-approvals` plugin.
   Only APPROVE permits an assessed operation. DENY, ESCALATE and provider failures
   block without waiting for a person. See [security documentation](website/docs/user-guide/security.md).

No production configuration or policy is included in this repository. Installing this
code alone does not enable cron Smart approval: the default remains `deny`. Enable it
only in the intended profile, preserving that profile's existing provider/model and policy.
Command approval is not permission to unsubscribe a user or modify their email; the
workflow must still obtain explicit consent for the exact unsubscribe/archive action.

## Updating and migration

Keep custom runtime deployments on `custom/jev-runtime`. Bring upstream changes into a
review branch, resolve conflicts once in source, run the relevant tests, and then advance
the custom branch. Blindly replacing the custom branch with upstream `main` discards the
custom behavior. An upstream merge can still require maintenance; the fork makes the
changes versioned and reviewable rather than reapplied by a plugin installer.

Migration is a separate operational step: record the installed revision and profile
configuration, select the fork's custom branch in the existing installation/update
mechanism, verify the reply-gate and approval-provider plugins, then enable cron Smart
only in the intended profile. Retain the previous revision and configuration for rollback.
Do not apply the old ingress patch on top of this branch; its source is already present.

After migration, run the actual newsletter job and verify real Gmail retrieval plus
persisted scan state. Scheduler `last_status: ok` alone is not proof that the scan worked.
An unsubscribe/archive end-to-end test requires explicit consent for its exact message.

## Focused verification

Use the repository's canonical `scripts/run_tests.sh` runner with its managed test
environment. The new regression files are `tests/tools/test_cron_smart.py` and
`tests/tools/test_cron_smart_execution.py`; they include real profile switching, tool
dispatch and child processes. Provider outcomes are simulated, so these tests do not
claim live JEV or Gmail connectivity. Also run existing approval, code-kernel, terminal
batch and ingress-policy suites when advancing the runtime branch.
