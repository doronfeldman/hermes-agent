# Custom Hermes runtime

This fork's runnable branch is `custom/jev-runtime`. Core changes are committed directly
to the source; installing this branch does not require applying external patch files.
`main` remains available as an upstream tracking branch.

The current upstream base is stable **v0.22.5**, commit
`bf0203a794a07aac3ff764972ab9087baa4e4f4e`, merged on the isolated review branch
`review/security-update-20261027`. The previous deployed fork revision was
`485c9b9f1669b7327714d3fc51277367c7ae9933`, based on Hermes 0.21.5
(`34f8ec3b407e50bad3ae27e4cd79d65212061356`); retain it for operational rollback.

## Carried changes

1. **Optional ingress policies**, commit `abf9fb5427991bc7123ae7597c34a4f4b0f55bba`.
   This preserves the exact core change previously distributed by
   [hermes-jev-reply-gate](https://github.com/doronfeldman/hermes-jev-reply-gate).
   The host API is generic; the reply-gate plugin remains a separately installed plugin.
   See [ingress policy documentation](website/docs/developer-guide/plugins/ingress-policies.md).
2. **Opt-in cron Smart approvals**, originally commit
   `485c9b9f1669b7327714d3fc51277367c7ae9933`. `approvals.cron_mode: smart` reuses the existing
   approval-model provider and policy, including the separate `jev-approvals` plugin.
   Only APPROVE permits an assessed operation. DENY, ESCALATE and provider failures
   block without waiting for a person. See [security documentation](website/docs/user-guide/security.md).

No production configuration or policy is included in this repository. Installing this
code alone does not enable cron Smart approval: the default remains `deny`. Enable it
only in the intended profile, preserving that profile's existing provider/model and policy.
Command approval is not permission to unsubscribe a user or modify their email; the
workflow must still obtain explicit consent for the exact unsubscribe/archive action.

## Stable integration notes (2026-10-27)

- Ingress policy settlement stays before upstream busy/burst admission. Recovery uses
  upstream replay, quarantine and holdback handling, extended with ingress arrival ordering.
- Cron Smart follows upstream dangerous-command detection and unconditional safety floors.
  Upstream removed Tirith; this fork does not restore its scanner or obsolete tests.
  Smart assessments still ignore reusable grants, require a strict APPROVE verdict,
  bound provider latency, and execute approved Python in a fresh process.
- Upstream approval reasoning configuration and the new plugin admission hook are retained.
- Managed tool pins are Python 3.14.9, uv 0.13.5 and Node 26.12.1. The Python lock pins
  certifi 2026.10.19. Upstream now uses a three-day Python dependency quarantine with
  a CA-trust exception for certifi; no local lock override is required.

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


### v0.22.5 review verification

On macOS arm64 with PM-managed Python 3.14.9:

- Fork tests: 57 passed; the initially skipped Telegram module then passed both
  tests after provisioning the `telegram` test extra through PM.
- Approval, code kernel, terminal batch, profile, ingress and spool regression
  selection: 669 passed, 7 platform/dependency skips across 38 files.
- Native Telegram, burst admission, cron profile isolation, TLS and PM selection:
  157 passed, 1 skipped, 1 failed. The failure is upstream's
  `test_only_pre_handoff_failure_reopens_admission[error_before_entry-cancel]`;
  the identical failure reproduces from an untouched archive of stable v0.22.5.
  This is a known baseline limitation, not a passing full-suite claim.
- Independent custom-diff review found no merge-specific defects. The custom diff
  against stable passes `git diff --check`.

These are local regression results. Linux qualification, installed plugin checks
and operational deployment verification remain separate rollout gates.
