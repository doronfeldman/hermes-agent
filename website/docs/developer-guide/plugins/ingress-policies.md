---
title: Gateway ingress policies
---

# Gateway ingress policies

`ctx.register_ingress_policy(async_callback)` registers an optional participation policy
before normalized group input reaches busy-session queueing or steering. Hermes owns the
existing transport connection, authorization, scheduling and canonical transcript.
A plugin supplies a decision; it must not acknowledge updates or persist messages itself.

```python
def register(ctx):
    async def decide(request):
        # Apply the plugin's explicit platform/chat scope, then classify ordinary text.
        return {"action": "allow"}
    ctx.register_ingress_policy(decide)
```

The callback receives one immutable `IngressRequest` with `platform`, `chat_id`,
`session_key`, `message_id`, complete attributed `text`, `reply_expected`, and chronological
`history` (`IngressMessage(role, content)` tuples). Routing identifiers are for local scope
checks; a plugin should project speaker attribution before sending content to a provider.
Hermes reads at most twelve complete prior text entries and 6,000 characters, excluding the
current event, without loading tool payloads or repairing transcripts. Missing, contended,
stale or insufficient context continues normal handling.

Return `{"action": "allow"}` to continue, or `{"action": "observe"}` to request context-only
persistence. An optional synchronous `commit_guard` on the observe intent must return exactly
`True` at the write boundary. Use this guard to invalidate a decision when settings change
while host authorization is pending. It runs under the owning profile scope in a worker;
keep it short and free of external side effects. A false/raising guard continues normally.
Hermes also rechecks registered callback identity and pending controls at that boundary.
Only one policy is supported per profile; multiple policies fail open.

The callback runs inside the canonical runtime profile, so `ctx.get_config` and
`agent.secret_scope.get_secret` resolve the owner's settings and credentials. The actual
author is restored for full authorization even where the conversation source is shared.
Commands, bot replies, explicitly addressed or unknown-targeted messages, media, internal
messages and pending controls bypass classification. Adapters must provide trustworthy
`reply_expected=False`; unknown targeting always continues. Telegram fills this metadata
using its existing mention, wake-word and reply predicates.

The host budgets one second from ingress arrival for preparation and classification,
including lane wait. Expired decisions continue in order. Commands bypass network decisions.
At 128 owned pending inputs, ordinary intake applies backpressure; there is no unbounded
classifier backlog. Reset/stop commands discard preceding pending work. A begun observation
write must settle before another owner can dispatch the event, even if its deadline expired.

Observations are idempotent within the conversation generation and platform message ID.
They remain in the canonical transcript and are supplied as context only, without rewriting
prior cached prompts. Reset generations reject stale decisions; ordinary agent appends and
compression preserve accepted observations. No independent plugin history is needed.

Shutdown and uncertain storage outcomes preserve accepted inputs in the existing Hermes
pending-message spool, with arrival order and message identity. Replay checks the transcript
transactionally, so an already committed observation is not appended again. Inputs whose
authorization timed out are preserved for operator review instead of inserted into a transcript.
Recovery preserves text using the existing shutdown semantics; it does not initiate a new
assistant turn. A failed recovery file stays available for a later recovery attempt.

The registration is optional and additive. A plugin requiring it should refuse activation
when the method is absent. Shell hooks are unsupported for this surface.
