"""Optional, platform-neutral reply policies before the gateway's busy/steer boundary.

Policies receive immutable bounded text. Hermes retains authorization, control routing,
ordering and transcript ownership. Register through PluginContext.register_ingress_policy.
"""
import asyncio
from dataclasses import dataclass
import logging

from gateway.ingress_store import IngressMessage, UnsettledObservation, observe, read_snapshot
from gateway.platforms.event import MessageType
from gateway.session_identity import replace_source

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngressRequest:
    platform: str
    chat_id: str
    session_key: str
    message_id: str
    text: str
    history: tuple[IngressMessage, ...]
    reply_expected: bool | None


def pending_control(runner, key):
    """Any unknown control state bypasses policy, just like a known pending prompt."""
    try:
        from tools import clarify_gateway, slash_confirm
        from tools.approval import has_blocking_approval
        state = runner._peek_session_state(key)
        return bool((state and state.persistent.update_prompt_pending)
                    or clarify_gateway.get_pending_for_session(key, include_choice_prompts=True)
                    or slash_confirm.get_pending(key) or has_blocking_approval(key))
    except Exception:
        return True


from gateway.ingress_queue import IngressQueue


def has_policy(event):
    """Read the already-loaded registry under canonical runtime ownership; never discover here."""
    from gateway.session_identity import identity_of
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from hermes_cli.plugins import iter_hook_callbacks
    identity = identity_of(event.source)
    if identity is None:
        return False
    token = set_hermes_home_override(identity.runtime_home)
    try:
        return bool(iter_hook_callbacks('pre_gateway_ingress'))
    finally:
        reset_hermes_home_override(token)


async def authorized(runner, event):
    """Full canonical authorization before accepting recoverable ingress ownership."""
    author = event.user_id or event.source.user_id
    if not author:
        return False
    source = replace_source(event.source, user_id=author,
                            user_name=event.user_name or event.source.user_name)
    async with runner._async_profile_scope_for_source(event.source):
        return await asyncio.to_thread(runner._is_user_authorized_for_source, source)


async def _decision(runner, event, key, previous):
    """True only when Hermes durably owns an observation; ordinary errors fail open."""
    if (event.message_type != MessageType.TEXT or event.media_urls or event.prompt_response
            or event.reply_expected is not False or event.reply_to_is_own_message
            or not event.message_id or not event.text or len(event.text) > 6000):
        return False
    async with runner._async_profile_scope_for_source(event.source):
        from hermes_cli.plugins import iter_hook_callbacks
        callbacks = iter_hook_callbacks('pre_gateway_ingress')
        # Several independent suppression policies cannot compose safely by accident.
        if len(callbacks) != 1 or pending_control(runner, key):
            return False
        if previous is None and runner._is_session_running(key):
            return False  # startup/eviction: active input may not have reached the transcript
        author = event.user_id or event.source.user_id
        if not author:
            return False
        source = replace_source(event.source, user_id=author, user_name=event.user_name or event.source.user_name)
        if not await asyncio.to_thread(runner._is_user_authorized_for_source, source):
            return False  # normal authorization remains responsible for rejection/UX
        try:
            snapshot = await asyncio.wait_for(asyncio.to_thread(
                read_snapshot, runner.session_store, key, required_message_id=previous,
                message_id=event.message_id), timeout=0.5)
        except Exception:
            return False
        if snapshot is None:
            return False
        if snapshot.already_observed:
            return (snapshot, None, None)
        text = event.text
        prefix = f'[{event.user_name or event.source.user_name or author}|{author}]\n'
        if not text.startswith(prefix):
            text = prefix + text
        request = IngressRequest(event.source.platform.value, event.source.chat_id, key,
                                 event.message_id, text, snapshot.history, event.reply_expected)
        try:
            result = await asyncio.wait_for(callbacks[0](request), timeout=2.0)
        except Exception:
            logger.debug('Ingress policy unavailable; continuing normal dispatch')
            return False
        if not isinstance(result, dict) or result.get('action') != 'observe':
            return False
        if pending_control(runner, key):
            return False
        if callbacks != iter_hook_callbacks('pre_gateway_ingress'):
            return False  # unload/reload while request was in flight
        if not await asyncio.to_thread(runner._is_user_authorized_for_source, source):
            return False
        guard = result.get("commit_guard")
        if guard is not None and not callable(guard):
            return False
        def still_current():
            return (callbacks == iter_hook_callbacks("pre_gateway_ingress")
                    and not pending_control(runner, key)
                    and (guard is None or guard() is True))
        return snapshot, text, still_current


async def evaluate(runner, event, key, previous, deadline=None):
    """Classification expires on arrival budget; a started write always settles first."""
    deadline = deadline if deadline is not None else asyncio.get_running_loop().time() + 1.0
    try:
        async with asyncio.timeout_at(deadline):
            decision = await _decision(runner, event, key, previous)
    except TimeoutError:
        return False
    if not decision:
        return False
    snapshot, text, guard = decision
    if text is None:
        return True
    async with runner._async_profile_scope_for_source(event.source):
        write = asyncio.create_task(asyncio.to_thread(observe, runner.session_store, snapshot,
                                                     event.message_id, text, commit_guard=guard))
        try:
            return await asyncio.shield(write)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(write)
            finally:
                raise
