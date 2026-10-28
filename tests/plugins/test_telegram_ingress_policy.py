"""One real PTB stream: native normalization plus the generic ingress queue."""
import asyncio
import pytest
from gateway.ingress import IngressQueue
from gateway.platforms.event import MessageType
from tests.plugins.test_telegram_update_admission import connected, update


@pytest.mark.asyncio
async def test_native_group_normalization_keeps_author_and_targeting(monkeypatch):
    async with connected(monkeypatch, extra={'require_mention': False,
        'observe_unmentioned_group_messages': True, 'allowed_chats': ['42'],
        'group_allowed_chats': ['42']}) as (adapter, app, _delivered):
        incoming = update(app.bot, group=True)
        event = adapter._build_message_event(incoming.message, MessageType.TEXT)
        shared = adapter._apply_telegram_group_observe_attribution(event)
        assert shared.reply_expected is False
        assert shared.source.user_id is None
        assert shared.user_id == '88'
        mentioned = update(app.bot, group=True, text='@offline_bot help')
        addressed = adapter._build_message_event(mentioned.message, MessageType.TEXT)
        assert addressed.reply_expected is True


@pytest.mark.asyncio
async def test_native_stream_delivers_command_while_generic_classifier_waits(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    async with connected(monkeypatch, extra={'require_mention': False,
        'group_allowed_chats': ['42']}) as (adapter, app, delivered):
        adapter._text_batch_delay_seconds = 0.001
        async def policy(event, key, previous, deadline):
            entered.set()
            await release.wait()
            return True
        adapter._ingress_queue = IngressQueue(adapter, policy)
        await app.process_update(update(app.bot, group=True))
        await asyncio.wait_for(entered.wait(), 2)
        await app.process_update(update(app.bot, uid=11, kind='command', group=True))
        assert [event.text for event in delivered] == ['/status']
        release.set()
        await asyncio.gather(*list(adapter._background_tasks), return_exceptions=True)
        assert [event.text for event in delivered] == ['/status']
