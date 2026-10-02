"""Behavioral contract at the real adapter admission boundary."""
import asyncio
import pytest
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.ingress import IngressQueue


class Adapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)
    async def connect(self): return True
    async def disconnect(self): pass
    async def send(self, *args, **kwargs): return SendResult(success=True)
    async def get_chat_info(self, chat_id): return {}


def event(text='chatter', room='room', mid='m1'):
    return MessageEvent(text=text, message_id=mid, reply_expected=False,
                        source=SessionSource(platform=Platform.TELEGRAM, chat_id=room,
                                             user_id='u', chat_type='group'))


@pytest.mark.asyncio
async def test_classifier_does_not_block_control_or_other_sessions_and_observe_never_queues():
    adapter = Adapter()
    reached, release = asyncio.Event(), asyncio.Event()
    delivered = []
    async def policy(ev, key, previous, deadline):
        if ev.source.chat_id == 'room':
            reached.set()
            await release.wait()
            return True
        return False
    async def dispatch(ev, key):
        delivered.append(ev.text)
        ev._gateway_accepted = True
    adapter._dispatch_admitted_message = dispatch
    adapter.set_message_handler(lambda ev: None)
    adapter._ingress_queue = IngressQueue(adapter, policy)
    await adapter.handle_message(event())
    await reached.wait()
    await adapter.handle_message(event('other', 'other'))
    await asyncio.sleep(0)
    assert delivered == ['other']
    await adapter.handle_message(event('/stop', mid='m2'))
    assert delivered == ['other', '/stop']
    release.set()
    await asyncio.sleep(0)
    assert 'chatter' not in delivered
    assert not adapter._pending_messages
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_same_session_decisions_are_ordered_and_receive_unpersisted_predecessor():
    adapter = Adapter()
    release, reached = asyncio.Event(), asyncio.Event()
    seen, delivered = [], []
    async def policy(ev, key, previous, deadline):
        seen.append((ev.message_id, previous))
        if ev.message_id == '1':
            reached.set()
            await release.wait()
        return False
    async def dispatch(ev, key):
        delivered.append(ev.message_id)
        ev._gateway_accepted = True
    adapter.set_message_handler(lambda ev: None)
    adapter._dispatch_admitted_message = dispatch
    adapter._ingress_queue = IngressQueue(adapter, policy)
    await adapter.handle_message(event(mid='1'))
    await reached.wait()
    await adapter.handle_message(event(mid='2'))
    await asyncio.sleep(0)
    assert seen == [('1', None)]
    release.set()
    for _ in range(10): await asyncio.sleep(0)
    assert delivered == ['1', '2']
    assert seen == [('1', None), ('2', '1')]
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_shutdown_preserves_accepted_classification_input_in_standard_spool(tmp_path, monkeypatch):
    import json
    import gateway.shutdown_flush as spool
    monkeypatch.setattr(spool, '_get_flush_dir', lambda: tmp_path)
    adapter = Adapter()
    reached = asyncio.Event()
    async def policy(ev, key, previous, deadline):
        reached.set()
        await asyncio.Event().wait()
    adapter.set_message_handler(lambda ev: None)
    adapter._ingress_queue = IngressQueue(adapter, policy)
    incoming = event('preserve me')
    await adapter.handle_message(incoming)
    await reached.wait()
    assert incoming._gateway_accepted
    await adapter.cancel_background_tasks()
    files = list(tmp_path.glob('*.json'))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload['data']['text'] == 'preserve me'
    assert payload['data']['message_id'] == 'm1'


@pytest.mark.asyncio
async def test_capacity_and_disable_never_let_newer_input_overtake_pending_lane():
    adapter = Adapter()
    reached, release = asyncio.Event(), asyncio.Event()
    delivered = []
    enabled = [True]
    async def policy(ev, key, previous, deadline):
        if ev.message_id == '1':
            reached.set()
            await release.wait()
        return False
    async def dispatch(ev, key):
        delivered.append(ev.message_id)
        ev._gateway_accepted = True
    adapter.set_message_handler(lambda ev: None)
    adapter._dispatch_admitted_message = dispatch
    queue = adapter._ingress_queue = IngressQueue(adapter, policy, enabled=lambda ev: enabled[0])
    queue.MAX_PENDING = 1
    await adapter.handle_message(event(mid='1'))
    await reached.wait()
    enabled[0] = False
    second = asyncio.create_task(adapter.handle_message(event(mid='2')))
    await asyncio.sleep(0)
    assert delivered == []
    assert not second.done()
    release.set()
    await second
    for _ in range(10): await asyncio.sleep(0)
    assert delivered == ['1', '2']
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_expired_queued_input_skips_new_classification_but_dispatches_in_order():
    adapter = Adapter()
    release, reached = asyncio.Event(), asyncio.Event()
    classified, delivered = [], []
    async def policy(ev, key, previous, deadline):
        classified.append(ev.message_id)
        reached.set()
        await release.wait()
        return False
    async def dispatch(ev, key):
        delivered.append(ev.message_id)
        ev._gateway_accepted = True
    adapter.set_message_handler(lambda ev: None)
    adapter._dispatch_admitted_message = dispatch
    queue = adapter._ingress_queue = IngressQueue(adapter, policy)
    await adapter.handle_message(event(mid='1'))
    await reached.wait()
    queue.BUDGET_SECONDS = 0  # next input's arrival budget is already exhausted
    await adapter.handle_message(event(mid='2'))
    release.set()
    for _ in range(10): await asyncio.sleep(0)
    assert classified == ['1']
    assert delivered == ['1', '2']
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_uncertain_owner_and_queued_successor_both_survive_for_recovery(tmp_path, monkeypatch):
    import json
    import gateway.shutdown_flush as spool
    from gateway.ingress_store import UnsettledObservation
    monkeypatch.setattr(spool, '_get_flush_dir', lambda: tmp_path)
    adapter = Adapter()
    reached, release = asyncio.Event(), asyncio.Event()
    async def policy(ev, key, previous, deadline):
        reached.set()
        await release.wait()
        raise UnsettledObservation('synthetic failure')
    adapter.set_message_handler(lambda ev: None)
    adapter._ingress_queue = IngressQueue(adapter, policy)
    await adapter.handle_message(event('one', mid='1'))
    await reached.wait()
    await adapter.handle_message(event('two', mid='2'))
    release.set()
    tasks = list(adapter._background_tasks)
    await asyncio.gather(*tasks, return_exceptions=True)
    await adapter.cancel_background_tasks()
    payloads = [json.loads(p.read_text()) for p in tmp_path.glob('*.json')]
    assert sorted(p['data']['text'] for p in payloads) == ['one', 'two']


@pytest.mark.asyncio
async def test_concurrent_authorization_cannot_reverse_arrival_order():
    adapter = Adapter()
    reached, release = asyncio.Event(), asyncio.Event()
    classified = []
    async def authorize(ev):
        if ev.message_id == '1':
            reached.set()
            await release.wait()
        return True
    async def policy(ev, key, previous, deadline):
        classified.append(ev.message_id)
        return True
    adapter.set_message_handler(lambda ev: None)
    adapter._ingress_queue = IngressQueue(adapter, policy, authorize=authorize)
    first = asyncio.create_task(adapter.handle_message(event(mid='1')))
    await reached.wait()
    second = asyncio.create_task(adapter.handle_message(event(mid='2')))
    await asyncio.sleep(0)
    assert classified == []
    release.set()
    await asyncio.gather(first, second)
    for _ in range(10): await asyncio.sleep(0)
    assert classified == ['1', '2']
    await adapter.cancel_background_tasks()
