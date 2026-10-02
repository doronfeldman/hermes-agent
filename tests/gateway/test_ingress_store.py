"""Ingress decisions never repair history or leave a queued late observation."""
import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionStore, SessionSource
from gateway.ingress_store import read_snapshot, observe


@pytest.fixture
def store(tmp_path):
    from hermes_state import SessionDB
    result = SessionStore(sessions_dir=tmp_path / 'sessions', config=GatewayConfig())
    result._db = SessionDB(tmp_path / 'state.db')
    yield result
    result._db.close()


def session(store):
    return store.get_or_create_session(SessionSource(platform=Platform.TELEGRAM, chat_id='room', user_id='u', chat_type='group'))


def test_snapshot_is_bounded_readonly_and_observation_is_idempotent(store):
    entry = session(store)
    for i in range(20):
        store._db.append_message(entry.session_id, 'user', 'x' * 900, platform_message_id=str(i))
    before = store._db.get_messages(entry.session_id)
    snap = read_snapshot(store, entry.session_key)
    assert len(snap.history) <= 12
    assert sum(len(m.content) for m in snap.history) <= 6000
    assert store._db.get_messages(entry.session_id) == before
    assert observe(store, snap, 'incoming', '[Sam|u]\nchatter') is True
    assert observe(store, snap, 'incoming', '[Sam|u]\nchatter') is True
    rows = store._db.get_messages(entry.session_id)
    assert len(rows) == 21
    assert rows[-1]['observed'] == 1
    assert rows[-1]['content'] == '[Sam|u]\nchatter'
    assert not store._dirty_transcripts


def test_snapshot_refuses_missing_context_and_unpersisted_predecessor(store):
    assert read_snapshot(store, 'absent') is None
    entry = session(store)
    assert read_snapshot(store, entry.session_key, required_message_id='previous') is None
    store._db.append_message(entry.session_id, 'user', 'question', platform_message_id='previous')
    assert read_snapshot(store, entry.session_key, required_message_id='previous') is not None
    store._dirty_transcripts[entry.session_id] = [{'role': 'user', 'content': 'pending'}]
    assert read_snapshot(store, entry.session_key) is None


def test_reset_during_classification_cannot_observe_into_new_or_old_session(store):
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='room', user_id='u', chat_type='group')
    fresh = store.get_or_create_session(source, force_new=True)
    assert fresh.session_id != snap.session_id
    assert observe(store, snap, 'incoming', 'old context') is False
    assert not store._db.get_messages(snap.session_id)
    assert not store._db.get_messages(fresh.session_id)


def test_failed_observation_never_enters_retry_queue(store, monkeypatch):
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    def broken(*args, **kwargs):
        raise OSError('disk failure')
    monkeypatch.setattr(store._db, '_execute_write', broken)
    assert observe(store, snap, 'incoming', 'chatter') is False
    assert not store._dirty_transcripts
    assert not store._db.get_messages(entry.session_id)


def test_replayed_observation_is_recognized_and_excluded_from_context(store):
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    assert observe(store, snap, 'once', 'chatter')
    replay = read_snapshot(store, entry.session_key, message_id='once')
    assert replay.already_observed
    assert replay.history == ()


def test_observation_survives_normal_agent_append_and_is_context_only(store):
    from gateway.run import _build_gateway_agent_history
    entry = session(store)
    store._db.append_message(entry.session_id, 'user', 'question', platform_message_id='q')
    snap = read_snapshot(store, entry.session_key)
    assert observe(store, snap, 'chatter', 'unrelated chat')
    store._db.append_messages_batch(entry.session_id, [{'role': 'assistant', 'content': 'answer'}])
    history, context = _build_gateway_agent_history(store._db.get_messages(entry.session_id))
    assert context == 'unrelated chat'
    assert [m['content'] for m in history] == ['question', 'answer']


def test_incomplete_context_entry_fails_open_instead_of_changing_its_meaning(store):
    entry = session(store)
    store._db.append_message(entry.session_id, 'user', 'x' * 6100 + ' do not do that')
    assert read_snapshot(store, entry.session_key) is None


@pytest.mark.parametrize('committed', [False, True])
def test_standard_spool_recovery_reconciles_unknown_observation_without_duplicate(store, tmp_path, monkeypatch, committed):
    import gateway.shutdown_flush as spool
    monkeypatch.setattr(spool, '_get_flush_dir', lambda: tmp_path)
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    if committed:
        assert observe(store, snap, 'once', 'chatter')
    payload = {entry.session_key: {'text': 'chatter', 'message_id': 'once'}}
    assert spool.flush_pending_to_file(payload, reason='ingress_unsettled') == 1
    resolver = lambda key, **kwargs: (entry.session_id, store._db)
    assert spool.recover_pending_to_db(store._db, session_resolver=resolver) == 1
    assert spool.flush_pending_to_file(payload, reason='ingress_unsettled') == 1
    assert spool.recover_pending_to_db(store._db, session_resolver=resolver) == 1
    rows = store._db.get_messages(entry.session_id)
    assert len(rows) == 1
    assert rows[0]['observed'] == int(committed)
    assert rows[0]['platform_message_id'] == 'once'


def test_write_retry_does_not_hold_gateway_routing_or_transcript_locks(store, monkeypatch):
    import threading
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    started, release = threading.Event(), threading.Event()
    original = store._db._execute_write
    def held(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    monkeypatch.setattr(store._db, '_execute_write', held)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor() as pool:
        result = pool.submit(observe, store, snap, 'msg', 'chatter')
        assert started.wait(3)
        try:
            assert store._lock.acquire(blocking=False)
            store._lock.release()
            assert store._get_transcript_drain_lock().acquire(blocking=False)
            store._get_transcript_drain_lock().release()
            # Reset while waiting for the writer makes the old decision stale.
            store._entries[entry.session_key].session_id = 'new-generation'
        finally:
            release.set()
        assert result.result() is False


def test_spool_recovery_uses_arrival_order_not_random_filename_or_cancellation_order(store, tmp_path, monkeypatch):
    import json
    import gateway.shutdown_flush as spool
    monkeypatch.setattr(spool, '_get_flush_dir', lambda: tmp_path)
    entry = session(store)
    for name, order, content in [('a.json', 2, 'second'), ('z.json', 1, 'first')]:
        (tmp_path / name).write_text(json.dumps({'reason': 'ingress_unsettled',
            'session_key': entry.session_key, 'ts': 1,
            'data': {'text': content, 'message_id': name, 'ingress_order': order}}))
    assert spool.recover_pending_to_db(store._db, session_resolver=lambda *args, **kwargs: (entry.session_id, store._db)) == 2
    assert [m['content'] for m in store._db.get_messages(entry.session_id)] == ['first', 'second']


def test_commit_guard_rechecks_after_waiting_for_writer(store, monkeypatch):
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    enabled = [True]
    execute = store._db._execute_write
    def unloaded_before_transaction(fn, **kwargs):
        enabled[0] = False
        return execute(fn, **kwargs)
    monkeypatch.setattr(store._db, '_execute_write', unloaded_before_transaction)
    assert observe(store, snap, 'incoming', 'chatter', commit_guard=lambda: enabled[0]) is False
    assert store._db.get_messages(entry.session_id) == []


def test_unsettled_writer_transaction_cannot_be_mistaken_for_safe_dispatch(store, monkeypatch):
    from gateway.ingress_store import UnsettledObservation
    entry = session(store)
    snap = read_snapshot(store, entry.session_key)
    def transaction_still_live(*args, **kwargs):
        store._db._conn.execute('BEGIN IMMEDIATE')
        raise OSError('commit and rollback unavailable')
    monkeypatch.setattr(store._db, '_execute_write', transaction_still_live)
    try:
        with pytest.raises(UnsettledObservation):
            observe(store, snap, 'incoming', 'chatter')
    finally:
        store._db._conn.rollback()
