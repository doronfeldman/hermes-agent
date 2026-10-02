"""Bounded read-only ingress context and idempotent transcript ownership.

Run off the event loop. No ordinary transcript retry queue is involved: returning
False means the caller may dispatch; an uncertain write raises and requires recovery.
"""
from dataclasses import dataclass
import time
from agent.message_metadata import stamp_message_uid
from hermes_state_messages import _INSERT_MESSAGE_SQL


@dataclass(frozen=True)
class IngressMessage:
    role: str
    content: str


@dataclass(frozen=True)
class Snapshot:
    session_key: str
    session_id: str
    history: tuple[IngressMessage, ...]
    already_observed: bool = False


class UnsettledObservation(RuntimeError):
    """Storage cannot confirm ownership. Preserve the input; do not dispatch it twice."""


def _current(store, key, sid=None):
    # No loading or I/O under shared locks. Unloaded/contended state means allow.
    if not store._lock.acquire(blocking=False):
        return None
    try:
        entry = store._entries.get(key)
        if entry is None or entry.suspended or (sid is not None and entry.session_id != sid):
            return None
        candidate = entry.session_id
    finally:
        store._lock.release()
    lock = store._get_transcript_drain_lock()
    if not lock.acquire(blocking=False):
        return None
    try:
        if store._dirty_transcripts.get(candidate) or store._follow_reroutes(candidate) != candidate:
            return None
    finally:
        lock.release()
    return candidate


def _existing(conn, sid, message_id):
    row = conn.execute('SELECT observed FROM messages WHERE session_id=? AND platform_message_id=? LIMIT 1',
                       (sid, message_id)).fetchone()
    return bool(row[0]) if row else None


def read_snapshot(store, session_key, *, required_message_id=None, message_id=None):
    sid = _current(store, session_key)
    if sid is None:
        return None
    db = store._db_for_key(session_key)
    if db is None:
        return None
    with db._read_ctx() as conn:
        session = conn.execute('SELECT end_reason FROM sessions WHERE id=?', (sid,)).fetchone()
        if session is None or session[0] is not None:
            return None
        if required_message_id and not conn.execute(
            'SELECT 1 FROM messages WHERE session_id=? AND platform_message_id=? LIMIT 1',
            (sid, required_message_id)).fetchone():
            return None
        duplicate = _existing(conn, sid, message_id) if message_id else None
        rows = conn.execute(
            "SELECT role, length(content), CASE WHEN length(content)<=6000 THEN content END "
            "FROM messages WHERE session_id=? AND (platform_message_id IS NULL OR platform_message_id != ?) "
            "AND active=1 AND role IN ('user','assistant') AND typeof(content)='text' "
            "AND (display_kind IS NULL OR display_kind NOT IN ('hidden','session_meta')) "
            "ORDER BY id DESC LIMIT 12", (sid, message_id or '')).fetchall()
    history, remaining = [], 6000
    for role, size, content in rows:
        if size > remaining:
            if not history:
                return None  # no complete most-recent entry: insufficient context
            break
        if content:
            history.append(IngressMessage(role, content))
            remaining -= size
    if _current(store, session_key, sid) is None:
        return None
    return Snapshot(session_key, sid, tuple(reversed(history)), duplicate is True)


def _insert(db, conn, sid, message_id, text, *, observed, timestamp=None):
    message = {'role': 'user', 'content': text, 'observed': observed,
               'platform_message_id': message_id,
               'display_metadata': {'ingress_observation': True} if observed else None}
    stamp_message_uid(message)
    params = db._message_row_params(sid, 'user', message, None, timestamp or time.time(), keep_reasoning=False)
    db._check_transcript_write_guards(conn, sid, None)
    conn.execute(_INSERT_MESSAGE_SQL, params)
    db._bump_session_counters(conn, sid, 1, 0, unit=True)


def observe(store, snapshot, message_id, text, *, commit_guard=None):
    """Settle observation once. No write or retry is left behind after False.

    Acquire the SQLite writer before validating the current routing generation. An
    observation admitted there precedes a later reset; resets already published reject
    it. Do not hold store/drain locks during disk I/O or SQLite lock retries.
    """
    if not message_id or _current(store, snapshot.session_key, snapshot.session_id) is None:
        return False
    sid = snapshot.session_id
    db = store._db_for_key(snapshot.session_key)
    if db is None:
        return False
    def write(conn):
        saved = _existing(conn, sid, message_id)
        if saved is not None:
            return saved
        if _current(store, snapshot.session_key, sid) is None:
            return False
        row = conn.execute('SELECT end_reason FROM sessions WHERE id=?', (sid,)).fetchone()
        if row is None or row[0] is not None:
            return False
        if commit_guard is not None:
            try:
                if commit_guard() is not True:
                    return False
            except Exception:
                return False
        _insert(db, conn, sid, message_id, text, observed=True)
        return True
    try:
        return db._execute_write(write, patience_s=0.25)
    except Exception:
        try:
            # A failed rollback may leave a live writer transaction. A separate reader
            # seeing no row cannot prove that this transaction will never commit later.
            with db._lock:
                if db._conn is None or db._conn.in_transaction:
                    raise UnsettledObservation("ingress writer has not settled")
            with db._read_ctx() as conn:
                return _existing(conn, sid, message_id) is True
        except Exception:
            raise UnsettledObservation('ingress observation settlement unavailable') from None


def recover_input(db, sid, message_id, text, timestamp):
    """Existing shutdown-spool replay; a committed observation wins over its recovery copy."""
    if not message_id:
        raise ValueError('ingress recovery requires a stable message ID')
    def write(conn):
        if _existing(conn, sid, message_id) is not None:
            return
        _insert(db, conn, sid, message_id, text, observed=False, timestamp=timestamp)
    db._execute_write(write)
