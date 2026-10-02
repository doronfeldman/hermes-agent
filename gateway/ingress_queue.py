"""Transient ordered admission lanes; durable recovery uses Hermes's existing pending spool."""
import asyncio
from collections import OrderedDict
from dataclasses import dataclass
import logging
import time

from gateway.ingress_store import UnsettledObservation

logger = logging.getLogger(__name__)


@dataclass
class _OwnedInput:
    key: str
    event: object
    arrival_order: int = 0
    authorized: bool = True
    settled: bool = False
    discarded: bool = False
    retaining: bool = False


@dataclass
class _Admission:
    lock: object
    references: int = 0
    generation: int = 0


class IngressQueue:
    """Own accepted inputs until observation, normal admission, or recovery spool settles.

    Commands bypass classification; reset commands explicitly discard older queued work.
    At capacity ordinary intake applies backpressure instead of overtaking an older lane.
    The one-second classification budget starts on arrival, including predecessor waits.
    """
    MAX_PENDING = 128
    BUDGET_SECONDS = 1.0

    def __init__(self, adapter, policy, *, enabled=None, bypass=None, authorize=None):
        self.adapter, self.policy = adapter, policy
        self.enabled, self.bypass, self.authorize = enabled, bypass, authorize
        self.tasks, self.owned, self.admissions = {}, {}, {}
        self.previous = OrderedDict()
        self.closing = False

    async def submit(self, event, key):
        if event.internal or event.is_command():
            from hermes_cli.commands import is_interrupt_then_dispatch
            if is_interrupt_then_dispatch(event.get_command()):
                admission = self.admissions.get(key)
                if admission is not None:
                    admission.generation += 1
                for task in tuple(self.tasks.get(key, ())):
                    self.owned[task].discarded = True
                    task.cancel()
                self.previous.pop(key, None)
            return False
        if self.closing:
            return False
        enabled = self.enabled is None or self.enabled(event)
        if not enabled and key not in self.tasks:
            return False
        if self.bypass is not None and self.bypass(event, key):
            self.previous[key] = ''  # a control response overtook this lane: its context is unknown
            return False
        if event.source.chat_type not in {'group', 'channel', 'thread'}:
            return False
        deadline = asyncio.get_running_loop().time() + self.BUDGET_SECONDS
        arrival_order = time.time_ns()
        admission = self.admissions.setdefault(key, _Admission(asyncio.Lock()))
        admission.references += 1
        generation = admission.generation
        try:
            async with admission.lock:
                return await self._admit(event, key, enabled, deadline, admission, generation, arrival_order)
        finally:
            admission.references -= 1
            if admission.references == 0:
                self.admissions.pop(key, None)

    async def _admit(self, event, key, enabled, deadline, admission, generation, arrival_order):
        authorized = True
        if self.authorize is not None:
            try:
                async with asyncio.timeout_at(deadline):
                    if not await self.authorize(event):
                        return False
            except TimeoutError:
                enabled = authorized = False  # dispatch in order; no policy or transcript recovery
        # Bounded host ownership. Existing decisions have the same arrival deadline, so
        # saturation sheds classification work as deadlines expire, preserving FIFO dispatch.
        while len(self.owned) >= self.MAX_PENDING:
            running = [t for t in self.owned if not t.done()]
            if not running:
                return False  # storage failure: preserve older inputs, do not keep classifying
            await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            await asyncio.sleep(0)  # completed-task ownership callbacks run before rechecking
        lane = self.tasks.setdefault(key, [])
        predecessor = lane[-1] if lane else None
        if admission.generation != generation:
            event._gateway_accepted = True  # explicit reset discarded this pre-reset input
            return True
        item = _OwnedInput(key, event, arrival_order, authorized)
        task = asyncio.create_task(self._work(item, predecessor, enabled, deadline), name='gateway-ingress')
        self.owned[task] = item
        lane.append(task)
        self.adapter._background_tasks.add(task)
        task.add_done_callback(self._done)
        event._gateway_accepted = True
        return True

    async def _work(self, item, predecessor, enabled, deadline):
        event, key = item.event, item.key
        try:
            if predecessor is not None:
                try:
                    await asyncio.shield(predecessor)
                except asyncio.CancelledError:
                    if asyncio.current_task().cancelling():
                        raise
                except Exception:
                    # The predecessor retains its own recovery receipt; successors remain
                    # recoverable too, rather than being silently lost or sent out of order.
                    raise UnsettledObservation('predecessor admission unresolved') from None
            previous = self.previous.get(key)
            handled = False
            if enabled and previous != '' and asyncio.get_running_loop().time() < deadline:
                try:
                    handled = await self.policy(event, key, previous, deadline)
                except (asyncio.CancelledError, UnsettledObservation):
                    raise
                except Exception:
                    logger.warning('Ingress policy failed; continuing normal dispatch')
            if handled:
                item.settled = True
                return
            self.previous[key] = event.message_id or ''
            self.previous.move_to_end(key)
            while len(self.previous) > self.MAX_PENDING:
                self.previous.popitem(last=False)
            # The ingress receipt is not a receipt from the next owner.
            event._gateway_accepted = False
            await self.adapter._dispatch_admitted_message(event, key)
            item.settled = event._gateway_accepted is True
        finally:
            # Also runs on cancellation mid-evaluation. A task cancelled before first entry
            # is handled by _done / shutdown, where the owned record already exists.
            if not item.settled and not item.discarded:
                await asyncio.shield(self._retain(item))

    async def _retain(self, item):
        if item.settled or item.discarded or item.retaining:
            return
        item.retaining = True
        try:
            from gateway.shutdown_flush import flush_pending_to_file
            payload = {'text': item.event.text, 'message_id': item.event.message_id,
                       'ingress_order': item.arrival_order, 'ingress_authorized': item.authorized}
            saved = await asyncio.to_thread(flush_pending_to_file, {item.key: payload}, reason='ingress_unsettled')
            item.settled = saved == 1
            if not item.settled:
                logger.error('Could not preserve pending ingress input')
        finally:
            item.retaining = False

    def _done(self, task):
        self.adapter._background_tasks.discard(task)
        item = self.owned.get(task)
        if item is None:
            return
        lane = self.tasks.get(item.key, [])
        if task in lane:
            lane.remove(task)
        if not lane:
            self.tasks.pop(item.key, None)
        if not task.cancelled() and task.exception() is not None:
            logger.error('Ingress admission deferred to recovery')
            self.previous[item.key] = ''
        if item.settled or item.discarded:
            self.owned.pop(task, None)

    async def preserve_pending(self):
        """Called by adapter shutdown after cancelling workers, before releasing ownership."""
        self.closing = True
        for task, item in list(self.owned.items()):
            await self._retain(item)
            if item.settled or item.discarded:
                self.owned.pop(task, None)
