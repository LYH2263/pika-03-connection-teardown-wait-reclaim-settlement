"""Cross-host tests for nested connection teardown settlement windows.

These tests exercise the same settlement semantics on one and the same
connection instance for every host surface:

* the core Connection window ledger and state restore;
* BaseConnection (the shared Select/Asyncio/Gevent/Tornado base) with the
  connection-workflow abort / transport abort fork;
* BlockingConnection with its pumped ``_closed_result`` sink;
* TwistedProtocolConnection with loseConnection plus Deferred reclaim.

The point is timing causality across nested teardown scopes, not the
is_open/is_closed booleans.
"""
# pylint: disable=W0212

import unittest
from unittest import mock

import pika.channel
import pika.connection
import pika.exceptions
from pika import connection
from pika.adapters import base_connection, blocking_connection

try:
    from twisted.internet import defer
    from twisted.internet import error as twisted_error
    from twisted.python.failure import Failure
    from pika.adapters import twisted_connection
    _TWISTED_AVAILABLE = True
except ImportError:  # Twisted is an optional dependency
    _TWISTED_AVAILABLE = False
    twisted_connection = None
    twisted_error = None
    defer = None
    Failure = None


class _ConstructibleConnection(connection.Connection):
    """Connection with abstract adapter methods overridden so it can be
    instantiated and driven directly without a TCP stack.
    """

    def _adapter_connect_stream(self):
        raise NotImplementedError

    def _adapter_disconnect_stream(self):
        raise NotImplementedError

    def _adapter_call_later(self, delay, callback):
        raise NotImplementedError

    def _adapter_remove_timeout(self, timeout_id):
        raise NotImplementedError

    def _adapter_add_callback_threadsafe(self, callback):
        raise NotImplementedError

    def _adapter_emit_data(self, data):
        raise NotImplementedError


def _make_connection(on_open_error=None, on_close=None):
    errors = []
    closed = []
    conn = _ConstructibleConnection(
        on_open_error_callback=on_open_error or
        (lambda _connection, error: errors.append(error)),
        on_close_callback=on_close or
        (lambda _connection, error: closed.append(error)),
        internal_connection_workflow=False)
    return conn, errors, closed


def _mock_channel(channel_number):
    channel = mock.Mock()
    channel.channel_number = channel_number
    return channel


def _event_names(window):
    return [event[0] for event in window.events]


class CoreSettlementWindowTests(unittest.TestCase):

    def setUp(self):
        self.connection, self.errors, self.closed = _make_connection()
        self.connection._set_connection_state(self.connection.CONNECTION_OPEN)
        self.connection._opened = True
        self.connection._error = None
        self.outer_channel = _mock_channel(1)
        self.connection._channels[1] = self.outer_channel

    def tearDown(self):
        del self.connection

    def test_nested_window_scopes_reclaim_and_restores_outer_view(self):
        error = pika.exceptions.ConnectionClosedByBroker(
            320, 'broker tore the stream down')

        with self.connection._teardown_window() as outer:
            # Inner teardown happens while the outer window is still waiting.
            with self.connection._teardown_window() as inner:
                inner_channel = _mock_channel(2)
                self.connection._channels[2] = inner_channel
                with mock.patch.object(_ConstructibleConnection,
                                       '_adapter_disconnect_stream'):
                    self.connection._terminate_stream(error)
                self.connection._on_stream_terminated(error)

                self.assertEqual(_event_names(inner), [
                    'terminate_requested',
                    'stream_terminated',
                    'channel_reclaim_deferred',
                    'channels_reclaimed',
                    'close_waiters_settled',
                    'state_reset_deferred',
                ])
                # The channel owned by the outer window is not meta-closed
                # by the inner teardown; the inner-created channel is.
                self.outer_channel._on_close_meta.assert_not_called()
                inner_channel._on_close_meta.assert_called_once_with(error)
                self.assertEqual(inner.abort_route, None)
                self.assertTrue(inner.terminate_requested)
                deferred = [
                    event for event in inner.events
                    if event[0] == 'channel_reclaim_deferred'
                ]
                self.assertEqual(deferred,
                                 [('channel_reclaim_deferred', 1, error)])

            # Inner window exited: outer view restored.
            self.assertIs(self.connection._teardown_settlement.current, outer)
            self.assertIsNone(self.connection._error)
            self.assertEqual(self.connection.connection_state,
                             self.connection.CONNECTION_OPEN)
            self.assertIn(1, self.connection._channels)
            self.assertNotIn(2, self.connection._channels)
            self.outer_channel._on_close_meta.assert_not_called()

            # Outer settlement finally reclaims the outer-owned channel.
            self.connection._on_stream_terminated(error)
            self.outer_channel._on_close_meta.assert_called_once_with(error)
            self.assertIn('channels_reclaimed', _event_names(outer))
            self.assertNotIn('channel_reclaim_deferred', _event_names(outer))

        # Outer window exited: entry snapshot is restored for outer waiters.
        self.assertIsNone(self.connection._teardown_settlement.current)
        self.assertIsNone(self.connection._error)
        self.assertEqual(self.connection.connection_state,
                         self.connection.CONNECTION_OPEN)
        self.assertIn(1, self.connection._channels)

    def test_no_window_keeps_legacy_termination_pin(self):
        error = pika.exceptions.ConnectionClosedByBroker(320, 'boom')
        self.connection._on_stream_terminated(error)

        # Every residual channel is meta-closed.
        self.outer_channel._on_close_meta.assert_called_once_with(error)
        # The global state wipe still runs: channels dict reset, pending
        # error cleared.
        self.assertEqual(self.connection._channels, {})
        self.assertIsNone(self.connection._error)
        self.assertEqual(self.connection.connection_state,
                         self.connection.CONNECTION_CLOSED)
        # on_close callback completion surface is served.
        self.assertEqual(self.closed, [error])

    def test_window_exception_exit_still_restores_and_pops(self):
        outer_channel = self.outer_channel
        with self.assertRaises(RuntimeError):
            with self.connection._teardown_window():
                with mock.patch.object(_ConstructibleConnection,
                                       '_adapter_disconnect_stream'):
                    self.connection._terminate_stream(
                        pika.exceptions.ConnectionClosedByBroker(320, 'x'))
                raise RuntimeError('inner failure')

        self.assertIsNone(self.connection._teardown_settlement.current)
        self.assertIsNone(self.connection._error)
        self.assertEqual(self.connection.connection_state,
                         self.connection.CONNECTION_OPEN)
        self.assertIn(1, self.connection._channels)
        outer_channel._on_close_meta.assert_not_called()

    def test_extra_participant_snapshot_restore_is_invoked(self):
        participant = mock.Mock()
        participant._settlement_snapshot.return_value = 'snapshot-token'
        with self.connection._teardown_window(participant):
            participant._settlement_snapshot.assert_called_once_with()
            participant._settlement_restore.assert_not_called()
        participant._settlement_restore.assert_called_once_with(
            'snapshot-token')


@unittest.skipUnless(_TWISTED_AVAILABLE, 'Twisted is not installed')
class TwistedTeardownSettlementTests(unittest.TestCase):
    """Nested teardown settlement semantics on the Twisted host.

    The Twisted pin stays distinct from the other hosts: physical teardown
    is graceful loseConnection (never abort), connection_lost normalizes
    ConnectionDone to a None error argument, and completion surfaces are
    Deferreds (ready, closed, pending _calls).
    """

    @staticmethod
    def _make_wrapper():
        reactor_mock = mock.Mock()
        wrapper = twisted_connection.TwistedProtocolConnection(
            custom_reactor=reactor_mock)
        return wrapper, reactor_mock

    @staticmethod
    def _attach_transport(wrapper):
        transport = mock.Mock(spec=['loseConnection', 'abort', 'write'])
        wrapper._impl.connection_made(transport)
        return transport

    @staticmethod
    def _mark_opened(wrapper):
        """Drive the wrapper through the same open-completion transition
        production uses (Connection.OpenOk processing) without a broker.
        """
        wrapper._impl._opened = True
        wrapper._impl.connection_state = connection.Connection.CONNECTION_OPEN
        wrapper._on_connection_ready(wrapper._impl)

    @staticmethod
    def _add_pending_call(wrapper):
        """Mimic a channel request Deferred parked in wrapper._calls."""
        outcome = {}
        d = defer.Deferred()
        d.addCallback(wrapper._clear_call, d)

        def _on_fail(failure):
            outcome['error'] = failure.value
            return None

        d.addErrback(_on_fail)
        wrapper._calls.add(d)
        return d, outcome

    @staticmethod
    def _watch_closed(wrapper):
        outcome = {}
        wrapper.closed.addCallback(lambda exc: outcome.setdefault('error', exc))
        return outcome

    def test_without_window_disconnect_is_lose_connection_not_abort(self):
        """Pin: with no window declared the adapter routes through
        loseConnection and never touches abort.
        """
        wrapper, _reactor = self._make_wrapper()
        transport = self._attach_transport(wrapper)

        wrapper._impl._adapter_disconnect_stream()

        transport.loseConnection.assert_called_once_with()
        transport.abort.assert_not_called()

    def test_nested_window_lose_connection_routed_once_and_reclaimed(self):
        """Inside a window the loseConnection route is ledgered, and a
        nested re-entry reclaims instead of routing the physical teardown a
        second time.
        """
        wrapper, _reactor = self._make_wrapper()
        transport = self._attach_transport(wrapper)
        error = pika.exceptions.StreamLostError('boom')

        with wrapper._settlement_window() as window:
            wrapper._impl._terminate_stream(error)

            transport.loseConnection.assert_called_once_with()
            transport.abort.assert_not_called()
            self.assertEqual(window.abort_route, 'loseConnection')
            self.assertIn('terminate_requested', _event_names(window))

            # Nested teardown intent must not hit the transport again
            wrapper._impl._adapter_disconnect_stream()
            transport.loseConnection.assert_called_once_with()
            self.assertIn('nested_teardown_reclaimed', _event_names(window))

    def test_connection_lost_normalizes_connection_done_for_open_connection(
            self):
        """Pin: ConnectionDone becomes self._error while _on_stream_terminated
        receives None; on-close waiters still get the ConnectionDone error.
        """
        wrapper, _reactor = self._make_wrapper()
        self._attach_transport(wrapper)
        self._mark_opened(wrapper)

        closed_args = []
        wrapper._impl.add_on_close_callback(
            lambda _connection, error: closed_args.append(error))

        wrapper._impl.connection_lost(Failure(twisted_error.ConnectionDone()))

        self.assertEqual(len(closed_args), 1)
        self.assertIsInstance(closed_args[0], twisted_error.ConnectionDone)

    def test_connection_lost_passes_unexpected_error_through(self):
        """Pin: a non-ConnectionDone Failure reaches on-close waiters as the
        wrapped exception (error argument is not normalized away).
        """
        wrapper, _reactor = self._make_wrapper()
        self._attach_transport(wrapper)
        self._mark_opened(wrapper)

        closed_args = []
        wrapper._impl.add_on_close_callback(
            lambda _connection, error: closed_args.append(error))
        lost = twisted_error.ConnectionLost()

        wrapper._impl.connection_lost(Failure(lost))

        self.assertIs(closed_args[0], lost)

    def test_close_reclaims_pending_calls_and_closed_surface_once(self):
        """Pin completion on the unwindowed path: pending _calls are
        errbacked, closed is callbacked exactly once, surfaces canonicalized.
        """
        wrapper, _reactor = self._make_wrapper()
        self._attach_transport(wrapper)
        self._mark_opened(wrapper)
        _pending, call_outcome = self._add_pending_call(wrapper)
        closed_outcome = self._watch_closed(wrapper)

        wrapper.connectionLost(Failure(twisted_error.ConnectionDone()))

        self.assertIsInstance(call_outcome['error'],
                              twisted_error.ConnectionDone)
        self.assertIsInstance(closed_outcome['error'],
                              twisted_error.ConnectionDone)
        self.assertIsNone(wrapper.closed)
        self.assertEqual(wrapper._calls, set())

    def test_nested_window_inner_loss_does_not_write_through_outer(self):
        """A stream lost inside a declared settlement window settles only
        the inner _calls/closed surfaces; the outer pending call and closed
        wait are untouched until the window exits and hands the outcome
        down exactly once.
        """
        wrapper, _reactor = self._make_wrapper()
        transport = self._attach_transport(wrapper)
        self._mark_opened(wrapper)
        _outer_pending, outer_call_outcome = self._add_pending_call(wrapper)
        outer_closed_outcome = self._watch_closed(wrapper)

        with wrapper._settlement_window() as window:
            self.assertIsNotNone(wrapper.closed)
            inner_closed_outcome = self._watch_closed(wrapper)
            _inner_pending, inner_call_outcome = self._add_pending_call(wrapper)

            wrapper.connectionLost(Failure(twisted_error.ConnectionDone()))

            # Inner surfaces settled in-window
            self.assertIsInstance(inner_call_outcome['error'],
                                  twisted_error.ConnectionDone)
            self.assertIsInstance(inner_closed_outcome['error'],
                                  twisted_error.ConnectionDone)
            # Outer waits must not be drained by the inner teardown
            self.assertNotIn('error', outer_call_outcome)
            self.assertNotIn('error', outer_closed_outcome)
            self.assertIsNone(wrapper._impl._transport)
            names = _event_names(window)
            self.assertIn('stream_terminated', names)
            self.assertIn('close_waiters_settled', names)

        # Hand-down on window exit: outer surfaces settle exactly once and
        # the outer scope's transport view is restored by the participant.
        self.assertIsInstance(outer_call_outcome['error'],
                              twisted_error.ConnectionDone)
        self.assertIsInstance(outer_closed_outcome['error'],
                              twisted_error.ConnectionDone)
        self.assertIsNone(wrapper.closed)
        self.assertEqual(wrapper._calls, set())
        self.assertIs(wrapper._impl._transport, transport)

    def test_nested_window_opening_abort_errbacks_outer_ready_once(self):
        """An abort while opening (ready still pending) errbacks the inner
        ready surface with AMQPConnectionError and hands that Failure down
        to the outer ready surface exactly once on window exit.
        """
        wrapper, _reactor = self._make_wrapper()
        self._attach_transport(wrapper)
        outer_ready_outcome = {}
        wrapper.ready.addErrback(lambda f: (outer_ready_outcome.setdefault(
            'error', f.value), None)[1])

        with wrapper._settlement_window():
            inner_ready = wrapper.ready
            self.assertIsNot(inner_ready, None)
            inner_ready_outcome = {}
            inner_ready.addErrback(lambda f: (inner_ready_outcome.setdefault(
                'error', f.value), None)[1])

            wrapper.connectionLost(Failure(twisted_error.ConnectionLost()))

            self.assertIsInstance(inner_ready_outcome['error'],
                                  pika.exceptions.AMQPConnectionError)
            self.assertNotIn('error', outer_ready_outcome)
            self.assertIsNone(wrapper.ready)

        self.assertIsInstance(outer_ready_outcome['error'],
                              pika.exceptions.AMQPConnectionError)
        self.assertIsNone(wrapper.ready)

    def test_window_without_terminal_event_keeps_outer_waits_pending(self):
        """A window around non-terminal inner work restores the outer ready
        surface untouched and merges still-pending inner calls back up.
        """
        wrapper, _reactor = self._make_wrapper()
        self._attach_transport(wrapper)
        outer_ready = wrapper.ready

        with wrapper._settlement_window():
            inner_pending, inner_outcome = self._add_pending_call(wrapper)
            self.assertIsNot(wrapper.ready, outer_ready)

        # Opening never completed or aborted: outer ready surface restored
        self.assertIs(wrapper.ready, outer_ready)
        self.assertFalse(outer_ready.called)
        # Still-pending inner work stays serviceable on the outer surface
        self.assertIn(inner_pending, wrapper._calls)
        self.assertNotIn('error', inner_outcome)


@unittest.skipUnless(_TWISTED_AVAILABLE, 'Twisted is not installed')
class CrossHostNestedSettlementContractTests(unittest.TestCase):
    """The SAME nested settlement causal sequence is executed on one
    connection instance for every host.

    An outer-owned pending wait stays pending while an inner settlement
    window tears the stream down; the inner settlement carries an error and
    ledger facts; after the inner window exits the outer wait settles exactly
    once. The completion surfaces stay host-specific (pumped sink / abort
    fork plus channel reclaim / Deferreds), but the nested settlement
    contract is identical. Missing any of the three hosts fails the chain.
    """

    HOSTS = ('blocking', 'base', 'twisted')

    def test_same_nested_settlement_semantics_on_every_host(self):
        for host in self.HOSTS:
            with self.subTest(host=host):
                evidence = getattr(self, '_drive_' + host)()
                self._assert_common_contract(evidence)

    @staticmethod
    def _assert_common_contract(evidence):
        # Timing causality on the same connection instance, not the
        # is_open/is_closed booleans.
        assert evidence['inner_settled'], 'inner scope did not settle'
        assert not evidence['outer_pending_during_inner'], (
            'inner teardown wrote through the outer pending wait')
        assert evidence['outer_settled'], (
            'outer wait did not settle after the inner window exited')
        assert evidence['outer_settle_count'] == 1, (
            'outer wait did not settle exactly once')
        assert type(evidence['inner_error']) is type(  # noqa: E721
            evidence['outer_error']), (
                'inner and outer errors describe different terminal causes')
        names = evidence['ledger']
        assert 'stream_terminated' in names
        assert 'close_waiters_settled' in names
        assert evidence['connection_before'] is evidence['connection_after']

    # -- BlockingConnection host -----------------------------------------

    def _drive_blocking(self):
        with mock.patch.object(blocking_connection.select_connection,
                               'SelectConnection',
                               spec_set=_BlockingImplTemplate) as select_class:
            impl_mock = select_class.return_value
            with mock.patch.object(blocking_connection.BlockingConnection,
                                   '_create_connection',
                                   return_value=impl_mock):
                conn = blocking_connection.BlockingConnection('params')

        anchor = _NoDisconnectCoreConnection(
            on_open_error_callback=lambda _connection, _error: None,
            on_close_callback=lambda _connection, _error: None,
            internal_connection_workflow=False)
        anchor._opened = True
        anchor.connection_state = anchor.CONNECTION_OPEN
        impl_mock.callbacks = anchor.callbacks
        impl_mock._teardown_window = anchor._teardown_window
        anchor.callbacks.add(0, pika.connection.Connection.ON_CONNECTION_CLOSED,
                             conn._closed_result.set_value_once, False)
        impl_mock._get_write_buffer_size = mock.Mock(return_value=0)
        impl_mock._transport = mock.NonCallableMock()
        impl_mock.ioloop.process_timeouts = mock.Mock()

        external_pending = blocking_connection._CallbackResult()
        broker_error = pika.exceptions.ConnectionClosedByBroker(
            541, 'INTERNAL_ERROR')

        connection_before = anchor
        with conn._settlement_window() as window:
            inner_sink = conn._current_closed_result()
            # Inner teardown on the same connection: terminate intent plus
            # stream-terminated completion.
            anchor._terminate_stream(broker_error)
            anchor._on_stream_terminated(broker_error)

            inner_settled = inner_sink.ready
            inner_error = inner_sink.value.error
            outer_pending_during_inner = (conn._closed_result.ready or
                                          external_pending.ready)
            ledger = _event_names(window)

        # Window exit hands the close down to the outer sink once; the
        # shared ioloop is not owned by a window and must stay open.
        outer_error = conn._closed_result.value.error
        self.assertEqual(impl_mock.ioloop.close.call_count, 0)
        self.assertIs(outer_error, broker_error)
        self.assertFalse(external_pending.ready)
        self.assertEqual(conn._closed_result_sinks, [conn._closed_result])

        return {
            'connection_before': connection_before,
            'connection_after': anchor,
            'inner_settled': inner_settled,
            'inner_error': inner_error,
            'outer_pending_during_inner': outer_pending_during_inner,
            'outer_settled': conn._closed_result.ready,
            'outer_settle_count': int(conn._closed_result.ready),
            'outer_error': outer_error,
            'ledger': ledger,
        }

    # -- BaseConnection host (Select/Asyncio shared base) ----------------

    def _drive_base(self):
        errors = []
        with mock.patch.object(_SettlementConstructibleBaseConnection,
                               '_adapter_connect_stream'):
            conn = _SettlementConstructibleBaseConnection(
                None,
                lambda _connection: None,
                lambda _connection, error: errors.append(error),
                lambda _connection, _error: None,
                None,
                internal_connection_workflow=True)
        workflow = mock.Mock()
        conn._connection_workflow = workflow
        outer_channel = _mock_channel(1)

        connection_before = conn
        with conn._teardown_window():
            # Outer opening close owns the single physical workflow abort.
            conn.close()
            conn._channels[1] = outer_channel
            self.assertEqual(workflow.abort.call_count, 1)

            with conn._teardown_window() as inner:
                # Inner re-entry to the abort fork while still aborting:
                # no second physical abort, and the stream completion only
                # defers the outer-owned channel reclaim.
                conn._abort_connection_workflow()
                conn._on_stream_terminated(conn._error)

                inner_settled = bool(errors)
                inner_error = errors[0]
                outer_pending_during_inner = bool(
                    outer_channel._on_close_meta.call_args_list)
                ledger = _event_names(inner)

            # Inner window exited: outer channel still not reclaimed.
            outer_channel._on_close_meta.assert_not_called()

            # The abort completion finally arrives at the outer scope,
            # which reclaims its channel exactly once.
            conn._handle_connection_workflow_failure(None)
            outer_settle_count = outer_channel._on_close_meta.call_count
            outer_error = outer_channel._on_close_meta.call_args[0][0]

        # The physical abort routed once despite two nested teardown intents.
        self.assertEqual(workflow.abort.call_count, 1)
        self.assertEqual(outer_settle_count, 1)

        return {
            'connection_before': connection_before,
            'connection_after': conn,
            'inner_settled': inner_settled,
            'inner_error': inner_error,
            'outer_pending_during_inner': outer_pending_during_inner,
            'outer_settled': outer_settle_count == 1,
            'outer_settle_count': outer_settle_count,
            'outer_error': outer_error,
            'ledger': ledger,
        }

    # -- Twisted host -----------------------------------------------------

    def _drive_twisted(self):
        wrapper = twisted_connection.TwistedProtocolConnection(
            custom_reactor=mock.Mock())
        transport = mock.Mock(spec=['loseConnection', 'abort', 'write'])
        wrapper._impl.connection_made(transport)
        wrapper._impl._opened = True
        wrapper._impl.connection_state = connection.Connection.CONNECTION_OPEN
        wrapper._on_connection_ready(wrapper._impl)

        # Outer-owned pending waits
        bottom_closed_events = []
        wrapper.closed.addCallback(bottom_closed_events.append)
        bottom_call_errors = []
        bottom_call = defer.Deferred()
        bottom_call.addErrback(
            lambda failure: (bottom_call_errors.append(failure.value), None)[1])
        wrapper._calls.add(bottom_call)

        done_error = twisted_error.ConnectionDone()
        connection_before = wrapper._impl

        with wrapper._settlement_window():
            with wrapper._settlement_window() as inner:
                # A pending call created by the inner scope
                inner_call = defer.Deferred()
                inner_outcome = {}
                inner_call.addErrback(lambda failure: (inner_outcome.setdefault(
                    'error', failure.value), None)[1])
                wrapper._calls.add(inner_call)

                # Inner stream loss on the same connection
                wrapper.connectionLost(Failure(done_error))

                inner_settled = 'error' in inner_outcome
                inner_error = inner_outcome.get('error')
                outer_pending_during_inner = bool(bottom_closed_events or
                                                  bottom_call_errors)
                ledger = _event_names(inner)

            # Inner exit hands down to the outer window, not to the bottom
            self.assertEqual(bottom_closed_events, [])
            self.assertEqual(bottom_call_errors, [])

        # Outer exit settles the bottom completion surfaces exactly once.
        self.assertEqual(len(bottom_closed_events), 1)
        self.assertEqual(len(bottom_call_errors), 1)
        self.assertIs(wrapper._impl._transport, transport)

        return {
            'connection_before': connection_before,
            'connection_after': wrapper._impl,
            'inner_settled': inner_settled,
            'inner_error': inner_error,
            'outer_pending_during_inner': outer_pending_during_inner,
            'outer_settled': bool(bottom_closed_events),
            'outer_settle_count': len(bottom_closed_events),
            'outer_error': bottom_closed_events[0],
            'ledger': ledger,
        }


class _NoDisconnectCoreConnection(_ConstructibleConnection):
    """Core connection whose adapter teardown is a no-op, so the stream
    state machine can be driven directly in host-contract tests.
    """

    def _adapter_disconnect_stream(self):
        return None


class _SettlementConstructibleBaseConnection(base_connection.BaseConnection):
    """Concrete BaseConnection for direct, broker-free construction."""

    @classmethod
    def create_connection(cls, *args, **kwargs):  # pylint: disable=W0221
        raise NotImplementedError


class _BlockingImplTemplate(
        blocking_connection.select_connection.SelectConnection):
    is_closed = None
    is_closing = None
    is_open = None
    _channels = None
    ioloop = None
    _transport = None
    _get_write_buffer_size = None
    callbacks = None
    _teardown_window = None


if __name__ == '__main__':
    unittest.main()
