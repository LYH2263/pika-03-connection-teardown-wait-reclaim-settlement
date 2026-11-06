"""Tests for pika.base_connection.BaseConnection."""

import socket
import unittest
from unittest import mock

import pika
import pika.tcp_socket_opts
from pika.adapters import base_connection

# If this is missing, set it manually. We need it to test tcp opt setting.
try:
    TCP_KEEPIDLE = socket.TCP_KEEPIDLE
except AttributeError:
    TCP_KEEPIDLE = 4


class ConstructibleBaseConnection(base_connection.BaseConnection):
    """Adds dummy overrides for `BaseConnection`'s abstract methods so that we can instantiate and
    test it.
    """

    @classmethod
    def create_connection(cls, *args, **kwargs):
        raise NotImplementedError


class BaseConnectionTests(unittest.TestCase):

    def setUp(self):
        with mock.patch.object(ConstructibleBaseConnection,
                               '_adapter_connect_stream'):
            self.connection = ConstructibleBaseConnection(
                None, None, None, None, None, internal_connection_workflow=True)
            self.connection._set_connection_state(
                ConstructibleBaseConnection.CONNECTION_OPEN)

    def test_repr(self):
        text = repr(self.connection)
        self.assertTrue(text.startswith('<ConstructibleBaseConnection'), text)

    def test_should_raise_value_exception_with_no_params_func_instead(self):
        self.assertRaises(ValueError,
                          ConstructibleBaseConnection,
                          lambda: True,
                          None,
                          None,
                          None,
                          None,
                          internal_connection_workflow=True)

    def test_proto_connection_lost_chains_original_error(self):
        try:
            raise ZeroDivisionError('division by zero')
        except ZeroDivisionError as exc:
            original = exc

        with mock.patch.object(self.connection,
                               '_on_stream_terminated') as term_mock:
            self.connection._proto_connection_lost(original)

        reason = term_mock.call_args[0][0]
        self.assertIsInstance(reason, pika.exceptions.StreamLostError)
        self.assertIs(reason.__cause__, original)
        self.assertIsNotNone(reason.__cause__.__traceback__)

    def test_proto_connection_lost_eof_has_no_cause(self):
        self.connection._got_eof = True

        with mock.patch.object(self.connection,
                               '_on_stream_terminated') as term_mock:
            self.connection._proto_connection_lost(None)

        reason = term_mock.call_args[0][0]
        self.assertIsInstance(reason, pika.exceptions.StreamLostError)
        self.assertIsNone(reason.__cause__)

    def test_tcp_options_with_dict_tcp_options(self):

        tcp_options = {'TCP_KEEPIDLE': 60}
        params = pika.ConnectionParameters(tcp_options=tcp_options)
        self.assertEqual(params.tcp_options, tcp_options)

        with mock.patch.dict('pika.tcp_socket_opts._SUPPORTED_TCP_OPTIONS',
                             {'TCP_KEEPIDLE': TCP_KEEPIDLE}):
            sock_mock = mock.Mock()
            pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

            expected = [
                mock.call.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                mock.call.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, 60)
            ]
            self.assertEqual(sock_mock.method_calls, expected)

    def test_tcp_options_with_invalid_tcp_options(self):

        tcp_options = {'TCP_EVIL_OPTION': 1234}
        params = pika.ConnectionParameters(tcp_options=tcp_options)
        self.assertEqual(params.tcp_options, tcp_options)

        sock_mock = mock.Mock()
        pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

        keepalive_call = mock.call.setsockopt(socket.SOL_SOCKET,
                                              socket.SO_KEEPALIVE, 1)
        self.assertNotIn(keepalive_call, sock_mock.method_calls)

    def test_tcp_options_with_none_tcp_options(self):

        params = pika.ConnectionParameters(tcp_options=None)
        self.assertIsNone(params.tcp_options)

        sock_mock = mock.Mock()
        pika.tcp_socket_opts.set_sock_opts(params.tcp_options, sock_mock)

        keepalive_call = mock.call.setsockopt(socket.SOL_SOCKET,
                                              socket.SO_KEEPALIVE, 1)
        self.assertNotIn(keepalive_call, sock_mock.method_calls)

class AbortForkAndSettlementTests(unittest.TestCase):
    """The opening-abort fork (connection-workflow abort vs transport abort)
    and the post-termination channel reclaim on the shared BaseConnection.
    """

    def _make_connection(self):
        errors = []
        closed = []
        with mock.patch.object(ConstructibleBaseConnection,
                               '_adapter_connect_stream'):
            connection = ConstructibleBaseConnection(
                None,
                lambda _conn: None,
                lambda _conn, error: errors.append(error),
                lambda _conn, error: closed.append(error),
                None,
                internal_connection_workflow=True)
        return connection, errors, closed

    def test_opening_without_transport_aborts_workflow(self):
        connection, errors, closed = self._make_connection()
        workflow = mock.Mock()
        connection._connection_workflow = workflow
        self.assertIsNone(connection._transport)
        channel = _mock_channel(1)

        with connection._teardown_window() as window:
            connection.close()

            # Fork pin: only the connection workflow is aborted.
            workflow.abort.assert_called_once_with()
            self.assertEqual(window.abort_route, 'workflow')

            # The abort completes asynchronously; a residual channel observed
            # meanwhile is reclaimed by stream termination.
            connection._channels[1] = channel
            connection._handle_connection_workflow_failure(None)

            channel._on_close_meta.assert_called_once_with(connection._error)
            self.assertIsInstance(connection._error,
                                  pika.exceptions.ConnectionOpenAborted)
            # Opening-abort completion surface: ON_CONNECTION_ERROR.
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0],
                                  pika.exceptions.ConnectionOpenAborted)
            self.assertEqual(closed, [])
            names = _event_names(window)
            self.assertIn('terminate_requested', names)
            self.assertIn('channels_reclaimed', names)
            self.assertIn('close_waiters_settled', names)
            self.assertIn('state_reset_deferred', names)
            # The inner-created channel stays gone after window-scoped
            # termination.
            self.assertEqual(connection.connection_state,
                             connection.CONNECTION_CLOSED)

        # Window exit restored the outer opening view.
        self.assertIsNone(connection._error)
        self.assertEqual(connection.connection_state,
                         connection.CONNECTION_INIT)
        self.assertNotIn(1, connection._channels)
        self.assertEqual(connection._connection_workflow, workflow)
        workflow.abort.assert_called_once_with()

    def test_opening_with_transport_aborts_transport(self):
        connection, errors, closed = self._make_connection()
        workflow = mock.Mock()
        transport = mock.Mock()
        connection._connection_workflow = workflow
        connection._transport = transport
        channel = _mock_channel(1)

        with connection._teardown_window() as window:
            connection.close()

            # Fork pin: transport.abort() only - workflow.abort() would
            # recurse through Connection.close().
            transport.abort.assert_called_once_with()
            workflow.abort.assert_not_called()
            self.assertEqual(window.abort_route, 'transport')

            # Transport abort completes asynchronously in connection_lost().
            connection._channels[1] = channel
            connection._proto_connection_lost(None)

            channel._on_close_meta.assert_called_once_with(connection._error)
            self.assertIsInstance(connection._error,
                                  pika.exceptions.ConnectionOpenAborted)
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0],
                                  pika.exceptions.ConnectionOpenAborted)
            self.assertEqual(closed, [])

        # The outer window keeps seeing the paired transport/workflow.
        self.assertIs(connection._transport, transport)
        self.assertIs(connection._connection_workflow, workflow)
        self.assertIsNone(connection._error)
        transport.abort.assert_called_once_with()

    def test_opened_graceful_close_routes_transport_and_closed_surface(self):
        connection, errors, closed = self._make_connection()
        transport = mock.Mock()
        connection._transport = transport
        connection._opened = True
        connection._set_connection_state(connection.CONNECTION_OPEN)
        client_error = pika.exceptions.ConnectionClosedByClient(
            200, 'Normal shutdown')
        channel = _mock_channel(1)
        connection._channels[1] = channel

        with connection._teardown_window() as window:
            # Post Connection.Close/CloseOk the stream is terminated.
            connection._error = client_error
            connection._terminate_stream(None)
            transport.abort.assert_called_once_with()
            self.assertEqual(window.abort_route, 'transport')

            connection._proto_connection_lost(None)
            channel._on_close_meta.assert_called_once_with(client_error)
            # Opened completion surface: ON_CONNECTION_CLOSED.
            self.assertEqual(closed, [client_error])
            self.assertEqual(errors, [])
            names = _event_names(window)
            close_waits = [
                event for event in window.events
                if event[0] == 'close_waiters_settled'
            ]
            self.assertEqual(close_waits,
                             [('close_waiters_settled', True, client_error)])

        # Outer-owned channel is put back un-meta-closed on window exit.
        self.assertIn(1, connection._channels)
        channel._on_close_meta.assert_called_once_with(client_error)

    def test_nested_repeat_teardown_does_not_double_abort(self):
        # Outer opening close already routed the workflow abort when an inner
        # scope requests teardown again on the same connection.
        connection, errors, closed = self._make_connection()
        workflow = mock.Mock()
        connection._connection_workflow = workflow
        outer_channel = _mock_channel(1)

        with connection._teardown_window() as outer:
            connection.close()
            workflow.abort.assert_called_once_with()
            connection._channels[1] = outer_channel

            with connection._teardown_window() as inner:
                # e.g. an error frame / heartbeat timeout during the abort
                connection._terminate_stream(
                    pika.exceptions.StreamLostError('again'))

                # No second workflow abort; reclaimed against the inner window
                # instead.
                workflow.abort.assert_called_once_with()
                self.assertIsNone(inner.abort_route)
                self.assertIn(('nested_teardown_reclaimed', 'workflow'),
                              inner.events)

                # Inner window-local termination must not meta-close the
                # channel the outer window already owned.
                connection._on_stream_terminated(connection._error)
                outer_channel._on_close_meta.assert_not_called()
                self.assertIn('channel_reclaim_deferred', _event_names(inner))

            # Inner window exited: outer abort intent and channel restored.
            outer_channel._on_close_meta.assert_not_called()
            self.assertIsInstance(connection._error,
                                  pika.exceptions.ConnectionOpenAborted)
            self.assertEqual(connection.connection_state,
                             connection.CONNECTION_CLOSING)

            # Outer completion finally reclaims its channel exactly once.
            connection._handle_connection_workflow_failure(None)
            outer_channel._on_close_meta.assert_called_once_with(
                connection._error)
            self.assertIsInstance(connection._error,
                                  pika.exceptions.ConnectionOpenAborted)
            self.assertIn('channels_reclaimed', _event_names(outer))
            self.assertNotIn('channel_reclaim_deferred', _event_names(outer))
            workflow.abort.assert_called_once_with()

    def test_nested_repeat_teardown_does_not_double_abort_transport(self):
        connection, errors, closed = self._make_connection()
        workflow = mock.Mock()
        transport = mock.Mock()
        connection._connection_workflow = workflow
        connection._transport = transport
        outer_channel = _mock_channel(1)

        with connection._teardown_window():
            connection.close()
            transport.abort.assert_called_once_with()
            workflow.abort.assert_not_called()
            connection._channels[1] = outer_channel

            with connection._teardown_window() as inner:
                connection._terminate_stream(
                    pika.exceptions.StreamLostError('again'))
                transport.abort.assert_called_once_with()
                workflow.abort.assert_not_called()
                self.assertEqual([
                    e for e in inner.events
                    if e[0] == 'nested_teardown_reclaimed'
                ], [('nested_teardown_reclaimed', 'transport')])

            connection._proto_connection_lost(None)
            outer_channel._on_close_meta.assert_called_once_with(
                connection._error)

    def test_no_window_keeps_legacy_abort_pin(self):
        connection, errors, closed = self._make_connection()
        workflow = mock.Mock()
        connection._connection_workflow = workflow
        channel = _mock_channel(1)

        connection.close()
        workflow.abort.assert_called_once_with()
        abort_error = connection._error
        connection._channels[1] = channel
        connection._handle_connection_workflow_failure(None)

        channel._on_close_meta.assert_called_once_with(abort_error)
        self.assertEqual(len(errors), 1)
        # Legacy global state wipe still runs with no window declared.
        self.assertEqual(connection._channels, {})
        self.assertIsNone(connection._error)
        self.assertIsNone(connection._connection_workflow)
        self.assertEqual(connection.connection_state,
                         connection.CONNECTION_CLOSED)

    def test_no_window_opened_disconnect_aborts_transport(self):
        connection, errors, closed = self._make_connection()
        transport = mock.Mock()
        connection._transport = transport
        connection._opened = True
        connection._set_connection_state(connection.CONNECTION_OPEN)

        connection._adapter_disconnect_stream()
        transport.abort.assert_called_once_with()
