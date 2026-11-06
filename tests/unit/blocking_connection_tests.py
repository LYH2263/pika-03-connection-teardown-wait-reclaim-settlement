"""Tests for pika.adapters.blocking_connection.BlockingConnection."""

import sys
import unittest
from unittest import mock
from unittest.mock import patch

import pika
import pika.channel
import pika.exceptions
from pika.adapters import blocking_connection
from pika.adapters.utils import nbio_interface


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    pass


class SelectConnectionTemplate(
        blocking_connection.select_connection.SelectConnection):
    # is_closed/is_closing/is_open/ioloop (properties) and
    # _get_write_buffer_size (method) are inherited and already part of the
    # mock spec. Only the instance attributes below need declaring at class
    # level so spec_set exposes them.
    _channels = {}  # noqa: RUF012
    _transport = None


class BlockingConnectionTests(unittest.TestCase):
    """Tests for BlockingConnection."""

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_constructor(self, _select_connection_class_mock):
        params = pika.ConnectionParameters()
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection') as _create_connection_mock:
            connection = blocking_connection.BlockingConnection(params)

        _create_connection_mock.assert_called_once_with(params, None)

        impl_mock = _create_connection_mock.return_value
        impl_mock.add_on_close_callback.assert_called_once_with(
            connection._closed_result.set_value_once)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_repr_when_create_connection_raises(self,
                                                _select_connection_class_mock):
        # `__init__` leaves `_impl` unassigned when `_create_connection` raises.
        # `__repr__` runs on that half-built instance from any traceback or
        # debugger, and must report the object instead of masking the original
        # error with an `AttributeError`.
        half_built = []

        def capture_and_raise(connection, _parameters, _impl_class):
            half_built.append(connection)
            raise RuntimeError('workflow failed')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_create_connection',
                autospec=True,
                side_effect=capture_and_raise), self.assertRaises(RuntimeError):
            blocking_connection.BlockingConnection(pika.ConnectionParameters())

        self.assertEqual(len(half_built), 1)
        self.assertEqual(repr(half_built[0]), '<BlockingConnection impl=None>')

        # `_create_connection()` calls `_cleanup()` on the same half-built
        # instance before re-raising, so that reader must tolerate the missing
        # attribute too.
        half_built[0]._cleanup()

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup(self,
                                             select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection(None)

        mock_connection = select_connection_class_mock.return_value
        with mock.patch.object(
                select_connection_class_mock,
                'create_connection',
                side_effect=lambda configs, on_done, custom_ioloop:
            [on_done(mock_connection),
             custom_ioloop.close(), None][2]):
            result = connection._create_connection(
                None, select_connection_class_mock)

            self.assertIs(result, mock_connection)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup_fails_with_open_error(
            self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection(None)

        exc_value = pika.exceptions.AMQPConnectionError('failed')
        with mock.patch.object(select_connection_class_mock,
                               'create_connection',
                               side_effect=lambda configs, on_done,
                               custom_ioloop: on_done(exc_value)):
            with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
                connection._create_connection(None,
                                              select_connection_class_mock)

            self.assertIs(cm.exception, exc_value)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        get_buffer_size_mock = mock.Mock(
            name='_get_write_buffer_size',
            side_effect=[100, 50, 0],
            spec=nbio_interface.AbstractStreamTransport.get_write_buffer_size)

        transport_mock = mock.NonCallableMock(
            spec_set=nbio_interface.AbstractStreamTransport)

        connection._impl._transport = transport_mock
        connection._impl._get_write_buffer_size = get_buffer_size_mock

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_user_initiated_close(self,
                                               select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        original_exc = pika.exceptions.ConnectionClosedByClient(200, 'success')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        connection._flush_output(lambda: False, lambda: True)

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_server_initiated_error_close(
            self, select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        original_exc = pika.exceptions.ConnectionClosedByBroker(
            404, 'not found')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (404, 'not found'))

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate,
                  is_closed=False)
    def test_flush_output_server_initiated_no_error_close(
            self, select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        original_exc = pika.exceptions.ConnectionClosedByBroker(200, 'ok')
        connection._closed_result.set_value_once(impl_mock, original_exc)

        impl_mock.is_closed = False
        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (200, 'ok'))

        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        impl_channel_mock = mock.Mock()
        connection._impl._channels = {1: impl_channel_mock}

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

        impl_channel_mock._get_cookie.return_value.close.assert_called_once_with(
            200, 'text')
        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_close_with_channel_closed_exception(self,
                                                 select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        channel1_mock = mock.Mock(is_open=True,
                                  close=mock.Mock(
                                      side_effect=pika.exceptions.ChannelClosed(
                                          -1, 'Just because'),
                                      spec_set=pika.channel.Channel.close),
                                  spec_set=blocking_connection.BlockingChannel)

        channel2_mock = mock.Mock(is_open=True,
                                  spec_set=blocking_connection.BlockingChannel)

        connection._impl._channels = {
            1:
                mock.Mock(_get_cookie=mock.Mock(
                    return_value=channel1_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                          spec_set=pika.channel.Channel),
            2:
                mock.Mock(_get_cookie=mock.Mock(
                    return_value=channel2_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                          spec_set=pika.channel.Channel)
        }

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

            channel1_mock.close.assert_called_once_with(200, 'text')
            channel2_mock.close.assert_called_once_with(200, 'text')
        impl_mock.close.assert_called_once_with(200, 'text')

    @unittest.skipIf(sys.version_info < (3, 8),
                     'mock args differ on 3.7 and earlier')
    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_update_secret(self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        impl_mock.is_open = True
        impl_mock._transport = None

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        with mock.patch.object(blocking_connection._CallbackResult,
                               'is_ready',
                               return_value=True):
            connection.update_secret('new_secret', 'reason')

        if sys.version_info < (3, 8):
            args_0 = select_connection_class_mock.return_value.update_secret.call_args[
                0]
            args_1 = select_connection_class_mock.return_value.update_secret.call_args[
                1]
            args_len = len(select_connection_class_mock.return_value.
                           update_secret.call_args)
        else:
            args_0 = select_connection_class_mock.return_value.update_secret.call_args.args[
                0]
            args_1 = select_connection_class_mock.return_value.update_secret.call_args.args[
                1]
            args_len = len(select_connection_class_mock.return_value.
                           update_secret.call_args.args)

        self.assertEqual(args_0, 'new_secret')
        self.assertEqual(args_1, 'reason')
        self.assertEqual(args_len, 3)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    @patch.object(blocking_connection,
                  'BlockingChannel',
                  spec_set=blocking_connection.BlockingChannel)
    def test_channel(self, blocking_channel_class_mock,
                     select_connection_class_mock):

        impl_mock = select_connection_class_mock.return_value

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection.channel()

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_sleep(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection'):
            connection = blocking_connection.BlockingConnection(None)

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_flush_output',
                               spec_set=connection._flush_output):
            connection.sleep(0.00001)

    def test_connection_blocked_evt(self):
        blocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Blocked('reason'))
        evt = blocking_connection._ConnectionBlockedEvt(blocked_buffer.append,
                                                        frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(blocked_buffer), 1)
        self.assertIs(blocked_buffer[0], frame)

    def test_connection_unblocked_evt(self):
        unblocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Unblocked())
        evt = blocking_connection._ConnectionUnblockedEvt(
            unblocked_buffer.append, frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(unblocked_buffer), 1)
        self.assertIs(unblocked_buffer[0], frame)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_data_events_raises_on_channel_closed_by_broker(
            self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        exc = pika.exceptions.ChannelClosedByBroker(406, 'consumer_timeout')

        impl_channel_mock = mock.Mock()
        impl_channel_mock.channel_number = 1
        channel = blocking_connection.BlockingChannel(impl_channel_mock,
                                                      connection)

        with mock.patch.object(channel, '_cleanup'):
            channel._on_channel_closed(impl_channel_mock, exc)

        with mock.patch.object(connection, '_flush_output'):
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as cm:
                connection.process_data_events(time_limit=0)

            self.assertEqual(cm.exception.reply_code, 406)
            self.assertEqual(cm.exception.reply_text, 'consumer_timeout')

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_process_data_events_raises_after_full_channel_cleanup(
            self, select_connection_class_mock):
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        impl_mock._channels = {}

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            connection = blocking_connection.BlockingConnection(None)

        exc = pika.exceptions.ChannelClosedByBroker(406, 'consumer_timeout')

        impl_channel_mock = mock.Mock()
        impl_channel_mock.channel_number = 1
        channel = blocking_connection.BlockingChannel(impl_channel_mock,
                                                      connection)

        with mock.patch.object(channel, '_cleanup'):
            channel._on_channel_closed(impl_channel_mock, exc)

        impl_mock._channels.pop(1, None)
        impl_channel_mock._cookie = None

        with mock.patch.object(connection, '_flush_output'):
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as cm:
                connection.process_data_events(time_limit=0)

            self.assertEqual(cm.exception.reply_code, 406)
            self.assertEqual(cm.exception.reply_text, 'consumer_timeout')

    @staticmethod
    def _make_open_connection(select_connection_class_mock):
        """
        Create a BlockingConnection and return it with its impl mock.

        Set attributes on the returned mock (not connection._impl) so pyright sees a MagicMock
        rather than the real SelectConnection properties.
        """
        impl_mock = select_connection_class_mock.return_value
        impl_mock.is_closed = False
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_create_connection',
                               return_value=impl_mock):
            return blocking_connection.BlockingConnection(None), impl_mock

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_is_closed_delegates_to_impl(self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.is_closed = True
        self.assertIs(connection.is_closed, True)
        impl_mock.is_closed = False
        self.assertIs(connection.is_closed, False)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_is_open_delegates_to_impl(self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.is_open = True
        self.assertIs(connection.is_open, True)
        impl_mock.is_open = False
        self.assertIs(connection.is_open, False)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_accept_consumer_cancel_ok_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.accept_consumer_cancel_ok = True
        self.assertIs(connection.accept_consumer_cancel_ok_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_basic_nack_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.basic_nack = True
        self.assertIs(connection.basic_nack_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_consumer_cancel_notify_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.consumer_cancel_notify = True
        self.assertIs(connection.consumer_cancel_notify_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_exchange_exchange_bindings_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.exchange_exchange_bindings = True
        self.assertIs(connection.exchange_exchange_bindings_supported, True)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SelectConnectionTemplate)
    def test_publisher_confirms_supported_delegates_to_impl(
            self, select_connection_class_mock):
        connection, impl_mock = self._make_open_connection(
            select_connection_class_mock)
        impl_mock.publisher_confirms = True
        self.assertIs(connection.publisher_confirms_supported, True)

class SettlementSelectConnectionTemplate(SelectConnectionTemplate):
    """Template additionally exposing the members rewired by teardown
    settlement tests (real callback manager and core window stack).
    """
    callbacks = None
    _teardown_window = None


class _SettlementCoreConnection(pika.connection.Connection):
    """Concrete core Connection that backs the settlement-window stack and
    callback manager of a mocked SelectConnection impl.
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


def _make_settlement_connection(impl_mock):
    """Build a BlockingConnection whose mocked impl shares a real core
    Connection callback manager and settlement-window anchor. Returns
    ``(blocking_connection, anchor_connection)``.
    """
    with mock.patch.object(blocking_connection.BlockingConnection,
                           '_create_connection',
                           return_value=impl_mock):
        conn = blocking_connection.BlockingConnection('params')

    anchor = _SettlementCoreConnection(
        on_open_error_callback=lambda _connection, _error: None,
        on_close_callback=lambda _connection, _error: None,
        internal_connection_workflow=False)
    impl_mock.callbacks = anchor.callbacks
    impl_mock._teardown_window = anchor._teardown_window
    anchor.callbacks.add(0, pika.connection.Connection.ON_CONNECTION_CLOSED,
                         conn._closed_result.set_value_once, False)
    impl_mock._get_write_buffer_size = mock.Mock(return_value=0)
    impl_mock._transport = mock.NonCallableMock(
        spec_set=nbio_interface.AbstractStreamTransport)
    impl_mock.ioloop.process_timeouts = mock.Mock()
    return conn, anchor


def _deliver_close(anchor, error):
    """Drive the on_close_callback completion through the real callback
    manager shared with the mocked impl.
    """
    anchor.callbacks.process(0, pika.connection.Connection.ON_CONNECTION_CLOSED,
                             anchor, anchor, error)


class BlockingTeardownSettlementTests(unittest.TestCase):
    """Nested teardown settlement semantics on the BlockingConnection host.
    """

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SettlementSelectConnectionTemplate,
                  is_open=True,
                  is_closed=False)
    def test_close_pumps_until_closed_result_ready(
            self, select_connection_class_mock):
        """close() must pump the ioloop until the on_close_callback
        completion lands in `_closed_result`, after closing open channels
        and calling `_impl.close` in that order.
        """
        impl_mock = select_connection_class_mock.return_value
        conn, anchor = _make_settlement_connection(impl_mock)

        blocking_channel = mock.Mock(is_open=True)
        impl_channel = mock.Mock()
        impl_channel._get_cookie.return_value = blocking_channel
        impl_mock._channels = {1: impl_channel}

        client_error = pika.exceptions.ConnectionClosedByClient(
            200, 'Normal shutdown')

        order = []
        blocking_channel.close.side_effect = (
            lambda *args: order.append('channel_close'))
        impl_mock.close.side_effect = lambda *args: order.append('impl_close')

        polls = {'count': 0}

        def poll_side_effect():
            polls['count'] += 1
            if polls['count'] == 2:
                _deliver_close(anchor, client_error)

        impl_mock.ioloop.poll.side_effect = poll_side_effect

        conn.close(200, 'Normal shutdown')

        # Pumps until close completion is observed, not merely buffer-flushed
        self.assertEqual(polls['count'], 2)
        # Channels are closed before the connection close is emitted
        self.assertEqual(order, ['channel_close', 'impl_close'])
        impl_mock.close.assert_called_once_with(200, 'Normal shutdown')
        # Outermost pump owns cleanup exactly once (note: cleanup resets the
        # bottom sink, so readiness is proven by the pump count above)
        self.assertEqual(impl_mock.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SettlementSelectConnectionTemplate,
                  is_open=False,
                  is_closed=False)
    def test_nested_window_inner_close_does_not_write_through_outer(
            self, select_connection_class_mock):
        """A close delivered inside a declared settlement window settles the
        inner sink; the outer sink and an unrelated pending wait are not
        drained, and the shared ioloop is not closed while the outer wait is
        still in scope.
        """
        impl_mock = select_connection_class_mock.return_value
        conn, anchor = _make_settlement_connection(impl_mock)

        broker_error = pika.exceptions.ConnectionClosedByBroker(
            320, 'CONNECTION_FORCED')
        outer_pending = blocking_connection._CallbackResult()

        with conn._settlement_window():
            inner_sink = conn._current_closed_result()
            self.assertIsNot(inner_sink, conn._closed_result)
            _deliver_close(anchor, broker_error)

            self.assertTrue(inner_sink.ready)
            # Outer close wait must not be drained by the inner teardown
            self.assertFalse(conn._closed_result.ready)
            # An unrelated pending wait must not be settled either
            self.assertFalse(outer_pending.ready)
            # Window scope must not close the shared ioloop
            self.assertEqual(impl_mock.ioloop.close.call_count, 0)

        # Window restores the outer sink and hands the observed close down
        self.assertIs(conn._current_closed_result(), conn._closed_result)
        self.assertEqual(conn._closed_result_sinks, [conn._closed_result])
        self.assertTrue(conn._closed_result.ready)
        self.assertIs(conn._closed_result.value.error, broker_error)
        self.assertFalse(outer_pending.ready)
        self.assertEqual(impl_mock.ioloop.close.call_count, 0)

        # The outer pump then settles against the handed-down error exactly
        # once, owning cleanup itself
        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker):
            conn._flush_output(lambda: False, lambda: False)
        self.assertEqual(impl_mock.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SettlementSelectConnectionTemplate,
                  is_open=False,
                  is_closed=False)
    def test_nested_window_client_close_hands_down_without_raising_in_window(
            self, select_connection_class_mock):
        """A client-initiated close inside a window hands down cleanly and
        the later outer pump returns normally (log+cleanup path), still
        exactly one cleanup.
        """
        impl_mock = select_connection_class_mock.return_value
        conn, anchor = _make_settlement_connection(impl_mock)

        client_error = pika.exceptions.ConnectionClosedByClient(
            200, 'Normal shutdown')

        with conn._settlement_window():
            _deliver_close(anchor, client_error)
            self.assertEqual(impl_mock.ioloop.close.call_count, 0)

        self.assertTrue(conn._closed_result.ready)
        # Outer pump sees a client close: no exception, cleanup once
        conn._flush_output()
        self.assertEqual(impl_mock.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SettlementSelectConnectionTemplate,
                  is_open=False,
                  is_closed=False)
    def test_reentrant_pump_inner_close_isolated_from_outer_pump(
            self, select_connection_class_mock):
        """An inner _flush_output pump (reentrant from an outer pump
        callback) must pump to its own inner sink, must not close the shared
        ioloop, and must hand the close down to the outer sink on exit.
        """
        impl_mock = select_connection_class_mock.return_value
        conn, anchor = _make_settlement_connection(impl_mock)

        broker_error = pika.exceptions.ConnectionClosedByBroker(
            541, 'INTERNAL_ERROR')

        def nested_pump():

            def inner_poll():
                _deliver_close(anchor, broker_error)

            impl_mock.ioloop.poll.side_effect = inner_poll
            with self.assertRaises(pika.exceptions.ConnectionClosedByBroker):
                conn._flush_output(lambda: False, lambda: False)
            # Inner pump is still nested: no shared-ioloop teardown
            self.assertEqual(impl_mock.ioloop.close.call_count, 0)
            # Outer sink untouched while the inner pump is unwinding
            self.assertTrue(conn._closed_result.ready)

        # Outer pump re-enters the pump from its first poll
        impl_mock.ioloop.poll.side_effect = lambda: nested_pump()
        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker):
            conn._flush_output(lambda: False, lambda: False)

        # Sink stack and pump depth are restored; the shared ioloop is torn
        # down exactly once, by the outermost pump after hand-down (in pin
        # the inner pump would have closed it out from under the outer one)
        self.assertEqual(conn._closed_result_sinks, [conn._closed_result])
        self.assertEqual(conn._pump_depth, 0)
        self.assertEqual(impl_mock.ioloop.close.call_count, 1)

    @patch.object(blocking_connection.select_connection,
                  'SelectConnection',
                  spec_set=SettlementSelectConnectionTemplate,
                  is_open=False,
                  is_closed=False)
    def test_pump_depth_resets_on_uncategorized_pump_exception(
            self, select_connection_class_mock):
        """An exception unrelated to connection close still unwinds the pump
        bookkeeping: depth zeroed and sink stack back at the bottom sink.
        """
        impl_mock = select_connection_class_mock.return_value
        conn, _anchor = _make_settlement_connection(impl_mock)

        impl_mock.ioloop.poll.side_effect = RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            conn._flush_output(lambda: False, lambda: False)

        self.assertEqual(conn._pump_depth, 0)
        self.assertEqual(conn._closed_result_sinks, [conn._closed_result])
        self.assertFalse(conn._closed_result.ready)
