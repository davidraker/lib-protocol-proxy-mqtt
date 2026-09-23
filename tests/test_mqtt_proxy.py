import json
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import paho.mqtt.client as mqtt
import pytest

import protocol_proxy.protocol.mqtt as mqtt_package
from protocol_proxy.protocol.mqtt import mqtt_proxy as module
from protocol_proxy.protocol.mqtt.mqtt_proxy import MQTTProxy, launch_mqtt
from protocol_proxy.proxy.launch import proxy_command_parser


@pytest.fixture
def proxy():
    manager_id, manager_token = uuid4(), uuid4()
    with mock.patch.object(module.mqtt, 'Client') as client_class:
        client = client_class.return_value
        client.is_connected.return_value = True
        client.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
        client.unsubscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)
        p = MQTTProxy(proxy_id=uuid4(), token=uuid4(), proxy_name='test', manager_address='127.0.0.1',
                      manager_port=1, manager_id=manager_id, manager_token=manager_token, host='broker', port=1883)
    p.send = mock.Mock(return_value=True)
    p.headers = SimpleNamespace(sender_id=manager_id, sender_token=manager_token)
    return p


def call(proxy, handler, message: dict):
    """Invoke a @callback-decorated handler the way the IPC layer does."""
    return handler(proxy, proxy.headers, json.dumps(message).encode('utf8'))


def test_package_exposes_proxy_class_lazily():
    assert mqtt_package.PROXY_CLASS is MQTTProxy
    assert mqtt_package.MQTTProxy is MQTTProxy
    with pytest.raises(AttributeError):
        mqtt_package.nope


def test_callbacks_registered(proxy):
    assert {'PUBLISH_REMOTE', 'SUBSCRIBE_REMOTE', 'UNSUBSCRIBE_REMOTE'} <= set(proxy.callbacks)
    assert proxy.topic_delimiter() == '/'
    assert proxy.get_unique_remote_id(('mqtt', 'a', 1)) == ('mqtt', 'a', 1)


def test_publish_remote_encodes_payloads(proxy):
    call(proxy, proxy.handle_publish_remote, {'topic': 't/1', 'payload': {'v': 1}, 'qos': 1, 'retain': True})
    proxy.mqtt.publish.assert_called_with('t/1', b'{"v": 1}', qos=1, retain=True)
    call(proxy, proxy.handle_publish_remote, {'topic': 't/2', 'payload': 'text'})
    proxy.mqtt.publish.assert_called_with('t/2', b'text', qos=0, retain=False)
    call(proxy, proxy.handle_publish_remote, {'topic': 't/3'})
    proxy.mqtt.publish.assert_called_with('t/3', None, qos=0, retain=False)


def test_publish_remote_rejects_bad_requests(proxy):
    call(proxy, proxy.handle_publish_remote, {'payload': 1})
    proxy.handle_publish_remote(proxy, proxy.headers, b'not json')
    proxy.mqtt.publish.assert_not_called()


def test_unauthenticated_requests_are_ignored(proxy):
    bad_headers = SimpleNamespace(sender_id=proxy.headers.sender_id, sender_token=uuid4())
    proxy.handle_publish_remote(proxy, bad_headers, json.dumps({'topic': 't', 'payload': 1}).encode())
    proxy.mqtt.publish.assert_not_called()


def test_subscribe_remote_dedupes_and_accepts_all_entry_forms(proxy):
    call(proxy, proxy.handle_subscribe_remote, {'topics': ['a/#', ['b', 1], {'topic': 'c', 'qos': 2}, 5]})
    assert proxy.subscribed_topics == {'a/#': 0, 'b': 1, 'c': 2}
    proxy.mqtt.subscribe.assert_called_once_with([('a/#', 0), ('b', 1), ('c', 2)])
    call(proxy, proxy.handle_subscribe_remote, {'topics': ['a/#'], })    # already subscribed
    call(proxy, proxy.handle_subscribe_remote, {'topic': 'd'})            # single-topic form
    assert proxy.mqtt.subscribe.call_count == 2
    proxy.mqtt.subscribe.assert_called_with([('d', 0)])


def test_subscribe_deferred_until_connected(proxy):
    proxy.mqtt.is_connected.return_value = False
    call(proxy, proxy.handle_subscribe_remote, {'topics': ['x']})
    proxy.mqtt.subscribe.assert_not_called()
    proxy.on_connect(proxy.mqtt, None, None, SimpleNamespace(is_failure=False), None)
    proxy.mqtt.subscribe.assert_called_once_with([('x', 0)])


def test_on_connect_failure_does_not_subscribe(proxy):
    proxy.subscribed_topics = {'x': 0}
    proxy.on_connect(proxy.mqtt, None, None, SimpleNamespace(is_failure=True), None)
    proxy.mqtt.subscribe.assert_not_called()


def test_unsubscribe_remote(proxy):
    proxy.subscribed_topics = {'a': 0, 'b': 0}
    call(proxy, proxy.handle_unsubscribe_remote, {'topics': ['a', 'zzz']})
    assert proxy.subscribed_topics == {'b': 0}
    proxy.mqtt.unsubscribe.assert_called_once_with(['a'])


def test_on_message_forwards_to_manager_peer(proxy):
    msg = SimpleNamespace(topic='a/b', payload=b'{"x": 1}', qos=1, retain=False, mid=7)
    proxy.on_message(proxy.mqtt, None, msg)
    peer, message = proxy.send.call_args.args
    assert peer is proxy.peers[proxy.manager]
    assert message.method_name == 'PUBLISH_LOCAL'
    body = json.loads(message.payload)
    assert bytes.fromhex(body['payload']) == b'{"x": 1}'
    assert body['topic'] == 'a/b' and body['qos'] == 1 and body['mid'] == 7


def test_on_message_never_raises(proxy):
    proxy.send.side_effect = RuntimeError('boom')
    proxy.on_message(proxy.mqtt, None, SimpleNamespace(topic='t', payload=b'', qos=0, retain=False, mid=1))


def test_main_loop_reconnects_with_backoff(proxy):
    proxy.mqtt.socket.return_value = None
    proxy.mqtt.reconnect.side_effect = OSError('refused')
    proxy._reconnect_delay = proxy.reconnect_min_delay = 1.0
    proxy.reconnect_max_delay = 4.0
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 4:
            proxy._stop = True
    with mock.patch.object(module, 'sleep', fake_sleep):
        proxy.main_loop()
    proxy.mqtt.connect_async.assert_called_once_with('broker', 1883, 60, '', 0)
    assert sleeps == [1.0, 2.0, 4.0, 4.0]
    proxy.mqtt.disconnect.assert_not_called()    # never connected, nothing to disconnect


def test_main_loop_runs_network_loop_and_disconnects_on_stop(proxy):
    proxy.mqtt.socket.return_value = object()
    proxy.mqtt.loop.side_effect = lambda timeout: setattr(proxy, '_stop', True) or mqtt.MQTT_ERR_SUCCESS
    proxy.main_loop()
    proxy.mqtt.reconnect.assert_not_called()
    proxy.mqtt.disconnect.assert_called_once()


def test_launch_parser_accepts_manager_style_values():
    parser, runner = launch_mqtt(proxy_command_parser())
    opts = parser.parse_args(['--proxy-id', uuid4().hex, '--proxy-name', 'n', '--manager-id', uuid4().hex,
                              '--host', 'h', '--tls', 'True', '--qos', '1', '--protocol', 'MQTTv5',
                              '--password', 'secret'])
    assert opts.tls is True and opts.qos == 1 and opts.protocol == 'MQTTv5' and opts.host == 'h'
    assert runner is module.run_proxy


def test_bad_protocol_rejected():
    with pytest.raises(ValueError):
        MQTTProxy(proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1', manager_port=1,
                  manager_id=uuid4(), manager_token=uuid4(), protocol='MQTTv9')
