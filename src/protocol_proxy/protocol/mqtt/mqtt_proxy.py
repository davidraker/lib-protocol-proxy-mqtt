"""MQTT protocol proxy.

Runs as a subprocess launched by a ProtocolProxyManager and bridges an MQTT broker to the
manager's IPC protocol:

  broker -> manager:  every received MQTT message is forwarded as ``PUBLISH_LOCAL`` with
                      ``{'topic', 'payload' (hex), 'qos', 'retain', 'mid'}``.
  manager -> broker:  ``PUBLISH_REMOTE`` ``{'topic', 'payload', 'qos'?, 'retain'?}``,
                      ``SUBSCRIBE_REMOTE`` / ``UNSUBSCRIBE_REMOTE`` ``{'topics': [...]}`` where each
                      entry is a topic filter string, a ``[topic, qos]`` pair, or ``{'topic', 'qos'}``.
"""
import sys

import json
import logging

from argparse import ArgumentParser
from typing import Any, Callable

import paho.mqtt.client as mqtt
from gevent import sleep

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.proxy.gevent import GeventProtocolProxy
from protocol_proxy.proxy.launch import launch, redact, str2bool

_log = logging.getLogger(__name__)

PROTOCOL_VERSIONS = {'MQTTv31': mqtt.MQTTv31, 'MQTTv311': mqtt.MQTTv311, 'MQTTv5': mqtt.MQTTv5}
TopicList = list[tuple[str, int]]


class MQTTProxy(GeventProtocolProxy):
    LAUNCHER = 'launch_mqtt'

    def __init__(self, *, host: str = 'localhost', port: int = 1883, keepalive: int = 60, bind_address: str = '',
                 bind_port: int = 0, client_id: str = '', username: str | None = None, password: str | None = None,
                 tls: bool = False, protocol: str = 'MQTTv311', qos: int = 0,
                 reconnect_min_delay: float = 1.0, reconnect_max_delay: float = 60.0, **kwargs):
        super().__init__(**kwargs)
        self.host, self.port, self.keepalive = host, port, keepalive
        self.bind_address, self.bind_port = bind_address, bind_port
        self.default_qos = qos
        self.reconnect_min_delay, self.reconnect_max_delay = reconnect_min_delay, reconnect_max_delay
        self._reconnect_delay = reconnect_min_delay
        self.subscribed_topics: dict[str, int] = {}    # topic filter -> qos

        self.register_callback(self.handle_publish_remote, 'PUBLISH_REMOTE')
        self.register_callback(self.handle_subscribe_remote, 'SUBSCRIBE_REMOTE')
        self.register_callback(self.handle_unsubscribe_remote, 'UNSUBSCRIBE_REMOTE')

        self.mqtt = self._create_client(client_id, username, password, tls, protocol)

    def _create_client(self, client_id: str, username: str | None, password: str | None, tls: bool,
                       protocol: str) -> mqtt.Client:
        if protocol not in PROTOCOL_VERSIONS:
            raise ValueError(f'Unknown MQTT protocol version "{protocol}". Choose from {list(PROTOCOL_VERSIONS)}.')
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                             protocol=PROTOCOL_VERSIONS[protocol])
        if username:
            client.username_pw_set(username, password)
        if tls:
            client.tls_set()
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message
        return client

    @staticmethod
    def topic_delimiter() -> str:
        return '/'

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        return unique_remote_id

    ##################
    # Broker connection
    ##################

    def main_loop(self):
        """Drive the paho network loop, reconnecting with exponential backoff while not stopped."""
        self.mqtt.connect_async(self.host, self.port, self.keepalive, self.bind_address, self.bind_port)
        try:
            while not self._stop:
                if self.mqtt.socket() is None:
                    try:
                        self.mqtt.reconnect()
                    except (OSError, ValueError) as e:
                        _log.warning(f'{self.proxy_name}: Unable to connect to MQTT broker @ {self.host}:{self.port}'
                                     f' ({e}). Retrying in {self._reconnect_delay:.0f}s.')
                        sleep(self._reconnect_delay)
                        self._reconnect_delay = min(self._reconnect_delay * 2, self.reconnect_max_delay)
                        continue
                rc = self.mqtt.loop(timeout=0.1)
                if rc != mqtt.MQTT_ERR_SUCCESS:
                    _log.debug(f'{self.proxy_name}: MQTT network loop returned {mqtt.error_string(rc)}')
                    sleep(0.1)
        finally:
            if self.mqtt.socket() is not None:
                self.mqtt.disconnect()
                self.mqtt.loop(timeout=0.1)

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any):
        """Restore subscriptions after every (re)connection."""
        if getattr(reason_code, 'is_failure', False):
            _log.warning(f'{self.proxy_name}: Broker @ {self.host}:{self.port} refused connection: {reason_code}')
            return
        self._reconnect_delay = self.reconnect_min_delay
        _log.info(f'{self.proxy_name}: Connected to MQTT broker @ {self.host}:{self.port}.')
        if self.subscribed_topics:
            self._subscribe(list(self.subscribed_topics.items()))

    def on_disconnect(self, client: mqtt.Client, userdata: Any, disconnect_flags: Any, reason_code: Any,
                      properties: Any):
        level = logging.INFO if self._stop else logging.WARNING
        _log.log(level, f'{self.proxy_name}: Disconnected from MQTT broker @ {self.host}:{self.port}: {reason_code}')

    def on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        """Forward a broker message to the manager. Must never raise into paho's network loop."""
        try:
            message = ProtocolProxyMessage(
                method_name='PUBLISH_LOCAL',
                payload=json.dumps({'topic': msg.topic, 'payload': msg.payload.hex(), 'qos': msg.qos,
                                    'retain': msg.retain, 'mid': msg.mid}).encode('utf8'))
            if not self.send(self.peers[self.manager], message):
                _log.warning(f'{self.proxy_name}: Unable to forward message on "{msg.topic}" to the manager.')
        except Exception as e:
            _log.warning(f'{self.proxy_name}: Error forwarding message on "{msg.topic}": {e!r}')

    ##################
    # Manager requests
    ##################

    def _decode(self, raw_message: bytes) -> dict | None:
        try:
            message = json.loads(raw_message.decode('utf8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            _log.warning(f'{self.proxy_name}: Received undecodable request: {e}')
            return None
        if not isinstance(message, dict):
            _log.warning(f'{self.proxy_name}: Expected a JSON object, got {type(message).__name__}.')
            return None
        return message

    @staticmethod
    def _encode_payload(payload: Any) -> bytes | None:
        if payload is None or isinstance(payload, (bytes, bytearray)):
            return payload
        if isinstance(payload, str):
            return payload.encode('utf8')
        return json.dumps(payload).encode('utf8')

    def _parse_topics(self, message: dict) -> TopicList:
        topics = message.get('topics') or ([message['topic']] if message.get('topic') else [])
        if isinstance(topics, str):
            topics = [topics]
        parsed: TopicList = []
        for entry in topics:
            if isinstance(entry, str):
                parsed.append((entry, self.default_qos))
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                parsed.append((str(entry[0]), int(entry[1])))
            elif isinstance(entry, dict) and entry.get('topic'):
                parsed.append((str(entry['topic']), int(entry.get('qos', self.default_qos))))
            else:
                _log.warning(f'{self.proxy_name}: Ignoring malformed topic entry: {entry!r}')
        return parsed

    def _subscribe(self, topics: TopicList):
        rc, _ = self.mqtt.subscribe(topics)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            _log.warning(f'{self.proxy_name}: Subscribe to {topics} failed: {mqtt.error_string(rc)}')

    @callback
    def handle_publish_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        if not (topic := message.get('topic')):
            _log.warning(f'{self.proxy_name}: PUBLISH_REMOTE without a topic: {message}')
            return
        qos = int(message.get('qos', self.default_qos))
        retain = bool(message.get('retain', False))
        info = self.mqtt.publish(topic, self._encode_payload(message.get('payload')), qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _log.warning(f'{self.proxy_name}: Publish to "{topic}" failed: {mqtt.error_string(info.rc)}')

    @callback
    def handle_subscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        new_topics = [(t, q) for t, q in self._parse_topics(message) if self.subscribed_topics.get(t) != q]
        self.subscribed_topics.update(new_topics)
        if new_topics and self.mqtt.is_connected():
            self._subscribe(new_topics)    # Otherwise on_connect subscribes once the broker is reachable.

    @callback
    def handle_unsubscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        if (message := self._decode(raw_message)) is None:
            return
        removed = [t for t, _ in self._parse_topics(message) if self.subscribed_topics.pop(t, None) is not None]
        if removed and self.mqtt.is_connected():
            rc, _ = self.mqtt.unsubscribe(removed)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                _log.warning(f'{self.proxy_name}: Unsubscribe from {removed} failed: {mqtt.error_string(rc)}')


def run_proxy(**kwargs) -> int:
    _log.info(f'Launching MQTT Proxy using parameters: {redact(kwargs)}.')
    return MQTTProxy(**kwargs).run()


def launch_mqtt(parser: ArgumentParser) -> tuple[ArgumentParser, Callable]:
    parser.add_argument('--host', type=str, default='localhost', help='Address of the MQTT broker.')
    parser.add_argument('--port', type=int, default=1883, help='Port of the MQTT broker.')
    parser.add_argument('--keepalive', type=int, default=60,
                        help='Maximum period in seconds between communications with the broker.')
    parser.add_argument('--bind-address', type=str, default='',
                        help='Local network interface to bind the client socket to.')
    parser.add_argument('--bind-port', type=int, default=0, help='Local port to bind the client socket to.')
    parser.add_argument('--client-id', type=str, default='',
                        help='MQTT client id. Generated by the broker if empty.')
    parser.add_argument('--username', type=str, default=None, help='Username for broker authentication.')
    parser.add_argument('--password', type=str, default=None, help='Password for broker authentication.')
    parser.add_argument('--tls', type=str2bool, default=False,
                        help='Use TLS with the system CA certificates (true/false).')
    parser.add_argument('--protocol', type=str, default='MQTTv311', choices=list(PROTOCOL_VERSIONS),
                        help='MQTT protocol version.')
    parser.add_argument('--qos', type=int, default=0, choices=[0, 1, 2],
                        help='Default QoS for subscriptions and publications.')
    parser.add_argument('--reconnect-min-delay', type=float, default=1.0,
                        help='Initial delay in seconds between reconnection attempts.')
    parser.add_argument('--reconnect-max-delay', type=float, default=60.0,
                        help='Maximum delay in seconds between reconnection attempts.')
    return parser, run_proxy

