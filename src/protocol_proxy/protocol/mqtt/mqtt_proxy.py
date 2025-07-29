from gevent import monkey
monkey.patch_all(thread=False)

import json
import logging
import paho.mqtt.client as mqtt
import sys

from argparse import ArgumentParser
from gevent import joinall, sleep, spawn
from typing import Type
from uuid import UUID

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.proxy import launch
from protocol_proxy.proxy.gevent import GeventProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class MQTTProxy(GeventProtocolProxy):
    def __init__(self, manager_address, manager_port, manager_id: UUID, manager_token: UUID, token: UUID, proxy_id: UUID,
                 host: str, port: int = 1883, keepalive: int = 60, bind_address: str = '',
                 bind_port: int = 0, **kwargs):
        _log.debug(f'IN MQTT PROXY, BEFORE SUPER')
        super(MQTTProxy, self).__init__(manager_address=manager_address, manager_port=manager_port,
                                        manager_id=manager_id, manager_token=manager_token,
                                        token=token, proxy_id=proxy_id, **kwargs)
        _log.debug(f'{self.proxy_name} IN MQTT PROXY AFTER SUPER')
        self.subscribed_topics: list[tuple[str, int]] = []  # Use [("$SYS/#", 0)] to test MQTT communication.

        self.register_callback(self.handle_publish_remote, 'PUBLISH_REMOTE')
        self.register_callback(self.handle_subscribe_remote, 'SUBSCRIBE_REMOTE')
        # _log.debug(f'{self.proxy_name} REGISTERED CALLBACKS, ABOUT TO CREATE CLIENT')
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message
        try:  # TODO: Proxy registration fails if the MQTT connection takes too long or fails. Fix this.
            _log.debug(
                f'MQTTProxy {self.proxy_name}: GOING TO RUN -- "mqtt.connect({host}, {port}, {keepalive}, {bind_address}, {bind_port})')
            success = self.mqtt.connect(host=host, port=port, keepalive=keepalive,
                                        bind_address=bind_address, bind_port=bind_port, )
            # _log.debug(f'{self.proxy_name} MQTT PROXY: AFTER CONNECTING')
        except (OSError, Exception) as e:
            _log.warning(f'Error connecting to MQTT broker @ {host}:{port} --- {e}')
        else:
            if success != 0:
                _log.warning(f'MQTTProxy {self.proxy_name}: Connection to broker @ {host}:{port} returned code {success}')
        # _log.debug(f'{self.proxy_name}: JUST BEFORE JOINALL')
        joinall([spawn(self.main_loop), spawn(self.select_loop)])

    def main_loop(self):
        while not self._stop:
            #_log.debug(f'{self.proxy_name}: IN MAIN LOOP')
            self.mqtt.loop()
            sleep(0.1)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        """The callback for when the client has successfully connected to the MQTT broker."""
        if self.subscribed_topics:
            client.subscribe(self.subscribed_topics)

    def on_message(self, client, userdata, msg):
        """The callback for when a PUBLISH message is received from the MQTT broker."""
        message = ProtocolProxyMessage(
            method_name='PUBLISH_LOCAL',
            payload=json.dumps({
                'topic': msg.topic,
                'mid': msg.mid,
                'qos': msg.qos,
                'payload': msg.payload.hex(),
                'timestamp': msg.timestamp
            }).encode('utf8')
        )
        self.send(self.peers[self.manager].socket_params, message)

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        return unique_remote_id

    @callback
    def handle_publish_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        _log.debug(f'MQTTProxy {self.proxy_name}: IN HANDLE PUBLISH REMOTE!')

    @callback
    def handle_subscribe_remote(self, headers: ProtocolHeaders, raw_message: bytes):
        _log.debug(f'MQTTProxy {self.proxy_name}: IN HANDLE SUBSCRIBE REMOTE!')
        message = json.loads(raw_message.decode('utf8'))
        self.subscribed_topics.extend([(topic, 0) for topic in message.get('topics')])
        self.mqtt.subscribe(self.subscribed_topics)


async def run_proxy(**kwargs):
    mp = MQTTProxy(**kwargs)
    await mp.start()

def launch_mqtt(parser: ArgumentParser) -> (ArgumentParser, Type[GeventProtocolProxy]):
    # _log.debug(f'IN LAUNCH MQTT')
    parser.add_argument('--host', type=str, default='test.mosquitto.org',
                                 help='Address of the MQTT broker.')
    parser.add_argument('--port', type=int, default=1883,
                                 help='Port of the MQTT broker.')
    parser.add_argument('--keepalive', type=int, default=60,
                                 help='Thing to print for testing.')
    parser.add_argument('--bind-address', type=str, default='',
                                 help='Maximum period in seconds between communications with the broker.'
                                      ' If no other messages are being exchanged, this controls the rate'
                                      ' at which the client will send ping messages to the broker.')
    parser.add_argument('--bind-port', type=int, default=0)
    # TODO: This can be bool or Literal, but argparse objects to that.
    # parser_concrete.add_argument('--clean_start', type=int, default=3,
    #                              help='Sets the MQTT v5.0 clean_start flag always, never or on the first'
    #                                   ' successful connect only, respectively. ')
    # parser_concrete.add_argument('--properties', type=?, ) #TODO: How/whether to do the properties argument?
    return parser, run_proxy

if __name__ == '__main__':
    sys.exit(launch(launch_mqtt))