"""Launch a real MQTTProxy subprocess against a GeventProtocolProxyManager.

Registration with the manager must succeed even though no broker is reachable (port 1 on localhost).
"""
import json
from uuid import uuid4

import pytest
from gevent import sleep, spawn

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.manager.gevent import GeventProtocolProxyManager
from protocol_proxy.protocol.mqtt import MQTTProxy


def test_proxy_registers_without_broker():
    manager = GeventProtocolProxyManager.get_manager(MQTTProxy)
    manager.start()
    loop = spawn(manager.select_loop)
    try:
        unique_remote_id = ('mqtt', '127.0.0.1', 1, uuid4().hex)
        peer = manager.get_proxy(unique_remote_id, host='127.0.0.1', port=1, reconnect_min_delay=0.5)
        manager.wait_peer_registered(peer, 30)
        assert peer.socket_params is not None, 'proxy never registered with the manager'
        assert peer.process.poll() is None, 'proxy process exited'
        # A request to the proxy is accepted (it is queued for the broker); the process stays healthy.
        sent = manager.send(remote=peer, message=ProtocolProxyMessage(
            method_name='SUBSCRIBE_REMOTE', payload=json.dumps({'topics': ['a/#']}).encode('utf8')))
        assert sent is True
        sleep(1)
        assert peer.process.poll() is None
    finally:
        manager.stop()
        loop.join(timeout=2)
        for p in list(manager.peers.values()):
            if p.process is not None and p.process.poll() is None:
                p.process.terminate()
