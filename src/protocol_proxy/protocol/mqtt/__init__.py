"""MQTT plugin for protocol_proxy.

Proxies are launched with ``python -m protocol_proxy.proxy --gevent <module>:<class>`` (see
protocol_proxy.proxy.launch), which monkey-patches the proxy process before this module is imported.
"""
from .mqtt_proxy import MQTTProxy, launch_mqtt, run_proxy

__all__ = ['MQTTProxy', 'PROXY_CLASS', 'launch_mqtt', 'run_proxy']

PROXY_CLASS = MQTTProxy
