"""MQTT plugin for protocol_proxy.

The proxy class is imported lazily so that running ``python -m protocol_proxy.protocol.mqtt.mqtt_proxy``
executes the module exactly once, and so that importing this package has no side effects.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .mqtt_proxy import MQTTProxy

__all__ = ['MQTTProxy', 'PROXY_CLASS']


def __getattr__(name: str):
    if name in ('MQTTProxy', 'PROXY_CLASS'):
        from .mqtt_proxy import MQTTProxy
        return MQTTProxy
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
