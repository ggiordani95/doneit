"""Messaging: the MessageBus protocol and its Redpanda adapter.

Application code depends on ``MessageBus`` only. The adapter speaks the Kafka
protocol, so the same code runs against Redpanda, Apache Kafka or a managed
Kafka service with no change beyond configuration.
"""

from swarm_messaging.bus import MessageBus, MessageHandler, MessageMeta
from swarm_messaging.redpanda import RedpandaMessageBus

__all__ = [
    "MessageBus",
    "MessageHandler",
    "MessageMeta",
    "RedpandaMessageBus",
]
