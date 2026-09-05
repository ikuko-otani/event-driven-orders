"""The real Kafka clients, assembled apart from the loops that use them.

confluent_kafka is imported here and nowhere else: common.poller and
common.consumer depend only on their own protocols, so a test can hand a loop
a fake without either side knowing about the other (design §5.2, §5.4).
"""

from confluent_kafka import Consumer, Producer

from common.consumer import ConsumerConfig
from common.settings import KafkaSettings


def build_producer(settings: KafkaSettings) -> Producer:
    """Assemble the producer the design requires.

    enable.idempotence is a correctness setting, not a tuning one: it implies
    acks=all, so a send is reported delivered only once the broker has stored
    it. Without it the poller would mark rows published that nobody received.
    """
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "enable.idempotence": True,
            "message.timeout.ms": settings.message_timeout_ms,
        }
    )


def build_consumer(settings: KafkaSettings, config: ConsumerConfig) -> Consumer:
    """Assemble the consumer the design requires.

    Auto-commit is off because the offset must follow the database
    transaction, never a timer (design §5.7). cooperative-sticky moves only
    the partitions that change owner during a rebalance (ADR-001), and
    earliest keeps an event published before this group first subscribed.
    """
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": config.group_id,
            "enable.auto.commit": False,
            "partition.assignment.strategy": "cooperative-sticky",
            "auto.offset.reset": "earliest",
        }
    )
