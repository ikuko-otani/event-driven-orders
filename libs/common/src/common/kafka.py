"""The real Kafka clients, assembled apart from the loops that use them.

confluent_kafka is imported here and nowhere else: common.poller and
common.consumer depend only on their own protocols, so a test can hand a loop
a fake without either side knowing about the other (design §5.2, §5.4).
"""

from confluent_kafka import Consumer, KafkaError, Producer

from common.consumer import ConsumerConfig
from common.settings import KafkaSettings

# Delivery failures that will fail again however often the row is retried: the
# message is malformed or too large, or the topic is not ours to write to.
# Everything else — a timeout, a dropped connection, a broker that is simply
# down — is transient and belongs to the poller's attempt counting (design §5.4).
# The list lives here because this is the only module that may know the client's
# constants, and the loop is handed the plain integers.
PERMANENT_DELIVERY_ERRORS: frozenset[int] = frozenset(
    {
        KafkaError.MSG_SIZE_TOO_LARGE,
        KafkaError.INVALID_MSG,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._VALUE_SERIALIZATION,
        KafkaError._KEY_SERIALIZATION,
    }
)


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
