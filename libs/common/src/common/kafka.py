"""The real Kafka producer, assembled apart from the loop that uses it.

confluent_kafka is imported here and nowhere else: common.poller depends only
on its own Producer protocol, so a test can hand the loop a fake without
either side knowing about the other (design §5.4).
"""

from confluent_kafka import Producer

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
