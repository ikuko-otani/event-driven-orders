"""order-api's consumer process, apart from the HTTP service (design §5.2, §6.2).

Everything specific to this service is decided here — the topic it reads, the
group it reads as, its dedup ledger and its handler — so the loop in
common.consumer stays free of anything order-api knows.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.consumer import ConsumerConfig, run_forever
from common.kafka import build_consumer, build_producer
from common.settings import DatabaseSettings, KafkaSettings
from order_api.handlers import handle_inventory_event
from order_api.models import ProcessedEvent

# group_id doubles as the processed_events consumer_name (design §3.2): the
# ledger keys on the logical consumer, so a redelivery landing on a different
# instance after a rebalance is still recognised as a duplicate.
CONFIG = ConsumerConfig(topic="inventory.events", group_id="order-api")


def run() -> None:
    """Consume inventory.events until the process is stopped.

    The engine is synchronous even though this service's HTTP side is not: the
    Kafka client blocks, so an async session here would buy nothing and would
    leave two engine configurations to keep in agreement (design §5.2).
    """
    engine = create_engine(DatabaseSettings().sync_url)
    run_forever(
        sessionmaker(engine),
        consumer=build_consumer(KafkaSettings(), CONFIG),
        producer=build_producer(KafkaSettings()),
        processed_events=ProcessedEvent,
        handler=handle_inventory_event,
        config=CONFIG,
    )


if __name__ == "__main__":
    run()
