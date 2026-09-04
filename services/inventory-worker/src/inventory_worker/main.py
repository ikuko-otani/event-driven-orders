"""inventory-worker's consumer process (design §5.2, §6.2).

Everything specific to this service is decided here — the topic it reads, the
group it reads as, its dedup ledger and its handler — so the loop in
common.consumer stays free of anything inventory-worker knows.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.consumer import ConsumerConfig, run_forever
from common.kafka import build_consumer
from common.settings import DatabaseSettings, KafkaSettings
from inventory_worker.handlers import handle_order_confirmed
from inventory_worker.models import ProcessedEvent

# group_id doubles as the processed_events consumer_name (design §3.2): the
# ledger keys on the logical consumer, so a redelivery landing on a different
# instance after a rebalance is still recognised as a duplicate.
CONFIG = ConsumerConfig(topic="orders.events", group_id="inventory-worker")


def run() -> None:
    """Consume orders.events until the process is stopped.

    The loop is handed a session factory rather than a session: it opens one
    per message, so a message that fails rolls back on its own and its
    redelivery starts from a clean transaction (design §5.2).
    """
    engine = create_engine(DatabaseSettings().sync_url)
    run_forever(
        sessionmaker(engine),
        consumer=build_consumer(KafkaSettings(), CONFIG),
        processed_events=ProcessedEvent,
        handler=handle_order_confirmed,
        config=CONFIG,
    )


if __name__ == "__main__":
    run()
