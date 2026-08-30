"""order-api's outbox poller, a process of its own (design §5.4, §6.2).

Everything specific to this service is decided here — its outbox table, its
topic, and connections it never shares with the API — so the loop in
common.poller stays free of anything order-api knows.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common.kafka import build_producer
from common.poller import PollerConfig, run_forever
from common.settings import DatabaseSettings, KafkaSettings
from order_api.models import Outbox

TOPIC = "orders.events"


def run() -> None:
    """Publish this service's outbox rows until the process is stopped."""
    engine = create_engine(DatabaseSettings().sync_url)
    run_forever(
        sessionmaker(engine),
        outbox=Outbox,
        producer=build_producer(KafkaSettings()),
        config=PollerConfig(topic=TOPIC),
    )


if __name__ == "__main__":
    run()
