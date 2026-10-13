"""order-api's outbox poller, a process of its own (design §5.4, §6.2).

Everything specific to this service is decided here — its outbox table, its
topic, and connections it never shares with the API — so the loop in
common.poller stays free of anything order-api knows.
"""

from sqlalchemy.orm import sessionmaker

from common.db import make_sync_engine
from common.kafka import PERMANENT_DELIVERY_ERRORS, build_producer
from common.observability import configure
from common.poller import PollerConfig, run_forever
from common.settings import KafkaSettings
from common.shutdown import stop_on_sigterm
from order_api.models import Outbox

TOPIC = "orders.events"


def run() -> None:
    """Publish this service's outbox rows until the process is stopped."""
    configure("order-api-poller")
    # A container stop is answered by finishing the unit in hand, not by a kill
    stop = stop_on_sigterm()
    engine = make_sync_engine()
    run_forever(
        sessionmaker(engine),
        outbox=Outbox,
        producer=build_producer(KafkaSettings()),
        config=PollerConfig(topic=TOPIC, permanent_errors=PERMANENT_DELIVERY_ERRORS),
        stop=stop,
    )


if __name__ == "__main__":
    run()
