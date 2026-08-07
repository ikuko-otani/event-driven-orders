import pathlib
import time

HEARTBEAT_PATH = pathlib.Path("/tmp/heartbeat")
INTERVAL_SECONDS = 5


def run() -> None:
    while True:
        HEARTBEAT_PATH.touch()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
