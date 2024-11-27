import asyncio

from udata_hydra import context
from udata_hydra.crawl.start_checks import start_checks
from udata_hydra.utils import queue  # noqa


def run() -> None:
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        asyncio.get_event_loop().run_until_complete(start_checks())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
