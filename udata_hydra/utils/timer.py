import logging
import time

log = logging.getLogger("udata-hydra")


class Timer:
    """
    A simple Timer class for code execution time measurement as a debug log.

    ```
    timer = Timer("my-timer")
    timer.mark("a-step")
    timer.stop()
    ```
    """
    steps = []

    def __init__(self, name) -> None:
        self.name = name
        self.steps.append(time.perf_counter())

    def mark(self, step: str) -> None:
        t_mark = time.perf_counter()
        t_delta = t_mark - self.steps[-1]
        self.steps.append(t_mark)
        log.debug(f"[{self.name}] {step} done in {t_delta:0.4f}s")

    def stop(self) -> None:
        t_delta = time.perf_counter() - self.steps[0]
        log.debug(f"[{self.name}] Total time {t_delta:0.4f}s")
