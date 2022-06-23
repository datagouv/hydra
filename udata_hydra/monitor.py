try:
    import curses
except ImportError:
    pass
from collections import defaultdict


class Monitor:
    """curses-based monitor interface"""

    def __init__(self):
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.screen.keypad(1)
        # hide cursor
        curses.curs_set(0)
        try:
            curses.start_color()
        except:  # noqa
            pass
        self.backoffs = defaultdict(int)
        curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
        self.red = curses.color_pair(1)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
        self.green = curses.color_pair(2)
        curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        self.yellow = curses.color_pair(3)

    def teardown(self):
        self.screen.keypad(0)
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def listen(self):
        self.screen.getch()

    def clear_line(self, line):
        self.screen.move(line, 0)
        self.screen.clrtoeol()

    def init(self, **kwargs):
        self.screen.addstr(0, 0, "🦀 udata-hydra crawling...", curses.A_BOLD)
        self.screen.addstr(
            1, 0, ", ".join([f"{k}: {v}" for k, v in kwargs.items()])
        )
        self.screen.refresh()

    def set_status(self, status):
        START = 3
        self.clear_line(START)
        self.screen.addstr(START, 0, status, curses.A_REVERSE)
        self.screen.refresh()

    def refresh(self, results):
        # start line after init and status
        START = 5

        # TODO: use config vars from crawl.py
        total = sum(results.values())
        max_len = len(str(max(results.values())))
        for i, status in enumerate(
            [("ok", "green"), ("timeout", "yellow"), ("error", "red")]
        ):
            self.clear_line(i + START)
            self.screen.addstr(i + START, 0, f"{status[0]}:")
            nb = results[status[0]]
            self.screen.addstr(
                i + START, 10, str(nb), getattr(self, status[1])
            )
            rate = round(nb / total * 100, 1)
            self.screen.addstr(i + START, max_len + 11, f"({rate}%)")

        self.screen.refresh()

    def draw_backoffs(self):
        START = 9
        self.clear_line(START)
        # TODO: check max width
        backoffs = [f"{d} ({c}r)" for d, c in self.backoffs.items()]
        msg = f"backoffs: {', '.join(backoffs)}"
        self.screen.addstr(START, 0, msg)
        self.screen.refresh()

    def add_backoff(self, domain, nb_req):
        self.backoffs[domain] = nb_req
        self.draw_backoffs()

    def remove_backoff(self, domain):
        if domain in self.backoffs:
            self.backoffs.pop(domain)
            self.draw_backoffs()
