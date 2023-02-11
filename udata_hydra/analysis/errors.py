class ParseException(Exception):

    def __init__(self, step, *args: object) -> None:
        self.step = step
        super().__init__(*args)
