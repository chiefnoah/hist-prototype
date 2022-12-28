
from pathlib import Path
from typing import Union
from .storage import IOHandler

class HistorySearcher:
    iohandler: IOHandler

    def __init__(self, path: Union[str, Path]) -> None:
        self.iohandler = IOHandler(open(path, "r+b"))