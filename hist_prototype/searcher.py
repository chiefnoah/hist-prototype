
from .storage import IOHandler
from typing import Optional

class HistorySearcher:
    io: IOHandler

    def __init__(self, iohandler: IOHandler) -> None:
        self.io = iohandler

    def search(self: "HistorySearcher", offset: int, search_key: bytes) -> Optional[int]:
        """Searches the history index for the given key and returns the corresponding data offset."""
