
from .storage import IOHandler
from .types import HistoryReadRequest, HistoryIndexNode
from typing import Optional

class HistorySearcher:
    io: IOHandler

    def __init__(self, iohandler: IOHandler) -> None:
        self.io = iohandler

    def search(self: "HistorySearcher",
        request: HistoryReadRequest
    ) -> Optional[int]:
        """Searches the history index for the tx and returns the corresponding data offset."""
        buf = self.io.read(request.offset, request.SIZE)
        node = HistoryIndexNode.deserialize(buf)
        while node.depth > 0:
            # TODO: binary search
            # Children MUST be sorted in ascending order
            for child in node.children:
                if child[0] > request.tx:
                    offset = child[1]
                    buf = self.io.read(offset, request.SIZE)
                    node = HistoryIndexNode.deserialize(buf)
                    break
        if node.depth > 0:
            return None
        # Search this "leaf" node for the last offset value that is before the requested tx
        for i, child in enumerate(node.children):
            if child[0] > request.tx:
                return node.children[i-1][1]