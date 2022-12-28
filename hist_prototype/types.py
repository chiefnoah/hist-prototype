from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple, TypeVar, Union, cast
from .constants import MAX_CHILDREN

class Serializable(Protocol):
    def serialize(self) -> bytes:
        ...


class Splittable(Protocol):
    """A type that can split itself into 2."""
    def split(self) -> "Splittable":
        ...


K = TypeVar("K", bound=Serializable)
V = TypeVar("V", bound=Serializable)

TX = int
Offset = int

# Helper types


@dataclass(frozen=True)
class Bytes:
    inner: bytes

    def serialize(self) -> bytes:
        return self.inner


# Errors
class NodeFullError(RuntimeError):
    def __init__(
        self,
        msg: str = "Node is full, cannot insert another node",
        *args: Tuple[Any],
        **kwargs: Dict[str, Any]
    ) -> None:
        super().__init__(msg, *args, **kwargs)

# Disk-representation types

DataPair = Tuple[TX, Offset]
CHILD_SIZE: int = 24
@dataclass(frozen=True)
class HistoryIndexNode(Serializable):
    # Depth is 0 if this is a leaf node (and it's children are TX, Offset pairs)
    # if > 0, then it's an intermediate node and children are HistoryIndexNodes
    depth: int
    children: List["HistoryIndexNode" | DataPair]

    def __post_init__(self: "HistoryIndexNode") -> None:
        assert len(self.children) == MAX_CHILDREN
        if self.depth == 0:
            assert all(isinstance(child, tuple) for child in self.children)
        else:
            assert all(isinstance(child, HistoryIndexNode) for child in self.children)

    def max_key(self: "HistoryIndexNode") -> TX:
        """Returns the maximum TX value this node contains."""
        if self.depth == 0:
            return cast(DataPair, self.children)[1]
        return cast("HistoryIndexNode", self.children[-1]).max_key()

    def serialize(self) -> bytes:
        #                  |TX  |Ofset              | depth (u16)
        buf = bytearray(((16 + 8) * MAX_CHILDREN) + 2)
        buf[0:2] = self.depth.to_bytes(2, "little")
        for i, child in enumerate(self.children):
            offset = (i*CHILD_SIZE) + 2
            if self.depth > 0:
                buf[offset:offset+CHILD_SIZE] = cast(HistoryIndexNode, child).serialize()
            else:
                child = cast(DataPair, child)
                buf[offset:offset+CHILD_SIZE] = child[0].to_bytes(16, "little") + child[1].to_bytes(8, "little")
        return buf