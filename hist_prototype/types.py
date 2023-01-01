from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Tuple, TypeVar, Type
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
        **kwargs: Dict[str, Any],
    ) -> None:
        super().__init__(msg, *args, **kwargs)


# Message-passing request types


@dataclass(frozen=True)
class HistoryReadRequest:
    """A request to read a serialized HistoryRecord from disk."""

    offset: int
    tx: int
    SIZE = ((16 + 8) * MAX_CHILDREN) + 2


@dataclass(frozen=True)
class WriteRequest:
    """A request to write a serialized LeafNode to disk."""

    offset: Optional[int]
    delete: bool
    value: bytes
    tx: int  # u128


# Disk-representation types

DataPair = Tuple[TX, Offset]
CHILD_SIZE: int = 24


@dataclass(frozen=True)
class HistoryIndexNode(Serializable):
    # Depth is 0 if this is a leaf node (and it's children are TX, Offset pairs)
    # if > 0, then it's an intermediate node and children are HistoryIndexNodes
    depth: int
    children: List[DataPair]

    def __post_init__(self: "HistoryIndexNode") -> None:
        if len(self.children) != MAX_CHILDREN:
            raise ValueError(f"Must have exactly {MAX_CHILDREN} children")
        assert all(isinstance(child, tuple) for child in self.children)

    def max_key(self: "HistoryIndexNode") -> TX:
        """Returns the maximum TX value this node contains."""
        return self.children[-1][0]

    def serialize(self) -> bytes:
        #                  |TX  |Offset              | depth (u16)
        buf = bytearray(((16 + 8) * MAX_CHILDREN) + 2)
        buf[0:2] = self.depth.to_bytes(2, "little")
        for i, child in enumerate(self.children):
            o = (i * CHILD_SIZE) + 2
            buf[o : o + CHILD_SIZE] = child[0].to_bytes(16, "little") + child[
                1
            ].to_bytes(8, "little")
        return bytes(buf)

    @classmethod
    def deserialize(cls: "Type[HistoryIndexNode]", buf: bytes) -> "HistoryIndexNode":
        depth = int.from_bytes(buf[0:2], "little")
        children: List[DataPair] = []
        for i in range(MAX_CHILDREN):
            o: Offset = (i * CHILD_SIZE) + 2
            tx: TX = int.from_bytes(buf[o : o + 16], "little")
            o = int.from_bytes(
                buf[o + 16 : o + CHILD_SIZE], "little"
            )
            children.append((tx, o))
        return HistoryIndexNode(depth, children)
