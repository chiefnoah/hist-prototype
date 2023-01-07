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
    value_size: int
    SIZE = ((16 + 8) * MAX_CHILDREN) + 2


@dataclass(frozen=True)
class WriteRequest:
    """A request to write a serialized LeafNode to disk."""

    offset: Optional[int]  # u64, 8 bytes
    delete: bool  # 1 byte
    value: bytes
    tx: int  # u129, 16 bytes

# Disk-representation types

# Type alias for TX
TX = int
# Can be either the actual value, or a pointer to the value + the values size on disk
# Size must be exactly equal to VALUE_SIZE
Value = bytes

DataTuple = Tuple[TX, Value]

# The size of the value field in bytes
# May contain the real value if the type is small enough
VALUE_SIZE: int = 12  # 12 bytes to allow for 64-bit offset and optionally 32-bit size
CHILD_SIZE: int = 16 + VALUE_SIZE


@dataclass(frozen=True)
class HistoryIndexNode(Serializable):
    # Depth is 0 if this is a leaf node (and it's children are TX, Offset pairs)
    # if > 0, then it's an intermediate node and children are HistoryIndexNodes
    depth: int
    children: List[DataTuple]

    def __post_init__(self: "HistoryIndexNode") -> None:
        if len(self.children) != MAX_CHILDREN:
            raise ValueError(f"Must have exactly {MAX_CHILDREN} children")
        assert all(isinstance(child, tuple) for child in self.children)
        for child in self.children:
            assert isinstance(child[0], int)
            assert isinstance(child[1], bytes)
            assert len(child[1]) == VALUE_SIZE

    def max_key(self: "HistoryIndexNode") -> TX:
        """Returns the maximum TX value this node contains."""
        return self.children[-1][0]

    def serialize(self) -> bytes:
        #                  |TX  |Offset              | depth (u16)
        buf = bytearray(((16 + 8) * MAX_CHILDREN) + 2)
        buf[0:2] = self.depth.to_bytes(2, "little")
        for i, child in enumerate(self.children):
            o = (i * CHILD_SIZE) + 2
            buf[o : o + CHILD_SIZE] = child[0].to_bytes(16, "little") + child[1]
        return bytes(buf)

    @classmethod
    def deserialize(cls: "Type[HistoryIndexNode]", buf: bytes) -> "HistoryIndexNode":
        depth = int.from_bytes(buf[0:2], "little")
        children: List[DataTuple] = []
        for i in range(MAX_CHILDREN):
            # offset into the buffer
            o = (i * CHILD_SIZE) + 2
            tx: TX = int.from_bytes(buf[o : o + 16], "little")
            c: bytes = buf[o + 16 : o + CHILD_SIZE]
            children.append((tx, c))
        return HistoryIndexNode(depth, children)
