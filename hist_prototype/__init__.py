from collections import deque
from dataclasses import dataclass
from enum import IntFlag
from functools import partial
from threading import RLock
from typing import (Generic, Iterable, List, Optional, Protocol, Tuple,
                    TypeVar, Union, cast)
from weakref import ReferenceType, ref

from .util import serialize_int

TX = int
Offset = int

BTreeNode = Union["BTreeENode", "BTreeLNode"]

# Tuple of a key midfix and a btree node
KeyPfxBTreeNode = Tuple[bytes, BTreeNode]


class Serializable(Protocol):
    def serialize(self, *args, **kwargs) -> bytes:
        ...


T = TypeVar("T", bound=Serializable)


@dataclass
class HistoryRecord(Generic[T]):
    tx: TX
    value: T
    delete: bool

    def serialize(self) -> bytes:
        output = bytearray()
        output[0:16] = self.tx.to_bytes(16, "little")
        value_bytes = self.value.serialize()
        output[16 : len(value_bytes)] = value_bytes
        return output


@dataclass(frozen=True)
class WriteRequest:
    offset: int
    delete: bool
    value: bytes
    tx: int  # u128


@dataclass
class BTreeENode:
    # The maximum key midfix
    max_key: bytes
    children: List[BTreeNode]
    depth: int = 1

    def search(self: "BTreeENode", search_key: bytes) -> Optional[BTreeNode]:
        if len(self.children) == 0:
            return None

    def put(self: "BTreeENode", key: bytes, value: bytes, delete: bool = False) -> None:
        ...


class BTreeLNodeFlags(IntFlag):
    DELETED: int = 1
    PERSIST_HISTORY: int = 1 << 1


class BTreeLNode(Generic[T]):

    DEFAULT_FLAGS: int = 0  # no defaults

    # Whether this record's most recent value was a delete operation
    delete: bool

    # Persistent flags. Most flags cannot be changed after a BTreeLNode's initialization.
    # TODO: this should be enforced
    flags: int
    # Weak reference to the key
    key: bytes
    # Weak referenve to the value
    value: ReferenceType[bytes]
    # The tx instant for the most recent write
    tx: int  # u128 or u64, I haven't decided yet
    # The offset in file where the
    current_value: T
    # The offset to the head block in the history file
    history_offset: int  # u64
    # A list of historical writes
    history: List[HistoryRecord]
    # The index in the last at which all lower records have been flushed to the history file
    history_write_index: int = 0

    lock: RLock

    def __init__(
        self: "BTreeLNode",
        # A weak reference to the stored key's bytes
        key: bytes,
        # A weak reference to the stored value's bytes
        value: ReferenceType[bytes],
        # Flags are xored to DEFAULT_ARGS, meaning you must explicitly
        # disable default flags by setting them as flags. Yes, this may be
        # slightly confusing behavior
        init_flags: int,
        # An iterable that generates TX + offset pairs for history
        history: Iterable[HistoryRecord],
        # The offset to the head block in the history file
        history_idx: int = 0,
        # The offset in the data log file at which the last set value was
        # written
        current_offset: int = 0,
        # The offset in the history file for the head node for this key
        history_offset: int = 0,
    ) -> None:
        self.key = key
        self.flags = init_flags ^ self.__class__.DEFAULT_FLAGS
        self.value = value
        self.history = list(history)
        self.history_write_index = history_idx
        self.current_offset = current_offset
        self.history_offset = history_offset
        self.lock = RLock()

    def add_record(
        self, offset: int, value: bytes, tx: int, delete: bool = False
    ) -> WriteRequest:
        with self.lock:
            self.history.append(
                HistoryRecord(tx=self.tx, value=self.value, delete=delete)
            )
            self.tx = tx
            self.value = ref(value)
            self.offset = offset
            return self.to_write_req(value)

    def to_write_req(self, value: bytes) -> WriteRequest:
        with self.lock:
            return WriteRequest(
                offset=self.offset, value=value, tx=self.tx, delete=self.delete
            )


class BHistoryTree:
    leaf_nodes: deque[BTreeLNode]
    intermediate_nodes: deque[BTreeENode]
    head: BTreeENode

    def __init__(
        self, leaf_nodes: Iterable[BTreeLNode], intermediate_nodes: Iterable[BTreeENode]
    ):
        self.leaf_nodes = deque(leaf_nodes)
        self.intermediate_nodes = deque(intermediate_nodes)
        if len(self.intermediate_nodes) == 0:
            self.intermediate_nodes.append(BTreeENode(max_key=b"", children=[]))

        self.head = self.intermediate_nodes[0]


def split_intermediate_node(node: BTreeENode) -> BTreeENode:
    """Splits the provided node into a newly allocated node, modifying the existing node
    in-place."""
    mid_point = len(node.children) // 2
    new_node_children = node.children[mid_point:]
    if node.depth > 1:
        max_key = cast(BTreeENode, new_node_children[-1]).max_key
    else:
        max_key = cast(BTreeLNode, new_node_children[-1]).key
    new_node = BTreeENode(children=new_node_children, depth=node.depth, max_key=max_key)
    node.children = node.children[0:mid_point]
    return new_node


def search_intermediate_node(
    node: BTreeENode, search_key: bytes
) -> Optional[BTreeNode]:
    """Searches a node's children for a node with the matching search key."""
    # If the depth is greater than 1, the children will be `BTreeENode`s
    if node.depth > 1:
        for child in node.children:
            child = cast(BTreeENode, child)
            if child.max_key > search_key:
                return child
    else:
        for child in node.children:
            child = cast(BTreeLNode, child)
            if child.key == search_key:
                return child
            elif child.key > search_key:
                return None
    return None


def main():
    btree = BHistoryTree([], [])


if __name__ == "__main__":
    main()
