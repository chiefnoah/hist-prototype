from collections import deque
from dataclasses import dataclass
from enum import IntFlag
from threading import RLock
from typing import (
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from weakref import ReferenceType, ref

from .util import serialize_int

# The maximum number of children BTreeLNode's are allowed to have
MAX_CHILDREN = 16

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
        return search_intermediate_node(self, search_key)


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
        value: bytes,
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
            self.value = value
            self.offset = offset
            return self.to_write_req(value)

    def to_write_req(self, value: bytes) -> WriteRequest:
        with self.lock:
            return WriteRequest(
                offset=self.offset, value=value, tx=self.tx, delete=self.delete
            )


K = TypeVar("K", bound=Serializable)
V = TypeVar("V", bound=Serializable)


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

    # TODO: merge search functionality from put and get into a generic `search` function
    def put(self: "BHistoryTree", key: K, value: V) -> None:
        key_bytes = key.serialize()
        node_stack: List[BTreeENode] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = search_intermediate_node(node_stack[-1], key_bytes)
            if new_node is None:
                raise RuntimeError("Unreachable code. This is a bug!!!")
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], BTreeENode)
        assert node_stack[-1].depth == 1
        # New key, let's create a new BTreeLNode
        new_node = BTreeLNode(
            key=key_bytes,
            value=value.serialize(),
            init_flags=BTreeLNodeFlags.PERSIST_HISTORY,
            history=[],
            history_idx=0,
            current_offset=0,
            history_offset=0,
        )
        # TODO: handle full nodes recursively
        try:
            insert_into_intermediate_node(node_stack[-1], new_node)
        except NodeFullError:
            recursively_split_node(node_stack.pop(), new_node, node_stack)

    def get(self: "BHistoryTree", key: K) -> bytes:
        key_bytes = key.serialize()
        node_stack: List[BTreeENode] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = search_intermediate_node(node_stack[-1], key_bytes)
            if new_node is None:
                raise RuntimeError("Unreachable code. This is a bug!!!")
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], BTreeENode)
        assert node_stack[-1].depth == 1
        leaf_node = node_stack[-1].search(key_bytes)
        if leaf_node is not None:
            return leaf_node.value
        return None


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
    if len(node.children) == 0:
        return None
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


class NodeFullError(RuntimeError):
    def __init__(self, msg="Node is full, cannot insert another node", *args) -> None:
        super().__init__(msg, *args)


def insert_into_intermediate_node(
    node: BTreeENode,
    new_node: BTreeNode,
) -> int:
    if len(node.children) + 1 >= MAX_CHILDREN:
        raise NodeFullError()
    if node.depth == 1:
        new_node = cast(BTreeLNode, new_node)
        assert isinstance(new_node, BTreeLNode)
        for i, child in enumerate(node.children):
            child = cast(BTreeLNode, child)
            # If the keys match, replace the node entirely
            if child.key == new_node.key:
                # TODO: this is the wrong behavior if we want to retain history
                node.children[i] = new_node
                return i
            # If the current childs key is of higher order than the node to insert,
            # insert the node at the childs position, moving everything after 1 index
            # later
            if child.key > new_node.key:
                node.children.insert(i, new_node)
                return i
        # If we get to this point, it means the new node is probably the new max
        node.children.append(new_node)
        return len(node.children)
    else:
        new_node = cast(BTreeENode, new_node)
        assert isinstance(node, BTreeENode)
        for i, child in enumerate(node.children):
            child = cast(BTreeENode, child)
            if child.max_key == new_node.max_key:
                # TODO: this is the wrong behavior if we want to retain history
                node.children[i] = new_node
                return i
            if child.max_key > new_node.max_key:
                node.children.insert(i, new_node)
                return i
        node.children.append(new_node)
        return len(node.children)

def recursively_splitcert_node(
    node: BTreeENode,
    new_node: BTreeNode,
    node_stack: List[BTreeENode],
) -> None:
    """Recursively splits a node, pushing the new node to the next level up."""
    new_node = split_intermediate_node(node)
    if len(node_stack) == 0:
        # We've reached the top of the tree, create a new root node
        new_root = BTreeENode(
            children=[node, new_node],
            depth=node.depth + 1,
            max_key=new_node.max_key,
        )
        return new_root
    else:
        # We have more levels to go, insert the new node into the parent
        parent = node_stack.pop()
        insert_into_intermediate_node(parent, new_node)
        return recursively_split_node(parent, new_node, node_stack)

@dataclass(frozen=True)
class Bytes:
    inner: bytes

    def serialize(self) -> bytes:
        return self.inner


def main():
    btree = BHistoryTree([], [])
    btree.put(Bytes(b"key1"), Bytes(b"value1"))


if __name__ == "__main__":
    main()
