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




# Tuple of a key midfix and a btree node
KeyPfxBTreeNode = Tuple[bytes, BTreeNode]

















def recursively_splitcert_node(
    node_to_insert: BTreeNode,
    node_stack: List[BTreeENode],
) -> BTreeENode:
    """Recursively splits nodes as necessary, inserting the new node into the parent."""
    if len(node_stack) == 0:
        raise ValueError("Node stack is empty, we cannot split.")
    old_parent = node_stack.pop()
    if not old_parent.full:
        insert_into_intermediate_node(old_parent, node_to_insert)
        # Return the root node
        return node_stack[0]
    # Otherwise we split
    new_node = split_intermediate_node(old_parent)
    if isinstance(node_to_insert, BTreeENode):
        k = node_to_insert.max_key
    elif isinstance(node_to_insert, BTreeLNode):
        k = node_to_insert.key
    else:
        raise ValueError("Unknown node type")
    # Insert the node we wanted to insert into the proper parent
    if k > old_parent.max_key:
        insert_into_intermediate_node(new_node, node_to_insert)
    else:
        insert_into_intermediate_node(old_parent, node_to_insert)
    if len(node_stack) == 0:
        # If we have no more parents, we need to create a new root node
        return BTreeENode(
            children=[old_parent, new_node],
            depth=old_parent.depth + 1,
            max_key=new_node.max_key,
        )
    # Insert the newly allocaed parent node into it's parent
    return recursively_splitcert_node(new_node, node_stack)


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
