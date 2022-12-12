from collections import deque
from typing import (
    Iterable,
    List,
    Optional,
    Generic,
)

from .leaf_node import LeafNode, LeafNodeFlags
from .intermediate_node import IntermediateNode
from .types import K, V, NodeFullError

class BHistoryTree(Generic[K, V]):
    leaf_nodes: deque[LeafNode[V]]
    intermediate_nodes: deque[IntermediateNode[V]]
    head: IntermediateNode[V]

    def __init__(
        self, leaf_nodes: Iterable[LeafNode[V]], intermediate_nodes: Iterable[IntermediateNode[V]]
    ):
        self.leaf_nodes = deque(leaf_nodes)
        self.intermediate_nodes = deque(intermediate_nodes)
        if len(self.intermediate_nodes) == 0:
            self.intermediate_nodes.append(IntermediateNode(max_key=b"", children=[]))

        self.head = self.intermediate_nodes[0]

    # TODO: merge search functionality from put and get into a generic `search` function
    def put(self: "BHistoryTree[K, V]", key: K, value: V) -> None:
        key_bytes = key.serialize()
        node_stack: List[IntermediateNode[V]] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = node_stack[-1].search(key_bytes)
            if new_node is None:
                raise RuntimeError("Unreachable code. This is a bug!!!")
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], IntermediateNode)
        assert node_stack[-1].depth == 1
        # TODO: check if the corresponding LeafNode already exists, and update it in-place if it does
        # New key, let's create a new BTreeLNode
        new_node = LeafNode(
            key=key_bytes,
            value=value.serialize(),
            init_flags=LeafNodeFlags.PERSIST_HISTORY,
            history=[],
            history_idx=0,
            current_offset=0,
            history_offset=0,
        )
        self.leaf_nodes.append(new_node)
        # TODO: handle full nodes recursively
        try:
            node_stack[-1].insert(new_node)
        except NodeFullError:
            self.split_nodes(node_stack)
            self.put(key, value)

    def split_nodes(self, node_stack: List[IntermediateNode[V]]) -> None:
        """Splits the nodes in the provided stack such that the bottom node is not full."""
        if not node_stack[-1].full:
            return
        old_parent = node_stack.pop()
        new_node = old_parent.split()
        # Maybe split the next layer up
        if len(node_stack) == 0:
            # We've reached the root, so we need to create a new root
            new_root = IntermediateNode(
                max_key=new_node.max_key, children=[old_parent, new_node], depth=old_parent.depth + 1
            )
            self.head = new_root
            self.intermediate_nodes.insert(0, new_root)
            return
        self.split_nodes(node_stack)

    def get(self: "BHistoryTree[K, V]", key: K) -> Optional[bytes]:
        key_bytes = key.serialize()
        node_stack: List[IntermediateNode[V]] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = node_stack[-1].search(key_bytes)
            if new_node is None:
                raise RuntimeError("Unreachable code. This is a bug!!!")
            assert isinstance(new_node, IntermediateNode)
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], IntermediateNode)
        assert node_stack[-1].depth == 1
        leaf_node = node_stack[-1].search(key_bytes)
        if leaf_node is not None:
            return leaf_node.value
        return None