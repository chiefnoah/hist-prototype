from collections import deque
from pathlib import Path
from typing import (
    Deque,
    Iterable,
    List,
    Optional,
    Generic,
    cast,
)


from .leaf_node import LeafNode, LeafNodeFlags
from .intermediate_node import IntermediateNode
from .types import K, V, NodeFullError, HistoryReadRequest
from .storage import IOHandler


class BufferedBTree(Generic[K, V]):
    leaf_nodes: Deque[LeafNode[V]]
    intermediate_nodes: Deque[IntermediateNode[V]]
    head: IntermediateNode[V]
    current_tx: int
    history_index: IOHandler
    data_log: IOHandler

    def __init__(
        self,
        leaf_nodes: Iterable[LeafNode[V]],
        intermediate_nodes: Iterable[IntermediateNode[V]],
        history_index: IOHandler,
        data_log: IOHandler,
        tx_epoch: int = 0,
    ):
        self.leaf_nodes = deque(leaf_nodes)
        self.intermediate_nodes = deque(intermediate_nodes)
        if len(self.intermediate_nodes) == 0:
            self.intermediate_nodes.append(IntermediateNode(max_key=b"", children=[]))

        self.head = self.intermediate_nodes[0]
        self.tx = tx_epoch
        self.history_index = history_index
        self.data_log = data_log

    # TODO: merge search functionality from put and get into a generic `search` function
    def put(
        self: "BufferedBTree[K, V]", key: K, value: Optional[V], delete: bool = False
    ) -> None:
        value_bytes = None
        if value is not None:
            value_bytes = value.serialize()
        key_bytes = key.serialize()
        node_stack: List[IntermediateNode[V]] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = node_stack[-1].node_for_insert(key_bytes)
            if new_node is None:
                raise RuntimeError("Unreachable code. This is a bug!!!")
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], IntermediateNode)
        assert node_stack[-1].depth == 1
        # Check if the key already exists
        maybe_leaf = node_stack[-1].search(key_bytes)
        if maybe_leaf is not None:
            maybe_leaf = cast(LeafNode[V], maybe_leaf)
            assert isinstance(maybe_leaf, LeafNode)
            # don't do anything with the WriteRequest for now
            _ = maybe_leaf.add_record(
                offset=0, value=value_bytes, tx=self.tx, delete=delete
            )
            self.tx += 1  # increment the tx counter
            return
        if delete:
            # If we're deleting a record that doesn't exist, just... don't do anything
            return
        # New key, let's create a new BTreeLNode
        new_leaf = LeafNode[V](
            key=key_bytes,
            value=value_bytes,
            tx=self.tx,
            init_flags=LeafNodeFlags.PERSIST_HISTORY,
            history=[],
            history_idx=0,
            current_offset=0,
            history_offset=0,
        )
        try:
            node_stack[-1].insert(new_leaf)
            node_stack.pop()
            while len(node_stack) > 0:
                n = node_stack.pop()
                if n.max_key < new_leaf.key:
                    n.max_key = new_leaf.key
                else:
                    break
        except NodeFullError:
            self.split_nodes(node_stack)
            return self.put(key, value)
        self.tx += 1  # increment the tx counter
        self.leaf_nodes.append(new_leaf)

    def delete(self: "BufferedBTree[K, V]", key: K) -> None:
        self.put(key, value=None, delete=True)

    def as_of(self: "BufferedBTree[K, V]", search_key: K, tx: int) -> Optional[bytes]:
        """Returns the value associated with search_key as of tx."""
        leaf_node = self._get(search_key)
        if leaf_node is not None:
            result = leaf_node.as_of(tx)
            if isinstance(result, HistoryReadRequest):
                raise NotImplementedError("TODO: implement read requests")
            return result
        return None

    def split_nodes(self, node_stack: List[IntermediateNode[V]]) -> None:
        """Splits the nodes in the provided stack such that the bottom node is not full."""
        assert len(node_stack) > 0
        if not node_stack[-1].full:
            return
        old_parent = node_stack.pop()
        new_node = old_parent.split()
        # Maybe split the next layer up
        if len(node_stack) == 0:
            # We've reached the root, so we need to create a new root
            new_root = IntermediateNode(
                max_key=new_node.max_key,
                children=[old_parent, new_node],
                depth=old_parent.depth + 1,
            )
            self.head = new_root
            self.intermediate_nodes.insert(0, new_root)
            return
        node_stack[-1].insert(new_node)
        self.split_nodes(node_stack)

    def _get(self: "BufferedBTree[K, V]", key: K) -> Optional[LeafNode[V]]:
        key_bytes = key.serialize()
        node_stack: List[IntermediateNode[V]] = [self.head]
        while node_stack[-1].depth > 1:
            new_node = node_stack[-1].search(key_bytes)
            if new_node is None:
                return None
            assert isinstance(new_node, IntermediateNode)
            node_stack.append(new_node)
        assert isinstance(node_stack[-1], IntermediateNode)
        assert node_stack[-1].depth == 1
        leaf_node = cast(LeafNode[V], node_stack[-1].search(key_bytes))
        if leaf_node is not None:
            return leaf_node
        return None

    def get(self: "BufferedBTree[K, V]", key: K) -> Optional[bytes]:
        leaf_node = self._get(key)
        if leaf_node is not None:
            return leaf_node.value
        return None
