from dataclasses import dataclass
from typing import List, Union, Optional, Generic, cast

from .leaf_node import LeafNode, MAX_CHILDREN
from .types import V, NodeFullError

BTreeNode = Union["IntermediateNode[V]", LeafNode[V]]

@dataclass
class IntermediateNode(Generic[V]):
    # The maximum key midfix
    max_key: bytes
    children: List[BTreeNode[V]]
    depth: int = 1

    def search(self: "IntermediateNode[V]", search_key: bytes) -> Optional[BTreeNode[V]]:
        return search_intermediate_node(self, search_key)

    def insert(self: "IntermediateNode[V]", node: BTreeNode[V]) -> None:
        insert_into_intermediate_node(self, node)

    def split(self: "IntermediateNode[V]") -> "IntermediateNode[V]":
        return split_intermediate_node(self)

    @property
    def full(self) -> bool:
        return len(self.children) >= MAX_CHILDREN

def search_intermediate_node(
    node: IntermediateNode[V], search_key: bytes
) -> Optional[BTreeNode[V]]:
    """Searches a node's children for a node with the matching search key."""
    if len(node.children) == 0:
        return None
    # If the depth is greater than 1, the children will be `BTreeENode`s
    if node.depth > 1:
        for child in node.children:
            child = cast(IntermediateNode[V], child)
            if child.max_key > search_key:
                return child
    else:
        for child in node.children:
            child = cast(LeafNode[V], child)
            if child.key == search_key:
                return child
            elif child.key > search_key:
                return None
    return None

def insert_into_intermediate_node(
    node: IntermediateNode[V],
    new_node: BTreeNode[V],
) -> int:
    if node.full:
        raise NodeFullError()
    if node.depth == 1:
        new_node = cast(LeafNode[V], new_node)
        assert isinstance(new_node, LeafNode)
        for i, child in enumerate(node.children):
            child = cast(LeafNode[V], child)
            # If the keys match, replace the node entirely
            try:
                if child.key == new_node.key:
                    # TODO: this is the wrong behavior if we want to retain history
                    node.children[i] = new_node
                    return i
            except AttributeError:
                raise RuntimeError("Unreachable code. This is a bug!!!")
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
        new_node = cast(IntermediateNode[V], new_node)
        assert isinstance(node, IntermediateNode)
        for i, child in enumerate(node.children):
            child = cast(IntermediateNode[V], child)
            if child.max_key == new_node.max_key:
                # TODO: this is the wrong behavior if we want to retain history
                node.children[i] = new_node
                return i
            if child.max_key > new_node.max_key:
                node.children.insert(i, new_node)
                return i
        node.children.append(new_node)
        return len(node.children)

def split_intermediate_node(node: IntermediateNode[V]) -> IntermediateNode[V]:
    """Splits the provided node into a newly allocated node, modifying the existing node
    in-place."""
    mid_point = len(node.children) // 2
    new_node_children = node.children[mid_point:]
    if node.depth > 1:
        max_key = cast(IntermediateNode[V], new_node_children[-1]).max_key
    else:
        max_key = cast(LeafNode[V], new_node_children[-1]).key
    new_node = IntermediateNode(children=new_node_children, depth=node.depth, max_key=max_key)
    node.children = node.children[0:mid_point]
    if node.depth > 1:
        node.max_key = node.children[-1].max_key
    else:
        node.max_key = node.children[-1].key
    return new_node