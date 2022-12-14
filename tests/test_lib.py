import pytest
from hist_prototype import Bytes, BHistoryTree
from hist_prototype.intermediate_node import MAX_CHILDREN, IntermediateNode
from unittest.mock import MagicMock
from typing import List, cast

from hist_prototype.leaf_node import LeafNode

@pytest.fixture
def btree():
    return BHistoryTree[Bytes, Bytes]([], [])

def test_simple(btree):
    btree.put(Bytes(b"key1"), Bytes(b"value1"))

    result = btree.get(Bytes(b"key1"))
    assert result == b"value1"


def test_many(btree: BHistoryTree[Bytes, Bytes]]):
    COUNT = 100_000
    for i in range(COUNT):
        btree.put(Bytes(f"key{i}".encode()), Bytes(f"value{i}".encode()))
        assert btree.get(Bytes(f"key{i}".encode())) == f"value{i}".encode()

    for i in range(COUNT):
        result = btree.get(Bytes(f"key{i}".encode()))
        assert result is not None


def test_split_nodes():
    root = IntermediateNode[Bytes](max_key=b"", children=[], depth=2)
    counter = 1
    for i in range(1, MAX_CHILDREN + 1):
        inode = IntermediateNode(
            max_key=f"key{i * MAX_CHILDREN}".encode(), children=[], depth=1
        )
        for _ in range(1, MAX_CHILDREN + 1):
            inode.insert(MagicMock(spec=LeafNode, key=f"key{counter}".encode()))
            counter += 1
        root.insert(inode)
    assert root.max_key == b"key99"  # byte-wise comparison means this is the max
    new_node = root.split()
    assert new_node is not None
    assert isinstance(new_node, IntermediateNode)
    assert root.max_key == b"key240"
    assert new_node.max_key == b"key99"


def test_tree_split_new_root():
    root = IntermediateNode[Bytes](
        max_key=b"zzz9",
        children=[MagicMock(spec=LeafNode, key=b"xyz")] * MAX_CHILDREN,
        depth=1,
    )
    # Create a tree with a root that is full
    tree = BHistoryTree[Bytes, Bytes](
        cast(List[LeafNode[Bytes]], root.children), [root]
    )
    tree.put(Bytes(b"abc"), Bytes(b"val"))
    result = tree.get(Bytes(b"abc"))
    assert result is not None
    assert result == b"val"


def test_insert_saves_history():
    btree = BHistoryTree[Bytes, Bytes]([], [])
    for i in range(4):
        btree.put(Bytes(b"key"), Bytes(f"value{i}".encode()))

    assert btree.leaf_nodes[0].value == b"value3"
    assert btree.leaf_nodes[0].history[0].value == b"value0"
    assert btree.leaf_nodes[0].history[1].value == b"value1"
    assert btree.leaf_nodes[0].history[2].value == b"value2"

def test_delete_removes_record():
    btree = BHistoryTree[Bytes, Bytes]([], [])
    btree.put(Bytes(b"key"), Bytes(b"value"))
    btree.delete(Bytes(b"key"))
    assert btree.get(Bytes(b"key")) is None

