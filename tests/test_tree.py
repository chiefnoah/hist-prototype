import pytest
from hist_prototype import Bytes, BufferedBTree
from hist_prototype.constants import MAX_CHILDREN
from hist_prototype.intermediate_node import IntermediateNode
from unittest.mock import MagicMock, patch
from typing import List, cast

from hist_prototype.leaf_node import LeafNode
from hist_prototype.storage import IOHandler


@pytest.fixture
def btree():
    return BufferedBTree[Bytes, Bytes]([], [], MagicMock(spec=IOHandler), MagicMock(spec=IOHandler))


def test_simple(btree: BufferedBTree[Bytes, Bytes]):
    btree.put(Bytes(b"key1"), Bytes(b"value1"))

    result = btree.get(Bytes(b"key1"))
    assert result == b"value1"


def test_many(btree: BufferedBTree[Bytes, Bytes]):
    COUNT = 100_000
    for i in range(COUNT):
        btree.put(Bytes(f"key{i}".encode()), Bytes(f"value{i}".encode()))
        assert btree.get(Bytes(f"key{i}".encode())) == f"value{i}".encode()

    for i in range(COUNT):
        result = btree.get(Bytes(f"key{i}".encode()))
        assert result is not None

# Patch out the max children constant to make this test pass
@patch('hist_prototype.intermediate_node.MAX_CHILDREN', 16)
def test_split_nodes():
    MAX_CHILDREN = 16
    root = IntermediateNode[Bytes](max_key=b"", children=[], depth=2)
    counter = 1
    for i in range(1, MAX_CHILDREN + 1):
        inode = IntermediateNode[Bytes](
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
    tree = BufferedBTree[Bytes, Bytes](
        cast(List[LeafNode[Bytes]], root.children), [root],
        MagicMock(spec=IOHandler), MagicMock(spec=IOHandler)
    )
    tree.put(Bytes(b"abc"), Bytes(b"val"))
    result = tree.get(Bytes(b"abc"))
    assert result is not None
    assert result == b"val"


def test_insert_saves_history(btree: BufferedBTree[Bytes, Bytes]):
    for i in range(4):
        btree.put(Bytes(b"key"), Bytes(f"value{i}".encode()))

    assert btree.leaf_nodes[0].value == b"value3"
    assert btree.leaf_nodes[0].history[0].value == b"value0"
    assert btree.leaf_nodes[0].history[1].value == b"value1"
    assert btree.leaf_nodes[0].history[2].value == b"value2"


def test_delete_removes_record(btree: BufferedBTree[Bytes, Bytes]):
    btree.put(Bytes(b"key"), Bytes(b"value"))
    btree.delete(Bytes(b"key"))
    assert btree.get(Bytes(b"key")) is None


def test_as_of_in_memory_query(btree: BufferedBTree[Bytes, Bytes]):
    btree.put(Bytes(b"key"), Bytes(b"value1"))
    btree.put(Bytes(b"key"), Bytes(b"value2"))
    btree.delete(Bytes(b"key"))
    btree.put(Bytes(b"key"), Bytes(b"value4"))
    assert btree.tx == 4
    # Check we can get a value that was correct as of the requested tx
    assert btree.as_of(Bytes(b"key"), 1) == b"value2"
    # If we request a tx that is older than the oldest insert, we get None
    assert btree.as_of(Bytes(b"key"), -1) is None
    # If we request a tx more recent than the most recent value, we get the current value
    assert btree.as_of(Bytes(b"key"), 5) == b"value4"