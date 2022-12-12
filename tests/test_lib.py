from hist_prototype import Bytes, BHistoryTree
from hist_prototype.intermediate_node import MAX_CHILDREN, IntermediateNode
from unittest.mock import MagicMock

from hist_prototype.leaf_node import LeafNode


def test_simple():
    btree = BHistoryTree[Bytes, Bytes]([], [])
    btree.put(Bytes(b"key1"), Bytes(b"value1"))

    result = btree.get(Bytes(b"key1"))
    assert  result == b"value1"

def test_many():
    btree = BHistoryTree[Bytes, Bytes]([], [])
    for i in range(100):
        btree.put(Bytes(f"key{i}".encode()), Bytes(f"value{i}".encode()))

    for i in range(100):
        result = btree.get(Bytes(f"key{i}".encode()))
        print(f"I: {i}")
        print(f"Result: {result}")
        #assert result is not None

def test_split_nodes():
    root = IntermediateNode[Bytes](max_key=b"", children=[], depth=2)
    for i in range(MAX_CHILDREN):
        inode = IntermediateNode(max_key=f"key{i}".encode(), children=[], depth=1)
        for _ in range(MAX_CHILDREN):
            inode.children.append(MagicMock(spec=LeafNode))
        root.children.append(inode)
    new_node = root.split()
    assert new_node is not None
    assert isinstance(new_node, IntermediateNode)