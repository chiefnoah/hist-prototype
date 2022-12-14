from hist_prototype.types import HistoryIndexNode, MAX_CHILDREN
import pytest


def test_serialize_history_index_node():
    children = [(i, i*2) for i in range(MAX_CHILDREN)]
    node = HistoryIndexNode(depth=0, children=children)
    b = node.serialize()
    assert isinstance(b, bytes)
    new_node = HistoryIndexNode.deserialize(b)
    for i, child in enumerate(new_node.children):
        assert child[0] == i
        assert child[1] == i*2

def test_history_index_node_size_check():
    with pytest.raises(ValueError):
        HistoryIndexNode(depth=0, children=[(0, 0)] * 100)
    with pytest.raises(ValueError):
        HistoryIndexNode(depth=0, children=[(0, 0)] * 1)
    with pytest.raises(ValueError):
        HistoryIndexNode(depth=0, children=[])
    HistoryIndexNode(depth=0, children=[(0, 0)] * MAX_CHILDREN)
