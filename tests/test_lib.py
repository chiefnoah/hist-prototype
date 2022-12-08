import hist_prototype as hp
from hist_prototype import Bytes, BHistoryTree


def test_simple():
    btree = hp.BHistoryTree([], [])
    btree.put(Bytes(b"key1"), Bytes(b"value1"))

    result = btree.get(Bytes(b"key1"))
    assert  result == b"value1"