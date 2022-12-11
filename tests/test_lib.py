import hist_prototype as hp
from hist_prototype import Bytes, BHistoryTree


def test_simple():
    btree = hp.BHistoryTree([], [])
    btree.put(Bytes(b"key1"), Bytes(b"value1"))

    result = btree.get(Bytes(b"key1"))
    assert  result == b"value1"

def test_many():
    btree = hp.BHistoryTree([], [])
    for i in range(100):
        btree.put(Bytes(f"key{i}".encode()), Bytes(f"value{i}".encode()))

    for i in range(100):
        result = btree.get(Bytes(f"key{i}".encode()))
        print(f"I: {i}")
        print(f"Result: {result}")
        #assert result is not None