from hist_prototype.storage_types import HistoryIndexEntry
from hist_prototype.constants import MAX_CHILDREN

def test_roundrip_serialize():
    hie = HistoryIndexEntry(
        offset=123,
        depth=3,
        children=[(1, i, i*2, i*3) for i in range(MAX_CHILDREN)]
    )

    b = hie.serialize()
    assert isinstance(b, bytes)
    assert len(b) == HistoryIndexEntry.SIZE

    de_hie = HistoryIndexEntry.deserialize(b, 111)

    assert de_hie.offset == 111
    assert de_hie.depth == 3

    for i, child in enumerate(de_hie.children):
        assert isinstance(child, tuple)
        assert len(child) == 4
        assert isinstance(child[0], int)
        assert isinstance(child[1], int)
        assert isinstance(child[2], int)
        assert child[0] == 1
        assert child[1] == i
        assert child[2] == i*2
        assert child[3] == i*3