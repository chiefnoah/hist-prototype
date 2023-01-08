from hist_prototype.storage_types import (
    HistoryIndexEntry,
    DataLogEntry,
    MainIndexEntry,
    ValueDataLogEntry,
)
from hist_prototype.constants import MAX_CHILDREN


def test_roundrip_serialize_history_index_entry():
    hie = HistoryIndexEntry(
        offset=123,
        depth=3,
        children=[(1, i, i * 2, i * 3) for i in range(MAX_CHILDREN)],
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
        assert child[2] == i * 2
        assert child[3] == i * 3


def test_roundtrip_serialize_data_log_entry():
    dle = DataLogEntry(flags=0, length=110, data=b"hello world" * 10)
    b = dle.serialize()
    assert len(b) == 110 + 1 + 8

    ddle = DataLogEntry.deserialize(b)

    assert len(ddle.data) == ddle.length


def test_roundtrip_serialize_value_data_log_entry():
    vdle = ValueDataLogEntry(
        flags=1, length=110, key_offset=123, data=b"hello world" * 10
    )
    b = vdle.serialize()
    assert len(b) == 110 + 1 + 8 + 8

    dvdle = ValueDataLogEntry.deserialize(b)

    assert len(dvdle.data) == dvdle.length

def test_roundtrip_serialize_main_index_entry():
    mie = MainIndexEntry(
        depth=3,
        entry_flags=0,
        children=[(1, i, i * 2) for i in range(MAX_CHILDREN)],
    )
    buf = mie.serialize()
    assert len(buf) == MainIndexEntry.SIZE

    dmie = MainIndexEntry.deserialize(buf)
    assert dmie.depth == 3
    assert dmie.entry_flags == 0
    for i, child in enumerate(dmie.children):
        assert isinstance(child, tuple)
        assert len(child) == 3
        assert isinstance(child[0], int)
        assert isinstance(child[1], int)
        assert isinstance(child[2], int)
        assert child[0] == 1
        assert child[1] == i
        assert child[2] == i * 2