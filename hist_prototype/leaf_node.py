from typing import Generic, List, Iterable
from dataclasses import dataclass
from threading import RLock
from enum import IntFlag

from .types import V, TX

# The maximum number of children BTreeLNode's are allowed to have
MAX_CHILDREN = 16


class LeafNodeFlags(IntFlag):
    DELETED: int = 1  # type: ignore
    PERSIST_HISTORY: int = 1 << 1  # type: ignore


@dataclass
class HistoryRecord:
    tx: TX
    value: bytes
    delete: bool

    def serialize(self) -> bytes:
        output = bytearray()
        output[0:16] = self.tx.to_bytes(16, "little")
        output[16 : len(self.value)] = self.value
        return output


@dataclass(frozen=True)
class WriteRequest:
    offset: int
    delete: bool
    value: bytes
    tx: int  # u128


class LeafNode(Generic[V]):

    DEFAULT_FLAGS: int = 0  # no defaults

    # Whether this record's most recent value was a delete operation
    delete: bool

    # Persistent flags. Most flags cannot be changed after a BTreeLNode's initialization.
    # TODO: this should be enforced
    flags: int
    # Weak reference to the key
    key: bytes
    # Weak referenve to the value
    value: bytes
    # The tx instant for the most recent write
    tx: int  # u128 or u64, I haven't decided yet
    # The offset in file where the
    current_value: V
    # The offset to the head block in the history file
    history_offset: int  # u64
    # A list of historical writes
    history: List[HistoryRecord]
    # The index in the last at which all lower records have been flushed to the history file
    history_write_index: int = 0

    lock: RLock

    def __init__(
        self: "LeafNode[V]",
        # A weak reference to the stored key's bytes
        key: bytes,
        # A weak reference to the stored value's bytes
        value: bytes,
        # The tranasaction instant for the most recent write
        tx: int,
        # Flags are xored to DEFAULT_ARGS, meaning you must explicitly
        # disable default flags by setting them as flags. Yes, this may be
        # slightly confusing behavior
        init_flags: int,
        # An iterable that generates TX + offset pairs for history
        history: Iterable[HistoryRecord],
        # The offset to the head block in the history file
        history_idx: int = 0,
        # The offset in the data log file at which the last set value was
        # written
        current_offset: int = 0,
        # The offset in the history file for the head node for this key
        history_offset: int = 0,
    ) -> None:
        self.key = key
        self.tx = tx
        self.flags = init_flags ^ self.__class__.DEFAULT_FLAGS
        self.value = value
        self.history = list(history)
        self.history_write_index = history_idx
        self.current_offset = current_offset
        self.history_offset = history_offset
        self.lock = RLock()
        self.delete = False

    def add_record(
        self, offset: int, value: bytes, tx: int, delete: bool = False
    ) -> WriteRequest:
        with self.lock:
            self.history.append(
                HistoryRecord(tx=self.tx, value=self.value, delete=delete)
            )
            self.tx = tx
            self.value = value
            self.offset = offset
            return self.to_write_req(value)

    def to_write_req(self, value: bytes) -> WriteRequest:
        with self.lock:
            return WriteRequest(
                offset=self.offset, value=value, tx=self.tx, delete=self.delete
            )
