from typing import Generic, List, Iterable, Optional, Union, Literal
from dataclasses import dataclass
from threading import RLock
from enum import IntFlag

from .types import V, TX, Splittable

# The maximum number of children BTreeLNode's are allowed to have
MAX_CHILDREN = 16


class LeafNodeFlags(IntFlag):
    DELETED: int = 1
    PERSIST_HISTORY: int = 1 << 1


@dataclass
class HistoryRecord:
    tx: TX
    value: Optional[bytes]
    delete: bool

    def serialize(self) -> bytes:
        value_size = 0
        if self.value is not None:
            value_size = len(self.value)
        output = bytearray()
        output[0:16] = self.tx.to_bytes(16, "little")
        output[16:value_size] = self.value or b""
        return output


@dataclass(frozen=True)
class ReadRequest:
    """A request to read a serialized HistoryRecord from disk."""

    offset: int
    tx: Optional[int]


@dataclass(frozen=True)
class WriteRequest:
    """A request to write a serialized LeafNode to disk."""

    offset: Optional[int]
    delete: bool
    value: bytes
    tx: int  # u128


class LeafNode(Splittable, Generic[V]):

    DEFAULT_FLAGS: int = 0  # no defaults

    # Whether this record's most recent value was a delete operation
    delete: bool

    # Persistent flags. Most flags cannot be changed after a BTreeLNode's initialization.
    # TODO: this should be enforced
    flags: int
    # Weak reference to the key
    key: bytes
    # The value currently associated with the key.
    # This could be none if the most recent update was a delete operation
    value: Optional[bytes]
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
        value: Optional[bytes],
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
        self, offset: int, value: Optional[bytes], tx: int, delete: bool = False
    ) -> WriteRequest:
        with self.lock:
            self.history.append(
                HistoryRecord(tx=self.tx, value=self.value, delete=delete)
            )
            self.tx = tx
            if delete:
                value = None
            self.delete = delete
            self.value = value
            self.offset = offset
            return self.to_write_req(value)

    def to_write_req(self, value: Optional[bytes]) -> WriteRequest:
        with self.lock:
            return WriteRequest(
                offset=self.offset, value=value or b"", tx=self.tx, delete=self.delete
            )

    def as_of(self: "LeafNode[V]", tx: int) -> Union[bytes, ReadRequest, None]:
        # If we have history that is older than the small buffer
        # and the requested tx is older than our buffer's history,
        # we need to search the history index for this tx's value
        if tx < self.history[-1].tx and self.history_offset > 0:
            return ReadRequest(self.history_offset, tx)
        with self.lock:
            if tx > self.tx and not self.delete:
                return self.value
            elif tx > self.tx and self.delete:
                return None
            index = 0
            for i, record in enumerate(self.history):
                if record.tx > tx:
                    index = i - 1
                    break
            else:
                # This executes if the above break didn't happen
                if len(self.history) == 0:
                    return None

            # This means we have no history and the as_of is asking for before this value
            # was ever set
            if self.history[index].tx > tx:
                return None
            history_node: HistoryRecord = self.history[index]
            return history_node.value

    def split(self) -> "LeafNode[V]":
        # TODO: move split logic out of function into member method for locking sanity
        raise NotImplementedError("Leaf nodes cannot be split")
