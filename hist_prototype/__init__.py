from dataclasses import dataclass
from typing import List, Tuple, Iterable, Union, TypeVar, Protocol, Generic
from weakref import ref, ReferenceType
from enum import Enum
from threading import RLock
from functools import partial

TX = int
Offset = int

# Tuple of a key midfix and a btree node
KeyPfxBTreeNode = Tuple[bytes, Union["BTreeENode", "BTreeLNode"]]

class Serializable(Protocol):

    def serialize(self, *args, **kwargs) -> bytes:
        ...

T = TypeVar("T", bound=[Serializable])


def int_serialize(self: int, *args, **kwargs) -> bytes:
    return self.to_bytes(size, 'little', *args, **kwargs)

int.serialize = int_serialize


@dataclass
class HistoryRecord(Generic[T]):
    tx: TX
    value: T

    def serialize(self) -> bytes:
        output = bytearray()
        output[0:16] = self.tx.to_bytes(16, 'little')
        value_bytes = self.value.serialize()
        output[16:len(value_bytes)] = value_bytes

@dataclass(frozen=True)
class WriteRequest:
    offset: int
    value: bytes
    tx: int  # u128

@dataclass
class BTreeENode:

    children: KeyPfxBTreeNode
    depth: int = 1

class BTreeLNodeFlags(Enum, int):

    WRITE_HISTORY: int = 1 << 2

class BTreeLNode(Generic[T]):

    DEFAULT_FLAGS: int = 0  # no defaults

    # Persistent flags. Most flags cannot be changed after a BTreeLNode's initialization.
    # TODO: this should be enforced
    flags: int
    # Weak reference to the key
    key: ReferenceType[bytes]
    # Weak referenve to the value
    value: ReferenceType[bytes]
    # The tx instant for the most recent write
    tx: int  # u128 or u64, I haven't decided yet
    # The offset in file where the
    current_value: T
    # The offset to the head block in the history file
    history_offset: int  # u64
    # A list of historical writes
    history: List[HistoryRecord]
    # The index in the last at which all lower records have been flushed to the history file
    history_write_index: int = 0

    lock: RLock

    def __init__(
            self: "BTreeLNode",
            # A weak reference to the stored key's bytes
            key: ReferenceType[bytes],
            # A weak reference to the stored value's bytes
            value: ReferenceType[bytes],
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
        self.flags = init_flags ^ self.__class__.DEFAULT_FLAGS
        self.value = value
        self.history = list(history)
        self.history_write_index = history_idx
        self.current_offset = current_offset
        self.history_offset = history_offset
        self.lock = RLock()

    def add_record(self, offset: int, value: bytes, tx: int) -> WriteRequest:
        with self.lock:
            self.history.append(HistoryRecord(tx=self.tx, value=self.value))
            self.tx = tx
            self.value = value
            self.offset = offset
            return self.to_write_req()

    def to_write_req(self) -> WriteRequest:
        with self.lock:
            WriteRequest(
                offset=self.offset,
                value=self.value.serialize(),
                tx=self.tx
            )
