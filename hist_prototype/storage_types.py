"""This file contains type definitions that represent on-disk represntations of data structures.

For types that it's relevant, we also include functions for serializing and deserializing them.
"""

from .types import Serializable, TX, Offset, DataLength
from .constants import MAX_CHILDREN

from dataclasses import dataclass
from typing import Tuple, List, Type, ClassVar
from math import ceil


"""The tuple of flags, tx, offset/data, length/data sets.

If depth > 0 in a node, then the offset and datalength represent the max and min (respectively)
of the children nodes in the subtree rooted at this node.
Otherwise, they may represent an offset + length into the datalog or combined
into a double-word sized raw value. The flags are used to indicate which is the case,
as well as several reserved bits for future use.
"""
ChildTuple = Tuple[int, TX, Offset, DataLength]
CHILD_TUPLE_SIZE = 1 + 16 + 8 + 8


@dataclass(frozen=True, slots=True)
class HistoryIndexEntry(Serializable):
    """A simple class that represents an entry in the history index.

    Notes
        Records in the history index are always full. It is generally not possible to have a non-full record,
        as we only persist records into the history index from the main index when the leaf node buffer in
        the main index is full. This allows us to make the assumption that all children in the history index
        are populated with real records, simplifying the logic for searching/reading from this history index.
    """

    """
    The offset in the history index file where this record resides.
   
    This is not serialized.
    """
    offset: int

    """
    The depth from the bottom (leaves) of the tree that this record is at.
    
    Size: u16 
    """
    depth: int

    """
    Children records for this node.
    
    If depth is 0, then these are flag, tx, offset triples.
    If depth is > 0, then these point to other HistoryIndexEntry records,
        with the `TX` portion of the triple being the *largest* TX owned
        by a real record in the subtree rooted at the child.
    The flags are primarily used to indicate whether the offset contains a real value
        or an offset into the data log. Of it's a real value, we can skip the extra
        seek + read, but it means we're limited to u64 sized records.
        As it's always the "client's" responsibility to ensure the data is interpreted
        properly (probably based on a type map), we don't need to store the type here.
    """
    children: List[ChildTuple]

    """
    The size of this record on disk in bytes. In general, it should not be overridden.
    """
    SIZE: ClassVar[int] = CHILD_TUPLE_SIZE * MAX_CHILDREN + 2

    def __post_init__(self) -> None:
        if len(self.children) != MAX_CHILDREN:
            raise ValueError(f"Must have exactly {MAX_CHILDREN} children")
        # TODO: type validation

    def serialize(self) -> bytes:
        buf = bytearray(self.SIZE)
        buf[0:2] = self.depth.to_bytes(1, "little", signed=False)
        for i, child in enumerate(self.children):
            of = i * CHILD_TUPLE_SIZE + 2
            buf[of : of + CHILD_TUPLE_SIZE] = (
                child[0].to_bytes(1, "little", signed=False)  # flags u8
                + child[1].to_bytes(16, "little", signed=False)  # tx u128
                + child[2].to_bytes(8, "little", signed=False)  # offset/value u64
                + child[3].to_bytes(8, "little", signed=False)  # data length/value u64
            )
        return bytes(buf)

    @classmethod
    def deserialize(
        cls: "Type[HistoryIndexEntry]", buf: bytes, offset: int
    ) -> "HistoryIndexEntry":
        """Deserializes a HistoryIndexEntry from a buffer.

        Note: offset is not an offset into buf, it's the offset in the file where this record is stored
            and must come from the `IOHandler` that is reading this record.
        """
        # offset = int.from_bytes(buf[0: 8], "little", signed=False)
        depth = int.from_bytes(buf[0:2], "little", signed=False)
        children: List[ChildTuple] = []
        # From here to the return was generated by Copilot!!!
        for i in range(MAX_CHILDREN):
            # Get the offset into buf for this child record
            o = (i * CHILD_TUPLE_SIZE) + 2
            flags = int.from_bytes(buf[o : o + 1], "little", signed=False)
            # increment it by the number of bytes read for each field
            o += 1
            tx = int.from_bytes(buf[o : o + 16], "little", signed=False)
            o += 16
            data_or_offset = int.from_bytes(buf[o : o + 8], "little", signed=False)
            o += 8
            data_or_length = int.from_bytes(buf[o : o + 8], "little", signed=False)
            o += 8
            children.append((flags, tx, data_or_offset, data_or_length))
        return cls(offset=offset, depth=depth, children=children)


@dataclass(frozen=True, slots=True)
class DataLogEntry(Serializable):
    """Represents a single entry in the data log.

    The DataLog contains the real, dynamic length values
    for keys and their corresponding values. We do not necessarily
    store keys immediately next to their values, though in practice
    this may be the case for new keys.
    """

    """The flags for this entry. This is serialized as the first byte of the entry.

    Right now, only 1 bit is used to indicate whether this entry is a key or a value.
    """
    flags: int

    """The length of this entry. This is serialized as the following 8 bytes of the entry."""
    length: int

    """The actual data for this entry. This is serialized after the length and must
    be exactly length bytes long
    """
    data: bytes

    def __post_init__(self) -> None:
        if len(self.data) != self.length:
            raise ValueError("Data must be exactly length long.")

    def serialize(self) -> bytes:
        buf = bytearray(self.length + 8 + 1)
        buf[0:1] = self.flags.to_bytes(1, "little", signed=False)
        buf[1:9] = self.length.to_bytes(8, "little", signed=False)
        buf[9:] = self.data
        return bytes(buf)

    @classmethod
    def deserialize(cls: "Type[DataLogEntry]", buf: bytes) -> "DataLogEntry":
        flags = int.from_bytes(buf[0:1], "little", signed=False)
        length = int.from_bytes(buf[1:9], "little", signed=False)
        data = buf[9:]
        return cls(flags=flags, length=length, data=data)


@dataclass(frozen=True, slots=True)
class ValueDataLogEntry(DataLogEntry):
    """Represents a value entry in the data log."""

    """key_offset points to this values key in the data log."""
    key_offset: int

    def __post_init__(self) -> None:
        # Call the super, we also want it's validation
        # super() for some reason doesn't work here, and I'm too lazy to figure out why
        DataLogEntry.__post_init__(self)
        if self.flags | 1 != 1:
            raise ValueError("Flags must be 1 for a value entry.")

    def serialize(self) -> bytes:
        buf = bytearray(self.length + 8 + 8 + 1)
        o = 0
        buf[o:1] = self.flags.to_bytes(1, "little", signed=False)
        o += 1
        buf[o : o + 8] = self.length.to_bytes(8, "little", signed=False)
        o += 8
        buf[o : o + 8] = self.key_offset.to_bytes(8, "little", signed=False)
        o += 8
        buf[o:] = self.data
        return bytes(buf)

    @classmethod
    def deserialize(cls: "Type[ValueDataLogEntry]", buf: bytes) -> "ValueDataLogEntry":
        o = 0
        flags = int.from_bytes(buf[o : o + 1], "little", signed=False)
        o += 1
        length = int.from_bytes(buf[o : o + 8], "little", signed=False)
        o += 8
        key_offset = int.from_bytes(buf[o : o + 8], "little", signed=False)
        o += 8
        data = buf[o:]
        return cls(flags=flags, length=length, key_offset=key_offset, data=data)


MainIndexChildren = Tuple[Offset, Offset, DataLength]
@dataclass(frozen=True, slots=True)
class MainIndexEntry:
    """Represents a single entry in the main index.

    The main index contains the offset of the history index entry for a given key or
    an intermediary node.
    """

    """The depth of this entry relative to the leave nodes in the tree.
    
    If this is 0, this is a leaf node, otherwise it is an intermediate node.
    """
    depth: int

    """A single-bit flag for every child entry.

    The bit-length of this field is determined by MAX_CHILDREN.
    """
    entry_flags: int

    """Children of this node.

    If this is an intermediate node (depth > 0), the children
    are pairs of (key_offset, child_node_offset, key_size) triples.
    If this is a leaf node (depth == 0), the pairs are (key_offset, current_value_offset, value_size) triples.
    All values in the triples are 64-bit unsigned integers.
    """
    children: List[MainIndexChildren]

    SIZE: int = 2 + ceil(MAX_CHILDREN / 8) + MAX_CHILDREN * (8 + 8 + 8)