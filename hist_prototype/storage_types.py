"""This file contains type definitions that represent on-disk represntations of data structures.

For types that it's relevant, we also include functions for serializing and deserializing them.
"""

from .types import Serializable, TX, Offset, DataLength
from .constants import MAX_CHILDREN

from dataclasses import dataclass
from typing import Tuple, List, Type, ClassVar


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
    The off
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

    """The length of this entry. This is serialized as the first 4 bytes of the entry."""
    length: int

    """The actual data for this entry. This is serialized after the length and must
    be exactly length bytes long
    """
    data: bytes

    def serialize(self) -> bytes:
        raise NotImplementedError()

    ...
