"""This file contains type definitions that represent on-disk represntations of data structures.

For types that it's relevant, we also include functions for serializing and deserializing them.
"""

from .types import Serializable, TX, Offset
from .constants import MAX_CHILDREN

from dataclasses import dataclass
from typing import Tuple, List, Type



@dataclass(frozen=True)
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
    children: List[Tuple[int, TX, Offset]]

    """
    The size of this record on disk. In general, it should not be overriden.
    """
    SIZE: int = (1 + 16 + 8) * MAX_CHILDREN + 8 + 2
    
    def serialize(self) -> bytes:
        return super().serialize()

    @classmethod
    def deserialize(cls: "Type[HistoryIndexEntry]", buf: bytes) -> "HistoryIndexEntry":
        offset = int.from_bytes(buf[0: 8], "little", signed=False)
        depth = int.from_bytes(buf[8: 10], "little", signed=False)
        children: List[Tuple[int, TX, Offset]] = []
        # From here to the return was generated by Copilot!!!
        for i in range(MAX_CHILDREN):
            o = (i * 25) + 10
            flag = int.from_bytes(buf[o: o + 1], "little", signed=False)
            tx = int.from_bytes(buf[o + 1: o + 17], "little", signed=False)
            offset = int.from_bytes(buf[o + 17: o + 25], "little", signed=False)
            children.append((flag, tx, offset))
        return cls(offset, depth, children)