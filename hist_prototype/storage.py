from threading import RLock
from hist_prototype.leaf_node import ReadRequest, WriteRequest
from typing import BinaryIO

class InvalidWriteRequest(ValueError):
    ...

class IOHandler:
    file: BinaryIO
    page_size: int
    lock: RLock

    def __init__(self: "IOHandler", file: BinaryIO, page_size: int = 256) -> None:
        self.file = file
        self.page_size = page_size
        self.lock = RLock()

    def write(self: "IOHandler", request: WriteRequest) -> int:
        if len(request.value) != self.page_size:
            raise InvalidWriteRequest(
                f"Value must be of length {self.page_size}, got {len(request.value)}"
            )
        with self.lock:
            offset = request.offset
            if offset is None:
                # Seek to the end
                self.file.seek(0, 2)
                # See where we're at in the file, assign that as the offset
                offset = self.file.tell()
            self.file.seek(offset)
            self.file.write(request.value)
        return offset

    def read(self: "IOHandler", request: ReadRequest) -> bytes:
        buf = bytearray(self.page_size)
        with self.lock:
            self.file.seek(request.offset)
            # file has a readinto, so why is type checking complaining?
            count = self.file.readinto(buf)  # type: ignore
            assert count == self.page_size
        return buf