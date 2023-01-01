from threading import RLock
from typing import BinaryIO
from .types import WriteRequest

class InvalidWriteRequest(ValueError):
    ...

class IOHandler:
    file: BinaryIO
    lock: RLock

    def __init__(self: "IOHandler", file: BinaryIO) -> None:
        self.file = file
        self.lock = RLock()

    def write(self: "IOHandler", request: WriteRequest) -> int:
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

    def read(self: "IOHandler", offset: int, size: int) -> bytes:
        buf = bytearray(size)
        with self.lock:
            self.file.seek(offset)
            # file has a readinto, so why is type checking complaining?
            count = self.file.readinto(buf)  # type: ignore
            # TODO: raise a better error here
            assert count == size
        return bytes(buf)