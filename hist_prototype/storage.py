from threading import RLock
from hist_prototype.leaf_node import ReadRequest, WriteRequest
from io import BytesIO

class InvalidWriteRequest(ValueError):
    ...

class IOHandler:
    file: BytesIO
    page_size: int
    lock: RLock

    def __init__(self: "IOHandler", file: BytesIO, page_size: int = 256) -> None:
        self.file = file
        self.page_size = page_size
        self.lock = RLock()

    def write(self: "IOHandler", request: WriteRequest) -> int:
        if len(request.value) != self.page_size:
            raise InvalidWriteRequest(
                f"Value must be of length {self.page_size}, got {len(request.value)}"
            )
        with self.lock:
            if request.offset is None:
                # Seek to the end
                self.file.seek(0, 2)
                # See where we're at in the file, assign that as the offset
                request.offset = self.file.tell()
            self.file.seek(request.offset)
            self.file.write(request.value)
        return request.offset

    def read(self: "IOHandler", request: ReadRequest) -> bytes:
        buf = bytearray(self.page_size)
        with self.lock:
            self.file.seek(request.offset)
            count = self.file.readinto(buf)
            assert count == self.page_size
        return buf
