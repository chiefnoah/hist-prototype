
from .storage_types import DataLogEntry
from .types import WriteRequest
from .storage import IOHandler

class DataLogger:

    io: IOHandler
    
    def __init__(self: "DataLogger", io: IOHandler):
        self.io = io
    
    def write(self: "DataLogger", record: DataLogEntry, tx: int) -> int:
        """Writes data to the data log and returns the offset."""
        return write_record(self.io, record, tx)
    
    def read(self: "DataLogger", offset: int, size: int) -> DataLogEntry:
        """Reads a data log entry from the data log."""
        return read_record(self.io, offset, size)

def write_record(io: IOHandler, record: DataLogEntry, tx: int) -> int:
    """Writes data to the data log and returns the offset."""
    request = WriteRequest(
        offset=None,
        delete=False,  # there are never deletes in the data logger
        value=record.serialize(),
        tx=tx
    )
    return io.write(request)

def read_record(io: IOHandler, offset: int, size: int) -> DataLogEntry:
    """Reads a data log entry from the data log."""
    buf = io.read(offset, size)
    return DataLogEntry.deserialize(buf)