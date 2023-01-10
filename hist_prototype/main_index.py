from .storage import IOHandler

class MainIndexer:
    io: IOHandler

    def __init__(self: "MainIndexer", io: IOHandler):
        self.io = io
        
    