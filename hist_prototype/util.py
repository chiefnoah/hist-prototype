def serialize_int(self: int, *args, **kwargs) -> bytes:
    """"""
    if "size" in kwargs:
        size = kwargs["size"]
    elif len(args) > 0:
        size = args[0]
    return self.to_bytes(size, "little", *args, **kwargs)
