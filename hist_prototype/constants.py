# The maximum number of children nodes are allowed to have
MAX_CHILDREN: int = 170
# The maximum size of a value in bytes
# Must fit into a unsigned 32-bit integer (4,294,967,295) bytes
MAX_VALUE_SIZE: int = 1024 ** 2  # 1Mb

__all__ = ['MAX_CHILDREN']