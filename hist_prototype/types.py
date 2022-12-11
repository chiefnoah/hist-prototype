from typing import Protocol, TypeVar, Tuple, Dict, Any


class Serializable(Protocol):
    def serialize(self) -> bytes:
        ...


K = TypeVar("K", bound=Serializable)
V = TypeVar("V", bound=Serializable)

TX = int
Offset = int


# Errors
class NodeFullError(RuntimeError):
    def __init__(
        self,
        msg: str = "Node is full, cannot insert another node",
        *args: Tuple[Any],
        **kwargs: Dict[str, Any]
    ) -> None:
        super().__init__(msg, *args, **kwargs)
