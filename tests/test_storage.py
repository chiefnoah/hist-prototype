import pytest
from io import BytesIO
from hist_prototype.storage import IOHandler
from hist_prototype.types import WriteRequest


@pytest.fixture
def iohandler() -> IOHandler:
    f = BytesIO()
    return IOHandler(f)


def test_simple_roundtrip(iohandler: IOHandler):
    req = WriteRequest(
        offset=None,
        delete=False,
        value=b'abcdefg',
        tx=123
    )
    iohandler.write(req)