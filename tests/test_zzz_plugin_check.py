import anyio
import pytest

@pytest.mark.anyio
async def test_trivial():
    await anyio.sleep(0)
