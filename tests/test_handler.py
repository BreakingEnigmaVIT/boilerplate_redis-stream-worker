"""Placeholder tests for ``handler.process_task``.

Replace or extend these once domain logic is implemented.
"""

import pytest

from worker.handler import process_task


@pytest.mark.asyncio
async def test_process_task_stub_contract():
    result = await process_task({"task_id": "t-123"})
    assert isinstance(result, dict)
    assert result.get("status") == "ok"
