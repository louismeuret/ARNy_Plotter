"""
Unit tests for memory-management helpers in src/core/tasks.py
"""

import os
import sys
import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestCheckMemoryLimits:
    """check_memory_limits should return a 2-tuple and raise MemoryLimitExceeded
    when thresholds are exceeded."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.tasks import check_memory_limits, MemoryLimitExceeded
        self.check = check_memory_limits
        self.MemoryLimitExceeded = MemoryLimitExceeded

    def test_returns_two_tuple_normally(self):
        result = self.check("test_task", "test-id-001")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_available_ram_is_positive(self):
        avail, _ = self.check("test_task", "test-id-002")
        if avail is not None:
            assert avail > 0

    def test_process_memory_is_positive(self):
        _, proc = self.check("test_task", "test-id-003")
        if proc is not None:
            assert proc > 0

    def test_raises_when_min_ram_exceeded(self, monkeypatch):
        """Force a very high MIN_AVAILABLE_RAM_MB so it always triggers."""
        import src.core.tasks as tasks_module
        monkeypatch.setattr(tasks_module, "MIN_AVAILABLE_RAM_MB", 999_999_999)
        with pytest.raises(self.MemoryLimitExceeded):
            self.check("test_task", "test-id-004")

    def test_raises_when_process_memory_exceeded(self, monkeypatch):
        """Force a very low MAX_TASK_MEMORY_MB so it always triggers."""
        import src.core.tasks as tasks_module
        monkeypatch.setattr(tasks_module, "MAX_TASK_MEMORY_MB", 0)
        monkeypatch.setattr(tasks_module, "MIN_AVAILABLE_RAM_MB", 0)
        with pytest.raises(self.MemoryLimitExceeded):
            self.check("test_task", "test-id-005")


class TestMemoryCheckpoint:
    """memory_checkpoint is an alias / thin wrapper over check_memory_limits."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src.core.tasks import memory_checkpoint
        self.checkpoint = memory_checkpoint

    def test_callable(self):
        assert callable(self.checkpoint)

    def test_returns_two_tuple(self):
        result = self.checkpoint("test_task", "test-id-006")
        assert isinstance(result, tuple) and len(result) == 2

    def test_raises_memory_limit_exceeded_when_configured(self, monkeypatch):
        import src.core.tasks as tasks_module
        from src.core.tasks import MemoryLimitExceeded
        monkeypatch.setattr(tasks_module, "MIN_AVAILABLE_RAM_MB", 999_999_999)
        with pytest.raises(MemoryLimitExceeded):
            self.checkpoint("test_task", "test-id-007")


class TestMemoryLimitExceededClass:
    def test_is_exception(self):
        from src.core.tasks import MemoryLimitExceeded
        assert issubclass(MemoryLimitExceeded, Exception)

    def test_can_be_raised_and_caught(self):
        from src.core.tasks import MemoryLimitExceeded
        with pytest.raises(MemoryLimitExceeded):
            raise MemoryLimitExceeded("test message")

    def test_message_preserved(self):
        from src.core.tasks import MemoryLimitExceeded
        try:
            raise MemoryLimitExceeded("critical RAM exceeded")
        except MemoryLimitExceeded as exc:
            assert "critical RAM exceeded" in str(exc)
