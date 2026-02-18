"""
Unit tests for the retry decorator (Fix #6).
"""

import time
import pytest
from unittest.mock import patch, MagicMock

from src.utils.retry import retry_on_failure, DEFAULT_MAX_RETRIES
from src.errors.exceptions import DatabaseError


class FakeTransientError(Exception):
    """Simulates a transient MongoDB error."""
    pass


class TestRetryOnFailure:
    """Test the retry_on_failure decorator."""

    def test_succeeds_first_try(self):
        """Function succeeds on first call — no retries needed."""
        call_count = 0

        @retry_on_failure(retryable_exceptions=(FakeTransientError,))
        def my_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = my_func()
        assert result == "ok"
        assert call_count == 1

    def test_retries_then_succeeds(self):
        """Function fails twice then succeeds on third try."""
        call_count = 0

        @retry_on_failure(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(FakeTransientError,),
        )
        def my_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise FakeTransientError("transient!")
            return "recovered"

        result = my_func()
        assert result == "recovered"
        assert call_count == 3

    def test_exhausts_retries_raises(self):
        """Function always fails — raises DatabaseError after exhausting retries."""

        @retry_on_failure(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(FakeTransientError,),
        )
        def my_func():
            raise FakeTransientError("always fails!")

        with pytest.raises(DatabaseError, match="failed after 2 retries"):
            my_func()

    def test_non_retryable_exception_propagates(self):
        """Non-retryable exceptions are NOT caught by the retry decorator."""

        @retry_on_failure(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(FakeTransientError,),
        )
        def my_func():
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            my_func()

    def test_exponential_backoff_timing(self):
        """Verify that delays increase exponentially."""
        call_times = []

        @retry_on_failure(
            max_retries=3,
            base_delay=0.05,
            backoff_factor=2.0,
            retryable_exceptions=(FakeTransientError,),
        )
        def my_func():
            call_times.append(time.time())
            if len(call_times) < 4:
                raise FakeTransientError("retry me")
            return "done"

        result = my_func()
        assert result == "done"
        assert len(call_times) == 4

        # Check delays are roughly increasing
        delay1 = call_times[1] - call_times[0]
        delay2 = call_times[2] - call_times[1]
        delay3 = call_times[3] - call_times[2]
        assert delay2 > delay1 * 1.5  # Backoff factor should make it grow

    def test_preserves_function_metadata(self):
        """Decorator preserves the wrapped function's name and docstring."""

        @retry_on_failure(retryable_exceptions=(FakeTransientError,))
        def documented_func():
            """This is my docs."""
            return 42

        assert documented_func.__name__ == "documented_func"
        assert "my docs" in documented_func.__doc__

    def test_max_delay_cap(self):
        """Delay should not exceed max_delay."""
        call_times = []

        @retry_on_failure(
            max_retries=5,
            base_delay=0.5,
            max_delay=0.1,  # Cap is LOWER than base — should clamp
            backoff_factor=10.0,
            retryable_exceptions=(FakeTransientError,),
        )
        def my_func():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise FakeTransientError("retry")
            return "done"

        result = my_func()
        assert result == "done"
        # Delays should be capped at 0.1s
        for i in range(1, len(call_times)):
            delay = call_times[i] - call_times[i - 1]
            assert delay < 0.3  # generous margin


class TestGridFSOrphanTracker:
    """Test the GridFSOrphanTracker (Fix #1)."""

    def test_track_and_clear(self):
        from src.services.gridfs_service import GridFSOrphanTracker
        from bson import ObjectId

        tracker = GridFSOrphanTracker()
        mock_bucket = MagicMock()
        oid = ObjectId()

        tracker.track(mock_bucket, oid)
        assert tracker.pending_count == 1

        tracker.clear()
        assert tracker.pending_count == 0
        mock_bucket.delete.assert_not_called()

    def test_track_and_cleanup(self):
        from src.services.gridfs_service import GridFSOrphanTracker
        from bson import ObjectId

        tracker = GridFSOrphanTracker()
        mock_bucket = MagicMock()
        oid1 = ObjectId()
        oid2 = ObjectId()

        tracker.track(mock_bucket, oid1)
        tracker.track(mock_bucket, oid2)
        assert tracker.pending_count == 2

        cleaned = tracker.cleanup()
        assert cleaned == 2
        assert tracker.pending_count == 0
        assert mock_bucket.delete.call_count == 2

    def test_cleanup_handles_errors(self):
        from src.services.gridfs_service import GridFSOrphanTracker
        from bson import ObjectId

        tracker = GridFSOrphanTracker()
        mock_bucket = MagicMock()
        mock_bucket.delete.side_effect = Exception("delete failed!")

        tracker.track(mock_bucket, ObjectId())
        # Should not raise, just log the error
        cleaned = tracker.cleanup()
        assert cleaned == 0

    def test_track_config_and_cleanup(self):
        from src.services.gridfs_service import GridFSOrphanTracker
        from bson import ObjectId

        tracker = GridFSOrphanTracker()
        mock_bucket = MagicMock()
        mock_collection = MagicMock()
        gridfs_id = ObjectId()
        config_id = ObjectId()

        tracker.track(mock_bucket, gridfs_id)
        tracker.track_config(mock_collection, config_id)
        assert tracker.pending_count == 2

        cleaned = tracker.cleanup()
        assert cleaned == 2
        mock_bucket.delete.assert_called_once_with(gridfs_id)
        mock_collection.delete_one.assert_called_once_with({"_id": config_id})
        assert tracker.pending_count == 0

    def test_track_config_clear(self):
        from src.services.gridfs_service import GridFSOrphanTracker
        from bson import ObjectId

        tracker = GridFSOrphanTracker()
        mock_collection = MagicMock()
        tracker.track_config(mock_collection, ObjectId())
        assert tracker.pending_count == 1

        tracker.clear()
        assert tracker.pending_count == 0
        mock_collection.delete_one.assert_not_called()
