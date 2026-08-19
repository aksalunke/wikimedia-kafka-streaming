from consumer.consumer import window_start_for, is_partial


class TestWindowStartFor:
    def test_snaps_down_to_boundary(self):
        assert window_start_for(610, window_seconds=300) == 600

    def test_exact_boundary_stays_put(self):
        assert window_start_for(600, window_seconds=300) == 600

    def test_just_before_next_boundary(self):
        assert window_start_for(899, window_seconds=300) == 600

    def test_small_window_size(self):
        # matches WINDOW_SECONDS = 30, used for fast local testing
        assert window_start_for(75, window_seconds=30) == 60


class TestIsPartial:
    def test_full_window_not_partial(self):
        assert is_partial(300.0, window_seconds=300) is False

    def test_startup_fragment_is_partial(self):
        # real value observed from a Ctrl+C mid-window
        assert is_partial(11.4, window_seconds=30) is True

    def test_window_with_poll_drift_still_not_partial(self):
        # real value observed on a genuinely complete window
        assert is_partial(30.2, window_seconds=30) is False