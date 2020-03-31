"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
import signal
import os

sys.path.append("../src")

import lbshared.signal_helper as signal_helper  # noqa: E402


class TestIntegrations(unittest.TestCase):
    def test_delay(self):
        saw_sigterm = False

        def capture_sigterm(*args, **kwargs):
            nonlocal saw_sigterm
            saw_sigterm = True

        og_sigterm_handler = signal.signal(signal.SIGTERM, capture_sigterm)
        try:
            with signal_helper.delay_signals():
                self.assertFalse(saw_sigterm)
                if hasattr(signal, 'raise_signal'):
                    # 3.8+
                    signal.raise_signal(signal.SIGTERM)
                else:
                    os.kill(os.getpid(), signal.SIGTERM)

                self.assertFalse(saw_sigterm)
            self.assertTrue(saw_sigterm)
        finally:
            signal.signal(signal.SIGTERM, og_sigterm_handler)


if __name__ == '__main__':
    unittest.main()
