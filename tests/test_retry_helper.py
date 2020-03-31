"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
import secrets
import time

sys.path.append("../src")

import lbshared.retry_helper as retry_helper  # noqa: E402


class TestIntegrations(unittest.TestCase):
    def test_handle(self):
        now = time.time()
        retry_helper.handle(1)
        self.assertLess(time.time(), now + 0.5)
        now = time.time()
        retry_helper.handle(1)
        self.assertGreaterEqual(time.time(), now + 1)


if __name__ == '__main__':
    unittest.main()
