"""Test the queries module"""
import unittest
import sys
import time

sys.path.append("../src")

from lbshared.lazy_integrations import LazyIntegrations as LazyItgs  # noqa: E402
import lbshared.ratelimits  # noqa: E402


COLLECTION = 'test_ratelimits'


DEFAULT_SETTINGS = lbshared.ratelimits.Settings(
    collection_name=COLLECTION,
    max_tokens=10,
    refill_amount=3,
    refill_time_ms=50,
    strict=False
)


class TestRatelimits(unittest.TestCase):
    def setUp(self):
        with LazyItgs() as itgs:
            itgs.kvs_db.create_if_not_exists()

    def tearDown(self):
        with LazyItgs() as itgs:
            itgs.kvs_db.force_delete()

    def test_consume_from_new_with_initialized_coll(self):
        with LazyItgs() as itgs:
            itgs.kvs_db.collection(COLLECTION).create_if_not_exists(ttl=1)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 3))

    def test_consume_from_new_with_uninitialized_coll(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 3))

    def test_consume_more_than_max_from_new(self):
        with LazyItgs() as itgs:
            self.assertFalse(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 11))

    def test_consume_after_no_refills(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 5))
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 5))

    def test_consume_too_much_after_no_refills(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 5))
            self.assertFalse(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 6))

    def test_consume_after_single_partial_refill(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 5))
            time.sleep(55)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 6))

    def test_consume_after_multiple_partial_refill(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 10))
            time.sleep(105)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 6))
            time.sleep(105)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 5))
            self.assertFalse(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 2))

    def test_consume_after_multiple_to_complete_refill(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 10))
            time.sleep(205)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 10))
            # TTL can take way too long for us to actually test reliably here since it happens
            # occassionally in the background, so we'll just fake it
            itgs.kvs_db.collection(COLLECTION).force_delete_doc('foo')
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 10))

    def test_consume_more_than_available_not_strict(self):
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 10))
            time.sleep(20)
            self.assertFalse(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 3))
            time.sleep(20)
            self.assertFalse(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 3))
            time.sleep(20)
            self.assertTrue(lbshared.ratelimits.consume(itgs, DEFAULT_SETTINGS, 'foo', 3))

    def test_consume_more_than_available_strict(self):
        settings = lbshared.ratelimits.Settings(
            collection_name=COLLECTION,
            max_tokens=10,
            refill_amount=3,
            refill_time_ms=50,
            strict=True
        )
        with LazyItgs() as itgs:
            self.assertTrue(lbshared.ratelimits.consume(itgs, settings, 'foo', 10))
            time.sleep(20)
            self.assertFalse(lbshared.ratelimits.consume(itgs, settings, 'foo', 3))
            time.sleep(20)
            self.assertFalse(lbshared.ratelimits.consume(itgs, settings, 'foo', 3))
            time.sleep(20)
            self.assertFalse(lbshared.ratelimits.consume(itgs, settings, 'foo', 3))


if __name__ == '__main__':
    unittest.main()
