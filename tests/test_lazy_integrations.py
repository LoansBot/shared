"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
import secrets
from lblogging import Level

sys.path.append("../src")

from lbshared.lazy_integrations import LazyIntegrations  # noqa: E402


class TestIntegrations(unittest.TestCase):
    def test_database(self):
        with LazyIntegrations() as itgs:
            itgs.read_cursor.execute('SELECT NOW()')
            row = itgs.read_cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(len(row), 1)

    def test_amqp(self):
        with LazyIntegrations() as itgs:
            itgs.channel.queue_declare('test_integrations')
            pub_body = secrets.token_urlsafe(16).encode('utf-8')
            itgs.channel.basic_publish(
                exchange='', routing_key='test_integrations', body=pub_body
            )
            for mf, props, body in itgs.channel.consume('test_integrations', inactivity_timeout=5):
                self.assertIsNotNone(mf)
                itgs.channel.basic_ack(mf.delivery_tag)
                itgs.channel.cancel()
                self.assertEqual(body, pub_body)
                break

    def test_logger(self):
        with LazyIntegrations() as itgs:
            itgs.logger.print(Level.DEBUG, 'Hello world!')

    def test_cache(self):
        with LazyIntegrations() as itgs:
            key = 'test_integrations'
            val = secrets.token_urlsafe(16).encode('utf-8')
            itgs.cache.set(key, val, expire=1)
            self.assertEqual(itgs.cache.get(key), val)
            itgs.cache.delete(key)
            self.assertIsNone(itgs.cache.get(key))


if __name__ == '__main__':
    unittest.main()
