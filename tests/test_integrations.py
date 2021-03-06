"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
import secrets

sys.path.append("../src")

import lbshared.integrations  # noqa: E402


class TestIntegrations(unittest.TestCase):
    def test_database(self):
        conn = lbshared.integrations.database()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT NOW()')
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(len(row), 1)
            cursor.close()
        finally:
            conn.close()

    def test_amqp(self):
        amqp = lbshared.integrations.amqp()
        try:
            channel = amqp.channel()
            channel.queue_declare('test_integrations')
            pub_body = secrets.token_urlsafe(16).encode('utf-8')
            channel.basic_publish(
                exchange='', routing_key='test_integrations', body=pub_body
            )
            for mf, props, body in channel.consume('test_integrations', inactivity_timeout=5):
                self.assertIsNotNone(mf)
                channel.basic_ack(mf.delivery_tag)
                channel.cancel()
                self.assertEqual(body, pub_body)
                break
            channel.close()
        finally:
            amqp.close()

    def test_cache(self):
        client = lbshared.integrations.cache()
        try:
            key = 'test_integrations'
            val = secrets.token_urlsafe(16).encode('utf-8')
            client.set(key, val, expire=1)
            self.assertEqual(client.get(key), val)
            client.delete(key)
            self.assertIsNone(client.get(key))
        finally:
            client.close()

    def test_kvs(self):
        conn = lbshared.integrations.kvstore()
        db = conn.database('test_db')
        self.assertTrue(db.create_if_not_exists())
        coll = db.collection('test_coll')
        self.assertTrue(coll.create_if_not_exists())

        key = secrets.token_urlsafe()
        my_secret = secrets.token_urlsafe()
        doc = coll.document(key)
        doc.body['my_secret'] = my_secret
        self.assertTrue(doc.create())

        doc = coll.document(key)
        self.assertTrue(doc.read())
        self.assertEqual(doc.key, key)
        self.assertEqual(doc.body.get('my_secret'), my_secret)

        my_secret = secrets.token_urlsafe()
        doc.body['my_secret'] = my_secret
        self.assertTrue(doc.compare_and_swap())

        doc = coll.document(key)
        self.assertTrue(doc.read())
        self.assertEqual(doc.key, key)
        self.assertEqual(doc.body.get('my_secret'), my_secret)

        self.assertTrue(doc.compare_and_delete())
        self.assertTrue(coll.force_delete())
        self.assertTrue(db.force_delete())


if __name__ == '__main__':
    unittest.main()
