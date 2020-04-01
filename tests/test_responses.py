"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
from pypika import PostgreSQLQuery as Query, Table, Parameter

sys.path.append("../src")

import lbshared.responses as responses  # noqa: E402
from lbshared.lazy_integrations import LazyIntegrations  # noqa: E402


class TestResponses(unittest.TestCase):
    def test_missing(self):
        with LazyIntegrations() as itgs:
            res = responses.get_response(itgs, 'my_missing_key')
            self.assertIsInstance(res, str)
            self.assertIn('my_missing_key', res)

    def test_existing(self):
        responses = Table('responses')
        with LazyIntegrations() as itgs:
            itgs.write_cursor.execute(
                Query.into(responses).columns(
                    responses.name,
                    responses.response_body,
                    responses.description
                ).insert(*[Parameter('%s') for _ in range(3)])
                .returning(responses.id).get_sql(),
                (
                    'my_response',
                    'I like to {foo} the {bar}',
                    'Testing desc'
                )
            )
            (respid,) = itgs.write_cursor.fetchone()
            try:
                itgs.write_conn.commit()
                res: str = responses.get_response(itgs, 'my_response', foo='open', bar='door')
                self.assertEqual(res, 'I like to open the door')

                res: str = responses.get_response(itgs, 'my_response', foo='eat', buzz='bear')
                self.assertIsInstance(res, str)
                self.assertTrue(res.startswith('I like to eat the '), res)
                # it's not important how we choose to format the error, but it
                # needs the missing key or debugging will be a pain
                self.assertIn('bar', res)
            finally:
                itgs.write_conn.rollback()
                itgs.write_cursor.execute(
                    Query.from_(responses).delete()
                    .where(responses.id == Parameter('%s'))
                    .get_sql(),
                    (respid,)
                )
                itgs.write_conn.commit()


if __name__ == '__main__':
    unittest.main()
