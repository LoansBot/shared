"""Verifies that all the connections that can be created using the integrations
module are able to service simple requests"""
import unittest
import sys
from pypika import Query, Table

sys.path.append("../src")

import lbshared.pypika_crits  # noqa: E402


class TestIntegrations(unittest.TestCase):
    def test_exists_no_joins(self):
        users = Table('users')
        cats = Table('cats')
        self.assertEqual(
            Query.from_(users).select(users.id).where(
                lbshared.pypika_crits.exists(
                    Query.from_(cats).where(cats.user_id == users.id)
                )
            ).get_sql(),
            (
                'SELECT "id" FROM "users" WHERE '
                + 'EXISTS (SELECT FROM "cats" WHERE "cats"."user_id"="users"."id")'
            )
        )

    def test_exists_with_joins(self):
        cats = Table('cats')
        users = Table('users')
        self.assertEqual(
            lbshared.pypika_crits.exists(
                Query.from_(cats)
                .join(users).on(users.id == cats.user_id)
                .where(cats.name == 'Kitty')
            )
            .get_sql(),
            (
                'EXISTS (SELECT FROM "cats" JOIN "users" ON "users"."id"="cats"."user_id"'
                + ' WHERE "cats"."name"=\'Kitty\')'
            )
        )


if __name__ == '__main__':
    unittest.main()
