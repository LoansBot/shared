"""Test the queries module"""
import unittest
import sys
import secrets

sys.path.append("../src")

import lbshared.queries  # noqa: E402


class TestQueries(unittest.TestCase):
    def test_convert_numbered_args_no_args(self):
        (query, args) = lbshared.queries.convert_numbered_args(
            'SELECT * FROM foo',
            []
        )
        self.assertEqual(query, 'SELECT * FROM foo')
        self.assertIsInstance(args, tuple)
        self.assertEqual(len(args), 0)

    def test_convert_numbered_args_one_arg(self):
        (query, args) = lbshared.queries.convert_numbered_args(
            'SELECT * FROM foo WHERE id = $1',
            ('baz',)
        )
        self.assertEqual(query, 'SELECT * FROM foo WHERE id = %s')
        self.assertEqual(args, ('baz',))

    def test_convert_numbered_args_two_args(self):
        (query, args) = lbshared.queries.convert_numbered_args(
            'SELECT * FROM foo JOIN bar ON bar.id = $2 WHERE foo.id = $1',
            (3, 7)
        )
        self.assertEqual(query, 'SELECT * FROM foo JOIN bar ON bar.id = %s WHERE foo.id = %s')
        self.assertEqual(args, (7, 3))

    def test_convert_numbered_args_duplicated_arg(self):
        (query, args) = lbshared.queries.convert_numbered_args(
            'SELECT * FROM foo WHERE ($1 IS NULL OR foo.id > $1)',
            (123,)
        )
        self.assertEqual(query, 'SELECT * FROM foo WHERE (%s IS NULL OR foo.id > %s)')
        self.assertEqual(args, (123, 123))

    def test_convert_numbered_args_duplicate_and_multiple(self):
        # This query doesn't make sense but that's not the point
        (query, args) = lbshared.queries.convert_numbered_args(
            'SELECT * FROM foo WHERE ($2 IS NULL OR foo.id > $1) AND ($1 IS NULL OR foo.id < $2)',
            (123, 76)
        )
        self.assertEqual(
            query,
            'SELECT * FROM foo WHERE (%s IS NULL OR foo.id > %s) AND (%s IS NULL OR foo.id < %s)'
        )
        self.assertEqual(args, (76, 123, 123, 76))


if __name__ == '__main__':
    unittest.main()
