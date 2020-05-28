"""Convenience functions for working with SQL queries"""
import pytypeutils as tus
import re


def convert_numbered_args(query, args):
    """Converts a query which was written using numbered args to the
    corresponding query using ordered arguments, making the required
    adjustments to the args array.

    This is best illustrated by example:

        (query, args) = convert_numbered_args(
            'SELECT * FROM foo WHERE bar = $2 AND baz > $1'
            (15, 'barval')
        )
        print(query) # 'SELECT * FROM foo WHERE bar = %s AND baz > %s'
        print(args) # ('barval', 15)

    This is extremely convenient if the order in which the query parameters are
    selected may differ from the order they appear in the query. This most
    commonly happens if a query may join with different tables depending on the
    arguments.

    This will assume it is given valid SQL and will make no attempts to verify
    the SQL. If there are gaps in the numbered parameters, or they do not start
    at 0, the behavior of this function is explicitly undefined.

    This does support numbered parameters which have duplicates, so for example

        (query, args) = convert_numbered_args(
            'SELECT * FROM foo WHERE bar = $1 AND baz > $1',
            (5,)
        )
        print(query) # 'SELECT * FROM foo WHERE bar = %s AND baz > %s'
        print(args) # (5, 5)

    Arguments:
        query (str): A SQL query string using numbered parameters.
        args (list, tuple): The list of arguments to pass to theq uery.

    Returns:
        query (str): The same SQL query using ordered parameters.
        args (tuple): The list of arguments to pass to the query.
    """
    tus.check(query=(query, str), args=(args, (list, tuple)))

    if not args:
        return (query, tuple())

    result_args = []
    pattern = r'\$(\d+)'

    for match in re.finditer(pattern, query):
        result_args.append(args[int(match.group(1)) - 1])

    result_query = re.sub(pattern, '%s', query)

    return (result_query, tuple(result_args))
