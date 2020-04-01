"""A collection of convenience functions for formatting responses."""
from collections import defaultdict
from .lazy_integrations import LazyIntegrations
from lblogging import Level
from pypika import PostgreSQLQuery as Query, Table, Parameter
import traceback


def get_response(itgs: LazyIntegrations, name: str, **replacements):
    """Get the formatted response with the given name using the given
    substitutions. Every substitution is formatted using str(), if replacements
    is missing any keys the response expects the event will be logged but a
    response will be returned. If replacements contains keys not expected
    by the response they are simply ignored.

    @param [LazyIntegrations] itgs The lazy integrations to use for connecting
        to the database and/or logger. This will only use a read cursor on the
        database.
    @param [str] name The name of the response to format
    @param [dict] replacements A map from the names of keys to substitute within
      the response body to an object which will be stringified.
    @return [str] The formatted response.
    """
    responses = Table('responses')
    itgs.read_cursor.execute(
        Query.from_(responses).select(responses.response_body)
        .where(responses.name == Parameter('%s')).limit(1).get_sql(),
        (name,)
    )
    row = itgs.read_cursor.fetchone()
    if row is None:
        itgs.logger.print(
            Level.WARN,
            'There was a request to format the response {} which is not a '
            'response name for which a response format exists! '
            'Stack trace:\n{}',
            name, ''.join(traceback.format_stack())
        )
        return f'ERROR: Unknown response: "{name}"'
    (unformatted,) = row

    def factory(key):
        itgs.logger.print(
            Level.WARN,
            'While formatting response {} there was a request to substitute {} '
            'but the only known substitutions are {}',
            name, key, ', '.join(replacements.keys())
        )
        return f'[ERROR: unknown substitution "{key}"]'

    format_dict = defaultdict(factory)
    return unformatted.format_map(format_dict)


def get_letter_response(itgs: LazyIntegrations, base_name: str, **replacements):
    """This is a helper method for formatting "letter responses", which are
    responses which have a title and body and the same substitutions are used
    for both. In practice some of the replacements for the body might look
    silly in the title, but it doesn't hurt to expose them there.

    This assumes that the response names are `f'{base_name}_title'` and
    `f'{base_name}_body'` respectively.

    @param [LazyIntegrations] itgs The lazy integrations to use for connecting
        to the database and/or logger. This will only use a read cursor on the
        database.
    @param [str] base_name The base of the response names for the title and
        body. Suffixed with _title and _body for the title and body
        respectively.
    @param [dict] replacements A dictionary where keys are the names of keys
        in the response to substitute, as if by format_map.
    @return [str, str] Two tuples; the first is the formatted title and the
        second is the formatted body.
    """
    return (
        get_response(itgs, base_name + '_title', **replacements),
        get_response(itgs, base_name + '_body', **replacements)
    )
