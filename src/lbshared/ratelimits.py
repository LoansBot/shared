"""Implements a fast Arango-backed ratelimitter, using the same algorithm as
https://github.com/smyte/ratelimit

If a users refill rate or maximum is changed, it will be applied retroactively
since the last time they consumed a token.
"""
from pydantic import BaseModel
import time
from lblogging import Level
import math


class Settings(BaseModel):
    """Describes the settings for a ratelimit. These can be changed at any
    time and may be different between consumers within the same collection.
    If the settings are changed for a particular consumer it is applied only
    when the resource is next consumed and it is applied retroactively since
    the same time a resource was consumed.

    Attributes:
        collection_name (str): The name of the arango collection where
            consumption history is stored.
        max_tokens (int): The maximum number of tokens the consumer can have
            in their pool.
        refill_amount (int): Every refill_time_ms ms, the consumer is given
            this number of tokens, so long as it does not push them over the
            maximum.
        refill_time_ms (int): The number of milliseconds between token refills.
        strict (bool): If true then attempting to consume a token when there
            are no available tokens will reset the timer for when you get your
            next token. This dissuades users from constantly retrying failed
            attempts.
    """
    collection_name: str
    max_tokens: int
    refill_amount: int
    refill_time_ms: int
    strict: bool


def setup_tokens_collection(itgs, settings):
    """Ensures the existence of the specified collection in arango for the
    purpose of storing our tokens."""
    coll = itgs.kvs_db.collection(settings.collection_name)
    return coll.create_if_not_exists(ttl=1)


def consume(itgs, settings, consumer, amt) -> bool:
    """Consumes the given number of tokens from the given consumer using the
    given settings. The settings for the consumer can change so long as the
    collection name is consistent and the ratelimit information will be
    seamlessly interpolated.

    This endpoint is concurrency-safe but not fair. That is to say, if there
    are many requests to consume the same resource we do not promise that the
    earlier requests will get the tokens. Furthermore if there is too much
    contention we will not consume tokens and return False even if there might
    be tokens available.

    Arguments:
        itgs (LazyIntegrations): The lazy integrations for connecting to arango
        settings (Settings): The ratelimit settings
        consumer (str): The unique identifier for the consumer. For resources
            which are shared by all consumers just use any fixed value here.
        amt (int): The amount of tokens to consume
        retry (int, None): This endpoint

    Returns:
        True if all amt tokens were available and consumed, False if they were
        not all available and the request should be rejected.
    """
    for i in range(8):
        try:
            result = _consume(itgs, settings, consumer, amt)
        except:  # noqa
            if i != 0:
                raise
            result = None
            itgs.logger.exception(Level.WARN)

        if result is not None:
            return result

        if i == 0:
            if setup_tokens_collection(itgs, settings):
                itgs.logger.print(
                    Level.INFO,
                    'lbshared.ratelimits auto-created tokens collection {}',
                    settings.collection_name
                )

        time.sleep(min(0.05 * (2 ** i), 1))

    return False


def _consume(itgs, settings, consumer, amt):
    doc = itgs.kvs_db.collection(settings.collection_name).document(consumer)

    existed = doc.read()
    cur_time = time.time()
    if not existed:
        doc.body = {
            'tokens': settings.max_tokens,
            'last_refill': cur_time
        }
    else:
        time_since_refill = cur_time - doc.body['last_refill']
        num_refills = int((time_since_refill * 1000) / settings.refill_time_ms)
        doc.body['tokens'] = min(
            settings.max_tokens,
            doc.body['tokens'] + num_refills * settings.refill_amount
        )
        doc.body['last_refill'] += num_refills * (settings.refill_time_ms / 1000.0)

    if doc.body['tokens'] >= amt:
        doc.body['tokens'] -= amt
        consumed = True
    else:
        if settings.strict:
            doc.body['last_refill'] = cur_time
        consumed = False

    refills_until_full = math.ceil(
        float(settings.max_tokens - doc.body['tokens']) / settings.refill_amount
    )
    time_full_at = doc.body['last_refill'] + (refills_until_full * settings.refill_time_ms / 1000.0)
    seconds_until_full = time_full_at - cur_time
    ttl = math.ceil(seconds_until_full)

    if existed:
        success = doc.compare_and_swap(ttl=ttl)
    else:
        success = doc.create(ttl=ttl)

    if not success:
        return None
    return consumed
