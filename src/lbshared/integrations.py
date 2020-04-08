"""
This package provides a light wrapper around the suggested environment
variables to open connections to the common integrations (The database,
RabbitMQ, etc).

This module is typically not used directly as it becomes very tedious to
track all of the instances very quickly. Instead, use LazyIntegrations so that
the actual opening and closing of connections can be masked.

Note that one should understand the
https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing
when using this. The key idea is that if any one of these connections go down,
the service should naturally recover soon after it comes back up.

One extremely painful attempt at this was by trying to restart the connection
whenever it dropped. This becomes extremely gnarly quickly, so the way this
and related packages are written is with the assumption that your program is
capable of being restarted relatively non-dangerously, and automatically
restarts on errors. Use the restart helper to ensure that getting restarted in
this way will not cause connection hammering (ie., backoff your attempts to
reconnect a little bit).

@see lazy_integrations An extensible and fairly efficient way to manage
    initializing and passing connections around to various services.
@see retry_helper Sleep a little bit between restarts to avoid rapidly cycling
    processes while a required integration is down.
@see signal_helper Delay optional signals (e.g. SIGINT) for a short period of
    time while in an important transaction.
"""
import psycopg2
import pika
from pymemcache.client import base as membase
import os
from pyArango.connection import Connection


def database():
    """
    Opens a new connection to the database.

    @return The psycopg2 connection to a write/read database.
    """
    return psycopg2.connect('')


def amqp():
    """
    Opens a new connection to the AMQP server.

    @return BlockingConnection A pika blocking connection to the AMQP server
    """
    parameters = pika.ConnectionParameters(
        os.environ['AMQP_HOST'],
        int(os.environ['AMQP_PORT']),
        os.environ['AMQP_VHOST'],
        pika.PlainCredentials(
            os.environ['AMQP_USERNAME'], os.environ['AMQP_PASSWORD']
        )
    )
    return pika.BlockingConnection(parameters)


def cache():
    """
    Opens a connection to the Memcached server.

    @return [Client] The memcached client
    """
    memcache_host = os.environ['MEMCACHED_HOST']
    memcache_port = int(os.environ['MEMCACHED_PORT'])
    return membase.Client((memcache_host, memcache_port))


def kvstore():
    """
    Opens a connection to the ArangoDB server

    @return [Connection] The ArangoDB connection
    """
    arango_url = os.environ['ARANGO_URL']
    arango_username = os.environ['ARANGO_USERNAME']
    arango_password = os.environ['ARANGO_PASSWORD']
    print(f'kvstore connecting to url={arango_url} username={arango_username}')
    print(f'password={arango_password} (password is a {type(arango_password)})')
    return Connection(
        arangoURL=arango_url,
        username=arango_username,
        password=arango_password,
        max_retries=2
    )
