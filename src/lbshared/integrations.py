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
from . import arango


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
    This doesn't actually do anything, since Arango operates on a rest API,
    but it returns the convienent object for using that rest api

    @return [Connection] The ArangoDB rest API wrapper
    """
    arango_urls = os.environ.get('ARANGO_URLS', 'http://localhost:8529').split(',')
    arango_username = os.environ.get('ARANGO_USERNAME', 'root')
    arango_password = os.environ.get('ARANGO_PASSWORD', '')

    return arango.Connection(
        arango.Cluster(arango_urls),
        arango.BasicAuth(arango_username, arango_password)
    )
