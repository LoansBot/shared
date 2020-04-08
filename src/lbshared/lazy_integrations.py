"""
This package provides a very simple interface to all the common integrations
(such as the database). It is written as a context-manager, and will lazily
load any connection upon request. It contains the logical split between
read-only and read/write database connections although it is not currently
used.

@example
    from lbshared.lazy_integrations import LazyIntegrations
    from lblogging import Level

    with LazyIntegrations() as itgs:
        itgs.logger.print(Level.DEBUG, 'no connection boilerplate!')
        itgs.read_cursor.execute(
            "SELECT NOW()"
        )
        print(f'db now={itgs.read_cursor.fetchone()[0]}')
    print('look! all cleaned up!')
"""
from . import integrations as itgs
from lblogging import Logger
import os


class LazyIntegrations:
    """Contains lazily-loaded connection properties, which are cleaned up
    when this object is exited. This object is a context manager.

    This is intended to reduce boilerplate around integrations as much as
    possible while being extensible in the future. Hence the interface provides
    the logger as its own integration (so that the connection can autocommit
    and logging can be split into its own database in the future), and is
    prepared for having follower-databases which are read-only.

    The read-connection should be assumed to be up to a few minutes behind the
    write-connection. Hence when using both of them there may not be
    consistency. To alleviate this, set "no_read_only" to True and the read
    connection will be the same as the write connection.

    Note that callees should not rely on having separate transactions between
    the read and write cursors. In most scenarios either only the read cursor
    should be used or no_read_only should be True.

    @param [bool] no_read_only If True, this MAY NOT initiate a separate
        read-able connection for the database. Otherwise, this MAY initiate
        a separate connection.
    @param [str] logger_iden The identifier to initialize the logger with;
        typically the file and function initializing the lazy integrations.
    """
    def __init__(self, no_read_only=False, logger_iden='lazy_integrations.py#logger'):
        self.closures = []
        self.no_read_only = no_read_only
        self.logger_iden = logger_iden
        self._logger = None
        self._conn = None
        self._cursor = None
        self._amqp = None
        self._channel = None
        self._cache = None
        self._arango_conn = None
        self._arango_db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        errors = []
        for closure in self.closures:
            try:
                closure(exc_type, exc_value, traceback)
            except Exception as e:  # noqa
                # we need to delay these to give other closures
                # an opportunity
                errors.append(e)

        if len(errors) == 1:
            raise errors[0]
        elif errors:
            raise Exception(f'Many errors while shutting down integrations: {errors}')
        return False

    @property
    def logger(self):
        """Fetch the logger instance, which will auto-commit"""
        if self._logger is not None:
            return self._logger

        logger_conn = itgs.database()
        logger_conn.autocommit = True
        self._logger = Logger(
            os.environ.get('APPNAME', 'lbshared'),
            self.logger_iden,
            logger_conn
        )
        self._logger.prepare()

        def closure(*args):
            self._logger.connection.close()

        self.closures.append(closure)
        return self._logger

    @property
    def read_cursor(self):
        """Fetches a database cursor that is only promised to support
        reads. This may be the same connection as write_conn."""
        return self.write_cursor

    @property
    def write_conn(self):
        """Fetch the writable database connection"""
        return self.write_conn_and_cursor[0]

    @property
    def write_cursor(self):
        "Fetch the writable database cursor"
        return self.write_conn_and_cursor[1]

    @property
    def write_conn_and_cursor(self):
        """Returns the writable database connection alongside the cursor. The
        connection can be used to commit."""
        if self._conn is not None:
            return (self._conn, self._cursor)

        self._conn = itgs.database()
        self._cursor = self._conn.cursor()

        def closure(*args):
            self._cursor.close()
            self._conn.close()

        self.closures.append(closure)
        return (self._conn, self._cursor)

    @property
    def amqp(self):
        """Get the advanced message queue pika instance, which is really
        only necessary if you need to declare custom channels"""
        return self.amqp_and_channel[0]

    @property
    def channel(self):
        """The AMQP channel to use."""
        return self.amqp_and_channel[1]

    @property
    def amqp_and_channel(self):
        """Get both the AMQP pika instance and the channel we are using"""
        if self._amqp is not None:
            return (self._amqp, self._channel)

        self._amqp = itgs.amqp()
        self._channel = self._amqp.channel()
        self._channel.confirm_delivery()

        def closure(*args):
            self._channel.close()
            self._amqp.close()

        self.closures.append(closure)

        return (self._amqp, self._channel)

    @property
    def cache(self):
        """Get the memcached client"""
        if self._cache is not None:
            return self._cache

        self._cache = itgs.cache()

        def closure(*args):
            self._cache.close()

        self.closures.append(closure)
        return self._cache

    @property
    def kvs_conn(self):
        """Get the connection for ArangoDB"""
        if self._arango_conn is not None:
            return self._arango_conn

        self._arango_conn = itgs.kvstore()

        def closure(*args):
            self._arango_conn.close()

        self.closures.append(closure)
        return self._arango_conn

    @property
    def kvs_db(self):
        """Get the Arango DB which all collections are in"""
        if self._arango_db is not None:
            return self._arango_db

        self._arango_db = self.kvs_conn.createDatabase(
            name=os.environ['ARANGO_DB']
        )
        return self._arango_db
