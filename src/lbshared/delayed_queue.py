"""A utility for working with delayed queues. This uses postgres for storing
the actual queue and arango for storing event-specific context information.
This will initialize the arango collection if it does not exist with a TTL of
365 days.
"""
import uuid
import requests.exceptions
from pypika import PostgreSQLQuery as Query, Table, Parameter, Order
from lbshared.signal_helper import delay_signals
from datetime import datetime
from lblogging import Level

QUEUE_TYPES = {
    'trust': 0
}
"""Maps from pretty names to the corresponding unique queue type value."""


def store_event(itgs, queue_type, event_at, event, commit=False):
    """Store that the given event should occur at the given time with the
    given context information.

    Example:

    ```py
    import lbshared.delayed_queue as delayed_queue
    from lbshared.lazy_integrations import LazyIntegrations as LazyItgs
    from datetime import datetime, timedelta


    with LazyItgs(no_read_only=True) as itgs:
        delayed_queue.store_event(
            itgs,
            delayed_queue.QUEUE_TYPES['trust'],
            datetime.now() + timedelta(days=3),
            {
                'additional': 'information',
                'goes': ['h', 'e', 'r', 'e']
            },
            commit=True
        )
    ```

    Arguments:
    - `itgs (LazyIntegrations)`: The integrations to use to connect to
        networked components. Must not be read-only.
    - `queue_type (int)`: The type of queue to connect to
    - `event_at (datetime)`: The time when the event should occur
    - `event (any)`: Any additional context information surrounding the event;
        this will not be indexed and cannot be searched on.
    - `commit (bool)`: If true this will commit the database change
        immediately. Otherwise the transaction will be left open.

    Returns:
    - event_uuid (str): The uuid assigned to the event
    """
    event_uuid = uuid.uuid4()

    coll = itgs.kvs_db.collection('delayed_queue')
    try:
        coll.create_or_overwrite_doc(event_uuid, event)
    except requests.exceptions.HTTPError:
        coll.create_if_not_exists(ttl=31622400)
        coll.create_or_overwrite_doc(event_uuid, event)

    del_queue = Table('delayed_queue')
    itgs.write_cursor.execute(
        Query.into(del_queue)
        .columns(del_queue.uuid, del_queue.queue_type, del_queue.event_at)
        .insert(*[Parameter('%s') for _ in range(3)])
        .get_sql(),
        (event_uuid, queue_type, event_at)
    )
    if commit:
        itgs.write_conn.commit()


def index_events(
        itgs, queue_type, limit, before_time=None,
        after_time=None, order='asc', integrity_failures='include'):
    """Get the next up to limit events from the given queue, ordered from oldest
    event times (i.e., most in the past) to newest event times (i.e., most in
    the future).

    This will include the stored event details on each event. The nature of this
    storage technique is integrity errors wherein the event is in the queue but
    the information has been lost are possible. This includes a few different
    techniques for handling integrity failures (see integrity_failures).

    Example:

    ```py
    import lbshared.delayed_queue as delayed_queue
    from lbshared.lazy_integrations import LazyIntegrations as LazyItgs

    with LazyItgs() as itgs:
        next_5_events = delayed_queue.index_events(
            itgs,
            delayed_queue.QUEUE_TYPES['trust'],
            5,
            integrity_failures='delete_and_commit'
        )

        for (ev_uuid, ev_at, ev) in next_5_events:
            print(f'Found event {ev_uuid} scheduled for {ev_at}...')
    ```

    Arguments:
    - `itgs (LazyIntegrations)`: The lazy integrations to use for connecting to
        networked components. It's suggested that even if integrity failures is
        set to delete_and_commit a read-write connection is only opened if
        necessary (i.e., start with a read connection and then have it open a
        read-write connection in addition if there is an integrity failure).
    - `queue_type (int)`: The unique queue type identifier to fetch events from.
    - `limit (int)`: The maximum number of events to return.
    - `before_time (datetime, None)`: If specified events which are before a
        given date are ignored. Useful when you want past-due events.
    - `after_time (datetime, None)`: If specified events which have an event
        time earlier than this point are not considered. Useful for pagination.
    - `order (str)`: The order that results are returned in. Either 'asc' for
        oldest to newest or 'desc' for newest to oldest.
    - `integrity_failures (str)`: How to handle events whose event information
        have been lost. Options are as follows:
        - `include`: They are returned but the `event` is replaced with `None`.
            This should be used if any additional cleanup needs to be
            performed besides just deleting the event.
        - `delete_and_commit`: The events are deleted from the queue. Fewer
            results will be returned than requested, but this will resolve
            itself once all the integrity failures have been handled. This
            should be used if no additional cleanup needs to be performed.

    Returns:
    - `events (enumerable[tuple])`: Up to limit events from the queue, where
        each event is returned as a tuple of 3 items - the event uuid,
        the event time, and the event metadata that was stored.
    """
    del_queue = Table('delayed_queue')
    query = (
        Query.from_(del_queue)
        .select(del_queue.uuid, del_queue.event_at)
        .where(del_queue.queue_type == Parameter('%s'))
        .orderby(del_queue.event_at, order=getattr(Order, order))
        .limit(limit)
    )
    args = [queue_type]
    if before_time is not None:
        query = query.where(del_queue.event_at < Parameter('%s'))
        args.append(before_time)

    if after_time is not None:
        query = query.where(del_queue.event_at > Parameter('%s'))
        args.append(after_time)

    itgs.read_cursor.execute(
        query.get_sql(),
        args
    )
    unaugmented = itgs.read_cursor.fetchall()

    result = []
    coll = itgs.kvs_db.collection('delayed_queue')
    commit_required = False
    for (ev_uuid, ev_at) in unaugmented:
        event = coll.read_doc(ev_uuid)
        if event is None:
            if integrity_failures == 'include':
                result.append((ev_uuid, ev_at, None))
            elif integrity_failures == 'delete_and_commit':
                itgs.write_cursor.execute(
                    Query.from_(del_queue)
                    .delete()
                    .where(del_queue.uuid == Parameter('%s'))
                    .get_sql(),
                    (ev_uuid,)
                )
                commit_required = True
            else:
                raise Exception(f'bad integrity failure technique: {integrity_failures}')
        else:
            result.append((ev_uuid, ev_at, event))

    if commit_required:
        itgs.write_conn.commit()

    return result


def delete_event(itgs, event_uuid, commit=False):
    """Delete the event with the given uuid from the event queue. This also
    deletes the event metadata.

    See Also: `consume_events`

    Arguments:
    - `itgs (LazyIntegrations): The lazy integrations to use to connect to
        networked components. Must not be read only.
    - `event_uuid (str)`: The unique identifier for the event to delete.
    - `commit (bool)`: True if the the delete should be commited to the
        database, false if it should not.

        WARNING: Not commiting here does not mean you can rollback and save the
        event - the event information is lost immediately after this call no
        matter what. Not commiting here is either for performance or because you
        have something else in the transaction which can be rolled back.

        WARNING: Not committing here means the event is free to be consumed by
        other consumers. Not great in a parallel context. It depends on the
        context if it's better to sometimes duplicate vs sometimes lose an
        event. A very strong implementation is available with `consume_events`

    Returns:
    - `success (bool)`: True if the event was in the database, false if it was
        not.
    """
    del_queue = Table('delayed_queue')
    itgs.write_cursor.execute(
        Query.from_(del_queue)
        .delete()
        .where(del_queue.uuid == Parameter('%s'))
        .returning(del_queue.id)
        .get_sql(),
        (event_uuid,)
    )
    success = itgs.write_cursor.fetchone() is not None

    if commit:
        itgs.write_conn.commit()

    itgs.kvs_db.collection('delayed_queue').force_delete_doc(event_uuid)
    return success


def consume_events(itgs, queue_type, limit, handler, rollback):
    """Consume up to limit events from the given queue using the given handler.
    If an unhandled exception occurs in the handler during processing, the
    event is forwarded to rollback before being requeued.

    This consumer is very stable and is suitable for a small number of consumers
    (<3 active consumers per queue, 1 consumer per queue recommended) and a
    moderate number of events (<6000 events/minute total in all queues),
    provided that the itgs object is not being used on another thread and event
    processing requires a very short amount of time. A recommended maximum
    amount of time for event processing using this consumer is 1 second. If
    processing requires more than one second this should be used to forward the
    events to the amqp service and then the main processing should be done via
    an amqp consumer. This is also how one can get more active consumers per
    queue.

    WARNING: This will commit the postgres connection. If a transaction is
    in progress it should be rolled back or commited prior to this call.

    WARNING: Integrity failures are handled as if by `delete_and_commit`. This
    is a soft requirement for this type of consumer to behave properly.

    Arguments:
    - `itgs (LazyIntegrations)`: The lazy integrations to use for connecting
        to third-party services.
    - `queue_type (int)`: The queue to consume events on.
    - `limit (int)`: The maximum number of events to consume.
    - `handler (function)`: The function which accepts
        `(ev_uuid, ev_at, ev, ctx)`, where the arguments are as follows:
        - `ev_uuid (str)`: The unique identifier associated with the event. The
            event has already been deleted from the queue.
        - `ev_at (datetime)`: The time the event was scheduled to run.
        - `ev (any)`: The free-form information attached to the event.
        - `ctx (dict)`: A dict which is initialized right before this call. It
            may be mutated to the caller if it needs to forward information to
            the rollback function in the event of a failure. Otherwise it can
            just be ignored.
    - `rollback (function)`: A function with the same signature as handler. The
        context is the same context that was passed to handler and may be used
        for information forwarding, such as how much needs to be rolled back.
        This is invoked only if an unhandled exception is raised during handler.
        The event will be requeud under a new uuid after the rollback completes
        successfully. If the rollback fails the event is lost.
    """
    past_due_events = index_events(
        itgs,
        queue_type,
        limit,
        before_time=datetime.now(),
        integrity_failures='delete_and_commit'
    )

    for (ev_uuid, ev_at, ev) in past_due_events:
        if not delete_event(itgs, ev_uuid, commit=True):
            # Another consumer already handled this event
            continue

        with delay_signals(itgs):
            context = {}
            try:
                handler(ev_uuid, ev_at, ev, context)
            except:  # noqa
                itgs.logger.exception(
                    Level.WARN,
                    'An unexpected error occurred processing {}',
                    ev_uuid
                )
                rollback(ev_uuid, ev_at, ev, context)
                store_event(itgs, queue_type, ev_at, ev, commit=True)
                raise
