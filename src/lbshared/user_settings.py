"""We store user settings - which are relatively long-lasting user-specific
variables which can generally be manipulated by the user - in arango with
no TTL. We do have default values for all of these settings, which means
most users, for the most part, have whatever the default values were when
they signed up.

To save some space, we keep track of all the times that we changed the default
settings. Each user setting compactly references the defaults that they came
from and only includes the differences from those defaults. This saves storage
space.
"""
from pydantic import BaseModel
from pypika import PostgreSQLQuery as Query, Table, Parameter
import time
import json


class UserSettings(BaseModel):
    """The actual settings for a user. This is not something we necessarily
    want to expose in its raw format. This is not meant to be returned or
    accepted from any endpoints.

    Attributes:
    - `non_req_response_opt_out (bool)`: False if the user should receive
        a public response from the LoansBot on any non-meta submission to
        the subreddit explaining his/her history, True if they should only
        receive such a response on request posts.
    - `borrower_req_pm_opt_out (bool)`: False if the user should receive
        a reddit private message from the LoansBot if any of their active
        borrowers makes a request thread, True if the user should not
        receive that pm.
    - `user_specific_ratelimit (bool)`: True if the users ratelimit is frozen,
        i.e., their ratelimit has been set such that it's not affected by
        changes to the default user ratelimit.
    - `ratelimit_max_tokens (int, None)`: None if and only if the user does not
        have a specific ratelimit. Otherwise, the maximum number of tokens that
        the user can accumulate.
    - `ratelimit_refill_amount (int, None)`: None if and only if the user does
        not have a specific ratelimit. Otherwise, the amount of tokens refilled
        at each interval.
    - `ratelimit_refill_time_ms (int, None)`: None if and only if the user
        does not have a specific ratelimit. Otherwise, the number of
        milliseconds between the user receiving more ratelimit tokens.
    - `ratelimit_strict (bool, None)`: None if and only if the user does not
        have a specific ratelimit. Otherwise, True if the users ratelimit
        interval should be reset when one of their requests are ratelimited and
        False if their should receive their ratelimit tokens every ratelimit
        interval even if we are actively ratelimiting them.
    """
    non_req_response_opt_out: bool
    borrower_req_pm_opt_out: bool
    global_ratelimit_applies: bool
    user_specific_ratelimit: bool
    ratelimit_max_tokens: int = None
    ratelimit_refill_amount: int = None
    ratelimit_refill_time_ms: int = None
    ratelimit_strict: bool = None


DEFAULTS = [
    UserSettings(
        non_req_response_opt_out=False,
        borrower_req_pm_opt_out=False,
        global_ratelimit_applies=True,
        user_specific_ratelimit=False
    )
]
"""This contains the default settings in the order that they were changed,
in ascending time order. We freeze users to a particular index in this,
whatever the last index is at the time, and then store modifications.
"""

SETTINGS_KEYS = tuple(DEFAULTS[-1].dict().keys())
"""The settings keys, which we use for fetching settings programmatically,
stored so we don't have to constantly regenerate them"""

USER_SETTINGS_COLLECTION = 'user-settings'
"""The collection within arango that we store user settings at."""


def get_settings(itgs, user_id: int) -> UserSettings:
    """Get the settings for the given user.

    Arguments:
    - `itgs (LazyIntegrations)`: The connections to use to connect to networked
        components.
    - `user_id (int)`: The id of the user whose settings should be fetched

    Returns:
    - `settings (UserSettings)`: The settings for that user.
    """
    doc = itgs.kvs_db.collection(USER_SETTINGS_COLLECTION).document(str(user_id))

    if not doc.read():
        doc.body['frozen'] = len(DEFAULTS) - 1
        if not doc.create():
            doc.read()

    base_settings = DEFAULTS[doc.body['frozen']]

    return UserSettings(
        **dict(
            [nm, doc.body.get(nm, getattr(base_settings, nm))]
            for nm in SETTINGS_KEYS
        )
    )


def set_settings(itgs, user_id: int, **values) -> list:
    """Set the given settings on the user. It's more efficient to do fewer
    calls with more values than more calls with fewer values. This guarrantees
    that the entire change is made, however of course if several calls
    occur at the same time, it's a race condition for who wins if their is
    overlap in the settings being changed.

    Note:
      This directly sets the settings. We usually store events alongside these
      so that we have a history of the settings. This function does not do that.
      One can use "create_settings_events" on the return value for this event to
      do that.

    Attributes:
    - `itgs (LazyIntegrations)`: The integrations to use to connect to the store
    - `user_id (int)`: The id of the user whose settings should be changed.
    - `values (dict[any])`: The values to set, where the key is the key in
        UserSettings and the value is the new value to set.

    Returns:
    - `changes (dict[str, dict])`: The actual changes which were applied. This
        has keys which are a subset of the keys of values. The keys from values
        which were going to be set to the same value they are currently are
        stripped. Each value in change has the following keys:
        + `old (any)`: The old value for this property
        + `new (any)`: The new value for this property
    """
    doc = itgs.kvs_db.collection(USER_SETTINGS_COLLECTION).document(str(user_id))
    if not doc.read():
        doc.body['frozen'] = len(DEFAULTS) - 1
        if not doc.create():
            if not doc.read():
                raise Exception('High contention on user settings object!')

    for i in range(10):
        if i > 0:
            time.sleep(0.1 * (2 ** i))

        base_settings = DEFAULTS[doc.body['frozen']]

        changes = {}
        for key, val in values.items():
            def_val = getattr(base_settings, key)
            old_val = doc.body.get(key, def_val)
            if old_val != val:
                changes[key] = {
                    'old': old_val,
                    'new': val
                }

            if val == def_val:
                if key in doc.body:
                    del doc.body[key]
            else:
                doc.body[key] = val

        if doc.compare_and_swap():
            return changes

        if not doc.read():
            doc.body = {}
            doc.body['frozen'] = len(DEFAULTS) - 1
            if not doc.create():
                raise Exception(f'Ludicrously high contention on user settings for {user_id}')

    raise Exception('All 10 attempts to set user settings failed')


def create_settings_events(
        itgs, user_id: int, changer_user_id: int, changes: dict, commit=False):
    """Create user settings events from the given list of changes. These allow
    us to maintain a history of a users settings and who changes them.

    Arguments:
    - `itgs (LazyIntegrations)`: The integrations to use to connect to
        networked components.
    - `user_id (int)`: The id of the user whose settings changed
    - `changer_user_id (int)`: The id of the user who changed the settings
    - `changes (dict[str, dict])`: A dictionary where the keys are the property
        names that changes and the values are dictionaries with a fixed shape;
        two keys "old" and "new" which correspond to the old and new value
        of this property respectively. We serialize using json.
    - `commit (bool)`: If True we will commit the changes immediately. Defaults
        to false as it's easier to read controllers if all commits are explicit,
        i.e., the controller at least says `commit=True`
    """
    events = Table('user_settings_events')
    sql = (
        Query.into(events).columns(
            events.user_id, events.changer_user_id, events.property_name,
            events.old_value, events.new_value
        ).insert(*[tuple(Parameter('%s') for _ in range(5)) for _ in changes])
        .get_sql()
    )
    args = []
    for (prop_name, change) in changes.items():
        args.append(user_id)
        args.append(changer_user_id)
        args.append(prop_name)
        args.append(json.dumps(change['old']))
        args.append(json.dumps(change['new']))

    itgs.write_cursor.execute(sql, args)
    if commit:
        itgs.write_conn.commit()
