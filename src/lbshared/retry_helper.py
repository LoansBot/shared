"""
A small module that will use a single file to store when the program was last
booted and, if it was too recent, will sleep a little bit. Typically called
at the start of the program.
"""
import os
import json
import time


FILENAME = '.retry_helper'


def handle(time_between_restarts=10):
    """This will store when this application last opened in FILENAME and
    sleep as necessary to ensure that when this file exits it's been at
    least the given time between restarts since the last time this function
    was called"""
    last_open_at = last_opened_at()
    store_opened_at()

    time_since_opened = time.time() - last_open_at
    if time_since_opened < time_between_restarts:
        time.sleep(time_between_restarts - time_since_opened)


def last_opened_at():
    """Determines when store_opened_at was last called"""
    if not os.path.exists(FILENAME):
        return None

    with open(FILENAME, 'r') as f:
        body = json.load(f)

    return body['last_started_at']


def store_opened_at():
    """Updates the retry helper file to indicate we were just run"""
    with open(FILENAME, 'w') as f:
        json.dump({'last_started_at': time.time()}, f)
