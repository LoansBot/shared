"""This module provides a convenient context manager for delaying suggestions
to close down. This is typically used while in a critical and extremely short-
lived section of code for data integrity. If the runtime on the section is long
enough that _not_ using this signal helper would be irresponsible, it probably
needs a recovery approach.

For obvious reasons, this signal helper doesn't prevent the program from being
shutdown from SIGQUIT or pulling the plug on the server.
"""
import signal
import typing
from contextlib import contextmanager
from .lazy_integrations import LazyIntegrations
from lblogging import Level


@contextmanager
def delay_signals(itgs: typing.Optional[LazyIntegrations] = None):
    """Delays signals until the end of the context manager. If integrations
    are provided, they are used for logging. This operation MAY be nested.
    This will only re-raise the most urgent signal received while delaying,
    so for example if this gets both a SIGINT and a SIGTERM, only SIGTERM is
    re-raised at the end of the block. Re-raising both would be unreliable."""
    callback, tmp_handlers = _get_delayed_handlers(itgs)
    old_handlers = _apply_handlers(*tmp_handlers)
    try:
        yield
    finally:
        _apply_handlers(*old_handlers)
        callback()


def _apply_handlers(sigint_handler, sigterm_handler):
    """Replaces the signal handlers for SIGINT and SIGTERM with the
    specified handlers, and returns the old ones."""
    old_sigint = signal.signal(signal.SIGINT, sigint_handler)
    old_sigterm = signal.signal(signal.SIGTERM, sigterm_handler)
    return (old_sigint, old_sigterm)


def _get_delayed_handlers(itgs: typing.Optional[LazyIntegrations]):
    """Get the handlers to use for sigint and sigterm signals"""
    captured_sigint = False
    captured_sigterm = False

    def capture_sigint(sig_num=None, frame=None):
        nonlocal captured_sigint
        _log_capture(itgs, 'SIGINT')
        captured_sigint = True

    def capture_sigterm(sig_num=None, frame=None):
        nonlocal captured_sigterm
        _log_capture(itgs, 'SIGTERM')
        captured_sigterm = True

    def callback():
        if captured_sigterm:
            _log_reraise(itgs, 'SIGTERM')
            signal.raise_signal(signal.SIGTERM)
            return
        if captured_sigint:
            _log_reraise(itgs, 'SIGINT')
            signal.raise_signal(signal.SIGINT)
            return

    return callback, (capture_sigint, capture_sigterm)


def _log_capture(itgs, signm):
    print(f'Capturing and delaying {signm} until end of a critical block')
    if itgs is not None:
        itgs.logger.print(
            Level.INFO,
            'Capturing and delaying {} until the end of a critical block',
            signm
        )


def _log_reraise(itgs, signm):
    print(f'Repeating captured {signm} (critical block finished)')
    if itgs is not None:
        itgs.logger.print(
            Level.INFO,
            'Repeating captured {} (critical block finished)',
            signm
        )
