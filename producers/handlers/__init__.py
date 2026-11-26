"""One handler class per Snapchain message type.

Every module in this package is imported here, so a handler registers itself
with ``@register`` just by being in a module in this directory. Supporting a new
message type means adding one handler class, in a new or existing module here;
the transformer and poller don't change.
"""

import importlib
import pkgutil

from .base import HANDLERS, MessageHandler, Topic, register

for _module in pkgutil.iter_modules(__path__):
    if _module.name != "base":
        importlib.import_module(f"{__name__}.{_module.name}")

__all__ = ["HANDLERS", "MessageHandler", "Topic", "register"]
