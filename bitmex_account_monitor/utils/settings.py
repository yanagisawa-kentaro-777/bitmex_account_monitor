import importlib
import os
import sys

import bitmex_account_monitor.utils.base_settings as base_settings


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def import_path(full_path):
    """
    Import a file with full path specification. Allows one to
    import from anywhere, something __import__ does not do.
    """
    path, filename = os.path.split(full_path)
    filename, ext = os.path.splitext(filename)
    sys.path.insert(0, path)
    module = importlib.import_module(filename, path)
    importlib.reload(module)  # Might be out of date
    del sys.path[0]
    return module


try:
    user_settings = import_path(os.path.join('.', 'settings'))
except Exception:
    user_settings = None

# Assemble settings.
settings = {}
settings.update(vars(base_settings))
if user_settings:
    settings.update(vars(user_settings))

# Main export
settings = DotDict(settings)
