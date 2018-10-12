# coding: utf-8

from __future__ import print_function

import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from storage import Storage

class TestStorage:
    def test_storage(self):
        """
        Storage
        """
        store = Storage({"foo": "bar"})
        assert store.foo == "bar"
        assert store.bar is None
        assert store["bar"] is None
        del store.foo
        assert store.foo is None
        store.foo = "bar"
        assert store.foo == "bar"

