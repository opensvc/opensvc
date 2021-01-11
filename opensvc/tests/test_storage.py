import pickle

import pytest

from utilities.storage import Storage


@pytest.mark.ci
class TestStorage:
    @staticmethod
    def test_storage():
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

    @staticmethod
    def test_storage_is_pickle_able():
        store = Storage({"foo": "bar"})
        store_str = pickle.dumps(store)
        assert pickle.loads(store_str) == store

    @staticmethod
    def test_storage_equal():
        assert Storage({"foo": "bar"}) == Storage({"foo": "bar"})
        assert Storage({"foo": "bar", "1": 2}) == Storage({"foo": "bar", "1": 2})

    @staticmethod
    def test_storage_not_equal():
        assert Storage({"foo": "bar"}) != Storage({"foo": "Foo"})
