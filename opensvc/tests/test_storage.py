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

