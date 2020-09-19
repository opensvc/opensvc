import pytest

from utilities.kv_store.kv_abstract import NoKey
from utilities.kv_store.kv_simple import KvSimple


@pytest.fixture(scope='function')
def simple():
    return KvSimple(name='tst', is_expired=lambda x: True)


@pytest.mark.ci
class TestKvSimple:
    @staticmethod
    def test_no_key(simple):
        with pytest.raises(NoKey):
            simple.read('toto')

    @staticmethod
    def test_create_then_retrieve_key(simple):
        key = 'foo'
        simple.create(key, {"bar": 1})
        assert simple.read(key) == {"bar": 1}

    @staticmethod
    def test_delete_key(simple):
        key = 'foo'
        simple.create(key, {"bar": 1})
        simple.delete(key)
        with pytest.raises(NoKey):
            simple.read(key)

    @staticmethod
    def test_update_key(simple):
        key = 'foo'
        simple.create(key, {"bar": 1})
        assert simple.read(key) == {"bar": 1}
        simple.update(key, {"bar": 2})
        assert simple.read(key) == {"bar": 2}

    @staticmethod
    def test_read_not_expired_remove_expired_key(simple):
        key = 'foo'
        simple.create(key, {"bar": 1})
        assert simple.read(key) == {"bar": 1}
        with pytest.raises(NoKey):
            simple.read_not_expired(key)
        with pytest.raises(NoKey):
            simple.read(key)

    @staticmethod
    def test_read_not_expired_does_not_remove_not_expired_key():
        key = 'foo'
        simple = KvSimple(is_expired=lambda x: False, name='tst')
        simple.create(key, {"bar": 1})
        assert simple.read(key) == {"bar": 1}
        assert simple.read_not_expired(key) == {"bar": 1}
        assert simple.read(key) == {"bar": 1}
