import pytest

import time

from utilities.auth.auth_factory import AuthFactory
from utilities.auth.auth_provider_abstract import AuthProviderAbstract
from utilities.kv_store.kv_null import KvNull
from utilities.kv_store.kv_simple import KvSimple


def create_token(user, password, scopes):
    return {
        "token": "%s %s %s %s" % (user, password, scopes, int(time.time())),
        "expired_at": int(time.time() + 20)
    }


@pytest.mark.ci
class Iam(AuthProviderAbstract):
    """Fake iam example class"""
    def __init__(
            self,
            user,
            password,
    ):
        self.user = user
        self.password = password

    def auth_info_to_key(self, **auth_info):
        return "%s_%s" % (self.user, auth_info)

    def creator_args(self, **auth_info):
        args = ()
        kwargs = {
            'user': self.user,
            'password': self.password,
            'scopes': auth_info['scopes']
        }
        return args, kwargs

    def creator(self, **auth_info):
        return create_token(**auth_info)

    @staticmethod
    def is_expired(auth_info):
        now = int(time.time())
        return auth_info.get('expired_at', now) < now


@pytest.fixture(scope='function')
def iam_factory():
    iam = Iam(
        user="User",
        password="FakePass",
    )
    store = KvSimple(
        name='Iam Kv store',
        is_expired=Iam.is_expired
    )
    return AuthFactory(kv_store=store, auth_provider=iam)


class TestIam:
    @staticmethod
    def test_get_header_use_cache_until_expired(mocker, iam_factory):
        time_mock = mocker.patch('utilities.auth.iam.time.time',
                                 side_effect=range(60))
        create_token_spy = mocker.patch('site_utils.auth.iam.create_token', wraps=create_token)
        tk_initial1 = iam_factory.get_headers({'scopes': ['read', 'write']})
        assert create_token_spy.call_count == 1
        tk_initial2 = iam_factory.get_headers({'scopes': ['write']})
        assert create_token_spy.call_count == 2
        for i in range(9):
            tk_new1 = iam_factory.get_headers({'scopes': ['read', 'write']})
            tk_new2 = iam_factory.get_headers({'scopes': ['write']})
            assert tk_new1 == tk_initial1
            assert tk_new2 == tk_initial2
        assert create_token_spy.call_count == 2
        for i in range(5):
            time_mock()
        assert iam_factory.get_headers({'scopes': ['write']}) != tk_initial2
        assert create_token_spy.call_count == 3

    @staticmethod
    def test_2_factory_can_use_same_store():
        iam1 = Iam(
            user="user1",
            password="pass1",
        )
        iam2 = Iam(
            user="user2",
            password="pass2",
        )
        store = KvSimple(
            name='Iam Kv store',
            is_expired=Iam.is_expired
        )
        factory1 = AuthFactory(kv_store=store, auth_provider=iam1)
        factory1_dup = AuthFactory(kv_store=store, auth_provider=iam1)
        factory2 = AuthFactory(kv_store=store, auth_provider=iam2)
        tk1 = factory1.get_headers({'scopes': ['write']})
        tk2 = factory2.get_headers({'scopes': ['write']})
        assert tk1 != tk2
        assert tk1 == factory1_dup.get_headers({'scopes': ['write']})
        assert tk2 == factory2.get_headers({'scopes': ['write']})

    @staticmethod
    def test_ensure_no_cache_when_null_kv_store_is_used(mocker):
        iam = Iam(
            user="user",
            password="pass",
        )
        store = KvNull(
            name='Iam Kv store null',
            is_expired=Iam.is_expired
        )
        factory = AuthFactory(kv_store=store, auth_provider=iam)
        create_token_spy = mocker.patch('site_utils.auth.iam.create_token', wraps=create_token)
        for _ in range(20):
            assert 'ACCOUNT_ID' in factory.get_headers({'scopes': ['read', 'write']})['Authorization']
        assert create_token_spy.call_count == 20
