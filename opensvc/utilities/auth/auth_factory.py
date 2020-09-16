from utilities.auth.auth_provider_abstract import AuthProviderAbstract
from utilities.kv_store.kv_abstract import KvAbstract, NoKey


class AuthFactory:
    def __init__(
            self,
            auth_provider: AuthProviderAbstract,
            kv_store: KvAbstract,
    ):
        self.kv_store = kv_store
        self.auth_provider = auth_provider

    def _get_auth(self, **auth_info: dict) -> dict:
        key = self.auth_provider.auth_info_to_key(**auth_info)
        try:
            value = self.kv_store.read_not_expired(key)
        except NoKey:
            args, kwargs = self.auth_provider.creator_args(**auth_info)
            value = self.auth_provider.creator(*args, **kwargs)
            self.kv_store.create(key, value)
        return value

    def get_headers(self, auth_info):
        auth = self._get_auth(**auth_info)
        return self._auth_to_headers(**auth)

    @staticmethod
    def _auth_to_headers(**auth):
        return {"Authorization": "Bearer %s" % auth.get('token', "none")}
