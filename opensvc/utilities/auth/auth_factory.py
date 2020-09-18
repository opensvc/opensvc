from utilities.kv_store.kv_abstract import NoKey


class AuthFactory(object):
    def __init__(
            self,
            auth_provider,
            kv_store,
    ):
        self.kv_store = kv_store
        self.auth_provider = auth_provider

    def _get_auth(self, **auth_info):
        key = self.auth_provider.auth_info_to_key(**auth_info)
        try:
            data = self.kv_store.read_not_expired(key)
        except NoKey:
            args, kwargs = self.auth_provider.creator_args(**auth_info)
            data = self.auth_provider.creator(*args, **kwargs)
            self.kv_store.create(key, data)
        return data

    def get_headers(self, auth_info):
        data = self._get_auth(**auth_info)
        return self.auth_provider.data_to_header(**data)

