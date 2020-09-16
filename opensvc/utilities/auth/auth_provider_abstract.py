class AuthProviderAbstract:
    def creator_args(self, **auth_info):
        raise NotImplemented

    def auth_info_to_key(self, **auth_info):
        raise NotImplemented

    def creator(self, *args, **auth_info):
        raise NotImplemented
