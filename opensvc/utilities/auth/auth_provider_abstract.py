class AuthProviderAbstract(object):
    def creator_args(self, **auth_info):
        raise NotImplementedError

    def auth_info_to_key(self, **auth_info):
        raise NotImplementedError

    def creator(self, *args, **auth_info):
        raise NotImplementedError
