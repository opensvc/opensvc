import core.status

from core.objects.svc import Svc
from utilities.naming import split_path, factory

class Vol(Svc):
    kind = "vol"

    def users(self, exclude=None):
        exclude = exclude or []
        users = []

        # purge lazies that may have changed due to claims
        # that occured in the lifespan of this object
        self.unset_lazy("cd")
        self.unset_lazy("children")

        for child in self.children:
            if child in exclude:
                continue
            name, namespace, kind = split_path(child)
            obj = factory(kind)(name=name, namespace=self.namespace, volatile=True, node=self.node)
            for res in obj.get_resources("volume"):
                if res.name != self.name:
                    continue
                if res.status() in (core.status.UP, core.status.STDBY_UP, core.status.WARN):
                    users.append(child)
        return users

