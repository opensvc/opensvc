from core.objects.svc import BaseSvc
from core.objects.pg import PgMixin
from utilities.lazy import lazy
from utilities.drivers import driver_import
from utilities.naming import list_services, factory, split_path

DEFAULT_STATUS_GROUPS = [
]

class Nscfg(PgMixin, BaseSvc):
    kind = "nscfg"

    def __init__(self, *args, **kwargs):
        if len(args) >= 1:
            args = list(args)
            args[0] = "namespace"
        else:
            kwargs["name"] = "namespace"
        BaseSvc.__init__(self, *args, **kwargs)

    @lazy
    def kwstore(self):
        from .nscfgdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .nscfgdict import KEYS
        return KEYS

    @lazy
    def nscfg(self):
        return lambda x: x

    @lazy
    def pg(self):
        """
        A lazy property to import the system-specific process group module
        on-demand and expose it as self.pg
        """
        try:
            mod = driver_import("pg", fallback=False)
        except ImportError:
            return
        except Exception as exc:
            print(exc)
            raise
        try:
            mod.DRIVER_BASENAME
        except AttributeError:
            return
        return mod

    def iterate_objects(self, volatile=True):
        for path in list_services(namespace=self.namespace, kinds=["svc", "vol"]):
            name, namespace, kind = split_path(path)
            obj = factory(kind)(name, namespace, volatile=volatile, node=self.node)
            yield obj

    def pg_data(self):
        data = []
        for obj in self.iterate_objects():
             for pid in obj.pg_pids():
                 data.append({
                     "pid": pid,
                     "path": obj.path,
                 })
        return data

    def pg_status(self):
        if not self.pg:
            return []
        data = self.pg_data()
        return data

    def pg_update(self, children=True):
        if not self.pg:
            return
        self.pg._create_pg(self)
        if not children:
            return
        for obj in self.iterate_objects(volatile=False):
            obj.pg_update()

    def pg_remove(self):
        if not self.pg:
            return
        if self.options.force:
            self.pg_kill()
        for obj in self.iterate_objects(volatile=False):
            obj.pg_remove()
        PgMixin.pg_remove(self)

    def pg_freeze(self):
        """
        Freeze all process of the process groups of the service.
        """
        if not self.pg:
            return
        self._pg_freeze()
        for obj in self.iterate_objects(volatile=False):
            obj.print_status_data_eval()

    def pg_thaw(self):
        """
        Thaw all process of the process groups of the service.
        """
        if not self.pg:
            return
        self._pg_thaw()
        for obj in self.iterate_objects(volatile=False):
            obj.print_status_data_eval()

    def pg_kill(self):
        """
        Kill all process of the process groups of the service.
        """
        if not self.pg:
            return
        self._pg_kill()
        for obj in self.iterate_objects(volatile=False):
            obj.print_status_data_eval()

