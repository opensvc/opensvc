import time

import core.exceptions as ex
from utilities.drivers import driver_import
from utilities.lazy import lazy

STATS_INTERVAL = 1


class PgMixin(object):
    @lazy
    def create_pg(self):
        return self.oget("DEFAULT", "create_pg")

    def get_pg_settings(self, s):
        d = {}
        options = (
            "cpus",
            "cpu_shares",
            "cpu_quota",
            "mems",
            "mem_oom_control",
            "mem_limit",
            "mem_swappiness",
            "vmem_limit",
            "blkio_weight",
        )

        for option in options:
            try:
                d[option] = self.conf_get(s, "pg_"+option)
            except ex.OptNotFound as exc:
                pass
            except ValueError:
                # keyword not supported. data resource for example.
                pass

        return d

    def _pg_freeze(self):
        """
        Wrapper function for the process group freeze method.
        """
        return self._pg_freezer("freeze")

    def _pg_thaw(self):
        """
        Wrapper function for the process group thaw method.
        """
        return self._pg_freezer("thaw")

    def _pg_kill(self):
        """
        Wrapper function for the process group kill method.
        """
        return self._pg_freezer("kill")

    def _pg_freezer(self, action):
        """
        Wrapper function for the process group methods.
        """
        if not self.create_pg:
            return
        if self.pg is None:
            return
        if action == "freeze":
            self.pg.freeze(self)
        elif action == "thaw":
            self.pg.thaw(self)
        elif action == "kill":
            self.pg.kill(self)

    def pg_remove(self):
        if self.pg is None:
            return
        if self.options.force:
            self.pg_kill()
        self.pg.remove_pg(self)

    def pg_pids(self):
        if self.pg is None:
            return []
        return sorted(self.pg.pids(self))

    def pg_update(self):
        nscfg = self.nscfg()
        if nscfg:
            nscfg.pg_update(children=False)
        for res in self.get_resources():
            res.create_pg()

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

    def pg_stats(self):
        if self.pg is None:
            return {}
        now = time.time()
        if now - self.stats_updated < STATS_INTERVAL:
            return self.stats_data
        self.stats_data = self.pg.get_stats(self)
        self.stats_updated = now
        return self.stats_data

    def pg_freeze(self):
        """
        Freeze all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action(["app", "container"], "_pg_freeze")
        else:
            self._pg_freeze()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

    def pg_thaw(self):
        """
        Thaw all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action(["app", "container"], "_pg_thaw")
        else:
            self._pg_thaw()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

    def pg_kill(self):
        """
        Kill all process of the process groups of the service.
        """
        if self.command_is_scoped():
            self.sub_set_action(["app", "container"], "_pg_kill")
        else:
            self._pg_kill()
            for resource in self.get_resources(["app", "container"]):
                resource.status(refresh=True)

    @lazy
    def pg_settings(self):
        return self.get_pg_settings("DEFAULT")



