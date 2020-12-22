"""
Defines the resource class, which is the parent class of every
resource driver.
"""
from __future__ import print_function

import json
import logging
import os
import sys
import time

import core.status
import utilities.lock
import core.exceptions as ex
import utilities.devices
import utilities.render.color
from env import Env
from core.capabilities import capabilities
from utilities.naming import factory
from utilities.cache import clear_cache
from utilities.lazy import lazy, set_lazy, unset_lazy
from utilities.storage import Storage
from utilities.proc import action_triggers, call, lcall, vcall

ALLOW_ACTION_WITH_NOACTION = [
    "presync",
    "set_provisioned",
    "set_unprovisioned",
]

LOCKER_TYPES = [
    "disk.scsireserv",
    "disk.radoslock",
]


class Resource(object):
    """
    Resource drivers parent class
    """
    default_optional = False
    refresh_provisioned_on_provision = False
    refresh_provisioned_on_unprovision = False

    def __init__(self,
                 rid=None,
                 type=None,
                 subset=None,
                 optional=None,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 tags=None,
                 standby=None,
                 enable_provision=False,
                 enable_unprovision=False,
                 shared=False,
                 promote_rw=False,
                 encap=False,
                 always_on=None,
                 **ignored):
        if tags is None:
            tags = set()
        self.svc = None
        self.rset = None
        self.rid = rid
        self.tags = tags
        self.type = type
        self.subset = subset
        self.optional = self.mangle_optional(optional, self.tags)
        self.standby = standby
        self.always_on = always_on or []
        self.disabled = disabled
        self.skip = False
        self.monitor = monitor
        self.nb_restart = restart
        self.rstatus = None
        self.skip_provision = not enable_provision
        self.skip_unprovision = not enable_unprovision
        self.shared = shared
        self.need_promote_rw = promote_rw
        self.encap = encap or "encap" in self.tags
        self.sort_key = rid
        self.info_in_status = []
        self.lockfd = None
        self.always_pg = False
        try:
            self.label = type
        except AttributeError:
            # label is a lazy prop of the child class
            pass
        self.status_logs = []
        self.can_rollback = False
        self.rollback_even_if_standby = False
        self.skip_triggers = set()
        self.driver_group = self.format_driver_group()
        self.driver_basename = self.format_driver_basename()
        self.rset_id = self.format_rset_id()
        self.last_status_info = {}

    def on_add(self):
        """
        Placeholder for a method run when adding the Resource to a BaseSvc,
        after the Resource::svc attribute is set.
        """
        pass

    @lazy
    def is_standby(self):
        if self.standby is not None:
            return self.standby
        if Env.nodename in self.always_on:
            return True
        if not hasattr(self, "svc"):
            return False
        if "nodes" in self.always_on and Env.nodename in self.svc.nodes:
            return True
        if "drpnodes" in self.always_on and Env.nodename in self.svc.drpnodes:
            return True
        return False

    def mangle_optional(self, optional, tags):
        if "noaction" in tags:
            return True
        if optional is None:
            return self.default_optional
        return optional

    @lazy
    def log(self):
        """
        Lazy init for the resource logger.
        """
        extra = {
            "path": self.svc.path,
            "node": Env.nodename,
            "sid": Env.session_uuid,
            "cron": self.svc.options.cron,
            "rid": self.rid,
            "subset": self.subset,
        }
        return logging.LoggerAdapter(logging.getLogger(self.log_label()), extra)

    @lazy
    def var_d(self):
        var_d = os.path.join(self.svc.var_d, self.rid)
        if not os.path.exists(var_d):
            os.makedirs(var_d)
        return var_d

    def set_logger(self, log):
        """
        Set the <log> logger as the resource logger, in place of the default
        lazy-initialized one.
        """
        set_lazy(self, "log", log)

    def fmt_info(self, keys=None):
        """
        Returns the resource generic keys sent to the collector upon push
        resinfo.
        """
        if keys is None:
            return []
        for idx, key in enumerate(keys):
            count = len(key)
            if count and key[-1] is None:
                key[-1] = ""
            if count == 2:
                keys[idx] = [
                    self.svc.path,
                    self.svc.node.nodename,
                    self.svc.topology,
                    self.rid
                ] + key
            elif count == 3:
                keys[idx] = [
                    self.svc.path,
                    self.svc.node.nodename,
                    self.svc.topology
                ] + key
        return keys

    def log_label(self):
        """
        Return the resource label used in logs entries.
        """
        if hasattr(self, "svc"):
            label = self.svc.loggerpath + '.'
        else:
            label = Env.nodename + "."

        if self.rid is None:
            label += self.type
            return label

        if self.subset is None:
            label += self.rid
            return label

        elements = self.rid.split('#')
        if len(elements) != 2:
            label += self.rid
            return label

        ridx = elements[1]
        label += "%s:%s#%s" % (self.driver_group, self.subset, ridx)
        return label

    def __str__(self):
        output = "object=%s rid=%s type=%s" % (
            self.__class__.__name__,
            self.rid,
            self.type
        )
        if self.optional:
            output += " opt=" + str(self.optional)
        if self.disabled:
            output += " disa=" + str(self.disabled)
        return output

    def __lt__(self, other):
        """
        Resources needing to be started or stopped in a specific order
        should redefine that.
        """
        if self.type in LOCKER_TYPES and other.type not in LOCKER_TYPES:
            if other.type == "disk.disk" and self.rid == other.rid + "pr":
                ret = False
            else:
                ret = True
        elif self.type not in LOCKER_TYPES and other.type in LOCKER_TYPES:
            if self.type == "disk.disk" and self.rid + "pr" == other.rid:
                ret = True
            else:
                ret = False
        elif self.type == "sync.zfssnap" and other.type == "sync.zfs":
            ret = True
        elif self.type == "sync.zfs" and other.type == "sync.zfssnap":
            ret = False
        else:
            ret = self.sort_key < other.sort_key
        return ret

    def save_exc(self):
        """
        A helper method to save stacks in the service log.
        """
        self.log.error("", exc_info=True)

    def setup_environ(self):
        """
        Setup environement variables for use by triggers and startup
        scripts. This method needs defining in each class with their
        class variable.
        Env vars names should, by convention, be prefixed by OPENSVC_
        """
        pass

    def is_optional(self):
        """
        Accessor for the optional resource property.
        """
        return self.optional

    def is_disabled(self):
        """
        Accessor for the disabled resource property.
        """
        if self.svc.disabled:
            return True
        return self.disabled

    def set_optional(self):
        """
        Set the optional resource property to True.
        """
        self.optional = True

    def unset_optional(self):
        """
        Set the optional resource property to False.
        """
        self.optional = False

    def disable(self):
        """
        Set the disabled resource property to True.
        """
        self.disabled = True

    def enable(self):
        """
        Set the disabled resource property to False.
        """
        self.disabled = False

    def clear_cache(self, sig):
        """
        Wraps the rcUtilities clear_cache function, setting the resource
        as object keyword argument.
        """
        clear_cache(sig, o=self)

    def action_triggers(self, trigger, action, **kwargs):
        """
        Executes a resource trigger. Guess if the shell mode is needed from
        the trigger syntax.
        """
        action_triggers(self, trigger, action, **kwargs)

    def handle_confirm(self, action):
        """
        Tasks can require a run confirmation. We want the confirmation checked
        before executing triggers.
        """
        if not hasattr(self, "confirm"):
            return
        if action != "run":
            return
        getattr(self, "confirm")()

    def action_main(self, action):
        """
        Shortcut the resource action if in dry-run mode.
        """
        if self.svc.options.dry_run:
            if self.rset.parallel:
                header = "+ "
            else:
                header = ""
            self.log.info("%s%s %s", header, action, self.label)
            return
        getattr(self, action)()

    def do_action(self, action):
        """
        Call the resource action method if implemented.

        If the action is a stopping action and the resource is flagged
        standby on this node, skip.

        Call the defined pre and post triggers.

        """
        if not hasattr(self, action):
            self.log.debug("%s action is not implemented", action)

        self.log.debug('do action %s', action)

        if action == "stop" and self.is_standby and not self.svc.options.force:
            standby_action = action+'standby'
            if hasattr(self, standby_action):
                self.action_main(standby_action)
                return
            else:
                self.log.info("skip '%s' on standby resource (--force to override)", action)
                return

        self.check_requires(action)
        self.handle_confirm(action)
        self.setup_environ()
        self.action_triggers("pre", action)
        self.action_triggers("blocking_pre", action, blocking=True)
        self.action_main(action)
        self.action_triggers("post", action)
        self.action_triggers("blocking_post", action, blocking=True)
        if self.need_refresh_status(action):
            self.status(refresh=True)
        return

    def need_refresh_status(self, action):
        """
        Return True for action known to be causing a resource status change.
        """
        actions = (
            "boot",
            "shutdown",
            "start",
            "startstandby",
            "stop",
            "rollback",
            "unprovision",
            "provision",
            "install",
            "create",
            "switch",
            "migrate"
        )
        if self.svc.options.dry_run:
            return False
        if "sync" in action and self.type.startswith("sync"):
            return True
        if action in actions:
            return True
        return False

    def skip_resource_action(self, action):
        """
        Return True if the action should be skipped.
        """
        actions = (
            "shutdown",
            "start",
            "startstandby",
            "stop",
            "provision",
            "run",
            "set_unprovisioned",
            "set_provisioned",
            "unprovision",
        )
        if not self.skip:
            return False
        if action.startswith("sync"):
            return True
        if action.startswith("_pg_"):
            return True
        if action in actions:
            return True
        return False

    def action(self, action):
        """
        Try to call the resource do_action() if:
        * action is not None
        * the resource is not skipped by resource selectors

        If the resource is disabled, the return status depends on the optional
        property:
        * if optional, return True
        * else return do_action() return value
        """
        if self.rid != "app" and not self.svc.encap and self.encap:
            self.log.debug('skip encap resource action: action=%s res=%s', action, self.rid)
            return

        if 'noaction' in self.tags and \
           not hasattr(self, "delayed_noaction") and \
           action not in ALLOW_ACTION_WITH_NOACTION:
            self.log.debug('skip resource action %s (noaction tag)', action)
            return

        self.log.debug('action: %s', action)

        if action is None:
            self.log.debug('action: action cannot be None')
            return True
        if self.skip_resource_action(action):
            self.log.debug('action: skip action on filtered-out resource')
            return True
        if self.is_disabled():
            self.log.debug('action: skip action on disabled resource')
            return True
        if not hasattr(self, action):
            self.log.debug('action: not applicable (not implemented)')
            return True

        try:
            self.progress()
            self.do_action(action)
        except ex.Undefined as exc:
            print(exc)
            return False
        except ex.ContinueAction as exc:
            if self.svc.options.cron:
                # no need to flood the logs for scheduled tasks
                self.log.debug(str(exc))
            else:
                self.log.info(str(exc))
        except ex.Error as exc:
            if self.optional:
                if len(str(exc)) > 0:
                    self.log.error(str(exc))
                self.log.info("ignore %s error on optional resource", action)
            else:
                raise

    def status_stdby(self, status):
        """
        This method modifies the passed status according to the standby
        property.
        """
        if not self.is_standby:
            return status
        if status == core.status.UP:
            return core.status.STDBY_UP
        elif status == core.status.DOWN:
            return core.status.STDBY_DOWN
        return status

    def try_status(self, verbose=False):
        """
        Catch status methods errors and push them to the resource log buffer
        so they will be display in print status.
        """
        try:
            return self._status(verbose=verbose)
        except Exception as exc:
            self.status_log(str(exc), "error")
            return core.status.UNDEF

    def _status(self, verbose=False):
        """
        The resource status evaluation method.
        To be implemented by drivers.
        """
        if verbose:
            self.log.debug("default resource status: undef")
        return core.status.UNDEF

    def force_status(self, status):
        """
        Force a resource status, bypassing the evaluation method.
        """
        self.rstatus = status
        self.status_logs = [("info", "forced")]
        self.write_status()

    def status(self, **kwargs):
        """
        Resource status evaluation method wrapper.
        Handles caching, nostatus tag and disabled flag.
        """
        verbose = kwargs.get("verbose", False)
        refresh = kwargs.get("refresh", False)
        ignore_nostatus = kwargs.get("ignore_nostatus", False)

        if self.is_disabled():
            return core.status.NA

        if not ignore_nostatus and "nostatus" in self.tags:
            self.status_log("nostatus tag", "info")
            return core.status.NA

        if self.rstatus is not None and not refresh:
            return self.rstatus

        last_status = self.load_status_last(refresh)

        if refresh:
            self.purge_status_last()
        else:
            self.rstatus = last_status

        # now the rstatus can no longer be None
        if self.rstatus == core.status.UNDEF or refresh:
            self.status_logs = []
            self.rstatus = self.try_status(verbose)
            self.rstatus = self.status_stdby(self.rstatus)
            self.last_status_info = self.status_info()
            self.log.debug("refresh status: %s => %s",
                           core.status.Status(last_status),
                           core.status.Status(self.rstatus))
            self.write_status()

        if self.rstatus in (core.status.UP, core.status.STDBY_UP) and \
           not self._is_provisioned_flag():
            self.write_is_provisioned_flag(True)

        return self.rstatus

    def write_status(self):
        """
        Helper method to janitor resource status cache and history in files.
        """
        self.write_status_last()
        self.write_status_history()

    @lazy
    def fpath_status_last(self):
        """
        Return the file path for the resource status cache.
        """
        return os.path.join(self.var_d, "status.last")

    @lazy
    def fpath_status_history(self):
        """
        Return the file path for the resource status history.
        """
        return os.path.join(self.var_d, "status.history")

    def purge_status_last(self):
        """
        Purge the on-disk resource status cache.
        """
        try:
            os.unlink(self.fpath_status_last)
        except:
            pass

    def purge_var_d(self, keep_provisioned=True):
        import glob
        import shutil
        paths = glob.glob(os.path.join(self.var_d, "*"))
        for path in paths:
            if keep_provisioned and path == os.path.join(self.var_d, "provisioned"):
                # Keep the provisioned flag to remember that the
                # resource was unprovisioned, even if the driver
                # says it is always provisioned.
                # This is necessary because the orchestrated
                # unprovision would retry the CRM action if a
                # resource reports it is still provisioned after
                # the first unprovision.
                continue
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.unlink(path)
            except OSError:
                # errno 39: not empty (racing with a writer)
                pass

    def has_status_last(self):
        return os.path.exists(self.fpath_status_last)

    def load_status_last(self, refresh=False):
        """
        Fetch the resource status from the on-disk cache.
        """
        try:
            with open(self.fpath_status_last, 'r') as ofile:
                data = json.load(ofile)
        except ValueError:
            return core.status.UNDEF
        except (OSError, IOError) as exc:
            if exc.errno != 2:
                # not EEXISTS
                self.log.debug(exc)
            return core.status.UNDEF

        try:
            status = core.status.Status(data["status"])
        except (IndexError, AttributeError, ValueError) as exc:
            self.log.debug(exc)
            return core.status.UNDEF

        if not refresh and hasattr(self, "set_label"):
            if hasattr(self, "_lazy_label"):
                attr = "_lazy_label"
            else:
                attr = "label"
            try:
                setattr(self, attr, data["label"])
            except (IndexError, AttributeError, ValueError):
                pass

        self.status_logs = data.get("log", [])

        if "info" in data:
            self.last_status_info = data["info"]

        return status

    def write_status_last(self):
        """
        Write the in-memory resource status to the on-disk cache.
        """
        data = {
            "status": str(core.status.Status(self.rstatus)),
            "label": self.label,
            "log": self.status_logs,
            "info": self.last_status_info,
        }
        dpath = os.path.dirname(self.fpath_status_last)
        if not os.path.exists(dpath):
            os.makedirs(dpath, 0o0755)
        with open(self.fpath_status_last, 'w') as ofile:
            json.dump(data, ofile)
            ofile.flush()

    def write_status_history(self):
        """
        Log a change to the resource status history file.
        """
        fpath = self.fpath_status_history
        try:
            with open(fpath, 'r') as ofile:
                lines = ofile.readlines()
                last = lines[-1].split(" | ")[-1].strip("\n")
        except:
            last = None
        current = core.status.Status(self.rstatus)
        if current == last:
            return
        log = logging.getLogger("status_history")
        logformatter = logging.Formatter("%(asctime)s | %(message)s")
        logfilehandler = logging.handlers.RotatingFileHandler(
            fpath,
            maxBytes=512000,
            backupCount=1,
        )
        logfilehandler.setFormatter(logformatter)
        log.addHandler(logfilehandler)
        log.error(current)
        logfilehandler.close()
        log.removeHandler(logfilehandler)

    def status_log(self, text, level="warn"):
        """
        Add a message to the resource status log buffer, for
        display in the print status output.
        """
        if len(text) == 0:
            return
        if (level, text) in self.status_logs:
            return
        for line in text.splitlines():
            self.status_logs.append((level, line))

    def status_logs_get(self, levels=None):
        """
        Return filtered messages from the the resource status log buffer.
        """
        if levels is None:
            levels = ["info", "warn", "error"]
        return [entry[1] for entry in self.status_logs
                if entry[0] in levels and entry[1] != ""]

    def status_logs_count(self, levels=None):
        """
        Return the number of status log buffer entries matching the
        specified levels.
        """
        if levels is None:
            levels = ["info", "warn", "error"]
        return len(self.status_logs_get(levels=levels))

    def status_logs_strlist(self):
        return ["%s: %s" % (lvl, msg) for (lvl, msg) in self.status_logs]

    def status_logs_str(self, color=False):
        """
        Returns the formatted resource status log buffer entries.
        """
        status_str = ""
        for level, text in self.status_logs:
            if len(text) == 0:
                continue
            entry = level + ": " + text + "\n"
            if color:
                if level == "warn":
                    color = utilities.render.color.color.BROWN
                elif level == "error":
                    color = utilities.render.color.color.RED
                else:
                    color = utilities.render.color.color.LIGHTBLUE
                status_str += utilities.render.color.colorize(entry, color)
            else:
                status_str += entry
        return status_str

    def call(self, *args, **kwargs):
        """
        Wrap rcUtilities call, setting the resource logger
        """
        kwargs["log"] = self.log
        return call(*args, **kwargs)

    def vcall(self, *args, **kwargs):
        """
        Wrap vcall, setting the resource logger
        """
        kwargs["log"] = self.log
        return vcall(*args, **kwargs)

    def lcall(self, *args, **kwargs):
        """
        Wrap lcall, setting the resource logger
        """
        kwargs["logger"] = self.log
        return lcall(*args, **kwargs)

    @staticmethod
    def wait_for_fn(func, tmo, delay, errmsg="waited too long for startup"):
        """
        A helper function to execute a test function until it returns True
        or the number of retries is exhausted.
        """
        for tick in range(tmo//delay):
            if func():
                return
            time.sleep(delay)
        raise ex.Error(errmsg)

    def base_devs(self):
        """
        List devices the resource holds at the base of the dev tree.
        """
        devps = self.sub_devs() | self.exposed_devs()
        devs = self.svc.node.devtree.get_devs_by_devpaths(devps)
        base_devs = set()
        for dev in devs:
            top_devs = dev.get_top_devs()
            for top_dev in top_devs:
                base_devs.add(os.path.realpath(top_dev.devpath[0]))
        return base_devs

    def sub_devs(self):
        """
        List devices the resource holds.
        """
        return set()

    def exposed_devs(self):
        """
        List devices the resource exposes.
        """
        return set()

    def base_disks(self):
        """
        List disks the resource holds at the base of the dev tree. Some
        resource have none, and can leave this function as is.
        """
        devs = self.base_devs()
        try:
            disks = utilities.devices.devs_to_disks(self, devs)
        except:
            disks = devs
        return disks

    def sub_disks(self):
        """
        List disks the resource holds. Some resource have none, and can leave
        this function as is.
        """
        devs = self.sub_devs()
        try:
            disks = utilities.devices.devs_to_disks(self, devs)
        except:
            disks = devs
        return disks

    def exposed_disks(self):
        """
        List disks the resource exposes. Some resource have none, and can leave
        this function as is.
        """
        devs = self.exposed_devs()
        try:
            disks = utilities.devices.devs_to_disks(self, devs)
        except:
            disks = devs
        return disks

    def presync(self):
        """
        A method called before a sync action is executed.
        """
        pass

    def postsync(self):
        """
        A method called after a sync action is executed.
        """
        pass

    @staticmethod
    def default_files_to_sync():
        """
        If files_to_sync() is not superceded, return an empty list as the
        default resource files to sync.
        """
        return []

    def files_to_sync(self):
        """
        Returns a list of files to contribute to sync#i0
        """
        return self.default_files_to_sync()

    def rollback(self):
        """
        Executes a resource stop if the resource start has marked the resource
        as rollbackable.
        """
        if self.can_rollback and not self.is_standby:
            self.stop()

    def stop(self):
        """
        The resource stop action entrypoint.
        """
        pass

    def startstandby(self):
        """
        Promote the action to start if the resource is flagged standby
        """
        if self.is_standby:
            self.start()

    def start(self):
        """
        The resource start action entrypoint.
        """
        pass

    def boot(self):
        """
        Clean up actions to do on node boot before the daemon starts.
        """
        pass

    def shutdown(self):
        """
        Always promote to the stop action
        """
        self.stop()

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
        if not self.svc.create_pg and not self.always_pg:
            return
        if self.svc.pg is None:
            return
        if action == "freeze":
            self.svc.pg.freeze(self)
        elif action == "thaw":
            self.svc.pg.thaw(self)
        elif action == "kill":
            self.svc.pg.kill(self)

    def pg_frozen(self):
        """
        Return True if the resource has its process group frozen
        """
        if not self.svc.create_pg and not self.always_pg:
            return False
        if self.svc.pg is None:
            return False
        return self.svc.pg.frozen(self)

    def create_pg(self):
        """
        Create a process group if this service asks for it and if possible.
        """
        if not self.svc.create_pg and not self.always_pg:
            return
        if self.svc.pg is None:
            return
        self.svc.pg.create_pg(self)

    def check_requires(self, action, cluster_data=None):
        """
        Iterate the resource 'requires' definition, and validate each
        requirement.
        """
        param = action + "_requires"
        if not hasattr(self, param):
            return
        requires = getattr(self, param)
        if len(requires) == 0:
            return
        for element in requires:
            self._check_requires(element, cluster_data=cluster_data)

    def _check_requires(self, element, cluster_data=None):
        """
        Validate a requires element, raising Error if the requirement is
        not met.
        """
        if element is None:
            return
        if element == "impossible":
            raise ex.ContinueAction("skip impossible requirement")
        if element.count("(") == 1:
            rid, states = element.rstrip(")").split("(")
            states = states.split(",")
        else:
            rid = element
            states = ["up", "stdby up"]
        if rid not in self.svc.resources_by_id:
            self.log.warning("ignore requires on %s: resource not found", rid)
            return
        if cluster_data:
            try:
                current_state = cluster_data[Env.nodename]["services"]["status"][self.svc.path]["resources"][rid]["status"]
            except KeyError:
                current_state = "undef"
        else:
            resource = self.svc.resources_by_id[rid]
            current_state = core.status.Status(resource.status())
        if current_state not in states:
            msg = "requires on resource %s in state %s, current state %s" % \
                  (rid, " or ".join(states), current_state)
            if self.svc.options.cron:
                raise ex.ContinueAction(msg)
            else:
                raise ex.Error(msg)

    def dns_update(self):
        """
        Placeholder for resource specific implementation of the dns update.
        """
        pass

    def oget(self, o, **kwargs):
        return self.svc.oget(self.rid, o, **kwargs)

    def conf_get(self, o, **kwargs):
        """
        Relay for the Svc::conf_get() method, setting the resource rid as the
        config section.
        """
        return self.svc.conf_get(self.rid, o, **kwargs)

    def _status_info(self):
        """
        Placeholder for driver implementation
        """
        return {}

    def status_info(self):
        data = self._status_info()
        if not self.shared:
            self.last_status_info = data
            return data
        sopts = self.schedule_options()
        if not sopts:
            self.last_status_info = data
            return data
        data["sched"] = {}
        for saction, sopt in sopts.items():
            data["sched"][saction] = self.schedule_info(sopt)
        self.last_status_info = data
        return data

    def info(self):
        data = [
          ["driver", self.type],
          ["standby", str(self.is_standby).lower()],
          ["optional", str(self.optional).lower()],
          ["disabled", str(self.disabled).lower()],
          ["monitor", str(self.monitor).lower()],
          ["shared", str(self.shared).lower()],
          ["encap", str(self.encap).lower()],
          ["restart", str(self.nb_restart)],
        ]
        if self.subset:
            data.append(["subset", self.subset])
        if len(self.tags) > 0:
            data.append(["tags", " ".join(self.tags)])
        if hasattr(self, "_info"):
            try:
                data += getattr(self, "_info")()
            except AttributeError:
                pass
            except Exception as e:
                print(e, file=sys.stderr)
        return self.fmt_info(data)

    ##########################################################################
    #
    # provisioning
    #
    ##########################################################################
    @lazy
    def provisioned_flag(self):
        """
        The full path to the provisioned state cache file.
        """
        return os.path.join(self.var_d, "provisioned")

    def provisioned_flag_mtime(self):
        """
        Return the provisioned state cache file modification time.
        """
        try:
            return os.path.getmtime(self.provisioned_flag)
        except Exception:
            return

    def provisioned_data(self):
        """
        Return the resource provisioned state from the on-disk cache and its
        state change time as a dictionnary.
        """
        if not hasattr(self, "provisioner"):
            return
        try:
            isprov = self.is_provisioned()
        except Exception as exc:
            self.status_log("provisioned: %s" % str(exc), "error")
            isprov = False
        data = {}
        if isprov is not None:
            data["state"] = isprov
        mtime = self.provisioned_flag_mtime()
        if mtime is not None:
            data["mtime"] = mtime
        return data

    def is_provisioned_flag(self):
        """
        Return the boolean provisioned state cached on disk.
        Return None if the file does not exist or is corrupted.
        """
        if not hasattr(self, "provisioner"):
            return
        return self._is_provisioned_flag()

    def _is_provisioned_flag(self):
        try:
            with open(self.provisioned_flag, 'r') as filep:
                return json.load(filep)
        except Exception:
            return

    def write_is_provisioned_flag(self, value, mtime=None):
        """
        Write a resource-private file containing the boolean provisioned
        state and state change time.
        """
        if not hasattr(self, "provisioner"):
            return
        if value is None:
            return
        try:
            with open(self.provisioned_flag, 'w') as filep:
                try:
                    json.dump(value, filep)
                    filep.flush()
                except ValueError:
                    return
        except Exception:
            # can happen in instance delete codepath
            return
        if mtime:
            os.utime(self.provisioned_flag, (mtime, mtime))

    def has_provisioned_flag(self):
        return os.path.exists(self.provisioned_flag)

    def remove_is_provisioned_flag(self):
        """
        Remove the provisioned state cache file. Used in the Svc::delete_resource()
        code path.
        """
        if not self.has_provisioned_flag():
            return
        os.unlink(self.provisioned_flag)

    def set_unprovisioned(self):
        """
        Exposed resource action to force the provisioned state to False in the cache file.
        """
        self.log.info("set unprovisioned")
        self.write_is_provisioned_flag(False)

    def set_provisioned(self):
        """
        Exposed resource action to force the provisioned state to True in the cache file.
        """
        self.log.info("set provisioned")
        self.write_is_provisioned_flag(True)

    def format_driver_group(self):
        try:
            return self.type.split(".", 1)[0]
        except ValueError:
            return self.type
        except AttributeError:
            return ""

    def format_driver_basename(self):
        try:
            return self.type.split(".", 1)[1]
        except (ValueError, IndexError, AttributeError):
            return ""

    def format_rset_id(self):
        if self.subset is not None:
            return "%s:%s" % (self.driver_group, self.subset)
        else:
            return self.driver_group

    def provision_shared_non_leader(self):
        self.log.info("non leader shared resource provisioning")
        self.write_is_provisioned_flag(True, mtime=1)

        # do not execute post_provision triggers
        self.skip_triggers.add("post_provision")
        self.skip_triggers.add("blocking_post_provision")

        if self.skip_provision:
            self.log.info("provision skipped (configuration directive)")
            return
        if hasattr(self, "provisioner_shared_non_leader"):
            getattr(self, "provisioner_shared_non_leader")()

    def provision(self):
        if self.shared and not self.svc.options.leader:
            self.provision_shared_non_leader()
            return
        self._provision()
        try:
            self.post_provision_start()
        except Exception:
            if self.skip_provision:
                # best effort
                pass
            else:
                raise

    def _provision(self):
        """
        Unimplemented is_provisioned() trusts provisioner() to do the right
        thing.
        """
        if self.skip_provision:
            self.log.info("provision skipped (configuration directive)")
            self.write_is_provisioned_flag(True)
            return
        if not hasattr(self, "provisioner"):
            return
        if self.is_provisioned(refresh=self.refresh_provisioned_on_provision) is True:
            self.log.info("%s already provisioned", self.label)
        else:
            getattr(self, "provisioner")()
        self.write_is_provisioned_flag(True)

    def unprovision(self):
        try:
            self.pre_provision_stop()
        except Exception:
            if self.skip_unprovision:
                # best effort
                pass
            else:
                raise
        if self.shared and not self.svc.options.leader:
            self.unprovision_shared_non_leader()
            return
        self._unprovision()

    def unprovision_shared_non_leader(self):
        self.log.info("non leader shared resource unprovisioning")

        # do not execute post_unprovision triggers
        self.skip_triggers.add("post_unprovision")
        self.skip_triggers.add("blocking_post_unprovision")

        if self.skip_unprovision:
            self.log.info("unprovision skipped (configuration directive)")
            return
        if not hasattr(self, "unprovisioner") and not hasattr(self, "unprovisioner_shared_non_leader"):
            return
        if hasattr(self, "unprovisioner_shared_non_leader"):
            getattr(self, "unprovisioner_shared_non_leader")()
        self.write_is_provisioned_flag(False)

    def _unprovision(self):
        """
        Unimplemented is_provisioned() trusts unprovisioner() to do the right
        thing.
        """
        if self.skip_provision or self.skip_unprovision:
            self.log.info("unprovision skipped (configuration directive)")
            return
        if not hasattr(self, "unprovisioner") and not hasattr(self, "unprovisioner_shared_non_leader"):
            return
        if self.is_provisioned(refresh=self.refresh_provisioned_on_unprovision) is False:
            self.log.info("%s already unprovisioned", self.label)
        else:
            getattr(self, "unprovisioner")()
        self.write_is_provisioned_flag(False)

    def post_provision_start(self):
        self.start()

    def pre_provision_stop(self):
        self.stop()

    def is_provisioned(self, refresh=False):
        if not hasattr(self, "provisioner") and not hasattr(self, "provisioner_shared_non_leader"):
            return True
        if "noaction" in self.tags:
            # can not determine state if we can't run an action to toggle it
            return
        if not refresh:
            flag = self.is_provisioned_flag()
            if flag is not None:
                return flag
        if hasattr(self, "provisioned"):
            value = getattr(self, "provisioned")()
        elif hasattr(self, "provisioner") and not self.has_provisioned_flag():
            value = False
        else:
            return
        if not self.shared or self.svc.options.leader or \
           (self.shared and not refresh and value):
            self.write_is_provisioned_flag(value)
        return value

    def promote_rw(self):
        if not self.need_promote_rw:
            return
        try:
            from utilities.devices import promote_dev_rw
        except ImportError:
            self.log.warning("promote_rw is not supported on this operating system")
            return
        for dev in self.base_devs():
            promote_dev_rw(dev, log=self.log)

    def progress(self):
        utilities.lock.progress(self.svc.lockfd, {"rid": self.rid})

    def unset_lazy(self, prop):
        """
        Expose the self.unset_lazy(...) utility function as a method,
        so Node() users don't have to import it from rcUtilities.
        """
        unset_lazy(self, prop)

    def reslock(self, action=None, timeout=30, delay=1, suffix=None):
        """
        Acquire the resource action lock.
        """
        if self.lockfd is not None:
            # already acquired
            return

        lockfile = os.path.join(self.var_d, "lock")
        if suffix is not None:
            lockfile = ".".join((lockfile, suffix))

        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire resource lock %s", details)

        try:
            lockfd = utilities.lock.lock(
                timeout=timeout,
                delay=delay,
                lockfile=lockfile,
                intent=action
            )
        except utilities.lock.LockTimeout as exc:
            raise ex.Error("timed out waiting for lock %s: %s" % (details, str(exc)))
        except utilities.lock.LockNoLockFile:
            raise ex.Error("lock_nowait: set the 'lockfile' param %s" % details)
        except utilities.lock.LockCreateError:
            raise ex.Error("can not create lock file %s" % details)
        except utilities.lock.LockAcquire as exc:
            raise ex.Error("another action is currently running %s: %s" % (details, str(exc)))
        except ex.Signal:
            raise ex.Error("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.Error("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def resunlock(self):
        """
        Release the service action lock.
        """
        utilities.lock.unlock(self.lockfd)
        self.lockfd = None

    def section_kwargs(self):
        stype = self.driver_basename if self.driver_basename else self.driver_group
        return self.svc.section_kwargs(self.rid, stype)

    def replace_volname(self, *args, **kwargs):
        path, vol = self.svc.replace_volname(*args, **kwargs)
        if not vol:
            return path, vol
        volrid = self.svc.get_volume_rid(vol.name)
        if volrid:
            self.svc.register_dependency("stop", volrid, self.rid)
            self.svc.register_dependency("start", self.rid, volrid)
        return path, vol

    def direct_environment_env(self, mappings):
        env = {}
        if not mappings:
            return env
        for mapping in mappings:
            try:
                var, val = mapping.split("=", 1)
            except Exception as exc:
                self.log.info("ignored environment mapping %s: %s", mapping, exc)
                continue
            var = var.upper()
            env[var] = val
        return env

    def kind_environment_env(self, kind, mappings):
        env = {}
        if mappings is None:
            return env
        for mapping in mappings:
            try:
                var, val = mapping.split("=", 1)
            except Exception as exc:
                self.log.info("ignored %s environment mapping %s: %s", kind, mapping, exc)
                continue
            try:
                name, key = val.split("/", 1)
            except Exception as exc:
                self.log.info("ignored %s environment mapping %s: %s", kind, mapping, exc)
                continue
            var = var.upper()
            obj = factory(kind)(name, namespace=self.svc.namespace, volatile=True, node=self.svc.node)
            if not obj.exists():
                self.log.info("ignored %s environment mapping %s: config %s does not exist", kind, mapping, name)
                continue
            if key not in obj.data_keys():
                self.log.info("ignored %s environment mapping %s: key %s does not exist", kind, mapping, key)
                continue
            val = obj.decode_key(key)
            env[var] = val
        return env

    def schedule_info(self, sopt):
        try:
            last = float(self.svc.sched.get_last(sopt.fname).strftime("%s.%f"))
        except Exception:
            return {}
        data = {
            "last": last,
        }
        return data

    def schedule_options(self):
        """
        Placeholder for driver implementation.
        Must return a dict of scheduler options indexed by scheduler action.
        Used by Svc::configure_scheduler() and Resource::status_info().
        """
        return {}

    def has_capability(self, cap):
        return capabilities.has("drivers.resource.%s" % cap)

    def clear_status_cache(self):
        self.rstatus = None
        self.status_logs = []
        self.last_status_info = {}

class DataResource(Resource):
    def __init__(self, rid, type="data", **kwargs):
        Resource.__init__(self, rid, type=type)
        self.options = Storage(kwargs)

    def _status_info(self):
        data = {}
        for key, val in self.section_kwargs().items():
            if val not in (None, []):
                data[key] = val
        return data

    def _status(self, verbose=False):
        return core.status.NA

