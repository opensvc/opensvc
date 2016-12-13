"""
Defines the resource class, which is the parent class of every
resource driver.
"""
from __future__ import print_function

import os
import logging
import sys
import time
import shlex

import rcExceptions as ex
import rcStatus
import rcUtilities as utils
from rcGlobalEnv import rcEnv
import rcColor

ALLOW_ACTION_WITH_NOACTION = [
    "presync",
]

class Resource(object):
    """
    Resource drivers parent class
    """
    label = None

    def __init__(self,
                 rid=None,
                 type=None,
                 subset=None,
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 tags=None,
                 always_on=None):
        if tags is None:
            tags = set()
        if always_on is None:
            always_on = set()
        self.svc = None
        self.rset = None
        self.rid = rid
        self.tags = tags
        self.type = type
        self.subset = subset
        self.optional = optional
        self.disabled = disabled
        self.skip = False
        self.monitor = monitor
        self.nb_restart = restart
        self.rstatus = None
        self.always_on = always_on
        if self.label is None:
            self.label = type
        self.status_logs = []
        self.can_rollback = False

    @utils.lazy
    def log(self):
        """
        Lazy init for the resource logger.
        """
        return logging.getLogger(self.log_label())

    def fmt_info(self, keys=None):
        """
        Returns the resource generic keys sent to the collector upon push
        resinfo.
        """
        if keys is None:
            return []
        for idx, key in enumerate(keys):
            if len(key) == 2:
                keys[idx] = [
                    self.svc.svcname,
                    self.svc.node.nodename,
                    self.svc.clustertype,
                    self.rid
                ] + key
            elif len(key) == 3:
                keys[idx] = [
                    self.svc.svcname,
                    self.svc.node.nodename,
                    self.svc.clustertype
                ] + key
        return keys

    def log_label(self):
        """
        Return the resource label used in logs entries.
        """
        label = ""
        if hasattr(self, "svc"):
            label += self.svc.svcname + '.'

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
        label += "%s:%s#%s" % (self.type.split(".")[0], self.subset, ridx)
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
        return self.rid < other.rid

    def save_exc(self):
        """
        A helper method to save stacks in the service log.
        """
        self.log.error("unexpected error. stack saved in the service debug log")
        self.log.debug("", exc_info=True)

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
        utils.clear_cache(sig, o=self)

    @staticmethod
    def get_trigger_cmdv(cmd, kwargs):
        """
        Return the cmd arg useable by subprocess Popen
        """
        if not kwargs.get("shell", False):
            if sys.version_info[0] < 3:
                cmdv = shlex.split(cmd.encode('utf8'))
                cmdv = [elem.decode('utf8') for elem in cmdv]
            else:
                cmdv = shlex.split(cmd)
        else:
            cmdv = cmd
        return cmdv

    def action_triggers(self, driver, action, **kwargs):
        """
        Executes a resource trigger. Guess if the shell mode is needed from
        the trigger syntax.
        """
        if "blocking" in kwargs:
            blocking = kwargs["blocking"]
            del kwargs["blocking"]
        else:
            blocking = False

        if driver == "":
            attr = action
        else:
            attr = driver+"_"+action

        if not hasattr(self, attr):
            return

        cmd = getattr(self, attr)

        if "|" in cmd or "&&" in cmd or ";" in cmd:
            kwargs["shell"] = True

        cmdv = self.get_trigger_cmdv(cmd, kwargs)

        if self.svc.options.dry_run:
            self.log.info("exec trigger %s", getattr(self, attr))
            return

        try:
            result = self.vcall(cmdv, **kwargs)
            ret = result[0]
        except OSError as exc:
            ret = 1
            if exc.errno == 8:
                self.log.error("%s exec format error: check the script shebang", cmd)
            else:
                self.log.error("%s error: %s", cmd, str(exc))
        except Exception as exc:
            ret = 1
            self.log.error("%s error: %s", cmd, str(exc))

        if blocking and ret != 0:
            raise ex.excError("%s trigger %s blocking error" % (driver, cmd))

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
        always_on on this node, skip.

        Call the defined pre and post triggers.

        """
        if not hasattr(self, action):
            self.log.debug("%s action is not implemented", action)

        self.log.debug('do action %s', action)

        if "stop" in action and rcEnv.nodename in self.always_on and not self.svc.force:
            standby_action = action+'standby'
            if hasattr(self, standby_action):
                self.action_main(standby_action)
                return
            else:
                self.log.info("skip '%s' on standby resource (--force to override)", action)
                return

        self.check_requires(action)
        self.setup_environ()
        self.action_triggers("pre", action)
        self.action_triggers("blocking_pre", action, blocking=True)
        self.action_main(action)
        self.action_triggers("post", action)
        self.action_triggers("blocking_post", action, blocking=True)
        if self.need_refresh_status(action):
            self.status(refresh=True, restart=False)
        return

    def need_refresh_status(self, action):
        """
        Return True for action known to be causing a resource status change.
        """
        actions = (
            "unprovision",
            "provision",
            "install",
            "create",
            "switch",
            "migrate"
        )
        if self.svc.options.dry_run:
            return False
        if "start" in action or "stop" in action:
            return True
        if "rollback" in action:
            return True
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
            "provision",
            "unprovision",
        )
        if not self.skip:
            return False
        if action.startswith("start") or action.startswith("stop"):
            return True
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
        if self.rid != "app" and not self.svc.encap and 'encap' in self.tags:
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
        if self.disabled:
            self.log.debug('action: skip action on disabled resource')
            return True
        try:
            self.do_action(action)
        except ex.excUndefined as exc:
            print(exc)
            return False
        except ex.excError as exc:
            if self.optional:
                if len(str(exc)) > 0:
                    self.log.error(str(exc))
                self.log.info("ignore %s error on optional resource", action)
            else:
                raise

    def status_stdby(self, status):
        """
        This function modifies the passed status according
        to this node inclusion in the always_on nodeset.
        """
        if rcEnv.nodename not in self.always_on:
            return status
        if status == rcStatus.UP:
            return rcStatus.STDBY_UP
        elif status == rcStatus.DOWN:
            return rcStatus.STDBY_DOWN
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
            return rcStatus.UNDEF

    def _status(self, verbose=False):
        """
        The resource status evaluation method.
        To be implemented by drivers.
        """
        if verbose:
            self.log.debug("default resource status: undef")
        return rcStatus.UNDEF

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
        Handles caching, resource restart, nostatus tag and disabled flag.
        """
        verbose = kwargs.get("verbose", False)
        refresh = kwargs.get("refresh", False)
        restart = kwargs.get("restart", False)
        ignore_nostatus = kwargs.get("ignore_nostatus", False)

        if self.disabled:
            return rcStatus.NA

        if not ignore_nostatus and "nostatus" in self.tags:
            self.status_log("nostatus tag", "info")
            return rcStatus.NA

        if self.rstatus is not None and not refresh:
            return self.rstatus

        last_status = self.load_status_last()

        if self.svc.options.refresh or refresh:
            self.purge_status_last()
        else:
            self.rstatus = last_status

        if self.rstatus is None or self.svc.options.refresh or refresh:
            self.status_logs = []
            self.rstatus = self.try_status(verbose)
            self.log.debug("refresh status: %s => %s",
                           rcStatus.status_str(last_status),
                           rcStatus.status_str(self.rstatus))
            self.write_status()

        if restart:
            self.do_restart(last_status)

        return self.rstatus

    def do_restart(self, last_status):
        """
        Restart a resource defined to be restarted when seen down.
        """
        restart_last_status = (
            rcStatus.UP,
            rcStatus.STDBY_UP,
            rcStatus.STDBY_UP_WITH_UP,
            rcStatus.STDBY_UP_WITH_DOWN
        )
        no_restart_status = (
            rcStatus.UP,
            rcStatus.STDBY_UP,
            rcStatus.NA,
            rcStatus.UNDEF,
            rcStatus.STDBY_UP_WITH_UP,
            rcStatus.STDBY_UP_WITH_DOWN,
        )
        if self.nb_restart == 0:
            return
        if self.rstatus in no_restart_status:
            return
        if last_status not in restart_last_status:
            self.status_log("not restarted because previous status is %s" % \
                            rcStatus.status_str(last_status), "info")
            return

        if not hasattr(self, 'start'):
            self.log.error("resource restart configured on resource %s with "
                           "no 'start' action support", self.rid)
            return

        if self.svc.frozen():
            msg = "resource restart skipped: service is frozen"
            self.log.info(msg)
            self.status_log(msg, "info")
            return

        for i in range(self.nb_restart):
            try:
                self.log.info("restart resource %s. try number %d/%d",
                              self.rid, i+1, self.nb_restart)
                self.action("start")
            except Exception as exc:
                self.log.error("restart resource failed: " + str(exc))
            self.rstatus = self.try_status()
            self.write_status()
            if self.rstatus == rcStatus.UP:
                self.log.info("monitored resource %s restarted.", self.rid)
                return
            if i + 1 < self.nb_restart:
                time.sleep(1)

    def write_status(self):
        """
        Helper method to janitor resource status cache and history in files.
        """
        self.write_status_last()
        self.write_status_history()

    def fpath_status_last(self):
        """
        Return the file path for the resource status cache.
        """
        dirname = os.path.join(rcEnv.pathvar, self.svc.svcname)
        fname = "resource.status.last." + self.rid
        fpath = os.path.join(dirname, fname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return fpath

    def fpath_status_history(self):
        """
        Return the file path for the resource status history.
        """
        dirname = os.path.join(rcEnv.pathvar, self.svc.svcname)
        fname = "resource.status.history." + self.rid
        fpath = os.path.join(dirname, fname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return fpath

    def purge_status_last(self):
        """
        Purge the on-disk resource status cache.
        """
        try:
            os.unlink(self.fpath_status_last())
        except:
            pass

    def load_status_last(self):
        """
        Fetch the resource status from the on-disk cache.
        """
        try:
            with open(self.fpath_status_last(), 'r') as ofile:
                lines = ofile.read().splitlines()
        except (OSError, IOError) as exc:
            self.log.debug(exc)
            return

        try:
            status_str = lines[0]
            status = rcStatus.status_value(status_str)
        except (AttributeError, ValueError) as exc:
            self.log.debug(exc)
            return

        if len(lines) > 1:
            for line in lines[1:]:
                if line.startswith("info: "):
                    self.status_logs.append(("info", line.replace("info: ", "", 1)))
                elif line.startswith("warn: "):
                    self.status_logs.append(("warn", line.replace("warn: ", "", 1)))
                elif line.startswith("error: "):
                    self.status_logs.append(("error", line.replace("error: ", "", 1)))
                else:
                    self.status_logs.append(("warn", line))

        return status

    def write_status_last(self):
        """
        Write the in-memory resource status to the on-disk cache.
        """
        status_str = rcStatus.status_str(self.rstatus)+'\n'
        if len(self.status_logs) > 0:
            status_str += '\n'.join([entry[0]+": "+entry[1] for entry in self.status_logs])+'\n'
        with open(self.fpath_status_last(), 'w') as ofile:
            ofile.write(status_str)

    def write_status_history(self):
        """
        Log a change to the resource status history file.
        """
        fpath = self.fpath_status_history()
        try:
            with open(fpath, 'r') as ofile:
                lines = ofile.readlines()
                last = lines[-1].split(" | ")[-1].strip("\n")
        except:
            last = None
        current = rcStatus.status_str(self.rstatus)
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
        self.status_logs.append((level, text))

    def status_logs_get(self, levels=None):
        """
        Return filtered messages from the the resource status log buffer.
        """
        if levels is None:
            levels = ["info", "warn", "error"]
        return [entry[1] for entry in self.status_logs if \
                entry[0] in levels and entry[1] != ""]

    def status_logs_count(self, levels=None):
        """
        Return the number of status log buffer entries matching the
        specified levels.
        """
        if levels is None:
            levels = ["info", "warn", "error"]
        return len(self.status_logs_get(levels=levels))

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
                    color = rcColor.color.BROWN
                elif level == "error":
                    color = rcColor.color.RED
                else:
                    color = rcColor.color.LIGHTBLUE
                status_str += rcColor.colorize(entry, color)
            else:
                status_str += entry
        return status_str

    def status_quad(self, color=True):
        """
        Returns the resource properties and status as a tuple, as
        excepted by svcmon, print status and the collector feed api.
        """
        status = self.status(verbose=True)
        encap = 'encap' in self.tags
        return (self.rid,
                self.type,
                rcStatus.status_str(status),
                self.label,
                self.status_logs_str(color=color),
                self.monitor,
                self.disabled,
                self.optional,
                encap)

    def call(self, *args, **kwargs):
        """
        Wrap rcUtilities call, setting the resource logger
        """
        kwargs["log"] = self.log
        return utils.call(*args, **kwargs)

    def vcall(self, *args, **kwargs):
        """
        Wrap vcall, setting the resource logger
        """
        kwargs["log"] = self.log
        return utils.vcall(*args, **kwargs)

    @staticmethod
    def wait_for_fn(func, tmo, delay, errmsg="Waited too long for startup"):
        """
        A helper function to execute a test function until it returns True
        or the number of retries is exhausted.
        """
        for tick in range(tmo//delay):
            if func():
                return
            time.sleep(delay)
        raise ex.excError(errmsg)

    def devlist(self):
        """
        List devices the resource holds.
        """
        return self.disklist()

    @staticmethod
    def default_disklist():
        """
        If not superceded, this method return an empty disk set.
        """
        return set()

    def disklist(self):
        """
        List disks the resource holds. Some resource have none, and can leave
        this function as is.
        """
        return self.default_disklist()

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

    def provision(self):
        """
        The resource provision action entrypoint.
        """
        pass

    def unprovision(self):
        """
        The resource unprovision action entrypoint.
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
        if self.can_rollback:
            self.stop()

    def stop(self):
        """
        The resource stop action entrypoint.
        """
        pass

    def startstandby(self):
        """
        Promote the action to start if the resource is flagged always_on on
        this node.
        """
        if rcEnv.nodename in self.always_on:
            self.start()

    def start(self):
        """
        The resource start action entrypoint.
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
        if hasattr(self, "svc"):
            create_pg = self.svc.create_pg
        else:
            create_pg = self.create_pg
        if not create_pg:
            return
        try:
            mod = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.log.info("process group are not supported on this platform")
            return
        except Exception as exc:
            print(exc)
            raise
        if action == "freeze":
            mod.freeze(self)
        elif action == "thaw":
            mod.thaw(self)
        elif action == "kill":
            mod.kill(self)

    def pg_frozen(self):
        """
        Return True if the resource has its process group frozen
        """
        if not self.svc.create_pg:
            return False
        try:
            mod = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.status_log("process group are not supported on this platform", "warn")
            return False
        return mod.frozen(self)

    def create_pg(self):
        """
        Create a process group if this service asks for it and if possible.
        """
        if not self.svc.create_pg:
            return
        try:
            mod = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.log.info("process group are not supported on this platform")
            return
        except Exception as exc:
            print(exc)
            raise
        mod.create_pg(self)

    def check_requires(self, action):
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
            self._check_requires(element)

    def _check_requires(self, element):
        """
        Validate a requires element, raising excError if the requirement is
        not met.
        """
        if element is None:
            return
        if element.count("(") == 1:
            rid, states = element.rstrip(")").split("(")
            states = states.split(",")
        else:
            rid = element
            states = ["up", "stdby up"]
        if rid not in self.svc.resources_by_id:
            self.log.warning("ignore requires on %s: resource not found", rid)
            return
        resource = self.svc.resources_by_id[rid]
        current_state = rcStatus.status_str(resource.status())
        if current_state not in states:
            raise ex.excError("requires on resource %s in state %s, "
                              "current state %s" % \
                              (rid, " or ".join(states), current_state))

    def dns_update(self):
        """
        Placeholder for resource specific implementation of the dns update.
        """
        pass
