# -*- coding: utf8 -*-

"""
This module implements the Node class.
The node
* handles communications with the collector
* holds the list of services
* has a scheduler
"""
from __future__ import absolute_import, division, print_function

import fnmatch
import logging
import os
import sys
import time
from errno import ECONNREFUSED, EPIPE

import foreign.six as six

import core.exceptions as ex
import core.logger
import core.objects.builder
from core.capabilities import capabilities
from core.comm import Crypt, DEFAULT_DAEMON_TIMEOUT
from core.contexts import want_context
from core.extconfig import ExtConfigMixin
from core.freezer import Freezer
from core.network import NetworksMixin
from core.scheduler import SchedOpts, Scheduler, sched_action
from env import Env
from utilities.loop_delay import delay
from utilities.naming import (ANSI_ESCAPE, factory, fmt_path, glob_services_config,
                              is_service, new_id, paths_data,
                              resolve_path, split_path, strip_path, svc_pathetc,
                              validate_kind, validate_name, validate_ns_name,
                              object_path_glob)
from utilities.selector import selector_config_match, selector_parse_fragment, selector_parse_op_fragment
from utilities.cache import purge_cache_expired
from utilities.converters import *
from utilities.drivers import driver_import
from utilities.lazy import (lazy, lazy_initialized, set_lazy, unset_all_lazy,
                            unset_lazy)
from utilities.lock import LOCK_EXCEPTIONS
from utilities.proc import call, justcall, vcall, which, check_privs, daemon_process_running, drop_option, find_editor, \
    init_locale, does_call_cmd_need_shell, get_call_cmd_from_str
from utilities.files import assert_file_exists, assert_file_is_root_only_writeable, makedirs
from utilities.render.color import formatter
from utilities.storage import Storage
from utilities.string import bdecode

try:
    from foreign.six.moves.urllib.request import Request, urlopen
    from foreign.six.moves.urllib.error import HTTPError
    from foreign.six.moves.urllib.parse import urlencode
except ImportError:
    # pylint false positive
    pass


if six.PY2:
    BrokenPipeError = IOError

init_locale()

DEFAULT_STATUS_GROUPS = [
    "hb",
    "arbitrator",
]

ACTION_ANY_NODE = (
    "collector_cli",
    "delete",
    "eval",
    "get",
    "set",
    "unset",
    "wait",
)
ACTION_ASYNC = {
    "freeze": {
        "target": "frozen",
        "progress": "freezing",
    },
    "thaw": {
        "target": "thawed",
        "progress": "thawing",
    },
}
ACTIONS_CUSTOM_REMOTE = (
    "drain",
    "ls",
    "logs",
    "ping",
    "events",
    "daemon_stats",
    "daemon_status",
    "daemon_blacklist_status",
    "daemon_join",
    "daemon_rejoin",
    "daemon_relay_status",
    "stonith",
)
ACTIONS_NOWAIT_RESULT = (
    "delete",
    "eval",
    "get",
    "set",
    "unset",
    "reboot",
    "shutdown",
    "updatepkg",
    "updateclumgr",
    "updatecomp",
    "daemon_restart",
)
ACTIONS_NO_PARALLEL = [
    "edit_config",
    "get",
    "print_config",
    "print_resource_status",
    "print_schedule",
    "print_status",
]

ACTIONS_NO_MULTIPLE_SERVICES = [
    "print_resource_status",
]

CONFIG_DEFAULTS = {
    "push_schedule": "00:00-06:00",
    "sync_schedule": "04:00-06:00",
    "comp_schedule": "02:00-06:00",
    "collect_stats_schedule": "@10",
    "no_schedule": "",
}

UNPRIVILEGED_ACTIONS = [
    "collector_cli",
]

STATS_INTERVAL = 30


class Node(Crypt, ExtConfigMixin, NetworksMixin):
    """
    Defines a cluster node.  It contain list of Svc.
    Implements node-level actions and checks.
    """
    def __str__(self):
        return self.nodename

    def __init__(self, log_handlers=None):
        ExtConfigMixin.__init__(self, default_status_groups=DEFAULT_STATUS_GROUPS)
        self.listener = None
        self.clouds = None
        self.paths = Storage(
            reboot_flag=os.path.join(Env.paths.pathvar, "REBOOT_FLAG"),
            last_boot_id=os.path.join(Env.paths.pathvar, "node", "last_boot_id"),
            tmp_cf=os.path.join(Env.paths.pathvar, "node.conf.tmp"),
            cf=Env.paths.nodeconf,
        )
        self.services = None
        self.options = Storage(
            cron=False,
            syncrpc=False,
            force=False,
            debug=False,
            stats_dir=None,
            begin=None,
            end=None,
            moduleset="",
            module="",
            node=None,
            ruleset_date="",
            objects=[],
            format=None,
            user=None,
            api=None,
            resource=[],
            mac=None,
            broadcast=None,
            param=None,
            value=None,
            extra_argv=[],
        )
        self.stats_data = {}
        self.stats_updated = 0
        log_file = os.path.join(Env.paths.pathlog, "node.log")
        self.logger = core.logger.initLogger(Env.nodename, log_file, handlers=log_handlers)
        extra = {"node": Env.nodename, "sid": Env.session_uuid}
        self.log = logging.LoggerAdapter(self.logger, extra)

    def get_node(self):
        """
        helper for the comm module to find the Node(), for accessing
        its configuration.
        """
        return self

    @lazy
    def cd(self):
        configs = []
        if os.path.exists(Env.paths.clusterconf):
            configs.append(Env.paths.clusterconf)
        if os.path.exists(Env.paths.nodeconf):
            configs.append(Env.paths.nodeconf)
        return self.parse_config_file(configs)

    @lazy
    def private_cd(self):
        return self.parse_config_file(self.paths.cf)

    @lazy
    def kwstore(self):
        from .nodedict import KEYS
        return KEYS

    @lazy
    def devnull(self):
        return os.open(os.devnull, os.O_RDWR)

    @lazy
    def var_d(self):
        var_d = os.path.join(Env.paths.pathvar, "node")
        makedirs(var_d)
        return var_d

    @property
    def svcs(self):
        if self.services is None:
            return None
        return list(self.services.values())

    @lazy
    def freezer(self):
        """
        Lazy allocator for the freezer object.
        """
        return Freezer("node")

    @lazy
    def sched(self):
        """
        Lazy initialization of the node Scheduler object.
        """
        return Scheduler(
            config_defaults=CONFIG_DEFAULTS,
            options=self.options,
            node=self,
            scheduler_actions={
                "checks": [SchedOpts(
                    "checks",
                    req_collector=True,
                )],
                "dequeue_actions": [SchedOpts(
                    "dequeue_actions",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushstats": [SchedOpts(
                    "stats",
                    req_collector=True,
                )],
                "collect_stats": [SchedOpts(
                    "stats_collection",
                    schedule_option="collect_stats_schedule"
                )],
                "pushpkg": [SchedOpts(
                    "packages",
                    req_collector=True,
                )],
                "pushpatch": [SchedOpts(
                    "patches",
                    req_collector=True,
                )],
                "pushasset": [SchedOpts(
                    "asset",
                    req_collector=True,
                )],
                "pushnsr": [SchedOpts(
                    "nsr",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushhp3par": [SchedOpts(
                    "hp3par",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushemcvnx": [SchedOpts(
                    "emcvnx",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushcentera": [SchedOpts(
                    "centera",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushnetapp": [SchedOpts(
                    "netapp",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushibmds": [SchedOpts(
                    "ibmds",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushfreenas": [SchedOpts(
                    "freenas",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushxtremio": [SchedOpts(
                    "xtremio",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushgcedisks": [SchedOpts(
                    "gcedisks",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushhds": [SchedOpts(
                    "hds",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushnecism": [SchedOpts(
                    "necism",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pusheva": [SchedOpts(
                    "eva",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushibmsvc": [SchedOpts(
                    "ibmsvc",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushvioserver": [SchedOpts(
                    "vioserver",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushsym": [SchedOpts(
                    "sym",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushbrocade": [SchedOpts(
                    "brocade", schedule_option="no_schedule",
                    req_collector=True,
                )],
                "pushdisks": [SchedOpts(
                    "disks",
                    req_collector=True,
                )],
                "sysreport": [SchedOpts(
                    "sysreport",
                    req_collector=True,
                )],
                "compliance_auto": [SchedOpts(
                    "compliance",
                    fname="last_comp_check",
                    schedule_option="comp_schedule",
                    req_collector=True,
                )],
                "rotate_root_pw": [SchedOpts(
                    "rotate_root_pw",
                    fname="last_rotate_root_pw",
                    schedule_option="no_schedule",
                    req_collector=True,
                )],
                "auto_reboot": [SchedOpts(
                    "reboot",
                    fname="last_auto_reboot",
                    schedule_option="no_schedule"
                )]
            },
        )

    @lazy
    def collector(self):
        """
        Lazy initialization of the node Collector object.
        """
        self.log.debug("initialize node::collector")
        from core.collector.rpc import CollectorRpc
        return CollectorRpc(node=self)

    @lazy
    def nodename(self):
        """
        Lazy initialization of the node name.
        """
        return Env.nodename

    @lazy
    def compliance(self):
        from core.compliance import Compliance
        comp = Compliance(self)
        return comp

    def check_privs(self, action):
        """
        Raise if the action requires root privileges but the current
        running user is not root.
        """
        if action in UNPRIVILEGED_ACTIONS:
            return
        check_privs()

    @lazy
    def quorum(self):
        try:
            return self.conf_get("cluster", "quorum")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def dns(self):
        try:
            return self.conf_get("cluster", "dns")
        except ex.OptNotFound as exc:
            return exc.default

    @lazy
    def env(self):
        try:
            return self.conf_get("node", "env")
        except ex.OptNotFound as exc:
            return exc.default

    def get_min_avail(self, keyword, metric, limit=100):
        total = self.stats().get(metric)  # mb
        if total in (0, None):
            return 0
        try:
            val = self.conf_get("node", keyword)
        except ex.OptNotFound as exc:
            val = exc.default
        if str(val).endswith("%"):
            val = int(val.rstrip("%"))
        else:
            val = val // 1024 // 1024  # b > mb
            val = int(val/total*100)
            if val > limit:
                # unreasonable
                val = limit
        return val

    @lazy
    def min_avail_mem(self):
        return self.get_min_avail("min_avail_mem", "mem_total", 50)

    @lazy
    def min_avail_swap(self):
        return self.get_min_avail("min_avail_swap", "swap_total", 100)

    @lazy
    def max_parallel(self):
        try:
            return self.conf_get("node", "max_parallel")
        except ex.OptNotFound as exc:
            return self.default_max_parallel()

    def default_max_parallel(self):
        nr = int(self.asset.get_cpu_threads()["value"])
        if nr == 0:
            nr = int(self.asset.get_cpu_cores()["value"])
        return max(2, nr//2)

    @lazy
    def dnsnodes(self):
        if not self.dns:
            return []
        from socket import gethostbyaddr
        nodes = []
        for ip in self.dns:
            try:
                data = gethostbyaddr(ip)
                names = [data[0]] + data[1]
            except Exception as exc:
                names = []
            for node in names:
                if node in self.cluster_nodes:
                    nodes.append(node)
                    break
        return nodes

    @lazy
    def arbitrators(self):
        arbitrators = []
        for section in self.conf_sections("arbitrator"):
            data = {
                "id": section,
            }
            try:
                data["name"] = self.conf_get(section, "name")
            except Exception:
                continue
            try:
                data["secret"] = self.conf_get(section, "secret")
            except Exception:
                continue
            try:
                data["timeout"] = self.conf_get(section, "timeout")
            except ex.OptNotFound as exc:
                data["timeout"] = exc.default
            arbitrators.append(data)
        return arbitrators

    @staticmethod
    def split_url(url, default_app=None):
        """
        Split a node.conf node.dbopensvc style url into a
        (protocol, host, port, app) tuple.
        """
        if url == 'None':
            return 'https', '127.0.0.1', '443', '/'

        # transport
        if url.startswith('https'):
            transport = 'https'
            url = url.replace('https://', '')
        elif url.startswith('http'):
            transport = 'http'
            url = url.replace('http://', '')
        else:
            transport = 'https'

        elements = url.split('/')
        if len(elements) < 1:
            raise ex.Error("url %s should have at least one slash")

        # app
        if len(elements) > 1:
            app = elements[1]
        else:
            app = default_app

        # host/port
        subelements = elements[0].split(':')
        if len(subelements) == 1:
            host = subelements[0]
            if transport == 'http':
                port = '80'
            else:
                port = '443'
        elif len(subelements) == 2:
            host = subelements[0]
            port = subelements[1]
        else:
            raise ex.Error("too many columns in %s" % ":".join(subelements))

        return transport, host, port, app

    @lazy
    def collector_env(self):
        """
        Return the collector connection elements parsed from the node config
        node.uuid, node.dbopensvc and node.dbcompliance as a Storage().
        """
        data = Storage()
        url = self.oget("node", "dbopensvc")
        if url:
            try:
                (
                    data.dbopensvc_transport,
                    data.dbopensvc_host,
                    data.dbopensvc_port,
                    data.dbopensvc_app
                ) = self.split_url(url, default_app="feed")
                data.dbopensvc = "%s://%s:%s/%s/default/call/xmlrpc" % (
                    data.dbopensvc_transport,
                    data.dbopensvc_host,
                    data.dbopensvc_port,
                    data.dbopensvc_app
                )
            except ex.Error as exc:
                self.log.error("malformed dbopensvc url: %s (%s)",
                               url, str(exc))
        else:
            data.dbopensvc_transport = None
            data.dbopensvc_host = None
            data.dbopensvc_port = None
            data.dbopensvc_app = None
            data.dbopensvc = None

        url = self.oget("node", "dbcompliance")
        if url:
            try:
                (
                    data.dbcompliance_transport,
                    data.dbcompliance_host,
                    data.dbcompliance_port,
                    data.dbcompliance_app
                ) = self.split_url(url, default_app="init")
                data.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (
                    data.dbcompliance_transport,
                    data.dbcompliance_host,
                    data.dbcompliance_port,
                    data.dbcompliance_app
                )
            except ex.Error as exc:
                self.log.error("malformed dbcompliance url: %s (%s)",
                               url, str(exc))
        else:
            data.dbcompliance_transport = data.dbopensvc_transport
            data.dbcompliance_host = data.dbopensvc_host
            data.dbcompliance_port = data.dbopensvc_port
            data.dbcompliance_app = "init"
            data.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (
                data.dbcompliance_transport,
                data.dbcompliance_host,
                data.dbcompliance_port,
                data.dbcompliance_app
            )

        node_uuid = self.oget("node", "uuid")
        if node_uuid:
            data.uuid = node_uuid
        else:
            data.uuid = ""
        return data

    def call(self, *args, **kwargs):
        """
        Wrap utilities call function, setting the node logger.
        """
        kwargs["log"] = self.log
        return call(*args, **kwargs)

    def vcall(self, *args, **kwargs):
        """
        Wrap utilities vcall function, setting the node logger.
        """
        kwargs["log"] = self.log
        return vcall(*args, **kwargs)

    @staticmethod
    def filter_ns(paths, namespace):
        if not namespace:
            return paths
        if namespace == "root":
            return [path for path in paths if split_path(path)[1] is None]
        return [path for path in paths if split_path(path)[1] == namespace]

    def svcs_selector(self, selector, namespace=None, local=False):
        """
        Given a selector string, return a list of service names.
        This exposed method only aggregates ORed elements.
        """
        # fully qualified name
        path = is_service(selector, namespace, local=local)
        if path:
            self.options.single_service = True
            paths = self.filter_ns([path], namespace)
            return paths

        if not local and os.environ.get("OSVC_ACTION_ORIGIN") != "daemon":
            # the daemon always submits actions with simple, local selector.
            # avoid round trips.
            try:
                data = self._daemon_object_selector(selector, namespace, kind=os.environ.get("OSVC_KIND"))
                if isinstance(data, list):
                    return data
            except Exception as exc:
                print(exc, file=sys.stderr)
                # fallback to local lookup
                pass

        # full listing and namespace full listing
        if selector is None:
            # only svc kind by default
            selector = "*"

        # fnmatch on names and service config/status filtering
        try:
            paths = self._svcs_selector(selector, namespace)
        finally:
            del self.services
            self.services = None
        paths = self.filter_ns(paths, namespace)
        return paths

    def _svcs_selector(self, selector, namespace=None):
        if want_context():
            raise ex.Error("daemon is unreachable")
        self.build_services()
        paths = [svc.path for svc in self.svcs]
        paths = self.filter_ns(paths, namespace)
        if "," in selector:
            ored_selectors = selector.split(",")
        else:
            ored_selectors = [selector]
        result = []
        for _selector in ored_selectors:
            for path in self.__svcs_selector(_selector, paths, namespace=namespace):
                if path not in result:
                    result.append(path)
        if len(result) == 0 and not re.findall(r"[,+*=^:~><]", selector):
            raise ex.Error("object not found")
        return result

    def __svcs_selector(self, selector, paths, namespace=None):
        """
        Given a selector string, return a list of service names.
        This method only intersect the ANDed elements.
        """
        if selector in (None, ""):
            return []
        path = is_service(selector, namespace, local=True)
        if path:
            return [path]
        if "+" in selector:
            anded_selectors = selector.split("+")
        else:
            anded_selectors = [selector]
        if selector in (None, ""):
            result = paths
        else:
            result = None
            for _selector in anded_selectors:
                _paths = self.___svcs_selector(_selector, paths, namespace)
                if result is None:
                    result = _paths
                else:
                    common = set(result) & set(_paths)
                    result = [name for name in result if name in common]
        return result

    def ___svcs_selector(self, selector, paths, namespace):
        """
        Given a basic selector string (no AND nor OR), return a list of service
        names.
        """
        pds = paths_data(paths)
        kind = os.environ.get("OSVC_KIND")
        if kind:
            pds = [pd for pd in pds if pd["kind"] == kind]
            paths = [pd["display"] for pd in pds]

        negate, selector, elts = selector_parse_fragment(selector)

        if len(elts) == 1:
            return object_path_glob(selector, pds=pds, namespace=namespace, kind=kind, negate=negate)

        try:
            param, op, value = selector_parse_op_fragment(elts)
        except ValueError:
            return []

        # config keyword match
        result = []
        for svc in self.svcs:
            ret = selector_config_match(svc, param, op, value)
            if ret ^ negate:
                result.append(svc.path)

        return result

    def build_services(self, *args, **kwargs):
        """
        Instanciate a Svc objects for each requested services and add it to
        the node.
        """
        if self.svcs is not None and \
                ('paths' not in kwargs or
                 (isinstance(kwargs['paths'], list) and len(kwargs['paths']) == 0)):
            return

        if 'paths' in kwargs and \
           isinstance(kwargs['paths'], list) and \
           len(kwargs['paths']) > 0 and \
           self.svcs is not None:
            paths_request = set(kwargs['paths'])
            paths_actual = set([s.path for s in self.svcs])
            if len(paths_request-paths_actual) == 0:
                return

        self.services = {}

        if want_context():
            # build volatile objects
            for path in kwargs['paths']:
                name, namespace, kind = split_path(path)
                self += factory(kind)(name, namespace=namespace, volatile=True,
                                      cf=os.devnull, node=self)
            return

        kwargs["node"] = self
        svcs, errors = core.objects.builder.build_services(*args, **kwargs)
        if 'paths' in kwargs:
            self.check_build_errors(kwargs['paths'], svcs, errors)

        opt_status = kwargs.get("status")
        for svc in svcs:
            if opt_status is not None and not svc.status() in opt_status:
                continue
            self += svc

    @staticmethod
    def check_build_errors(paths, svcs, errors):
        """
        Raise error if the service builder did not return a Svc object for
        each service we requested.
        """
        if isinstance(paths, list):
            n_args = len(paths)
        else:
            n_args = 1
        n_svcs = len(svcs)
        if n_svcs == n_args:
            return 0
        msg = ""
        if n_args > 1:
            msg += "%d services validated out of %d\n" % (n_svcs, n_args)
        if len(errors) == 1:
            msg += errors[0]
        else:
            msg += "\n".join(["- "+err for err in errors])
        if n_args == 0 and not msg:
            return 0
        raise ex.Error(msg)

    def rebuild_services(self, paths):
        """
        Delete the list of Svc objects in the Node object and create a new one.

        Args:
          paths: add only Svc objects for services specified
        """
        del self.services
        self.services = None
        self.build_services(paths=paths, node=self)

    def close(self):
        """
        Stop the node class workers
        """
        if lazy_initialized(self, "devnull"):
            os.close(self.devnull)

        import gc
        import threading
        gc.collect()
        for thr in threading.enumerate():
            if thr.name == 'QueueFeederThread' and thr.ident is not None:
                thr.join(1)

    def make_temp_config(self):
        """
        Copy the current service configuration file to a temporary
        location for edition.
        If the temp file already exists, propose the --discard
        or --recover options.
        """
        import shutil
        if os.path.exists(self.paths.tmp_cf):
            if self.options.recover:
                pass
            elif self.options.discard:
                shutil.copy(Env.paths.nodeconf, self.paths.tmp_cf)
            else:
                self.edit_config_diff()
                print("%s exists: node conf is already being edited. Set "
                      "--discard to edit from the current configuration, "
                      "or --recover to open the unapplied config" %
                      self.paths.tmp_cf, file=sys.stderr)
                raise ex.Error
        else:
            shutil.copy(Env.paths.nodeconf, self.paths.tmp_cf)
        return self.paths.tmp_cf

    def edit_config_diff(self):
        """
        Display the diff between the current config and the pending
        unvalidated config.
        """
        from subprocess import call

        def diff_capable(opts):
            cmd = ["diff"] + opts + [Env.paths.nodeconf, self.paths.cf]
            cmd_results = justcall(cmd)
            if cmd_results[2] == 0:
                return True
            return False

        if not os.path.exists(self.paths.tmp_cf):
            return
        if diff_capable(["-u", "--color"]):
            cmd = ["diff", "-u", "--color", Env.paths.nodeconf, self.paths.tmp_cf]
        elif diff_capable(["-u"]):
            cmd = ["diff", "-u", Env.paths.nodeconf, self.paths.tmp_cf]
        else:
            cmd = ["diff", Env.paths.nodeconf, self.paths.tmp_cf]
        call(cmd)

    def nodeconf_csum(self):
        from utilities.files import fsum
        return fsum(Env.paths.nodeconf)

    def edit_config(self):
        """
        Execute an editor on the node configuration file.
        When the editor exits, validate the new configuration file.
        If validation pass, install the new configuration,
        else keep the previous configuration in place and offer the
        user the --recover or --discard choices for its next edit
        config action.
        """
        try:
            editor = find_editor()
        except ex.Error as err:
            print(err, file=sys.stderr)
            return 1
        from utilities.files import fsum
        path = self.make_temp_config()
        os.system(' '.join((editor, path)))
        if fsum(path) == self.nodeconf_csum():
            os.unlink(path)
            return 0
        results = self._validate_config(path=path)
        if results["errors"] == 0:
            import shutil
            shutil.copy(path, Env.paths.nodeconf)
            os.unlink(path)
        else:
            print("your changes were not applied because of the errors "
                  "reported above. you can use the edit config command "
                  "with --recover to try to fix your changes or with "
                  "--discard to restart from the live config")
        return results["errors"] + results["warnings"]

    def purge_status_last(self):
        """
        Purge the cached status of each and every services and resources.
        """
        for svc in self.svcs:
            svc.purge_status_last()

    def __iadd__(self, svc):
        """
        Implement the Node() += Svc() operation, setting the node backpointer
        in the added service, storing the service in a list
        """
        if not hasattr(svc, "path"):
            return self
        if self.services is None:
            self.services = {}
        self.services[svc.path] = svc
        return self

    def action(self, action, options=None):
        """
        The node action wrapper.
        Looks up which method to handle the action (some are not implemented
        in the Node class), and call the handling method.
        """
        try:
            self.async_action(action)
        except ex.AbortAction:
            return 0
        try:
            return self._action(action, options)
        except LOCK_EXCEPTIONS as exc:
            self.log.warning(exc)
            return 1

    @sched_action
    def _action(self, action, options=None):
        if "_json_" in action:
            self.options.format = "json"
            action = action.replace("_json_", "_")
        if action.startswith("json_"):
            self.options.format = "json"
            action = "print" + action[4:]

        if action.startswith("compliance_"):
            if self.options.cron and action == "compliance_auto" and \
               self.oget('compliance', 'auto_update'):
                self.compliance.updatecomp = True
                self.compliance.node = self
            ret = getattr(self.compliance, action)()
        elif action.startswith("collector_") and action != "collector_cli":
            from core.collector.actions import CollectorActions
            coll = CollectorActions(self.options, self)
            data = getattr(coll, action)()
            self.print_data(data)
            ret = 0
        elif action.startswith("print"):
            getattr(self, action)()
            ret = 0
        else:
            ret = getattr(self, action)()

        if action in ACTION_ASYNC and os.environ.get("OSVC_ACTION_ORIGIN") != "daemon":
            self.wake_monitor()

        if ret is None:
            ret = 0
        elif isinstance(ret, bool):
            if ret:
                return 1
            else:
                return 0
        return ret

    @formatter
    def print_data(self, data, default_fmt=None):
        """
        A dummy method decorated by the formatter function.
        The formatter needs self to access the formatting options, so this
        can't be a staticmethod.
        """
        return data

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        return self.sched.print_schedule()

    def collect_stats(self):
        """
        Choose the os specific stats collection module and call its collect
        method.
        """
        try:
            import utilities.stats.collector
        except ImportError:
            return
        utilities.stats.collector.collect(self)

    def pushstats(self):
        """
        Set stats range to push to "last successful pushstat => now"

        Enforce a minimum interval of 21m, and a maximum of 1450m.

        The scheduled task that collects system statistics from system tools
        like sar, and sends the data to the collector.
        A list of metrics can be disabled from the task configuration section,
        using the 'disable' option.
        """
        fpath = self.sched.get_timestamp_f(self.sched.actions["pushstats"].fname,
                                           success=True)
        try:
            with open(fpath, "r") as ofile:
                buff = ofile.read()
            start = datetime.datetime.strptime(buff, "%Y-%m-%d %H:%M:%S.%f\n")
            now = datetime.datetime.now()
            delta = now - start
            interval = delta.days * 1440 + delta.seconds // 60 + 10
        except:
            interval = 1450
        if interval < 21:
            interval = 21

        def get_disable_stats():
            """
            Returns the list of stats metrics collection disabled through the
            configuration file stats.disable option.
            """
            disable = self.oget("stats", "disable")
            if not disable:
                return []
            return disable

        disable = get_disable_stats()
        return self.collector.call('push_stats',
                                   stats_dir=self.options.stats_dir,
                                   stats_start=self.options.begin,
                                   stats_end=self.options.end,
                                   interval=interval,
                                   disable=disable)

    @lazy
    def asset(self):
        from utilities.asset import Asset
        return Asset(self)

    def pushpkg(self):
        """
        The pushpkg action entrypoint.
        Inventories the installed packages.
        """
        self.collector.call('push_pkg')

    def pushpatch(self):
        """
        The pushpatch action entrypoint.
        Inventories the installed patches.
        """
        self.collector.call('push_patch')

    def pushasset(self):
        """
        The pushasset action entrypoint.
        Inventories the server properties.
        """
        data = self.asset.get_asset_dict()
        try:
            if self.options.format is None:
                self.print_asset(data)
                return
            self.print_data(data)
        finally:
            self.collector.call('push_asset', self, data)

    def print_asset(self, data):
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        head_node = tree.add_node()
        head_node.add_column(Env.nodename, color.BOLD)
        head_node.add_column("Value", color.BOLD)
        head_node.add_column("Source", color.BOLD)
        for key in sorted(data):
            _data = data[key]
            node = head_node.add_node()
            if key not in ("targets", "lan", "uids", "gids", "hba", "hardware"):
                if _data["value"] is None:
                    _data["value"] = ""
                node.add_column(_data["title"], color.LIGHTBLUE)
                if "formatted_value" in _data:
                    node.add_column(_data["formatted_value"])
                else:
                    node.add_column(_data["value"])
                node.add_column(_data["source"])

        if 'uids' in data:
            node = head_node.add_node()
            node.add_column("uids", color.LIGHTBLUE)
            node.add_column(str(len(data['uids'])))
            node.add_column(self.asset.s_probe)

        if 'gids' in data:
            node = head_node.add_node()
            node.add_column("gids", color.LIGHTBLUE)
            node.add_column(str(len(data['gids'])))
            node.add_column(self.asset.s_probe)

        if 'hardware' in data:
            node = head_node.add_node()
            node.add_column("hardware", color.LIGHTBLUE)
            node.add_column(str(len(data['hardware'])))
            node.add_column(self.asset.s_probe)
            for _data in data['hardware']:
                _node = node.add_node()
                _node.add_column("%s %s" % (_data["type"], _data["path"]))
                _node.add_column("%s: %s [%s]" % (_data["class"], _data["description"], _data["driver"]))

        if 'hba' in data:
            node = head_node.add_node()
            node.add_column("host bus adapters", color.LIGHTBLUE)
            node.add_column(str(len(data['hba'])))
            node.add_column(self.asset.s_probe)
            for _data in data['hba']:
                _node = node.add_node()
                _node.add_column(_data["hba_id"])
                _node.add_column(_data["hba_type"])

                if 'targets' in data:
                    hba_targets = [_d for _d in data['targets'] if _d["hba_id"] == _data["hba_id"]]
                    if len(hba_targets) == 0:
                        continue
                    __node = _node.add_node()
                    __node.add_column("targets")
                    for _d in hba_targets:
                        ___node = __node.add_node()
                        ___node.add_column(_d["tgt_id"])

        if 'lan' in data:
            node = head_node.add_node()
            node.add_column("ip addresses", color.LIGHTBLUE)
            n = 0
            for mac, _data in data['lan'].items():
                for __data in _data:
                    _node = node.add_node()
                    addr = __data["addr"]
                    if __data["mask"]:
                        addr += "/" + __data["mask"]
                    _node.add_column(addr)
                    _node.add_column(__data["intf"])
                    n += 1
            node.add_column(str(n))
            node.add_column(self.asset.s_probe)

        tree.out()

    def pushnsr(self):
        """
        The pushnsr action entrypoint.
        Inventories Networker Backup Server index databases.
        """
        self.collector.call('push_nsr')

    def pushhp3par(self):
        """
        The push3par action entrypoint.
        Inventories HP 3par storage arrays.
        """
        self.collector.call('push_hp3par', self.options.objects)

    def pushnetapp(self):
        """
        The pushnetapp action entrypoint.
        Inventories NetApp storage arrays.
        """
        self.collector.call('push_netapp', self.options.objects)

    def pushcentera(self):
        """
        The pushcentera action entrypoint.
        Inventories Centera storage arrays.
        """
        self.collector.call('push_centera', self.options.objects)

    def pushemcvnx(self):
        """
        The pushemcvnx action entrypoint.
        Inventories EMC VNX storage arrays.
        """
        self.collector.call('push_emcvnx', self.options.objects)

    def pushibmds(self):
        """
        The pushibmds action entrypoint.
        Inventories IBM DS storage arrays.
        """
        self.collector.call('push_ibmds', self.options.objects)

    def pushgcedisks(self):
        """
        The pushgcedisks action entrypoint.
        Inventories Google Compute Engine disks.
        """
        self.collector.call('push_gcedisks', self.options.objects)

    def pushfreenas(self):
        """
        The pushfreenas action entrypoint.
        Inventories FreeNas storage arrays.
        """
        self.collector.call('push_freenas', self.options.objects)

    def pushxtremio(self):
        """
        The pushxtremio action entrypoint.
        Inventories XtremIO storage arrays.
        """
        self.collector.call('push_xtremio', self.options.objects)

    def pushhds(self):
        """
        The pushhds action entrypoint.
        Inventories Hitachi storage arrays.
        """
        self.collector.call('push_hds', self.options.objects)

    def pushnecism(self):
        """
        The pushnecism action entrypoint.
        Inventories NEC iSM storage arrays.
        """
        self.collector.call('push_necism', self.options.objects)

    def pusheva(self):
        """
        The pusheva action entrypoint.
        Inventories HP EVA storage arrays.
        """
        self.collector.call('push_eva', self.options.objects)

    def pushibmsvc(self):
        """
        The pushibmsvc action entrypoint.
        Inventories IBM SVC storage arrays.
        """
        self.collector.call('push_ibmsvc', self.options.objects)

    def pushvioserver(self):
        """
        The pushvioserver action entrypoint.
        Inventories IBM vio server storage arrays.
        """
        self.collector.call('push_vioserver', self.options.objects)

    def pushdorado(self):
        """
        The pushdorado action entrypoint.
        Inventories Huawei Dorado storage arrays.
        """
        self.collector.call('push_dorado', self.options.objects)

    def pushhcs(self):
        """
        The pushhcs action entrypoint.
        Inventories Hitachi Command Suite storage arrays.
        """
        self.collector.call('push_hcs', self.options.objects)

    def pushsym(self):
        """
        The pushsym action entrypoint.
        Inventories EMC Symmetrix server storage arrays.
        """
        self.collector.call('push_sym', self.options.objects)

    def pushbrocade(self):
        """
        The pushsym action entrypoint.
        Inventories Brocade SAN switches.
        """
        self.collector.call('push_brocade', self.options.objects)

    def auto_rotate_root_pw(self):
        """
        The rotate_root_pw node action entrypoint.
        """
        self.rotate_root_pw()

    def unschedule_reboot(self):
        """
        Unflag the node for reboot during the next allowed period.
        """
        if not os.path.exists(self.paths.reboot_flag):
            print("reboot already not scheduled")
            return
        os.unlink(self.paths.reboot_flag)
        print("reboot unscheduled")

    def schedule_reboot(self):
        """
        Flag the node for reboot during the next allowed period.
        """
        if not os.path.exists(self.paths.reboot_flag):
            with open(self.paths.reboot_flag, "w") as ofile:
                ofile.write("")
        import stat
        statinfo = os.stat(self.paths.reboot_flag)
        if statinfo.st_uid != 0:
            os.chown(self.paths.reboot_flag, 0, -1)
            print("set %s root ownership" % self.paths.reboot_flag)
        if statinfo.st_mode & stat.S_IWOTH:
            mode = statinfo.st_mode ^ stat.S_IWOTH
            os.chmod(self.paths.reboot_flag, mode)
            print("set %s not world-writable" % self.paths.reboot_flag)
        print("reboot scheduled")

    def schedule_reboot_status(self):
        """
        Display information about the next scheduled reboot
        """
        import stat
        if os.path.exists(self.paths.reboot_flag):
            statinfo = os.stat(self.paths.reboot_flag)
        else:
            statinfo = None

        if statinfo is None or \
           statinfo.st_uid != 0 or statinfo.st_mode & stat.S_IWOTH:
            print("reboot is not scheduled")
            return

        sch = self.sched.actions["auto_reboot"][0]
        try:
            schedule = self.sched.get_schedule_raw(sch.section, sch.schedule_option)
        except Exception:
            schedule = ""

        print("reboot is scheduled")
        print("reboot schedule: %s" % schedule)

        if not schedule:
            return

        result, _ = self.sched.get_schedule("reboot", "schedule").get_next()
        if result:
            print("next reboot slot:", result.strftime("%a %Y-%m-%d %H:%M"))
        else:
            print("next reboot slot: none")

    def auto_reboot(self):
        """
        The scheduler task executing the node reboot if the scheduler
        constraints are satisfied and the reboot flag is set.
        """
        try:
            assert_file_exists(self.paths.reboot_flag)
            assert_file_is_root_only_writeable(self.paths.reboot_flag)
        except Exception as error:
            print('%s. abort scheduled reboot' % error)
            return
        once = self.oget("reboot", "once")
        if once:
            print("remove %s and reboot" % self.paths.reboot_flag)
            os.unlink(self.paths.reboot_flag)
        self.reboot()

    def pushdisks(self):
        """
        The pushdisks node action entrypoint.

        Send to the collector the list of disks visible on this node, and
        their use by service.
        """
        data = self.push_disks_data()
        if self.options.format is None:
            self.print_push_disks(data)
        else:
            self.print_data(data)
        self.collector.call('push_disks', data)

    def print_push_disks(self, data):
        from utilities.render.forest import Forest
        from utilities.render.color import color

        tree = Forest()
        head_node = tree.add_node()
        head_node.add_column(Env.nodename, color.BOLD)
        head_node.add_column("DiskGroup", color.BOLD)
        head_node.add_column("Size.Used", color.BOLD)
        head_node.add_column("Vendor", color.BOLD)
        head_node.add_column("Model", color.BOLD)

        if len(data["disks"]) > 0:
            disks_node = head_node.add_node()
            disks_node.add_column("disks", color.BROWN)

        if len(data["served_disks"]) > 0:
            sdisks_node = head_node.add_node()
            sdisks_node.add_column("served disks", color.BROWN)

        for disk_id, disk in data["disks"].items():
            disk_node = disks_node.add_node()
            disk_node.add_column(disk_id, color.LIGHTBLUE)
            disk_node.add_column(disk["dg"])
            disk_node.add_column(print_size(disk["size"]))
            disk_node.add_column(disk["vendor"])
            disk_node.add_column(disk["model"])

            for path, service in disk["services"].items():
                svc_node = disk_node.add_node()
                svc_node.add_column(path, color.LIGHTBLUE)
                svc_node.add_column(disk["dg"])
                svc_node.add_column(print_size(service["used"]))

            if disk["used"] < disk["size"]:
                svc_node = disk_node.add_node()
                svc_node.add_column(Env.nodename, color.LIGHTBLUE)
                svc_node.add_column(disk["dg"])
                svc_node.add_column(print_size(disk["size"] - disk["used"]))

        for disk_id, disk in data["served_disks"].items():
            disk_node = disks_node.add_node()
            disk_node.add_column(disk_id, color.LIGHTBLUE)
            disk_node.add_column(disk["dg"])
            disk_node.add_column(print_size(disk["size"]))
            disk_node.add_column(disk["vdisk_id"])

        tree.out()

    def push_disks_data(self):
        if self.svcs is None:
            self.build_services()

        data = {
            "disks": {},
            "served_disks": {},
        }

        for svc in self.svcs:
            # hash to add up disk usage inside a service
            for r in svc.get_resources():
                if hasattr(r, 'devmap') and hasattr(r, 'vm_hostname'):
                    for dev_id, vdev_id in r.devmap():
                        try:
                            disk_id = self.diskinfo.disk_id(dev_id)
                        except:
                            continue
                        try:
                            disk_size = self.diskinfo.disk_size(dev_id)
                        except:
                            continue
                        data["served_disks"][disk_id] = {
                            "dev_id": dev_id,
                            "vdev_id": vdev_id,
                            "vdisk_id": r.vm_hostname+'.'+vdev_id,
                            "size": disk_size,
                            "cluster": self.cluster_name,
                        }

                try:
                    devpaths = r.sub_devs()
                except Exception as e:
                    print(e)
                    devpaths = []

                for devpath in devpaths:
                    for d, used, region in self.devtree.get_top_devs_usage_for_devpath(devpath):
                        disk_id = self.diskinfo.disk_id(d)
                        if disk_id is None or disk_id == "":
                            continue
                        if disk_id.startswith(Env.nodename+".loop"):
                            continue
                        dev = self.devtree.get_dev_by_devpath(d)
                        disk_size = dev.size

                        if disk_id not in data["disks"]:
                            data["disks"][disk_id] = {
                                "size": disk_size,
                                "dg": dev.dg,
                                "vendor": self.diskinfo.disk_vendor(d),
                                "model": self.diskinfo.disk_model(d),
                                "used": 0,
                                "services": {},
                            }

                        if svc.path not in data["disks"][disk_id]["services"]:
                            data["disks"][disk_id]["services"][svc.path] = {
                                "svcname": svc.path,
                                "used": used,
                                "region": region
                            }
                        else:
                            # consume space at service level
                            data["disks"][disk_id]["services"][svc.path]["used"] += used
                            if data["disks"][disk_id]["services"][svc.path]["used"] > disk_size:
                                data["disks"][disk_id]["services"][svc.path]["used"] = disk_size

                        # consume space at disk level
                        data["disks"][disk_id]["used"] += used
                        if data["disks"][disk_id]["used"] > disk_size:
                            data["disks"][disk_id]["used"] = disk_size

        done = []

        try:
            devpaths = self.devlist()
        except Exception as e:
            print(e)
            devpaths = []

        for devpath in devpaths:
            disk_id = self.diskinfo.disk_id(devpath)
            if disk_id is None or disk_id == "":
                continue
            if disk_id.startswith(Env.nodename+".loop"):
                continue
            if re.match(r"/dev/rdsk/.*s[01345678]", devpath):
                # don't report partitions
                continue

            # Linux Node:devlist() reports paths, so we can have duplicate
            # disks here.
            if disk_id in done:
                continue
            done.append(disk_id)
            if disk_id in data["disks"]:
                continue

            dev = self.devtree.get_dev_by_devpath(devpath)
            disk_size = dev.size

            data["disks"][disk_id] = {
                "size": disk_size,
                "dg": dev.dg,
                "vendor": self.diskinfo.disk_vendor(devpath),
                "model": self.diskinfo.disk_model(devpath),
                "used": 0,
                "services": {},
            }

        return data

    def shutdown(self):
        """
        The shutdown node action entrypoint.
        To be overloaded by child classes.
        """
        self.log.warning("to be implemented")

    def reboot(self):
        """
        The reboot node action entrypoint.
        """
        self.do_triggers("reboot", "pre")
        self.log.info("reboot")
        self._reboot()

    def do_triggers(self, action, when):
        """
        Determine which triggers need to be executed for the action and
        executes them when appropriate.
        """
        trigger = self.oget(action, when)
        blocking_trigger = self.oget(action, "blocking_"+when)
        if trigger:
            self.log.info("execute trigger %s", trigger)
            try:
                self.do_trigger(trigger)
            except ex.Error:
                pass
        if blocking_trigger:
            self.log.info("execute blocking trigger %s", blocking_trigger)
            try:
                self.do_trigger(blocking_trigger)
            except ex.Error:
                if when == "pre":
                    self.log.error("blocking pre trigger error: abort %s", action)
                raise

    def do_trigger(self, cmd, err_to_warn=False):
        """
        The trigger execution wrapper.
        """
        shell = False
        if does_call_cmd_need_shell(cmd):
            shell = True
        cmd = get_call_cmd_from_str(cmd, shell=shell)
        ret, out, err = self.vcall(cmd, err_to_warn=err_to_warn, shell=shell)
        if ret != 0:
            raise ex.Error((ret, out, err))

    def suicide(self, method, delay=0):
        self.log.info('node commit suicide in %s seconds using method %s', delay, method)
        _suicide = {
            "crash": self.sys_crash,
            "reboot": self.sys_reboot,
        }.get(method)
        if _suicide:
            _suicide(delay)
        else:
            self.log.warning("invalid commit suicide method %s", method)

    def sys_reboot(self, delay=0):
        pass

    def sys_crash(self, delay=0):
        pass

    def _reboot(self):
        """
        A system reboot method to be implemented by child classes.
        """
        self.log.warning("to be implemented")

    @lazy
    def sysreport_mod(self):
        try:
            import core.sysreport
            return core.sysreport
        except ImportError:
            print("sysreport is not supported on this os")
            return

    def sysreport(self):
        """
        The sysreport node action entrypoint.

        Send to the collector a tarball of the files the user wants to track
        that changed since the last call.
        If the force option is set, send all files the user wants to track.
        """
        if self.sysreport_mod is None:
            return
        self.sysreport_mod.SysReport(node=self).sysreport(force=self.options.force)

    def get_prkey(self):
        """
        Returns the persistent reservation key.
        Once generated from the algorithm, the prkey is written to the config
        file to ensure its stability.
        """
        hostid = self.oget("node", "prkey")
        if hostid:
            if len(hostid) > 18 or not hostid.startswith("0x") or \
               len(set(hostid[2:]) - set("0123456789abcdefABCDEF")) > 0:
                raise ex.Error("prkey in node.conf must have 16 significant"
                               " hex digits max (ex: 0x90520a45138e85)")
            return hostid
        self.log.info("can't find a prkey forced in node.conf. generate one.")
        hostid = "0x"+self.hostid()
        self.set_multi(["node.prkey="+hostid])
        return hostid

    def prkey(self):
        """
        Print the persistent reservation key.
        """
        print(self.get_prkey())

    @staticmethod
    def hostid():
        """
        Return a stable host unique id
        """
        from utilities.hostid import hostid
        return hostid()

    def checks(self):
        """
        The checks node action entrypoint.
        Runs health checks.
        """
        drvs = self.checks_drivers()
        data = drvs.do_checks()
        if self.options.format is None:
            drvs.print_checks(data)
        else:
            self.print_data(data)
        self.collector.call('push_checks', data)

    def checks_drivers(self, checkers=None):
        import drivers.check
        if self.svcs is None:
            self.build_services()
        objs = [svc for svc in self.svcs if svc.kind in ["vol", "svc"]]
        return drivers.check.Checks(objs, node=self, checkers=checkers)

    def wol(self):
        """
        Send a Wake-On-LAN packet to a mac address on the broadcast address.
        """
        import utilities.wakeonlan
        if self.options.mac is None:
            print("missing parameter. set --mac argument. multiple mac "
                  "addresses must be separated by comma", file=sys.stderr)
            print("example 1 : --mac 00:11:22:33:44:55", file=sys.stderr)
            print("example 2 : --mac 00:11:22:33:44:55,66:77:88:99:AA:BB",
                  file=sys.stderr)
            return 1
        if self.options.broadcast is None:
            print("missing parameter. set --broadcast argument. needed to "
                  "identify accurate network to use", file=sys.stderr)
            print("example 1 : --broadcast 10.25.107.255", file=sys.stderr)
            print("example 2 : --broadcast 192.168.1.5,10.25.107.255",
                  file=sys.stderr)
            return 1
        macs = self.options.mac.split(',')
        broadcasts = self.options.broadcast.split(',')
        udpports = str(self.options.port).split(',')
        for brdcast in broadcasts:
            for mac in macs:
                for port in udpports:
                    req = utilities.wakeonlan.wolrequest(macaddress=mac, broadcast=brdcast, udpport=port)
                    if not req.check_broadcast():
                        print("Error : skipping broadcast address <%s>, not in "
                              "the expected format 123.123.123.123" % req.broadcast,
                              file=sys.stderr)
                        break
                    if not req.check_mac():
                        print("Error : skipping mac address <%s>, not in the "
                              "expected format 00:11:22:33:44:55" % req.mac,
                              file=sys.stderr)
                        continue
                    if req.send():
                        print("Sent Wake On Lan packet to mac address <%s>" % req.mac)
                    else:
                        print("Error while trying to send Wake On Lan packet to "
                              "mac address <%s>" % req.mac, file=sys.stderr)

    def allocate_rid(self, group, sections):
        """
        Return an unused rid in <group>.
        """
        prefix = group + "#"
        rids = [section for section in sections if section.startswith(prefix)]
        idx = 1
        while True:
            rid = "#".join((group, str(idx)))
            if rid in rids:
                idx += 1
                continue
            return rid

    def register_as_node(self):
        """
        Returns a node registration unique id, authenticating to the
        collector as a user.
        """
        data = self.collector.call('register_node')
        if data is None:
            raise ex.Error("failed to obtain a registration number")
        elif isinstance(data, dict) and "ret" in data and data["ret"] != 0:
            msg = "failed to obtain a registration number"
            if "msg" in data and len(data["msg"]) > 0:
                msg += "\n" + data["msg"]
            raise ex.Error(msg)
        elif isinstance(data, list):
            raise ex.Error(data[0])
        return data

    def snooze(self):
        """
        Snooze notifications on the node.
        """
        if self.options.duration is None:
            print("set --duration", file=sys.stderr)
            raise ex.Error
        try:
            data = self.collector_rest_post("/nodes/self/snooze", {
                "duration": self.options.duration,
            })
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])
        print(data.get("info", ""))

    def unsnooze(self):
        """
        Unsnooze notifications on the node.
        """
        try:
            data = self.collector_rest_post("/nodes/self/snooze")
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])
        print(data.get("info", ""))

    def register_as_user(self):
        """
        Returns a node registration unique id, authenticating to the
        collector as a node.
        """
        try:
            data = self.collector_rest_post("/register", {
                "nodename": Env.nodename,
                "app": self.options.app
            })
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(data["error"])
        return data["data"]["uuid"]

    def register(self):
        """
        Do anonymous or indentified node register to obtain a node uuid
        that will be used as a password valid for the current hostname used
        as a username in the application code context.
        """
        if self.options.user is not None:
            register_fn = "register_as_user"
        else:
            register_fn = "register_as_node"
        try:
            uuid = getattr(self, register_fn)()
        except ex.Error as exc:
            print(exc, file=sys.stderr)
            return 1

        try:
            self.set_multi(["node.uuid="+uuid])
            self.unset_lazy("private_cd")
            self.unset_lazy("cd")
            self.unset_lazy("collector_env")
        except ex.Error:
            print("failed to write registration number: %s" % uuid,
                  file=sys.stderr)
            return 1

        print("registered")
        self.options.syncrpc = True
        self.pushasset()
        self.pushdisks()
        self.pushpkg()
        self.pushpatch()
        self.sysreport()
        self.checks()
        return 0

    def service_action_worker(self, svc, action, options):
        """
        The method the per-service subprocesses execute
        """
        try:
            return svc.action(action, options)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            return 1
        #finally:
        #    self.close()

    @lazy
    def diskinfo(self):
        from utilities.diskinfo import DiskInfo
        return DiskInfo()

    @lazy
    def devtree(self):
        from utilities.devtree import DevTree
        tree = DevTree()
        tree.load()
        return tree

    def devlist(self):
        """
        Return the node's top-level device paths
        """
        devpaths = []
        for dev in self.devtree.get_top_devs():
            if len(dev.devpath) > 0:
                devpaths.append(dev.devpath[0])
        return devpaths

    def updatecomp(self):
        """
        Downloads and installs the compliance module archive from the url
        specified as node.repocomp or node.repo in node.conf.
        """
        repocomp = self.oget("node", "repocomp")
        repo = self.oget("node", "repo")
        if repocomp:
            pkg_name = repocomp.strip('/') + "/current"
        elif repo:
            pkg_name = repo.strip('/') + "/compliance/current"
        else:
            if self.options.cron:
                return 0
            print("node.repo or node.repocomp must be set in node.conf",
                  file=sys.stderr)
            return 1

        from utilities.uri import Uri
        print("get %s" % pkg_name)
        secure = self.oget("node", "secure_fetch")
        try:
            with Uri(pkg_name, secure=secure).fetch() as fpath:
                self._updatecomp(fpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            if self.options.cron:
                return 0
            return 1
        return 0

    def _updatecomp(self, fpath):
        """
        Installs the compliance module archive from the downloaded archive.
        """
        def do(fpath):
            tmpp = os.path.join(Env.paths.pathtmp, 'compliance')
            backp = os.path.join(Env.paths.pathtmp, 'compliance.bck')
            compp = os.path.join(Env.paths.pathvar, 'compliance')
            makedirs(compp)
            import shutil
            try:
                shutil.rmtree(backp)
            except (OSError, IOError):
                pass
            print("extract compliance in", Env.paths.pathtmp)
            import tarfile
            tar = tarfile.open(fpath)
            os.chdir(Env.paths.pathtmp)
            try:
                tar.extractall()
                tar.close()
            except (OSError, IOError):
                print("failed to unpack", file=sys.stderr)
                return 1
            upstream_mods_d = os.path.join(tmpp, "com.opensvc")
            prev_upstream_mods_d = os.path.join(compp, "com.opensvc")
            if not os.path.exists(upstream_mods_d) and \
               os.path.exists(prev_upstream_mods_d):
                print("merge upstream com.opensvc compliance objects")
                shutil.copytree(prev_upstream_mods_d, upstream_mods_d,
                                symlinks=True)
            print("install new compliance")
            for root, dirs, files in os.walk(tmpp):
                for fpath in dirs:
                    os.chown(os.path.join(root, fpath), 0, 0)
                    for fpath in files:
                        os.chown(os.path.join(root, fpath), 0, 0)
            shutil.move(compp, backp)
            shutil.move(tmpp, compp)


    def updatepkg(self):
        """
        Downloads and upgrades the OpenSVC agent, using the system-specific
        packaging tools.
        """
        branch = self.oget("node", "branch")
        if branch:
            branch = "/" + branch
        else:
            branch = ""
        try:
            pkg_format = self.conf_get("node", "pkg_format")
        except ex.OptNotFound as exc:
            pkg_format = exc.default

        if pkg_format == "tar":
            modname = 'utilities.packages.update.osf1'
        else:
            modname = 'utilities.packages.update.'+Env.module_sysname

        import importlib
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            print("updatepkg not implemented on", Env.sysname, file=sys.stderr)
            return 1
        repopkg = self.oget("node", "repopkg")
        repo = self.oget("node", "repo")
        if repopkg:
            pkg_name = repopkg.strip('/') + "/" + mod.repo_subdir + branch + '/current'
        elif repo:
            pkg_name = repo.strip('/') + "/packages/" + mod.repo_subdir + branch + '/current'
        else:
            print("node.repo or node.repopkg must be set in node.conf",
                  file=sys.stderr)
            return 1

        from utilities.uri import Uri
        print("get %s" % pkg_name)
        secure = self.oget("node", "secure_fetch")
        try:
            with Uri(pkg_name, secure=secure).fetch() as fpath:
                print("updating opensvc")
                mod.update(fpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            return 1
        os.system("%s node pushasset" % Env.paths.om)
        return 0

    def updateclumgr(self):
        """
        Downloads and installs the cluster manager bundle archive from the url
        specified as node.repopkg or node.repo in node.conf.
        """
        import daemon.shared as shared
        api_version = str(shared.API_VERSION)
        repopkg = self.oget("node", "repopkg")
        repo = self.oget("node", "repo")
        if repopkg:
            bundle_basename = repopkg.strip('/')
        elif repo:
            bundle_basename = repo.strip('/')
        else:
            if self.options.cron:
                return 0
            print("node.repo or node.repopkg must be set in node.conf",
                  file=sys.stderr)
            return 1
        bundle_name = bundle_basename + "/cluster-manager/" + api_version + '/current'
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        fpath = tmpf.name
        tmpf.close()
        try:
            ret = self._updateclumgr(bundle_name, fpath)
        finally:
            if os.path.exists(fpath):
                os.unlink(fpath)
        return ret

    def _updateclumgr(self, bundle_name, fpath):
        """
        Downloads and installs the cluster manager bundle archive from the url
        specified by the bundle_name argument. The download destination file
        is specified by fpath. The caller is responsible for its deletion.
        """
        def do(fpath):
            tmpp = os.path.join(Env.paths.pathtmp, 'html')
            backp = Env.paths.pathhtml + '.bck'
            htmlp = Env.paths.pathhtml
            makedirs(htmlp)
            makedirs(tmpp)

            print("extract cluster manager in", tmpp)
            import tarfile
            tar = tarfile.open(fpath)
            os.chdir(tmpp)
            try:
                tar.extractall()
                tar.close()
            except (OSError, IOError):
                print("failed to unpack", file=sys.stderr)
                return 1
            os.chdir("/")

            print("install new cluster manager in %s" % htmlp)
            for root, dirs, files in os.walk(tmpp):
                for fpath in dirs:
                    os.chown(os.path.join(root, fpath), 0, 0)
                    for fpath in files:
                        os.chown(os.path.join(root, fpath), 0, 0)

            import shutil
            try:
                shutil.rmtree(backp)
            except (OSError, IOError):
                pass

            shutil.move(htmlp, backp)
            shutil.move(tmpp, htmlp)

        from utilities.uri import Uri
        print("get %s" % bundle_name)
        secure = self.oget("node", "secure_fetch")
        try:
            with Uri(bundle_name, secure=secure).fetch() as fpath:
                do(fpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)
            if self.options.cron:
                return 0
            return 1
        return 0

    def array(self):
        """
        Execute a array command, passing extra_argv to the array driver.
        """
        array_name = None
        for idx, arg in enumerate(self.options.extra_argv):
            if arg.startswith("--array="):
                array_name = arg[arg.index("=")+1:]
                break
            if (arg == "-a" or arg == "--array") and idx+1 < len(self.options.extra_argv):
                array_name = self.options.extra_argv[idx+1]
                break

        if array_name is None:
            raise ex.Error("can not determine array driver (no --array)")

        ref_section = "array#" + array_name
        section = None
        for s in self.conf_sections(cat="array"):
            if s == ref_section:
                section = s
                break
            try:
                name = self.oget(s, "name")
            except Exception as exc:
                continue
            if name == array_name:
                section = s
                break
        if section is None:
            raise ex.Error("array '%s' not found in configuration" % array_name)

        try:
            driver = self.oget(section, "type")
        except Exception:
            raise ex.Error("'%s.type' keyword must be set" % section)

        driver = driver.lower()
        try:
            mod = driver_import("array", driver)
        except ImportError as exc:
            raise ex.Error("array driver %s load error: %s" % (driver, str(exc)))
        return mod.main(self.options.extra_argv, node=self)

    def get_ruser(self, node):
        """
        Returns the remote user to use for remote commands on the node
        specified as argument.
        If not specified as node.ruser in the node configuration file,
        the root user is returned.
        """
        default = "root"
        ruser = self.oget("node", "ruser")
        if ruser == default:
            return ruser
        node_ruser = {}
        elements = ruser.split()
        for element in elements:
            subelements = element.split("@")
            if len(subelements) == 1:
                default = element
            elif len(subelements) == 2:
                _ruser, _node = subelements
                node_ruser[_node] = _ruser
            else:
                continue
        if node in node_ruser:
            return node_ruser[node]
        return default

    def dequeue_actions(self):
        """
        The dequeue_actions node action entrypoint.
        Poll the collector action queue until emptied.
        """
        while True:
            actions = self.collector.call('collector_get_action_queue')
            if actions is None:
                raise ex.Error("unable to fetch actions scheduled by the collector")
            n_actions = len(actions)
            if n_actions == 0:
                break
            data = []
            reftime = time.time()
            for action in actions:
                ret, out, err = self.dequeue_action(action)
                out = ANSI_ESCAPE.sub('', out)
                err = ANSI_ESCAPE.sub('', err)
                data.append((action.get('id'), ret, out, err))
                now = time.time()
                if now > reftime + 2:
                    # this action was long, update the collector now
                    self.collector.call('collector_update_action_queue', data)
                    reftime = time.time()
                    data = []
            if len(data) > 0:
                self.collector.call('collector_update_action_queue', data)

    @staticmethod
    def dequeue_action(action):
        """
        Execute the node or object action described in payload element
        received from the collector's action queue.
        """
        if action.get("svc_id") in (None, "") or action.get("svcname") in (None, ""):
            cmd = [Env.paths.om, "node"]
        else:
            cmd = [Env.paths.om, "svc", "-s", action.get("svcname")]
        cmd += shlex.split(action.get("command", ""))
        print("dequeue action %s" % " ".join(cmd))
        out, err, ret = justcall(cmd)
        return ret, out, err

    def rotate_root_pw(self):
        """
        Generate a random password, send it to the collector and set it as
        the root user password.
        """
        passwd = self.genpw()

        from core.collector.actions import CollectorActions
        coll = CollectorActions(self.options, self)
        try:
            getattr(coll, 'rotate_root_pw')(passwd)
        except Exception as exc:
            print("unexpected error sending the new password to the collector "
                  "(%s). Abording password change." % str(exc), file=sys.stderr)
            return 1

        try:
            from utilities.password import change_root_pw
        except ImportError:
            print("not implemented")
            return 1
        ret = change_root_pw(passwd)
        if ret == 0:
            print("root password changed")
        else:
            print("failed to change root password")
        return ret

    @staticmethod
    def genpw():
        """
        Returns a random password.
        """
        import string
        chars = string.ascii_letters + string.digits + r'+/'
        assert 256 % len(chars) == 0
        pwd_len = 16
        return ''.join(chars[ord(c) % len(chars)] for c in os.urandom(pwd_len))

    def scanscsi(self):
        """
        Rescans the scsi host buses for new logical units discovery.
        """
        self._scanscsi(self.options.hba, self.options.target, self.options.lun)

    def _scanscsi(self, hba=None, target=None, lun=None, log=None):
        log = log if log else self.log
        if not hasattr(self.diskinfo, 'scanscsi'):
            raise ex.Error("scanscsi is not implemented on %s" % Env.sysname)
        return self.diskinfo.scanscsi(
            hba=hba,
            target=target,
            lun=lun,
            log=log,
        )

    def cloud_get(self, section):
        """
        Get the cloud object instance handling the config file section passed
        as argument. If not already instanciated, create the instance and
        store it in a dict hashed by section name.
        """
        if not section.startswith("cloud"):
            return

        if not section.startswith("cloud#"):
            raise ex.InitError("cloud sections must have a unique name in "
                               "the form '[cloud#n] in %s" % Env.paths.nodeconf)

        if self.clouds and section in self.clouds:
            return self.clouds[section]

        try:
            cloud_type = self.oget(section, 'type')
        except Exception:
            raise ex.InitError("type option is mandatory in cloud section "
                               "in %s" % Env.paths.nodeconf)

        auth_dict = self.section_kwargs(section, cloud_type)

        try:
            mod = driver_import("cloud", cloud_type.lower())
        except ImportError:
            raise ex.InitError("cloud type '%s' is not supported" % cloud_type)

        if self.clouds is None:
            self.clouds = {}
        cloud = mod.Cloud(section, auth_dict)
        self.clouds[section] = cloud
        return cloud

    def can_parallel(self, action, svcs, options):
        """
        Returns True if the action can be run in a subprocess per service
        """
        try:
            import concurrent.futures
        except ImportError:
            return False
        if Env.sysname == "Windows":
            return False
        if len(svcs) < 2:
            return False
        if options.parallel and action not in ACTIONS_NO_PARALLEL:
            return True
        return False

    @staticmethod
    def action_need_aggregate(action, options):
        """
        Returns True if the action returns data from multiple sources (nodes
        or services) to arrange for display.
        """
        if action in ("pg_pids"):
            return True
        if action.startswith("print_") and options.format in ("json", "flat_json"):
            return True
        if action.startswith("json_"):
            return True
        if action.startswith("collector_"):
            return True
        if "_json_" in action:
            return True
        return False

    def do_svcs_action(self, action, options):
        """
        The services action wrapper.
        Takes care of
        * parallelization of the action in per-service subprocesses
        * collection and aggregation of returned data and errors
        """
        svcs = [] + self.svcs
        if action == "monitor":
            from commands.svcmon import svcmon
            if not options.sections:
                options.sections = "services"
            elif "services" not in options.sections:
                options.sections += ",services"
            options.parm_svcs = ",".join([o.path for o in self.svcs])
            svcmon(self, options)
            return
        if action == "ls":
            data = strip_path(sorted([svc.path for svc in svcs if svc.path in options.svcs]), options.namespace)
            if options.format == "json":
                print(json.dumps(data, indent=4, sort_keys=True))
            elif data:
                print("\n".join(data))
            return

        err = 0
        errs = {}
        data = Storage()
        data.outs = {}
        need_aggregate = self.action_need_aggregate(action, options)
        begin = time.time()

        if not options.cron:
            # File cache janitoring.
            # Skip for tasks: the scheduler will purge the session cache itself, without dirlisting.
            purge_cache_expired()
        self.log.debug("session uuid: %s", Env.session_uuid)

        if action in ACTIONS_NO_MULTIPLE_SERVICES and len(svcs) > 1:
            print("action '%s' is not allowed on multiple services" % action, file=sys.stderr)
            return 1

        parallel = self.can_parallel(action, svcs, options)
        if parallel:
            data.procs = {}
            data.svcs = {}

        timeout = 0
        if not options.local and options.wait:
            # submit all async actions and wait only after to avoid
            # max_parallel breaking inter service dependencies
            timeout = convert_duration(options.time)
            options.wait = False

        if parallel:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for svc in svcs:
                    data.svcs[svc.path] = svc
                    data.procs[executor.submit(self.service_action_worker, svc, action, options)] = svc.path
                for future in concurrent.futures.as_completed(data.procs):
                    path = data.procs[future]
                    ret = future.result()
                    errs[path] = ret
                    if ret > 0:
                        # r is negative when data.procs[path] is killed by signal.
                        # in this case, we don't want to decrement the err counter.
                        err += ret
        else:
            for svc in svcs:
                try:
                    ret = svc.action(action, options)
                    if need_aggregate:
                        if ret is not None:
                            data.outs[svc.path] = ret
                    elif action.startswith("print_") or action == "pg_stats":
                        self.print_data(ret)
                        if isinstance(ret, dict):
                            if "error" in ret:
                                err += 1
                    else:
                        if ret is None:
                            ret = 0
                        elif isinstance(ret, list):
                            ret = 0
                        elif isinstance(ret, dict):
                            if "error" in ret:
                                print(ret["error"], file=sys.stderr)
                            else:
                                print("unsupported format for this action", file=sys.stderr)
                            ret = 1
                        if ret > 0:
                            err += ret
                        errs[svc.path] = ret
                except ex.Error as exc:
                    ret = 1
                    err += ret
                    if not need_aggregate:
                        print("%s: %s" % (svc.path, exc), file=sys.stderr)
                    continue
                except ex.Signal:
                    break

        if timeout:
            # async actions wait
            for svc in svcs:
                if errs.get(svc.path, -1) != 0:
                    continue
                try:
                    global_expect = svc.last_global_expect
                except AttributeError:
                    continue
                try:
                    _timeout = timeout - (time.time() - begin)
                    svc.wait_daemon_mon_action(global_expect, wait=True, timeout=_timeout, begin=begin)
                except Exception as exc:
                    err += 1

        if need_aggregate:
            if self.options.single_service:
                path = svcs[0].path
                if path not in data.outs:
                    return 1
                self.print_data(data.outs[path])
            else:
                self.print_data(data.outs)

        if options.watch or options.stats:
            from commands.svcmon import svcmon
            options.sections = ["services"]
            svcmon(self, options)
        return err

    def collector_cli(self):
        """
        The collector cli entrypoint.
        """
        data = {}

        if self.options.user is None and self.options.config is None and os.getuid() == 0:
            if self.options.user is None:
                user, password = self.collector_auth_node()
                data["user"] = user
                data["password"] = password
                data["save"] = False
            if self.options.api is None:
                if self.collector_env.dbopensvc is None:
                    raise ex.Error("node.dbopensvc is not set in node.conf")
                data["api"] = self.collector_env.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        else:
            data["user"] = self.options.user
            data["password"] = self.options.password
            data["api"] = self.options.api
            data["save"] = self.options.save

        data["insecure"] = self.options.insecure
        data["refresh_api"] = self.options.refresh_api
        data["fmt"] = self.options.format
        if self.options.config:
            data["config"] = self.options.config
        argv = [word for word in self.options.extra_argv]
        argv = drop_option("--refresh-api", argv, drop_value=False)
        argv = drop_option("--insecure", argv, drop_value=False)
        argv = drop_option("--save", argv, drop_value=False)
        argv = drop_option("--help", argv, drop_value=False)
        argv = drop_option("--debug", argv, drop_value=False)
        argv = drop_option("--format", argv, drop_value=True)
        argv = drop_option("--color", argv, drop_value=True)
        argv = drop_option("--api", argv, drop_value=True)
        argv = drop_option("--config", argv, drop_value=True)
        argv = drop_option("--user", argv, drop_value=True)
        argv = drop_option("--password", argv, drop_value=True)
        return self._collector_cli(data, argv)

    def _collector_cli(self, data, argv):
        from core.collector.cli import Cli
        cli = Cli(**data)
        return cli.run(argv=argv)

    def download_from_safe(self, safe_id, path=None):
        import base64
        if safe_id.startswith("safe://"):
            safe_id = safe_id[7:].lstrip("/")
        fpath = os.path.join(Env.paths.safe, safe_id)
        if os.path.exists(fpath):
            with open(fpath, "r") as ofile:
                buff = ofile.read()
            try:
                clustername, nodename, data = self.decrypt(buff)
                data = data["data"]
                try:
                    return base64.urlsafe_b64decode(data)
                except TypeError:
                    return base64.urlsafe_b64decode(data.encode())
            except Exception:
                pass
        rpath = "/safe/%s/download" % safe_id
        api = self.collector_api(path=path)
        request = self.collector_request(rpath)
        if api["url"].startswith("https"):
            try:
                import ssl
                kwargs = {"context": ssl._create_unverified_context()}
            except:
                kwargs = {}
        else:
            raise ex.Error("refuse to submit auth tokens through a non-encrypted transport")
        try:
            f = urlopen(request, **kwargs)
        except HTTPError as e:
            try:
                err = json.loads(e.read())["error"]
                e = ex.Error(err)
            except ValueError:
                pass
            raise e
        buff = b""
        for chunk in iter(lambda: f.read(4096), b""):
            buff += chunk
        data = {"data": bdecode(base64.urlsafe_b64encode(buff))}
        makedirs(Env.paths.safe)
        with open(fpath, 'w') as df:
            pass
        os.chmod(fpath, 0o0600)
        with open(fpath, 'w') as df:
            df.write(self.encrypt(data, encode=False))
        f.close()
        return buff

    def collector_api(self, path=None):
        """
        Prepare the authentication info, either as node or as user.
        Fetch and cache the collector's exposed rest api metadata.
        """
        if self.collector_env.dbopensvc is None:
            raise ex.Error("node.dbopensvc is not set in node.conf")
        elif self.collector_env.dbopensvc_host == "none":
            raise ex.Error("node.dbopensvc is set to 'none' in node.conf")
        data = {}
        if self.options.user is None:
            username, password = self.collector_auth_node()
            if path:
                username = path+"@"+username
        else:
            username, password = self.collector_auth_user()
        data["username"] = username
        data["password"] = password
        data["url"] = self.collector_env.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        if not data["url"].startswith("http"):
            data["url"] = "https://%s" % data["url"]
        return data

    def collector_auth_node(self):
        """
        Returns the authentcation info for login as node
        """
        username = Env.nodename
        node_uuid = self.oget("node", "uuid")
        if not node_uuid:
            raise ex.Error("the node is not registered yet. use 'om node register [--user <user>]'")
        return username, node_uuid

    def collector_auth_user(self):
        """
        Returns the authentcation info for login as user
        """
        username = self.options.user

        if self.options.password and self.options.password != "?":
            return username, self.options.password

        import getpass
        try:
            password = getpass.getpass()
        except EOFError:
            raise KeyboardInterrupt()
        return username, password

    def collector_request(self, rpath, path=None):
        """
        Make a request to the collector's rest api
        """
        import base64
        api = self.collector_api(path=path)
        url = api["url"]
        if not url.startswith("https"):
            raise ex.Error("refuse to submit auth tokens through a "
                           "non-encrypted transport")
        request = Request(url+rpath)
        auth_string = '%s:%s' % (api["username"], api["password"])
        if six.PY3:
            base64string = base64.encodestring(auth_string.encode()).decode()
        else:
            base64string = base64.encodestring(auth_string)
        base64string = base64string.replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        return request

    def collector_rest_get(self, rpath, data=None, path=None):
        """
        Make a GET request to the collector's rest api
        """
        return self.collector_rest_request(rpath, data=data, path=path)

    def collector_rest_post(self, rpath, data=None, path=None):
        """
        Make a POST request to the collector's rest api
        """
        return self.collector_rest_request(rpath, data, path=path, get_method="POST")

    def collector_rest_put(self, rpath, data=None, path=None):
        """
        Make a PUT request to the collector's rest api
        """
        return self.collector_rest_request(rpath, data, path=path, get_method="PUT")

    def collector_rest_delete(self, rpath, data=None, path=None):
        """
        Make a DELETE request to the collector's rest api
        """
        return self.collector_rest_request(rpath, data, path=path, get_method="DELETE")

    @staticmethod
    def set_ssl_context(kwargs):
        """
        Python 2.7.9+ verifies certs by default and support the creationn
        of an unverified context through ssl._create_unverified_context().
        This method add an unverified context to a kwargs dict, when
        necessary.
        """
        try:
            import ssl
            kwargs["context"] = ssl._create_unverified_context()
        except (ImportError, AttributeError):
            pass
        return kwargs

    def collector_rest_request(self, rpath, data=None, path=None, get_method="GET"):
        """
        Make a request to the collector's rest api
        """
        if data is not None and get_method == "GET":
            if len(data) == 0 or not isinstance(data, dict):
                data = None
            else:
                rpath += "?" + urlencode(data)
                data = None

        request = self.collector_request(rpath, path=path)
        if get_method:
            request.get_method = lambda: get_method
        if data is not None:
            try:
                request.add_data(urlencode(data))
            except AttributeError:
                request.data = urlencode(data).encode('utf-8')
        kwargs = {}
        kwargs = self.set_ssl_context(kwargs)
        try:
            ufile = urlopen(request, **kwargs)
        except HTTPError as exc:
            try:
                err = json.loads(exc.read())["error"]
                exc = ex.Error(err)
            except (ValueError, TypeError):
                pass
            raise exc
        except IOError as exc:
            if hasattr(exc, "reason"):
                raise ex.Error(getattr(exc, "reason"))
            raise ex.Error(str(exc))
        data = json.loads(ufile.read().decode("utf-8"))
        ufile.close()
        return data

    def collector_rest_get_to_file(self, rpath, fpath):
        """
        Download bulk chunked data from the collector's rest api
        """
        request = self.collector_request(rpath)
        kwargs = {}
        kwargs = self.set_ssl_context(kwargs)
        try:
            ufile = urlopen(request, **kwargs)
        except HTTPError as exc:
            try:
                err = json.loads(exc.read())["error"]
                exc = ex.Error(err)
            except ValueError:
                pass
            raise exc
        with open(fpath, 'wb') as ofile:
            os.chmod(fpath, 0o0600)
            for chunk in iter(lambda: ufile.read(4096), b""):
                ofile.write(chunk)
        ufile.close()

    def svc_conf_from_templ(self, name, namespace, kind, template):
        """
        Download a provisioning template from the collector's rest api,
        and installs it as the service configuration file.
        """
        tmpfpath = self.svc_conf_tempfile()
        try:
            int(template)
            url = "/provisioning_templates/"+str(template)+"?props=tpl_definition&meta=0"
        except ValueError:
            url = "/provisioning_templates?filters=tpl_name="+template+"&props=tpl_definition&meta=0"
        data = self.collector_rest_get(url)
        if "error" in data:
            raise ex.Error(data["error"])
        if len(data["data"]) == 0:
            raise ex.Error("service not found on the collector")
        if len(data["data"][0]["tpl_definition"]) == 0:
            raise ex.Error("service has an empty configuration")
        try:
            return json.loads(data["data"][0]["tpl_definition"])
        except Exception:
            pass
        with open(tmpfpath, "w") as ofile:
            os.chmod(tmpfpath, 0o0600)
            ofile.write(data["data"][0]["tpl_definition"].replace("\\n", "\n").replace("\\t", "\t"))
        try:
            return self.svc_conf_from_file(name, namespace, kind, tmpfpath)
        finally:
            try:
                os.unlink(tmpfpath)
            except OSError:
                pass

    def svc_conf_tempfile(self, content=None):
        import tempfile
        tmpf = tempfile.NamedTemporaryFile()
        tmpfpath = tmpf.name
        tmpf.close()
        with open(tmpfpath, "w"):
            pass
        os.chmod(tmpfpath, 0o0600)
        if content is None:
            return tmpfpath
        with open(tmpfpath, "w") as f:
            f.write(content)
        return tmpfpath

    def svc_conf_from_uri(self, name, namespace, kind, fpath):
        """
        Download a provisioning template from an arbitrary uri,
        and installs it as the service configuration file.
        """
        from utilities.uri import Uri
        print("get %s" % fpath)
        secure = self.oget("node", "secure_fetch")
        try:
            with Uri(fpath, secure=secure).fetch() as tmpfpath:
                return self.svc_conf_from_file(name, namespace, kind, tmpfpath)
        except IOError as exc:
            print("download failed", ":", exc, file=sys.stderr)

    def svc_conf_from_file(self, name, namespace, kind, fpath):
        """
        Installs a local template as the service configuration file.
        """
        if not os.path.exists(fpath):
            raise ex.Error("%s does not exists" % fpath)
        try:
            with open(fpath, "r") as f:
                return json.load(f)
        except Exception as exc:
            pass
        svc = factory(kind)(name, namespace=namespace, cf=fpath, node=self, volatile=True)
        svc.options.format = "json"
        data = {
            svc.path: svc._print_config()
        }
        return data

    def svc_conf_from_stdin(self, name, namespace, kind):
        feed = ""
        for line in sys.stdin.readlines():
            feed += line
        data = None
        if not feed:
            raise ex.Error("empty feed")
        try:
            data = json.loads("".join(feed))
        except ValueError:
            tmpfpath = self.svc_conf_tempfile(content=feed)
            try:
                data = self.svc_conf_from_file(name, namespace, kind, tmpfpath)
            finally:
                os.unlink(tmpfpath)
        return data

    def svc_conf_from_selector(self, selector):
        paths = self.svcs_selector(selector)
        data = {}
        for _path in paths:
            name, _namespace, kind = split_path(_path)
            svc = factory(kind)(name, _namespace, node=self, volatile=True)
            svc.options.format = "json"
            data[_path] = svc.print_config()
        return data

    def install_svc_conf_from_data(self, name, namespace, kind, data, restore=False, info=None):
        """
        Installs a service configuration file from section, keys and values
        fed from a data structure.
        """
        if info is None:
            info = self.install_service_info(name, namespace, kind)

        if not os.path.exists(info.cf):
            # freeze before the installing the config so the daemon never
            # has a chance to consider the new service unfrozen and take undue
            # action before we have the change to modify the service config
            path = fmt_path(name, namespace, kind)
            Freezer(path).freeze()

        if not restore:
            try:
                del data["DEFAULT"]["id"]
            except KeyError:
                pass

        svc = factory(kind)(name, namespace=namespace, cf=info.cf, cd=data, node=self)
        svc.commit()
        svc.postinstall()

    def install_service_info(self, name, namespace, kind):
        validate_kind(kind)
        validate_ns_name(namespace)
        validate_name(name)
        data = Storage()
        data.path = fmt_path(name, namespace, kind)
        data.pathetc = svc_pathetc(data.path, namespace)
        data.cf = os.path.join(data.pathetc, name+'.conf')
        data.id = new_id()
        makedirs(data.pathetc)
        return data

    def svc_conf_set(self, data=None, kw=None, env=None, interactive=False):
        if data is None:
            data = {}
        if kw is None:
            kw = {}
        for _kw in kw:
            if "=" not in _kw:
                continue
            _kw, val = _kw.split("=", 1)
            if "." in _kw:
                section, option = _kw.split(".", 1)
            else:
                section = "DEFAULT"
                option = _kw
            if section not in data:
                data[section] = {}
            data[section][option] = val
        current_env = data.get("env", {})
        new_env = self.svc_conf_setenv(env, interactive, current_env)
        if new_env:
            data["env"] = new_env
        return data

    def svc_conf_setenv(self, args=None, interactive=False, env=None):
        """
        For each option in the 'env' section of the configuration file,
        * rewrite the value using the value specified in a corresponding
          --env <option>=<value> commandline arg
        * or prompt for the value if --interactive is set, and rewrite
        * or leave the value as is, considering the default is accepted
        """
        if args is None:
            args = []
        if env is None:
            env = {}

        explicit_options = []

        for arg in args:
            idx = arg.index("=")
            option = arg[:idx]
            value = arg[idx+1:]
            env[option] = value
            explicit_options.append(option)

        if not interactive:
            return env

        if not os.isatty(0):
            raise ex.Error("--interactive is set but input fd is not a tty")

        def get_href(ref):
            ref = ref.strip("[]")
            try:
                response = urlopen(ref)
                return response.read()
            except:
                return ""

        def print_comment(comment):
            """
            Print a env keyword comment. For use in the interactive service
            create codepath.
            """
            import re
            comment = re.sub(r"(\[.+://.+])", lambda m: get_href(m.group(1)), comment)
            print(comment)

        from foreign.six.moves import input
        for key, default_val in env.items():
            if key.endswith(".comment"):
                continue
            if key in explicit_options:
                continue
            comment_key = key + ".comment"
            comment = env.get(comment_key)
            if comment:
                print_comment(comment)
            newval = input("%s [%s] > " % (key, str(default_val)))
            if newval != "":
                env[key] = newval
        return env

    def install_service(self, path, fpath=None, template=None,
                        restore=False, resources=None, kw=None, namespace=None,
                        env=None, interactive=False, provision=False, node=None):
        """
        Pick a collector's template, arbitrary uri, or local file service
        configuration file fetching method. Run it, and create the
        service symlinks and launchers directory.
        """
        if kw is None:
            kw = []
        if resources is None:
            resources = []
        if fpath is not None and template is not None:
            raise ex.Error("--config and --template can't both be specified")

        data = None
        installed = []
        env_to_merge = {}

        if path:
            name, _namespace, kind = split_path(path)
            if not namespace:
                namespace = _namespace
        else:
            name = "dummy"
            kind = "svc"

        if sys.stdin and env in (["-"], ["stdin"], ["/dev/stdin"]):
            env_to_merge = self.svc_conf_from_stdin(name, namespace, kind)
            env = []

        if template and want_context():
            req = {
                "action": "create",
                "options": {
                    "path": path,
                    "namespace": namespace,
                    "provision": provision,
                    "template": template,
                    "restore": restore,
                    "data": env_to_merge,
                }
            }
            result = self.daemon_post(req, timeout=DEFAULT_DAEMON_TIMEOUT, node=node)
            status, error, info = self.parse_result(result)
            if status:
                raise ex.Error(error)
            return

        # convert to a pivotal dataset: dict of configs, indexed by path
        if sys.stdin and fpath in ("-", "stdin", "/dev/stdin"):
            data = self.svc_conf_from_stdin(name, namespace, kind)
        elif fpath and "://" not in fpath and not os.path.exists(fpath):
            data = self.svc_conf_from_selector(fpath)
        elif template is not None:
            if "://" in template:
                data = self.svc_conf_from_uri(name, namespace, kind, template)
            elif os.path.exists(template):
                data = self.svc_conf_from_file(name, namespace, kind, template)
            else:
                data = self.svc_conf_from_templ(name, namespace, kind, template)
        elif fpath is not None:
            if "://" in fpath:
                data = self.svc_conf_from_uri(name, namespace, kind, fpath)
            else:
                data = self.svc_conf_from_file(name, namespace, kind, fpath)
        else:
            data = self.svc_conf_from_args(kind, resources)

        _data = {}
        if isinstance(data, dict):
            if "metadata" in data:
                tmppath = fmt_path(data["metadata"]["name"], data["metadata"]["namespace"], data["metadata"]["kind"])
                del data["metadata"]
                _data = {tmppath: data}
            else:
                for tmppath, __data in data.items():
                    try:
                        split_path(tmppath)
                    except ValueError:
                        raise ex.Error("invalid injected data format: %s is not a path" % tmppath)
                    if "metadata" in __data:
                        del __data["metadata"]
                    _data[tmppath] = __data
        elif isinstance(data, list):
            for __data in data:
                try:
                    tmppath = fmt_path(__data["metadata"]["name"], __data["metadata"]["namespace"], __data["metadata"]["kind"])
                except (ValueError, KeyError):
                    raise ex.Error("invalid injected data format: list need a metadata section in each entry")
                del __data["metadata"]
                _data[tmppath] = __data

        if _data:
            if path:
                 if len(_data) > 1:
                     raise ex.Error("multiple configs available to create a single service")
                 # force the new path
                 for tmppath, __data in _data.items():
                     break
                 if tmppath.endswith("svc/dummy"):
                     raise ex.Error("no path in deployment data")
                 _data = {
                     path: __data,
                 }
            data = _data
        else:
            if path:
                data = {path: {}}

        if data:
            for tmppath in data:
                data[tmppath] = self.svc_conf_set(data[tmppath], kw, env, interactive)
                if tmppath in env_to_merge:
                    if "env" in env_to_merge[tmppath]:
                        _env_to_merge = env_to_merge[tmppath]["env"]
                    else:
                        _env_to_merge = env_to_merge[tmppath]
                elif "env" in env_to_merge:
                    _env_to_merge = env_to_merge["env"]
                else:
                    _env_to_merge = env_to_merge
                if isinstance(_env_to_merge, dict) and _env_to_merge:
                    if "env" not in data[tmppath]:
                        data[tmppath]["env"] = _env_to_merge
                    else:
                        data[tmppath]["env"].update(_env_to_merge)

        if want_context() or node:
            req = {
                "action": "create",
                "options": {
                    "namespace": namespace,
                    "provision": provision,
                    "restore": restore,
                    "data": data,
                }
            }
            result = self.daemon_post(req, timeout=DEFAULT_DAEMON_TIMEOUT, node=node)
            status, error, info = self.parse_result(result)
            if status:
                raise ex.Error(error)
            return

        if path and not data:
            info = self.install_service_info(name, namespace, kind)
        elif not data:
            raise ex.Error("feed service configurations to stdin and set --config=-")
        else:
            for _path, _data in data.items():
                if namespace:
                    # discard namespace in path, use --namespace value instead
                    name, _, kind = split_path(_path)
                    _namespace = namespace
                else:
                    name, _namespace, kind = split_path(_path)
                info = self.install_service_info(name, _namespace, kind)
                print("create %s" % info.path)
                self.install_svc_conf_from_data(name, _namespace, kind, _data, restore, info)
                installed.append(info.path)
            self.wake_monitor()
            return installed

        if data is not None:
            self.install_svc_conf_from_data(name, namespace, kind, data, restore, info)
        installed.append(info.path)
        self.wake_monitor()
        return installed

    def set_rlimit(self, nofile=4096):
        """
        Set the operating system nofile rlimit to a sensible value for the
        number of services configured.
        """
        #self.log.debug("len self.svcs <%s>", len(self.svcs))
        n_conf = sum(1 for _ in glob_services_config())
        proportional_nofile = 64 * n_conf
        if proportional_nofile > nofile:
            nofile = proportional_nofile

        try:
            import resource
            _vs, _vg = resource.getrlimit(resource.RLIMIT_NOFILE)
            if _vs < nofile:
                self.log.debug("raise nofile resource from %d limit to %d", _vs, nofile)
                if nofile > _vg:
                    _vg = nofile
                resource.setrlimit(resource.RLIMIT_NOFILE, (nofile, _vg))
            else:
                self.log.debug("current nofile %d already over minimum %d", _vs, nofile)
        except Exception as exc:
            self.log.debug(str(exc))

    def svc_conf_from_args(self, kind, resources=None):
        """
        Create a new service from resource definitions passed as individual
        dictionaries in json format.
        """
        if resources is None:
            resources = []
        defaults = {}
        sections = {}
        rtypes = {}

        for r in resources:
            try:
                d = json.loads(r)
            except:
                raise ex.Error("can not parse resource: %s" % r)
            if "rid" in d:
                section = d["rid"]
                if "#" not in section:
                    raise ex.Error("%s must be formatted as 'rtype#n'" % section)
                l = section.split('#')
                if len(l) != 2:
                    raise ex.Error("%s must be formatted as 'rtype#n'" % section)
                rtype = l[1]
                if rtype in rtypes:
                    rtypes[rtype] += 1
                else:
                    rtypes[rtype] = 0
                del d["rid"]
                if section in sections:
                    sections[section].update(d)
                else:
                    sections[section] = d
            elif "rtype" in d and d["rtype"] == "env":
                del d["rtype"]
                if "env" in sections:
                    sections["env"].update(d)
                else:
                    sections["env"] = d
            elif "rtype" in d and d["rtype"] != "DEFAULT":
                if "rid" in d:
                    del d["rid"]
                rtype = d["rtype"]
                if rtype in rtypes:
                    section = "%s#%d" % (rtype, rtypes[rtype])
                    rtypes[rtype] += 1
                else:
                    section = "%s#0" % rtype
                    rtypes[rtype] = 1
                if section in sections:
                    sections[section].update(d)
                else:
                    sections[section] = d
            else:
                if "rtype" in d:
                    del d["rtype"]
                defaults.update(d)

        obj = factory(kind)("dummy", namespace="dummy", volatile=True, node=self)
        from core.keywords import MissKeyNoDefault, KeyInvalidValue
        try:
            defaults.update(obj.kwstore.update("DEFAULT", defaults))
            for section, d in sections.items():
                sections[section].update(obj.kwstore.update(section, d))
        except (MissKeyNoDefault, KeyInvalidValue):
            raise ex.Error
        del obj

        sections["DEFAULT"] = defaults
        return defaults

    def create_path(self, paths, namespace):
        if isinstance(paths, list):
            if len(paths) != 1:
                raise ex.Error("only one service must be specified")
            path = paths[0]

        try:
           path.encode("ascii")
        except Exception:
           raise ex.Error("the service name must be ascii-encodable")

        path = resolve_path(path, namespace)
        return path

    def create_service(self, paths, options):
        """
        The "om <kind> create" entrypoint.
        """
        ret = 0
        if paths:
            path = self.create_path(paths, options.namespace)
        else:
            path = None

        try:
            paths = self.install_service(path, fpath=options.config,
                                         template=options.template,
                                         restore=options.restore,
                                         resources=options.resource,
                                         kw=options.kw,
                                         namespace=options.namespace,
                                         env=options.env,
                                         interactive=options.interactive,
                                         provision=options.provision)
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 1

        if want_context():
            return ret

        # force a refresh of self.svcs
        try:
            self.rebuild_services(paths)
        except ex.Error as exc:
            print(exc, file=sys.stderr)
            ret = 1

        for svc in self.svcs:
            if options.provision:
                svc.action("provision", options)
            if hasattr(svc, "on_create"):
                getattr(svc, "on_create")()

        return ret

    def wait(self):
        """
        Wait for a condition on the monitor thread data.
        Catch broken pipe.
        """
        path = self.options.jsonpath_filter
        server = self.options.server
        duration = self.options.duration
        verbose = self.options.verbose
        begin = time.time()
        try:
            self._wait(server, path, duration)
            if self.options.verbose:
                print("elapsed %.2f seconds"% (time.time() - begin))
        except KeyboardInterrupt:
            return 1
        except (OSError, IOError) as exc:
            if exc.errno == EPIPE:
                return 1

    def _wait(self, server=None, path=None, duration=None):
        """
        Wait for a condition on the monitor thread data or
        a local event data.
        """
        from math import ceil
        duration = convert_duration(duration)
        if duration is None:
            timeout = None
            left = None
        elif duration == 0:
            timeout = 0
            left = 0
        else:
            timeout = _wait_get_time() + duration
            left = duration
        while True:
            if left is None:
                req_duration = 10
            elif left > 10:
                req_duration = 10
            else:
                req_duration = ceil(left)
            result = self.daemon_get(
                {
                    "action": "wait",
                    "options": {
                        "condition": path,
                        "duration": req_duration,
                    },
                },
                server=server,
                timeout=11,
            )
            status, error, info = self.parse_result(result)
            if status == 501:
                raise ex.Error(error)
            if result.get("data", {}).get("satisfied"):
                break
            if left is not None:
                left = timeout - _wait_get_time()
            if left is not None and left < 1:
                print("timeout", file=sys.stderr)
                raise KeyboardInterrupt()
            _wait_delay(0.2)  # short-loop prevention
            if left is not None:
                left = timeout - _wait_get_time()

    def events(self, server=None):
        try:
            self._events(server=server)
        except ex.Signal:
            return
        except (OSError, IOError) as exc:
            if exc.errno == EPIPE:
                return

    def _events(self, server=None):
        if self.options.server:
            server = self.options.server
        elif server is None:
            server = Env.nodename
        for msg in self.daemon_events(server):
            if self.options.format == "json":
                print(json.dumps(msg))
                sys.stdout.flush()
            else:
                kind = msg.get("kind")
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), msg.get("nodename", ""), kind)
                if kind == "patch":
                    for event in msg["data"]:
                        try:
                            key, val = event
                            line = "  %s => %s" % (".".join([str(k) for k in key]), str(val))
                        except ValueError:
                            line = "  %s deleted" % ".".join([str(k) for k in event[0]])
                        print(line)
                        sys.stdout.flush()
                elif kind == "event":
                    for key, val in msg.get("data", {}).items():
                        print("  %s=%s" % (str(key).upper(), str(val)))

    def logs(self):
        node = "*"
        if self.options.local:
            node = None
        elif self.options.node:
            node = self.options.node
        nodes = self.nodes_selector(node)
        auto = sorted(nodes, reverse=True)
        self._backlogs(server=self.options.server, node=node,
                       backlog=self.options.backlog,
                       debug=self.options.debug,
                       auto=auto)
        if not self.options.follow:
            return
        try:
            self._followlogs(server=self.options.server, node=node,
                             debug=self.options.debug,
                             auto=auto)
        except ex.Signal:
            return
        except (OSError, IOError) as exc:
            if exc.errno == EPIPE:
                return

    def _backlogs(self, server=None, node=None, backlog=None, debug=False, auto=None):
        from utilities.render.color import colorize_log_line
        lines = []
        for line in self.daemon_backlogs(server, node, backlog, debug):
            line = colorize_log_line(line, auto=auto)
            if line:
                print(line)
                sys.stdout.flush()

    def _followlogs(self, server=None, node=None, debug=False, auto=None):
        from utilities.render.color import colorize_log_line
        for line in self.daemon_logs(server, node, debug):
            line = colorize_log_line(line, auto=auto)
            if line:
                print(line)
                sys.stdout.flush()

    def print_devs(self):
        if self.options.reverse:
            self.devtree.print_tree_bottom_up(devices=self.options.devices,
                                              verbose=self.options.verbose)
        else:
            self.devtree.print_tree(devices=self.options.devices,
                                    verbose=self.options.verbose)

    @formatter
    def print_config(self):
        """
        print_config node action entrypoint
        """
        if self.options.format is not None:
            return self.print_config_data()
        if not os.path.exists(Env.paths.nodeconf):
            return
        from utilities.render.color import print_color_config
        print_color_config(Env.paths.nodeconf)

    @formatter
    def ls(self):
        if self.options.node:
            node = self.options.node
        else:
            node = "*"
        data = self.nodes_selector(node)
        if self.options.format is not None:
            return data
        for node in data:
            print(node)

    def nodes_selector(self, selector, data=None):
        if selector in ("*", None):
            if data:
                # if data is provided (by svcmon usually), it is surely
                # more up-to-date then the cluster_node lazy relying on
                # the config lazy
                return sorted([node for node in data])
            elif want_context():
                return sorted([node for node in self.nodes_info])
            else:
                return self.cluster_nodes
        if selector == "":
            return []
        if isinstance(selector, (list, tuple, set)):
            return selector
        selector = selector.strip()
        if not re.search(r"[*?=,+]", selector):
            if re.search(r"\s", selector):
                # simple node list
                return selector.split()
            elif selector in self.cluster_nodes:
                return [selector]
        if data is None:
            data = self.nodes_info
        if data is None:
            # daemon down, at least decide if the local node matches
            data = {Env.nodename: {"labels": self.labels}}

        nodes = []
        for selector in selector.split():
            _nodes = self._nodes_selector(selector, data)
            for node in _nodes:
                if node not in nodes:
                    nodes.append(node)
        return nodes

    def _nodes_selector(self, selector, data=None):
        nodes = set([node for node in data])
        anded_selectors = selector.split("+")
        for _selector in anded_selectors:
            nodes = nodes & self.__nodes_selector(_selector, data)
        return sorted(list(nodes))

    def __nodes_selector(self, selector, data):
        nodes = set()
        ored_selectors = selector.split(",")
        for _selector in ored_selectors:
            nodes = nodes | self.___nodes_selector(_selector, data)
        return nodes

    def ___nodes_selector(self, selector, data):
        try:
            negate = selector[0] == "!"
            selector = selector.lstrip("!")
        except IndexError:
            negate = False
        if selector == "*":
            matching = set([node for node in data])
        elif selector.endswith(":"):
            label = selector.rstrip(":")
            matching = set([node for node, _data in data.items() \
                            if label in _data.get("labels", {})])
        elif "=" in selector:
            label, value = selector.split("=", 1)
            matching = set([node for node, _data in data.items() \
                            if _data.get("labels", {}).get(label) == value])
        else:
            matching = set([node for node in data if fnmatch.fnmatch(node, selector)])
        if negate:
            return set([node for node in data]) - matching
        else:
            return matching

    @lazy
    def agent_version(self):
        import utilities.version
        return utilities.version.agent_version()

    def frozen(self):
        """
        Return True if the node frozen flag is set.
        """
        return self.freezer.node_frozen()

    def freeze(self):
        """
        Set the node frozen flag.
        """
        self.freezer.node_freeze()

    def thaw(self):
        """
        Unset the node frozen flag.
        """
        self.freezer.node_thaw()

    def stonith(self):
        self._stonith(self.options.node)

    def _stonith(self, node):
        if node in (None, ""):
            raise ex.Error("--node is mandatory")
        if node == Env.nodename:
            raise ex.Error("refuse to stonith ourself")
        if node not in self.cluster_nodes:
            raise ex.Error("refuse to stonith node %s not member of our cluster" % node)
        try:
            cmd = self._get("stonith#%s.cmd" % node)
        except (ex.OptNotFound, ex.Error) as exc:
            raise ex.Error("the stonith#%s.cmd keyword must be set in "
                              "node.conf" % node)
        cmd = shlex.split(cmd)
        self.log.info("stonith node %s", node)
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def prepare_async_options(self):
        options = {}
        options.update(self.options)
        for opt in ("node", "cluster"):
            if opt in options:
                del options[opt]
        return options

    def async_action(self, action, timeout=None, wait=None):
        """
        Set the daemon global expected state if the action can be handled
        by the daemons.
        """
        if wait is None:
            wait = self.options.wait
        if timeout is None:
            timeout = self.options.time
        if (want_context() and action not in ACTIONS_CUSTOM_REMOTE and (self.options.node or action not in ACTION_ASYNC)) or \
           self.options.node and action not in ACTIONS_CUSTOM_REMOTE:
            options = self.prepare_async_options()
            sync = action not in ACTIONS_NOWAIT_RESULT
            ret = self.daemon_node_action(action=action, options=options, node=self.options.node, sync=sync, action_mode=False)
            if ret == 0:
                raise ex.AbortAction()
            else:
                raise ex.Error()
        if self.options.local:
            return
        if action not in ACTION_ASYNC:
            return
        begin = time.time()
        data = self.set_node_monitor(global_expect=ACTION_ASYNC[action]["target"])
        for line in data.get("info", []):
            self.log.info(line)
            if " already " in line:
                raise ex.AbortAction()
        if not wait:
            raise ex.AbortAction()
        self.poll_async_action(ACTION_ASYNC[action]["target"], timeout=timeout, begin=begin)

    def poll_async_action(self, global_expect, timeout=None, begin=None):
        """
        Display an asynchronous action progress until its end or timeout
        """
        try:
            if global_expect == "frozen":
                self._wait(path="monitor.frozen=frozen", duration=timeout)
            elif global_expect == "thawed":
                self._wait(path="monitor.frozen=thawed", duration=timeout)
        except KeyboardInterrupt:
            raise ex.Error

    #
    # daemon actions
    #
    def daemon_collector_xmlrpc(self, *args, **kwargs):
        data = self.daemon_post(
            {
                "action": "collector_xmlrpc",
                "options": {
                    "args": args,
                    "kwargs": kwargs,
                },
            },
            server=self.options.server,
            silent=True,
            timeout=2,
        )
        if data is None or data["status"] != 0:
            # the daemon is not running or refused the connection,
            # tell the collector ourselves
            self.collector.call(*args, **kwargs)

    @lazy
    def nodes_info(self):
        if not want_context() or os.environ.get("OSVC_ACTION_ORIGIN") == "daemon":
            try:
                with open(Env.paths.nodes_info, "r") as ofile:
                    return json.load(ofile)
            except Exception as exc:
                pass
        import socket
        try:
            return self._daemon_nodes_info(silent=True)["data"]
        except (KeyError, TypeError, socket.error):
            return

    def get_ssh_pubkey(self, user="root", key_type="rsa"):
        path = os.path.expanduser("~%s/.ssh/id_%s.pub" % (user, key_type))
        try:
            with open(path, "r") as f:
                buff = f.read()
        except OSError:
            return
        # strip comment
        return self.normalize_ssh_key(buff)

    def _daemon_get_ssh(self):
        data = self.daemon_get(
            {
                "action": "ssh_key",
            },
            node="*",
            silent=True,
            timeout=5,
        )
        return data

    @staticmethod
    def normalize_ssh_key(buff):
        return " ".join(re.split(r"\s+", buff.strip())[0:2])

    def update_ssh_authorized_keys(self):
        data = self._daemon_get_ssh()
        errs = 0
        path = os.path.expanduser("~root/.ssh/authorized_keys")
        keys = []
        short_keys = []
        for node, _data in data.get("nodes", {}).items():
            try:
                _data = _data["data"]
                _data["key"] = self.normalize_ssh_key(_data["key"])
            except KeyError:
                errs +=1
                print("node %s key not found" % node, file=sys.stderr)
                continue
            _data["node"] = node
            keys.append(_data)
            short_keys.append(_data["key"])
        if not keys:
            print("no keys to install")
            return
        try:
            with open(path, "r") as f:
                current_keys = f.read().split("\n")
                need_newline = current_keys and current_keys[-1] != ""
                # strip comments
                current_keys = [self.normalize_ssh_key(buff) for buff in current_keys]
        except OSError:
            current_keys = []
            need_newline = False
        keys = [k for k in keys if k["key"] not in current_keys]
        if not keys:
            print("all keys already installed")
        else:
            with open(path, "a") as f:
                if need_newline:
                    f.write("\n")
                for key in keys:
                    f.write("%s %s@%s\n" % (key["key"], key["user"], key["node"]))
                    print("install %s@%s key" % (key["user"], key["node"]))
        fstat = os.stat(path)
        current_mask = int(fstat.st_mode & 0o777)
        mask = 0o600
        if current_mask != mask:
            print("chmod", oct(mask), path)
            os.chmod(path, mask)
        if fstat.st_uid != 0 or fstat.st_gid != 0:
            print("chown 0:0", path)
            os.chown(path, 0, 0)

    def _daemon_nodes_info(self, silent=False, refresh=False, server=None):
        data = self.daemon_get(
            {
                "action": "nodes_info",
            },
            server=server,
            silent=silent,
            timeout=5,
        )
        return data

    def _daemon_lock(self, name, timeout=None, silent=False, on_error=None):
        if timeout is not None:
            request_timeout = timeout + DEFAULT_DAEMON_TIMEOUT
        else:
            request_timeout = timeout
        data = self.daemon_post(
            {
                "action": "lock",
                "options": {"name": name, "timeout": timeout},
            },
            silent=silent,
            timeout=request_timeout,
        )
        lock_id = data.get("data", {}).get("id")
        if not lock_id and on_error == "raise":
            raise ex.Error("cluster lock error")
        return lock_id

    def _daemon_unlock(self, name, lock_id, timeout=None, silent=False):
        if timeout is not None:
            request_timeout = timeout + DEFAULT_DAEMON_TIMEOUT
        else:
            request_timeout = timeout
        data = self.daemon_post(
            {
                "action": "unlock",
                "options": {"name": name, "lock_id": lock_id, "timeout": timeout},
            },
            silent=silent,
            timeout=request_timeout,
        )
        status, error, info = self.parse_result(data)
        if error:
            print(error, file=sys.stderr)
        return status

    def _daemon_object_selector(self, selector="*", namespace=None, kind=None, server=None):
        data = self.daemon_get(
            {
                "action": "object_selector",
                "options": {
                    "selector": selector,
                    "namespace": namespace,
                    "kind": kind,
                },
            },
            server=server,
            timeout=5,
        )
        return data

    def _daemon_status(self, silent=False, refresh=False, server=None, selector=None, namespace=None):
        data = self.daemon_get(
            {
                "action": "daemon_status",
                "options": {
                    "refresh": refresh,
                    "selector": selector,
                    "namespace": namespace,
                },
            },
            server=server,
            silent=silent,
            timeout=5,
        )
        return data

    @formatter
    def daemon_lock_show(self):
        data = self.daemon_get(
            {
                "action": "cluster/locks",
            },
            timeout=DEFAULT_DAEMON_TIMEOUT,
            with_result=True,
            server=self.options.server
        )
        status, error, info = self.parse_result(data)
        if error:
            raise ex.Error(error)
        if status != 0:
            raise ex.Error("cluster/locks api return status %s" % status)
        locks = data['data']
        if self.options.format in ("json", "flat_json"):
            return locks
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        node = tree.add_node()
        node.add_column("name", color.BOLD)
        node.add_column("id", color.BOLD)
        node.add_column("requester", color.BOLD)
        node.add_column("requested", color.BOLD)
        for name in sorted(locks.keys()):
            lock_info = locks[name]
            leaf = node.add_node()
            leaf.add_column(name, color.BROWN)
            leaf.add_column(lock_info['id'])
            leaf.add_column(lock_info['requester'])
            leaf.add_column(lock_info['requested'])
        print(tree)

    def daemon_lock_release(self):
        timeout = convert_duration(self.options.timeout)
        self._daemon_unlock(self.options.name, self.options.id, timeout=timeout)

    def daemon_stats(self, paths=None, node=None):
        if node:
            pass
        elif self.options.node:
            node = self.options.node
        else:
            node = '*'
        data = self._daemon_stats(paths=paths, node=node, server=self.options.server)
        return self.print_data(data, default_fmt="flat_json")

    def _daemon_stats(self, paths=None, silent=False, node=None, server=None):
        data = self.daemon_get(
            {
                "action": "daemon_stats",
                "options": {
                    "selector": ",".join(paths) if paths else "**",
                }
            },
            node=node,
            server=server,
            silent=silent,
            timeout=5,
        )
        if data is None or data.get("status", 0) != 0:
            return
        if "nodes" in data:
            data = dict((n, d["data"]) for n, d in data["nodes"].items())
        else:
            data = data["data"]
        return data

    def daemon_status(self, paths=None, server=None):
        if server:
            pass
        elif self.options.server:
            server = self.options.server
        data = self._daemon_status(server=server)
        if data is None or data.get("status", 0) != 0:
            return

        if self.options.format in ("json", "flat_json") or self.options.jsonpath_filter:
            self.print_data(data)
            return

        from utilities.render.cluster import format_cluster
        print(format_cluster(paths=paths, node=self.cluster_nodes, data=data))
        return 0

    def daemon_blacklist_clear(self):
        """
        Tell the daemon to clear the senders blacklist
        """
        data = self.daemon_post(
            {"action": "blacklist_clear"},
            server=self.options.server,
            timeout=DEFAULT_DAEMON_TIMEOUT,
        )
        status, error, info = self.parse_result(data)
        if error:
            print(error, file=sys.stderr)
        return status

    def daemon_dns_dump(self):
        """
        Dump the content of the cluster zone.
        """
        try:
            dns = self.dns
        except Exception:
            return
        from utilities.dns import zone_list
        data = zone_list("", dns)
        if data is None:
            return
        if self.options.format in ("json", "flat_json"):
            self.print_data(data)
            return
        widths = {
            "qname": 0,
            "qtype": 0,
            "ttl": 0,
            "content": 0,
        }
        for record in data:
            for key in ("qname", "qtype", "ttl", "content"):
                length = len(str(record[key]))
                if length > widths[key]:
                    widths[key] = length
        fmt = "{:%d}  IN   {:%d}  {:%d}  {:%d}" % (widths["qname"], widths["qtype"], widths["ttl"], widths["content"])
        for record in sorted(data, key=lambda x: x["qname"]):
            print(fmt.format(record["qname"], record["qtype"], record["ttl"], record["content"]))
        return

    def daemon_relay_status(self):
        """
        Show the daemon senders blacklist
        """
        secret = None
        cluster_name = None
        if self.options.server and self.options.server not in (None, Env.nodename):
            cluster_name = "join"
            for section in self.conf_sections("hb"):
                try:
                    relay = self.conf_get(section, "relay")
                except (ValueError, ex.OptNotFound):
                    continue
                if relay not in self.options.server:
                    continue
                try:
                    secret = self.conf_get(section, "secret")
                    break
                except ex.OptNotFound:
                    continue

        data = self.daemon_get(
            {"action": "daemon_relay_status"},
            server=self.options.server,
            secret=secret,
            cluster_name=cluster_name,
            timeout=5,
        )
        if self.options.format in ("json", "flat_json") or self.options.jsonpath_filter:
            self.print_data(data)
            return

        if data is None:
            return

        from utilities.render.forest import Forest
        from utilities.render.color import color

        tree = Forest()
        head = tree.add_node()
        head.add_column("cluster_id/node")
        head.add_column("cluster_name")
        head.add_column("last")
        head.add_column("elapsed")
        head.add_column("ipaddr")
        head.add_column("size")
        now = time.time()

        for nodename, _data in sorted(data.items(), key=lambda x: x[0]):
             try:
                 updated = _data.get("updated", 0)
                 size = _data.get("size", 0)
                 ipaddr = _data.get("ipaddr", "")
                 cluster_name = _data.get("cluster_name", "")
             except AttributeError:
                 continue
             node = head.add_node()
             node.add_column(nodename, color.BOLD)
             node.add_column(cluster_name)
             node.add_column("%s" % datetime.datetime.fromtimestamp(updated).strftime("%Y-%m-%d %H:%M:%S"))
             node.add_column("%s" % print_duration(now-updated))
             node.add_column("%s" % ipaddr)
             node.add_column("%s" % print_size(size, unit="B"))
        tree.out()

    def daemon_blacklist_status(self):
        """
        Show the daemon senders blacklist
        """
        data = self.daemon_get(
            {"action": "daemon_blacklist_status"},
            server=self.options.server,
            timeout=5,
        )
        status, error, info = self.parse_result(data)
        if status:
            raise ex.Error(error)
        print(json.dumps(data, indent=4, sort_keys=True))

    def _ping(self, node, timeout=5):
        """
        Fetch the daemon senders blacklist as a ping test, from either
        a peer or an arbitrator node, swiching between secrets as appropriate.
        """
        if not node or node in self.cluster_nodes or node in (Env.nodename, "127.0.0.1"):
            cluster_name = None
            secret = None
        elif node in self.cluster_drpnodes:
            try:
                secret = self.conf_get("cluster", "secret", impersonate=node)
            except ex.OptNotFound:
                raise ex.Error("unable to find the node %(node)s cluster secret. set cluster.secret@drpnodes or cluster.secret@%(node)s" % dict(node=node))
            try:
                cluster_name = self.conf_get("cluster", "name", impersonate=node)
            except ex.OptNotFound:
                raise ex.Error("unable to find the node %(node)s cluster name. set cluster.name@drpnodes or cluster.name@%(node)s" % dict(node=node))
        elif want_context():
            # relay must be tested from a cluster node
            data = self.daemon_node_action(action="ping", options={"node": node}, node="ANY", action_mode=False)
            status, error, info = self.parse_result(data)
            return status
        else:
            secret = None
            for section in self.conf_sections("arbitrator"):
                try:
                    arbitrator = self.conf_get(section, "name")
                except Exception:
                    continue
                if arbitrator != node:
                    continue
                try:
                    secret = self.conf_get(section, "secret")
                    cluster_name = "join"
                    node = "raw://" + node
                    break
                except Exception:
                    self.log.warning("missing 'secret' in configuration section %s" % section)
                    continue
            if secret is None:
                raise ex.Error("unable to find a secret for node '%s': neither in cluster.nodes, cluster.drpnodes nor arbitrator#*.name" % node)
        data = self.daemon_get(
            {"action": "daemon_blacklist_status"},
            server=node,
            cluster_name=cluster_name,
            secret=secret,
            timeout=timeout,
        )
        if data is None or "status" not in data or data["status"] != 0:
            return 1
        return 0

    def ping(self):
        ret = 0
        if not self.options.node or want_context():
            return self.ping_node(self.options.node)
        nodes = self.nodes_selector(self.options.node)
        if not nodes:
            # maybe an ip addr
            return self.ping_node(self.options.node)
        for node in nodes:
            if self.ping_node(node):
                ret = 1
        return ret

    def ping_node(self, node):
        try:
            ret = self._ping(node)
        except ex.Error as exc:
            print(exc)
            ret = 2
        if not node:
            node = "default endpoint"
        if ret == 0:
            print("%s is alive" % node)
        elif ret == 1:
            print("%s is not alive" % node)
        return ret

    def drain(self):
        """
        Tell the daemon to freeze and drain all local object instances.
        """
        wait = self.options.wait
        time = self.options.time
        if wait and time:
            request_timeout = convert_duration(time) + DEFAULT_DAEMON_TIMEOUT
        elif wait:
            request_timeout = None
        else:
            request_timeout = DEFAULT_DAEMON_TIMEOUT
        data = self.daemon_post(
            {
                "action": "node_drain",
                "options": {
                    "wait": wait,
                    "time": time,
                }
            },
            server=self.options.server or self.options.node,
            timeout=request_timeout
        )
        if data is None:
            return 1
        status, error, info = self.parse_result(data)
        if error:
            print(error, file=sys.stderr)
        return status

    def daemon_shutdown(self):
        """
        Tell the daemon to shutdown all local object instances then die.
        """
        if not self._daemon_running():
            return
        data = self.daemon_post(
            {"action": "daemon_shutdown"},
            server=self.options.server,
        )
        if data is None:
            return 1
        status, error, info = self.parse_result(data)
        if error:
            print(error, file=sys.stderr)
        return status

    def _daemon_stop(self):
        """
        Tell the daemon to die or stop a specified thread.
        """
        if not self._daemon_running():
            return
        options = {}
        if self.options.thr_id:
            options["thr_id"] = self.options.thr_id
        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            options["upgrade"] = True
        data = self.daemon_post(
            {"action": "daemon_stop", "options": options},
            server=self.options.node,
        )
        status, error, info = self.parse_result(data)
        if error:
            print(error, file=sys.stderr)
        if status:
            return data
        while True:
            if not self._daemon_running():
                break
            time.sleep(0.1)
        return data

    def daemon_stop(self):
        data = None
        if self.options.thr_id is None and self.daemon_handled_by_systemd():
            # 'systemctl restart <osvcunit>' when the daemon has been started
            # manually causes a direct 'om daemon start', which fails,
            # and systemd fallbacks to 'om daemon stop' and leaves the
            # daemon stopped.
            # Detect this situation and stop the daemon ourselves.
            if self.daemon_active_systemd():
                self.daemon_stop_systemd()
            else:
                if self._daemon_running():
                    data = self._daemon_stop()
        elif self._daemon_running():
            data = self._daemon_stop()
        if data is None:
            return
        if data.get("status") == 0:
            return
        raise ex.Error(json.dumps(data, indent=4, sort_keys=True))

    def daemon_start(self):
        if self.options.thr_id:
            return self.daemon_start_thread()
        if self.daemon_running() == 0:
            self.log.info('daemon is already started')
            return
        if self.options.foreground:
            return self.daemon_start_foreground()
        return self.daemon_start_native()

    def daemon_start_foreground(self):
        import daemon.main
        daemon.main.main(args=["-f"])

    def daemon_start_native(self):
        """
        Can be overloaded by node<os>
        """
        if self.daemon_handled_by_systemd():
            return self.daemon_start_systemd()
        os.chdir(Env.paths.pathsvc)
        return os.system(sys.executable+" -m opensvc.daemon")

    def daemon_start_thread(self):
        options = {}
        options["thr_id"] = self.options.thr_id
        data = self.daemon_post(
            {"action": "daemon_start", "options": options},
            server=self.options.server,
            timeout=DEFAULT_DAEMON_TIMEOUT,
        )
        if data.get("status") == 0:
            return
        raise ex.Error(json.dumps(data, indent=4, sort_keys=True))

    def daemon_running(self):
        if self._daemon_running():
            return 0
        else:
            return 1

    def _daemon_running(self):
        if self.options.thr_id:
            data = self._daemon_status()
            if self.options.thr_id not in data:
                return False
            return data[self.options.thr_id].get("state") == "running"
        return daemon_process_running()

    def daemon_start_systemd(self):
        """
        Do daemon start through the systemd, so that the daemon is
        service status is correctly reported.
        """
        os.system("systemctl reset-failed opensvc-agent")
        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            os.system("systemctl set-environment OPENSVC_AGENT_UPGRADE=1")
        os.system("systemctl restart opensvc-agent")
        os.system("systemctl unset-environment OPENSVC_AGENT_UPGRADE")

    def daemon_stop_systemd(self):
        """
        Do daemon stop through the systemd, so that the daemon is not restarted
        by systemd, and the systemd service status is correctly reported.
        """
        if os.environ.get("OPENSVC_AGENT_UPGRADE"):
            os.system("systemctl set-environment OPENSVC_AGENT_UPGRADE=1")
        os.system("systemctl stop opensvc-agent")
        os.system("systemctl unset-environment OPENSVC_AGENT_UPGRADE")

    def daemon_restart_systemd(self):
        """
        Do daemon restart through the systemd, so that the daemon pid is updated
        in systemd, and the systemd service status is correctly reported.
        """
        os.system("systemctl restart opensvc-agent")

    def daemon_handled_by_systemd(self):
        """
        Return True if the system has systemd.
        """
        if which("systemctl") is None:
            return False
        if os.environ.get("LOGNAME") is None:
            # do as if we're not handled by systemd if we're already run by
            # systemd
            return False
        return True

    def daemon_active_systemd(self):
        cmd = ["systemctl", "show", "-p" "ActiveState", "opensvc-agent.service"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        if out.split("=")[-1].strip() == "active":
            return True
        return False

    def daemon_restart(self):
        if self.daemon_handled_by_systemd():
            # 'systemctl restart <osvcunit>' when the daemon has been started
            # manually causes a direct 'om daemon start', which fails,
            # and systemd fallbacks to 'om daemon stop' and leaves the
            # daemon stopped.
            # Detect this situation and stop the daemon ourselves.
            if not self.daemon_active_systemd():
                if self._daemon_running():
                    self._daemon_stop()
            return self.daemon_restart_systemd()
        if self._daemon_running():
            self._daemon_stop()
        self.daemon_start()

    @lazy
    def sorted_cluster_nodes(self):
        return sorted(self.cluster_nodes)

    def daemon_leave(self):
        cluster_nodes = self.sorted_cluster_nodes
        if len(self.sorted_cluster_nodes) == 0:
            self.log.info("local node is not member of a cluster")
            return
        if Env.nodename in cluster_nodes:
            cluster_nodes.remove(Env.nodename)

        if not self.frozen():
            self.freeze()
            self.log.info("freeze local node")
        else:
            self.log.info("local node is already frozen")
        self.log.warning("DO NOT UNFREEZE before verifying split services won't double-start")

        # leave other nodes
        options = {"thr_id": "tx", "wait": True}
        data = self.daemon_post(
            {"action": "daemon_stop", "options": options},
            server=Env.nodename,
            timeout=DEFAULT_DAEMON_TIMEOUT,
        )

        errors = 0
        for nodename in cluster_nodes:
            data = self.daemon_post(
                {"action": "leave"},
                server=nodename,
                timeout=DEFAULT_DAEMON_TIMEOUT,
            )
            if data is None:
                self.log.error("leave node %s failed", nodename)
                errors += 1
            else:
                self.log.info("leave node %s", nodename)

        # remove obsolete node configurations
        todo = []
        for section in self.private_cd:
            if section == "cluster" or \
               section.startswith("hb#") or \
               section.startswith("arbitrator#"):
                self.log.info("remove configuration %s", section)
                todo.append(section)
        self.delete_sections(todo)

        # remove obsolete cluster configurations
        svc = factory("ccfg")(node=self)
        todo = []
        for section in svc.conf_sections():
            if section == "cluster" or \
               section.startswith("hb#") or \
               section.startswith("arbitrator#"):
                svc.log.info("remove configuration %s", section)
                todo.append(section)
        svc.delete_sections(todo)
        svc.unset_multi(["DEFAULT.id"])

        self.unset_lazy("cluster_nodes")
        self.unset_lazy("sorted_cluster_nodes")

    def daemon_join(self):
        if self.options.secret is None:
            raise ex.Error("--secret must be set")
        if not self.options.node:
            raise ex.Error("--node must be set")
        self._daemon_join(self.options.node, self.options.secret)

    def daemon_rejoin(self):
        if not self.options.node:
            raise ex.Error("--node must be set")
        self._daemon_join(self.options.node, self.cluster_key)

    def _daemon_join(self, *args):
        from daemon.shared import CONFIG_LOCK
        locked = CONFIG_LOCK.acquire()
        if not locked:
            return
        try:
            self.__daemon_join(*args)
        finally:
            CONFIG_LOCK.release()

    def __daemon_join(self, joined, secret):
        # freeze and remember the initial frozen state
        initially_frozen = self.frozen()
        if not initially_frozen:
            self.freeze()
            self.log.info("freeze local node")
        else:
            self.log.info("local node is already frozen")

        data = self.daemon_post(
            {"action": "join"},
            server=joined,
            cluster_name="join",
            secret=secret,
            timeout=120,
        )
        if data is None:
            raise ex.Error("join node %s failed" % joined)
        if "err" in data:
            raise ex.Error("join node %s failed: %s" % (joined, data.get("err")))

        data = data.get("data")
        if data is None:
            raise ex.Error("join failed: no data in response")
        ndata = data.get("node", {}).get("data", {})
        toadd = []
        toremove = []
        sectoremove = []

        cluster_name = ndata.get("cluster", {}).get("name")
        if cluster_name:
            toadd.append("cluster.name=" + cluster_name)
            self.set_lazy("cluster_name", cluster_name)
        else:
            toremove.append("cluster.name")
        cluster_nodes = ndata.get("cluster", {}).get("nodes")
        if cluster_nodes and isinstance(cluster_nodes, list):
            toadd.append("cluster.nodes=" + cluster_nodes)
        else:
            toremove.append("cluster.nodes")
        cluster_id = ndata.get("cluster", {}).get("id")
        if cluster_id:
            toadd.append("cluster.id=" + cluster_id)
        else:
            toremove.append("cluster.id")
        cluster_drpnodes = ndata.get("cluster", {}).get("drpnodes")
        if isinstance(cluster_drpnodes, list) and len(cluster_drpnodes) > 0:
            toadd.append("cluster.drpnodes=" + cluster_drpnodes)
        else:
            toremove.append("cluster.drpnodes")
        dns = ndata.get("cluster", {}).get("dns")
        if isinstance(dns, list) and len(dns) > 0:
            toadd.append("cluster.dns=" + dns)
        else:
            toremove.append("cluster.dns")
        quorum = ndata.get("cluster", {}).get("quorum", False)
        if quorum:
            toadd.append("cluster.quorum=true")
        else:
            toremove.append("cluster.quorum")
        peer_env = ndata.get("node", {}).get("env")
        if peer_env and peer_env != self.env:
            toadd.append("node.env="+peer_env)
        else:
            toremove.append("node.env")
        cluster_key = ndata.get("cluster", {}).get("secret")
        if cluster_key:
            # secret might be bytes, when passed from rejoin
            toadd.append("cluster.secret=" + bdecode(secret))
        else:
            toremove.append("cluster.secret")

        config = self.private_cd
        for section, _data in ndata.items():
            if section.startswith("hb#"):
                if section in config:
                    self.log.info("update heartbeat %s", section)
                    sectoremove.append(section)
                else:
                    self.log.info("add heartbeat %s", section)
                for option, value in _data.items():
                    toadd.append("%s.%s=%s" % (section, option, value))
            elif section.startswith("stonith#"):
                if section in config:
                    self.log.info("update stonith %s", section)
                    sectoremove.append(section)
                else:
                    self.log.info("add stonith %s", section)
                for option, value in _data.items():
                    toadd.append("%s.%s=%s" % (section, option, value))
            elif section.startswith("arbitrator#"):
                if section in config:
                    self.log.info("update arbitrator %s", section)
                    sectoremove.append(section)
                else:
                    self.log.info("add arbitrator %s", section)
                for option, value in _data.items():
                    toadd.append("%s.%s=%s" % (section, option, value))
            elif section.startswith("pool#"):
                if section in config:
                    self.log.info("update pool %s", section)
                    sectoremove.append(section)
                else:
                    self.log.info("add pool %s", section)
                for option, value in _data.items():
                    toadd.append("%s.%s=%s" % (section, option, value))
            elif section.startswith("network#"):
                if section in config:
                    self.log.info("update network %s", section)
                    sectoremove.append(section)
                else:
                    self.log.info("add network %s", section)
                for option, value in _data.items():
                    toadd.append("%s.%s=%s" % (section, option, value))

        # remove obsolete hb configurations
        for section in config:
            if section.startswith("hb#") and section not in ndata:
                self.log.info("remove heartbeat %s", section)
                sectoremove.append(section)

        # remove obsolete stonith configurations
        for section in config:
            if section.startswith("stonith#") and section not in ndata:
                self.log.info("remove stonith %s", section)
                sectoremove.append(section)

        # remove obsolete arbitrator configurations
        for section in config:
            if section.startswith("arbitrator#") and section not in ndata:
                self.log.info("remove arbitrator %s", section)
                sectoremove.append(section)

        # remove obsolete pool configurations
        for section in config:
            if section.startswith("pool#") and section not in ndata:
                self.log.info("remove pool %s", section)
                sectoremove.append(section)

        # remove obsolete network configurations
        for section in config:
            if section.startswith("network#") and section not in ndata:
                self.log.info("remove network %s", section)
                sectoremove.append(section)

        self.log.info("join node %s", joined)
        self.set_lazy("cluster_key", bdecode(secret))
        self.delete_sections(sectoremove)
        self.unset_multi(toremove)
        self.set_multi(toadd)

        # install cluster config
        cluster_config_data = data.get("cluster", {}).get("data")
        cluster_config_mtime = data.get("cluster", {}).get("mtime")
        if cluster_config_data:
            if not cluster_name:
                cluster_name = cluster_config_data.get("cluster", {}).get("name", "default")
            if not cluster_nodes:
                cluster_nodes = cluster_config_data.get("cluster", {}).get("nodes", [])
            self.set_lazy("cluster_name", cluster_name)
            self.set_lazy("cluster_key", bdecode(secret))

            self.install_svc_conf_from_data("cluster", None, "ccfg", cluster_config_data, restore=True)
            os.utime(Env.paths.clusterconf, (cluster_config_mtime, cluster_config_mtime))


        errors = 0
        if not cluster_config_data or not cluster_config_data.get("cluster", {}).get("nodes"):
            # join other nodes
            for nodename in cluster_nodes.split():
                if nodename in (Env.nodename, joined):
                    continue
                data = self.daemon_post(
                    {"action": "join"},
                    server=nodename,
                    cluster_name="join",
                    secret=secret,
                    timeout=DEFAULT_DAEMON_TIMEOUT,
                )
                if data is None:
                    self.log.error("join node %s failed", nodename)
                    errors += 1
                else:
                    self.log.info("join node %s", nodename)

        # leave node frozen if initially frozen or we failed joining all nodes
        if initially_frozen:
            self.log.warning("local node is left frozen as it was already before join")
        elif errors > 0:
            self.log.warning("local node is left frozen due to join errors")
        else:
            self.thaw()
            self.log.info("thaw local node")

    def set_node_monitor(self, status=None, local_expect=None, global_expect=None):
        options = {
            "status": status,
            "local_expect": local_expect,
            "global_expect": global_expect,
        }
        try:
            data = self.daemon_post(
                {"action": "node_monitor", "options": options},
                server=self.options.server,
                silent=True,
                timeout=DEFAULT_DAEMON_TIMEOUT,
            )
            if data is None:
                raise ex.Error("the daemon is not running")
            if data and data["status"] != 0:
                if data.get("error"):
                    raise ex.Error("set monitor status failed: %s" % data.get("error"))
                else:
                    raise ex.Error("set monitor status failed")
        except ex.Error:
            raise
        except Exception as exc:
            raise ex.Error("set monitor status failed: %s" % str(exc))
        return data

    def daemon_node_action(self, action=None, options=None, server=None, node=None, sync=True, collect=False, action_mode=True):
        """
        Execute a node action on a peer node.
        If sync is set, wait for the action result.
        """
        if options is None:
            options = {}
        if want_context():
            if not node:
                if action in ACTION_ANY_NODE:
                    node = "ANY"
                else:
                    raise ex.Error("the --node <selector> option is required")
        if action_mode:
            self.log.info("request node action '%s' on node %s", action, node)
        req = {
            "action": "node_action",
            "options": {
                "action": action,
                "options": options,
                "sync": sync,
            }
        }
        try:
            data = self.daemon_post(
                req,
                server=server,
                node=node,
                silent=True,
                timeout=DEFAULT_DAEMON_TIMEOUT,
            )
        except Exception as exc:
            self.log.error("node action on node %s failed: %s",
                           node, exc)
            return 1

        status, error, info = self.parse_result(data)
        if error:
            self.log.error(error)

        def print_node_data(nodename, data):
            outs = False
            if data.get("out") and len(data["out"]) > 0:
                for line in data["out"].splitlines():
                   print(line)
                   outs = True
            if data.get("err") and len(data["err"]) > 0:
                for line in data["err"].splitlines():
                   print(line, file=sys.stderr)
                   outs = True
            if not outs:
                if nodename:
                    prefix = nodename + ": "
                else:
                    prefix = ""
                if data.get("status"):
                    print("%sfailed" % prefix)
                else:
                    print("%spassed" % prefix)

        if collect:
            if "data" not in data:
                return 0
            data = data["data"]
            return data["ret"], data.get("out", ""), data.get("err", "")
        else:
            if "nodes" in data:
                if self.options.format in ("json", "flat_json"):
                    if len(data["nodes"]) == 1:
                        for _data in data["nodes"].values():
                            return _data
                    return data
                else:
                    ret = 0
                    for n, _data in data["nodes"].items():
                        status = _data.get("status", 0)
                        _data = _data.get("data", {})
                        print_node_data(n, _data)
                        ret += _data.get("ret", 0) + status
                    return ret
            else:
                if "data" not in data:
                    return 0
                data = data["data"]
                print_node_data(server, data)
                return data.get("ret", 0)

    def daemon_events(self, server=None, selector=None, full=False, namespace=None):
        req = {
            "action": "events",
            "options": {
                "selector": selector,
                "namespace": namespace,
                "full": full,
            },
        }
        while True:
            for msg in self.daemon_stream(req, server=server):
                if msg is None:
                    continue
                yield msg

            # retry until daemon restart
            time.sleep(1)

            # the node.conf might have changed (join, set, ...)
            self.unset_lazy("cluster_name")
            self.unset_lazy("cluster_names")
            self.unset_lazy("cluster_key")

    def daemon_backlogs(self, server=None, node=None, backlog=None, debug=False):
        req = {
            "action": "node_backlogs",
            "options": {
                "backlog": backlog,
                "debug": debug,
            }
        }
        result = self.daemon_get(req, server=server, node=node, timeout=5)
        if "nodes" in result:
            lines = []
            for logs in result["nodes"].values():
                if not isinstance(logs, list):
                    continue
                lines += logs
        else:
            lines = result
        try:
            return sorted(lines, key=lambda x: x.get("t", 0))
        except AttributeError:
            return []

    def daemon_logs(self, server=None, node=None, debug=False):
        req = {
            "action": "node_logs",
            "options": {
                "debug": debug,
            }
        }
        for lines in self.daemon_stream(req, server=server, node=node):
            if lines is None:
                break
            for line in lines:
                yield line

    def wake_monitor(self):
        if not daemon_process_running():
            # no need to wake to monitor when the daemon is not running
            return
        options = {
            "immediate": True,
        }
        try:
            data = self.daemon_post(
                {
                    "action": "wake_monitor",
                    "options": options
                },
                server=self.options.server,
                silent=True,
                timeout=DEFAULT_DAEMON_TIMEOUT,
            )
            status, error, info = self.parse_result(data)
            if status and data.get("errno") != ECONNREFUSED:
                if error:
                    self.log.warning("wake monitor failed: %s", error)
                else:
                    self.log.warning("wake monitor failed")
        except Exception as exc:
            self.log.warning("wake monitor failed: %s", str(exc))


    def array_show(self):
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        node = tree.add_node()
        node.add_column("name", color.BOLD)
        node.add_column("type", color.BOLD)
        for name in self.array_names():
            leaf = node.add_node()
            leaf.add_column(name)
            leaf.add_column(self.oget("array#"+name, "type"))
        print(tree)

    def array_ls(self):
        for name in self.array_names():
            print(name)

    def array_names(self):
        data = set()
        for section in self.conf_sections("array"):
            data.add(section.split("#")[-1])
        return sorted(list(data))

    ##########################################################################
    #
    # Pool
    #
    ##########################################################################
    @formatter
    def pool_ls(self):
        data = self.pool_ls_data()
        if self.options.format in ("json", "flat_json"):
            return data
        print("\n".join([name for name in data]))

    def pool_ls_data(self):
        data = set(["default", "shm"])
        for section in self.conf_sections("pool"):
            data.add(section.split("#")[-1])
        return sorted(list(data))

    @formatter
    def pool_status(self):
        data = self.pool_status_data()
        if self.options.name:
            try:
                data = {self.options.name: data[self.options.name]}
            except KeyError:
                data = {}
        if self.options.format in ("json", "flat_json"):
            return data
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        node = tree.add_node()
        node.add_column("name", color.BOLD)
        node.add_column("type", color.BOLD)
        node.add_column("caps", color.BOLD)
        node.add_column("head", color.BOLD)
        node.add_column("vols", color.BOLD)
        node.add_column("size", color.BOLD)
        node.add_column("used", color.BOLD)
        node.add_column("free", color.BOLD)
        for name, _data in data.items():
            leaf = node.add_node()
            leaf.add_column(name, color.BROWN)
            if _data is None:
                continue
            leaf.add_column(_data["type"])
            leaf.add_column(",".join(_data["capabilities"]))
            leaf.add_column(_data.get("head",""))
            leaf.add_column(len(_data["volumes"]))
            for key in ("size", "used", "free"):
                if _data.get(key, -1) < 0:
                    val = "-"
                else:
                    val = print_size(_data[key], unit="k", compact=True)
                leaf.add_column(val)
            if not self.options.verbose:
                continue
            vols_node = leaf.add_node()
            vols_node.add_column("volume", color.BOLD)
            vols_node.add_column("size", color.BOLD)
            vols_node.add_column("children", color.BOLD)
            vols_node.add_column("orphan", color.BOLD)
            for vol in sorted(_data.get("volumes", []), key=lambda x: x["path"]):
                vol_node = vols_node.add_node()
                vol_node.add_column(vol["path"])
                vol_node.add_column(print_size(vol["size"], unit="b", compact=True))
                vol_node.add_column(",".join(vol["children"]))
                vol_node.add_column(str(vol["orphan"]).lower())

        print(tree)

    def pools_volumes(self):
        try:
            data = self._daemon_status(silent=True)["monitor"]
        except Exception as exc:
            return {}
        pools = {}
        done = []
        for nodename, ndata in data["nodes"].items():
            if not isinstance(ndata, dict):
                continue
            for path, sdata in ndata.get("services", {}).get("status", {}).items():
                if path in done:
                    continue
                done.append(path)
                poolname = sdata.get("pool")
                children = sdata.get("children", [])
                vdata = {
                    "path": path,
                    "size": sdata.get("size", 0),
                    "children": children,
                    "orphan": not children or not any(child in data["services"] for child in children),
                }
                try:
                    pools[poolname].append(vdata)
                except Exception:
                    pools[poolname] = [vdata]
        return pools

    def pool_status_data(self, usage=True, pools=None):
        all_pools = self.pool_ls_data()
        if pools:
            pools = [p["name"] for p in pools if p["name"] in all_pools]
        else:
            pools = all_pools
        data = {}
        procs = {}
        volumes = self.pools_volumes()

        def job(self, name, volumes):
            try:
                pool = self.get_pool(name)
                d = pool.pool_status(usage=usage)
            except Exception as exc:
                d = {
                    "name": name,
                    "type": "unknown",
                    "capabilities": [],
                    "head": "err: " + str(exc),
                }
            if name in volumes:
                d["volumes"] = sorted(volumes[name], key=lambda x: x["path"])
            else:
                d["volumes"] = []
            return d

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for name in pools:
                procs[executor.submit(job, self, name, volumes)] = name
            for future in concurrent.futures.as_completed(procs):
                name = procs[future]
                data[name] = future.result()
        return data

    def find_pool(self, poolname=None, pooltype=None, access=None, size=None, fmt=None, shared=False, usage=True):
        candidates1 = []
        candidates = []
        cause = []
        for pool in self.pool_status_data(usage=False).values():
            if shared is True and "shared" not in pool["capabilities"]:
                cause.append((pool["name"], "not shared capable"))
                continue
            if fmt is False and "blk" not in pool["capabilities"]:
                cause.append((pool["name"], "not blk capable"))
                continue
            if access and access not in pool["capabilities"]:
                caps = ','.join(pool["capabilities"])
                cause.append((pool["name"], "not %s capable (%s)" % (access, caps)))
                continue
            if pooltype and pool["type"] != pooltype:
                cause.append((pool["name"], "wrong type: %s, requested %s" % (pool["type"], pooltype)))
                continue
            if not pooltype and pool["type"] == "shm":
                cause.append((pool["name"], "volatile, type not requested, assume persistence is expected."))
                continue
            if poolname and pool["name"] != poolname:
                cause.append((pool["name"], "not named %s" % poolname))
                continue
            candidates1.append(pool)

        if usage:
            for pool in self.pool_status_data(usage=True, pools=candidates1).values():
                if size and "free" in pool and pool["free"] < size//1024+1:
                    cause.append((pool["name"], "not enough free space: %s free, %s requested" % (print_size(pool["free"], unit="KB", compact=True), print_size(size, unit="B", compact=True))))
                    continue
                candidates.append(pool)
        else:
            candidates = candidates1

        if not candidates:
            cause = "\n".join(["    discard pool %s: %s" % (name, reason) for name, reason in cause])
            raise ex.Error(cause)

        def shared_weight(pool):
            if not shared and "shared" in pool["capabilities"]:
                # try not to select a shared capable pool when the resource
                # doesn't require the shared cap
                return 0
            return 1

        def free_weight(pool):
            return pool.get("free", 0)

        def weight(pool):
            return (shared_weight(pool), free_weight(pool))

        candidates = sorted(candidates, key=lambda x: weight(x))
        return self.get_pool(candidates[-1]["name"])

    def get_pool(self, poolname):
        try:
            section = "pool#"+poolname
        except TypeError:
            raise ex.Error("invalid pool name: %s" % poolname)
        if poolname not in ("shm", "default") and not section in self.cd:
            raise ex.Error("pool not found: %s" % poolname)
        if poolname == "shm":
            ptype = "shm"
        else:
            try:
                ptype = self.conf_get(section, "type")
            except ex.OptNotFound as exc:
                ptype = exc.default
        mod = driver_import("pool", ptype)
        return mod.Pool(node=self, name=poolname, log=self.log)

    def pool_create_volume(self):
        self._pool_create_volume(poolname=self.options.pool,
                                 name=self.options.name,
                                 namespace=self.options.namespace,
                                 size=self.options.size,
                                 access=self.options.access,
                                 nodes=self.options.nodes,
                                 shared=self.options.shared,
                                 fmt=not self.options.blk)

    def _pool_create_volume(self, poolname=None, **kwargs):
        try:
            pool = self.get_pool(poolname)
        except ImportError as exc:
            raise ex.Error(str(exc))
        pool.create_volume(**kwargs)

    ##########################################################################
    #
    # Stats
    #
    ##########################################################################
    def score(self, data):
        """
        Higher scoring nodes get best placement ranking.
        """
        score = 100 / max(data.get("load_15m", 1), 1)
        score += 100 + data.get("mem_avail", 0)
        score += 2 * (100 + data.get("swap_avail", 0))
        return int(score // 7)

    def stats_meminfo(self):
        """
        OS-specific implementations
        """
        return {}

    def stats(self, refresh=False):
        now = time.time()
        if not refresh and self.stats_data and \
           self.stats_updated > now - STATS_INTERVAL:
            return self.stats_data
        data = {}
        self.stats_updated = now
        try:
            data["load_15m"] = round(os.getloadavg()[2], 1)
        except:
            # None < 0 == True
            pass
        try:
            meminfo = self.stats_meminfo()
        except OSError as exc:
            self.log.error("failed to get mem/swap info: %s", exc)
            meminfo = None
        if isinstance(meminfo, dict):
            data.update(meminfo)
        data["score"] = self.score(data)
        self.stats_data = data
        return data

    def get_tid(self):
        return

    def cpu_time(self, stat_path='/proc/stat'):
        return 0.0

    def pid_cpu_time(self, pid):
        return 0.0

    def tid_cpu_time(self, tid):
        return 0.0

    def pid_mem_total(self, pid):
        return 0.0

    def tid_mem_total(self, tid):
        return 0.0

    ##########################################################################

    def set_lazy(self, prop, val):
        """
        Expose the set_lazy(self, ...) utility function as a method,
        so Node() users don't have to import it from utilities.
        """
        set_lazy(self, prop, val)

    def unset_lazy(self, prop):
        """
        Expose the unset_lazy(self, ...) utility function as a method,
        so Node() users don't have to import it from utilities.
        """
        unset_lazy(self, prop)

    @lazy
    def hooks(self):
        """
        A hash of hook command sets, indexed by event.
        """
        data = {}
        for section in self.conf_sections("hook"):
            try:
                command = tuple(self.conf_get(section, "command"))
            except Exception:
                continue
            events = self.conf_get(section, "events")
            for event in events:
                if event not in data:
                    data[event] = set()
                data[event].add(command)
        return data

    def write_boot_id(self):
        with open(self.paths.last_boot_id, "w") as ofile:
            ofile.write(self.asset.get_boot_id())

    def last_boot_id(self):
        try:
            with open(self.paths.last_boot_id, "r") as ofile:
                return ofile.read()
        except Exception:
            return

    @lazy
    def targets(self):
        return self.asset.get_targets()

    def unset_all_lazy(self):
        unset_all_lazy(self)

    def delete(self):
        self.delete_sections(self.options.kw)

    @lazy
    def oci(self):
        oci = self.oget("node", "oci")
        if oci:
            return oci
        if "node.x.podman" in capabilities:
            return "podman"
        else:
            return "docker"

    @formatter
    def scan_capabilities(self):
        return capabilities.scan(node=self)

    @formatter
    def print_capabilities(self):
        return capabilities.data

    def post_commit(self):
        self.unset_all_lazy()


# helper for tests mock
_wait_get_time = time.time
_wait_delay = delay
