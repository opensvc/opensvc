from __future__ import print_function
from svc import Svc
from freezer import Freezer
import svcBuilder
import xmlrpcClient
import os
import datetime
import time
import sys
import json
from rcGlobalEnv import rcEnv
import rcCommandWorker
import socket
import rcLogger
import rcUtilities
import rcExceptions as ex
from subprocess import *
from rcScheduler import *
from rcConfigParser import RawConfigParser
from rcColor import formatter

if sys.version_info[0] >= 3:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError
    from urllib.parse import urlencode
else:
    from urllib2 import Request, urlopen
    from urllib2 import HTTPError
    from urllib import urlencode

try:
    import base64
except:
    pass

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

if sys.version_info[0] < 3:
    BrokenPipeError = IOError

deprecated_keywords = {
  "node.host_mode": "env",
  "node.environnement": "asset_env",
  "node.environment": "asset_env",
}

reverse_deprecated_keywords = {
  "node.env": ["host_mode"],
  "node.asset_env": ["environnement", "environment"],
}

actions_no_parallel = [
  'edit_config',
  'get',
  'print_config',
  'print_resource_status',
  'print_schedule',
  "print_status",
]

actions_no_multiple_services = [
  "print_resource_status",
]


class Options(object):
    def __init__(self):
        self.cron = False
        self.syncrpc = False
        self.force = False
        self.debug = False
        self.stats_dir = None
        self.begin = None
        self.end = None
        self.moduleset = ""
        self.module = ""
        self.ruleset_date = ""
        self.waitlock = -1
        self.parallel = False
        self.objects = []
        os.environ['LANG'] = 'C'

class Node(Svc, Freezer, Scheduler):
    """ Defines a cluster node.  It contain list of Svc.
        Implements node-level actions and checks.
    """
    def __str__(self):
        s = self.nodename
        return s

    def __init__(self):
        self.ex_monitor_action_exit_code = 251
        self.auth_config = None
        self.nodename = socket.gethostname().lower()
        self.setup_sync_flag = os.path.join(rcEnv.pathvar, 'last_setup_sync')
        self.reboot_flag = os.path.join(rcEnv.pathvar, "REBOOT_FLAG")
        self.config_defaults = {
          'clusters': '',
          'node_env': 'TST',
          'push_schedule': '00:00-06:00@361 mon-sun',
          'sync_schedule': '04:00-06:00@121 mon-sun',
          'comp_schedule': '02:00-06:00@241 sun',
          'collect_stats_schedule': '@10',
          'no_schedule': '',
        }
        self.svcs = None
        try:
            self.clusters = list(set(self.config.get('node', 'clusters').split()))
        except:
            self.clusters = []
        self.load_config()
        self.options = Options()
        Scheduler.__init__(self)
        Freezer.__init__(self, '')
        self.unprivileged_actions = [
          "collector_cli",
          "print_schedule",
        ]
        self.collector = xmlrpcClient.Collector(node=self)
        self.cmdworker = rcCommandWorker.CommandWorker()
        try:
            rcos = __import__('rcOs'+rcEnv.sysname)
        except ImportError:
            rcos = __import__('rcOs')
        self.os = rcos.Os()
        rcEnv.logfile = os.path.join(rcEnv.pathlog, "node.log")
        self.log = rcLogger.initLogger(rcEnv.nodename)
        self.set_collector_env()
        self.scheduler_actions = {
	 "checks": SchedOpts("checks"),
	 "dequeue_actions": SchedOpts("dequeue_actions", schedule_option="no_schedule"),
	 "pushstats": SchedOpts("stats"),
	 "collect_stats": SchedOpts("stats_collection", schedule_option="collect_stats_schedule"),
	 "pushpkg": SchedOpts("packages"),
	 "pushpatch": SchedOpts("patches"),
	 "pushasset": SchedOpts("asset"),
	 "pushnsr": SchedOpts("nsr", schedule_option="no_schedule"),
	 "pushhp3par": SchedOpts("hp3par", schedule_option="no_schedule"),
	 "pushemcvnx": SchedOpts("emcvnx", schedule_option="no_schedule"),
	 "pushcentera": SchedOpts("centera", schedule_option="no_schedule"),
	 "pushnetapp": SchedOpts("netapp", schedule_option="no_schedule"),
	 "pushibmds": SchedOpts("ibmds", schedule_option="no_schedule"),
	 "pushdcs": SchedOpts("dcs", schedule_option="no_schedule"),
	 "pushfreenas": SchedOpts("freenas", schedule_option="no_schedule"),
	 "pushgcedisks": SchedOpts("gcedisks", schedule_option="no_schedule"),
	 "pushhds": SchedOpts("hds", schedule_option="no_schedule"),
	 "pushnecism": SchedOpts("necism", schedule_option="no_schedule"),
	 "pusheva": SchedOpts("eva", schedule_option="no_schedule"),
	 "pushibmsvc": SchedOpts("ibmsvc", schedule_option="no_schedule"),
	 "pushvioserver": SchedOpts("vioserver", schedule_option="no_schedule"),
	 "pushsym": SchedOpts("sym", schedule_option="no_schedule"),
	 "pushbrocade": SchedOpts("brocade", schedule_option="no_schedule"),
	 "pushdisks": SchedOpts("disks"),
	 "sysreport": SchedOpts("sysreport"),
	 "compliance_auto": SchedOpts("compliance", fname="node"+os.sep+"last_comp_check", schedule_option="comp_schedule"),
	 "auto_rotate_root_pw": SchedOpts("rotate_root_pw", fname="node"+os.sep+"last_rotate_root_pw", schedule_option="no_schedule"),
	 "auto_reboot": SchedOpts("reboot", fname="node"+os.sep+"last_auto_reboot", schedule_option="no_schedule")
        }

    def split_url(self, url, default_app=None):
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

        l = url.split('/')
        if len(l) < 1:
            raise

        # app
        if len(l) > 1:
            app = l[1]
        else:
            app = default_app

        # host/port
        l = l[0].split(':')
        if len(l) == 1:
            host = l[0]
            if transport == 'http':
                port = '80'
            else:
                port = '443'
        elif len(l) == 2:
            host = l[0]
            port = l[1]
        else:
            raise

        return transport, host, port, app

    def set_collector_env(self):
        if self.config is None:
            self.load_config
        if self.config is None:
            return
        if self.config.has_option('node', 'dbopensvc'):
            url = self.config.get('node', 'dbopensvc')
            try:
                rcEnv.dbopensvc_transport, rcEnv.dbopensvc_host, rcEnv.dbopensvc_port, rcEnv.dbopensvc_app = self.split_url(url, default_app="feed")
                rcEnv.dbopensvc = "%s://%s:%s/%s/default/call/xmlrpc" % (rcEnv.dbopensvc_transport, rcEnv.dbopensvc_host, rcEnv.dbopensvc_port, rcEnv.dbopensvc_app)
            except Exception as e:
                self.log.error("malformed dbopensvc url: %s (%s)" % (rcEnv.dbopensvc, str(e)))
        else:
            rcEnv.dbopensvc_transport = None
            rcEnv.dbopensvc_host = None
            rcEnv.dbopensvc_port = None
            rcEnv.dbopensvc_app = None
            rcEnv.dbopensvc = None

        if self.config.has_option('node', 'dbcompliance'):
            url = self.config.get('node', 'dbcompliance')
            try:
                rcEnv.dbcompliance_transport, rcEnv.dbcompliance_host, rcEnv.dbcompliance_port, rcEnv.dbcompliance_app = self.split_url(url, default_app="init")
                rcEnv.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (rcEnv.dbcompliance_transport, rcEnv.dbcompliance_host, rcEnv.dbcompliance_port, rcEnv.dbcompliance_app)
            except Exception as e:
                self.log.error("malformed dbcompliance url: %s (%s)" % (rcEnv.dbcompliance, str(e)))
        else:
            rcEnv.dbcompliance_transport = rcEnv.dbopensvc_transport
            rcEnv.dbcompliance_host = rcEnv.dbopensvc_host
            rcEnv.dbcompliance_port = rcEnv.dbopensvc_port
            rcEnv.dbcompliance_app = "init"
            rcEnv.dbcompliance = "%s://%s:%s/%s/compliance/call/xmlrpc" % (rcEnv.dbcompliance_transport, rcEnv.dbcompliance_host, rcEnv.dbcompliance_port, rcEnv.dbcompliance_app)

        if self.config.has_option('node', 'uuid'):
            rcEnv.uuid = self.config.get('node', 'uuid')
        else:
            rcEnv.uuid = ""

    def call(self, cmd=['/bin/false'], cache=False, info=False,
             errlog=True, err_to_warn=False, err_to_info=False,
             outlog=False):
        """Use subprocess module functions to do a call
        """
        return rcUtilities.call(cmd, log=self.log,
                                cache=cache,
                                info=info, errlog=errlog,
                                err_to_warn=err_to_warn,
                                err_to_info=err_to_info,
                                outlog=outlog)

    def vcall(self, cmd, err_to_warn=False, err_to_info=False):
        """Use subprocess module functions to do a call and
        log the command line using the resource logger
        """
        return rcUtilities.vcall(cmd, log=self.log,
                                 err_to_warn=err_to_warn,
                                 err_to_info=err_to_info)

    def build_services(self, *args, **kwargs):
        if self.svcs is not None and \
           ('svcnames' not in kwargs or \
           (type(kwargs['svcnames']) == list and len(kwargs['svcnames'])==0)):
            return

        if 'svcnames' in kwargs and \
           type(kwargs['svcnames']) == list and \
           len(kwargs['svcnames'])>0 and \
           self.svcs is not None:
            svcnames_request = set(kwargs['svcnames'])
            svcnames_actual = set([s.svcname for s in self.svcs])
            svcnames_request = list(svcnames_request-svcnames_actual)
            if len(svcnames_request) == 0:
                return

        self.svcs = []
        autopush = True
        if 'autopush' in kwargs:
            if not kwargs['autopush']:
                autopush = False
            del kwargs['autopush']
        svcs, errors = svcBuilder.build_services(*args, **kwargs)
        for svc in svcs:
            self += svc
        if autopush:
            for svc in self.svcs:
                if svc.collector_outdated():
                    svc.autopush()

        if 'svcnames' in kwargs:
            if type(kwargs['svcnames']) == list:
                n = len(kwargs['svcnames'])
            else:
                n = 1
            if len(self.svcs) != n:
                msg = ""
                if n > 1:
                    msg += "%d services validated out of %d\n" % (len(self.svcs), n)
                if len(errors) == 1:
                    msg += errors[0]
                else:
                    msg += "\n".join(list(map(lambda x: "- "+x, errors)))
                raise ex.excError(msg)
        import rcLogger
        rcLogger.set_namelen(self.svcs)


    def close(self):
        self.collector.stop_worker()
        self.cmdworker.stop_worker()

    def edit_config(self):
        cf = os.path.join(rcEnv.pathetc, "node.conf")
        return self.edit_cf(cf)

    def edit_authconfig(self):
        cf = os.path.join(rcEnv.pathetc, "auth.conf")
        return self.edit_cf(cf)

    def edit_cf(self, cf):
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        from rcUtilities import which
        if not which(editor):
            print("%s not found" % editor, file=sys.stderr)
            return 1
        os.environ["LANG"] = "en_US.UTF-8"
        return os.system(' '.join((editor, cf)))

    def write_config(self):
        for o in self.config_defaults:
            if self.config.has_option('DEFAULT', o):
                self.config.remove_option('DEFAULT', o)
        for s in self.config.sections():
            if '#sync#' in s:
                self.config.remove_section(s)
        import tempfile
        import shutil
        try:
            fp = tempfile.NamedTemporaryFile()
            fname = fp.name
            fp.close()
            with open(fname, "w") as fp:
                self.config.write(fp)
            shutil.move(fname, rcEnv.nodeconf)
        except Exception as e:
            print("failed to write new %s (%s)" % (rcEnv.nodeconf, str(e)), file=sys.stderr)
            raise Exception()
        try:
            os.chmod(rcEnv.nodeconf, 0o0600)
        except:
            pass
        self.load_config()

    def purge_status_last(self):
        for s in self.svcs:
            s.purge_status_last()

    def read_cf(self, fpath, defaults={}):
        import codecs
        config = RawConfigParser(defaults)
        if not os.path.exists(fpath):
            return config
        with codecs.open(fpath, "r", "utf8") as f:
            if sys.version_info[0] >= 3:
                config.read_file(f)
            else:
                config.readfp(f)
        return config

    def load_config(self):
        try:
            self.config = self.read_cf(rcEnv.nodeconf, self.config_defaults)
        except:
            self.config = None

    def load_auth_config(self):
        if self.auth_config is not None:
            return
        self.auth_config = self.read_cf(rcEnv.authconf)

    def setup_sync_outdated(self):
        """ return True if one configuration file has changed in the last 10'
            else return False
        """
        import datetime
        import glob
        cfs = glob.glob(os.path.join(rcEnv.pathetc, '*.conf'))
        if not os.path.exists(self.setup_sync_flag):
            return True
        for cf in cfs:
            try:
                mtime = os.stat(cf).st_mtime
                f = open(self.setup_sync_flag)
                last = float(f.read())
                f.close()
            except:
                return True
            if mtime > last:
                return True
        return False

    def __iadd__(self, s):
        if not isinstance(s, Svc):
            return self
        if self.svcs is None:
            self.svcs = []
        s.node = self
        if not hasattr(s, "clustername") and len(self.clusters) == 1:
            s.clustername = self.clusters[0]
        self.svcs.append(s)
        return self

    def action(self, a):
        if "_json_" in a:
            self.options.format = "json"
            a = a.replace("_json_", "_")
        if a.startswith("json_"):
            self.options.format = "json"
            a = "print"+a[4:]
        if a.startswith("compliance_"):
            from compliance import Compliance
            o = Compliance(self)
            if self.options.cron and a == "compliance_auto" and \
               self.config.has_option('compliance', 'auto_update') and \
               self.config.getboolean('compliance', 'auto_update'):
                o.updatecomp = True
                o.node = self
            return getattr(o, a)()
        elif a.startswith("collector_") and a != "collector_cli":
            from collector import Collector
            o = Collector(self.options, self)
            data = getattr(o, a)()
            return self.print_data(data)
        else:
            return getattr(self, a)()

    @formatter
    def print_data(self, data):
        return data

    def scheduler(self):
        self.options.cron = True
        for action in self.scheduler_actions:
            try:
                self.action(action)
            except:
                self.log.exception("")

    def get_push_objects(self, s):
        if len(self.options.objects) > 0:
            return self.options.objects
        try:
            objects = self.config.get(s, "objects").split(",")
        except Exception as e:
            objects = ["magix123456"]
            print(e)
        return objects

    def collect_stats(self):
        if self.skip_action("collect_stats"):
            return
        self.task_collect_stats()

    @scheduler_fork
    def collect_stats(self):
        try:
            m = __import__("rcStatsCollect"+rcEnv.sysname)
        except ImportError:
            return
        m.collect(self)

    def pushstats(self):
        # set stats range to push to "last pushstat => now"

        ts = self.get_timestamp_f(self.scheduler_actions["pushstats"].fname)
        try:
            with open(ts, "r") as f:
                buff = f.read()
            start = datetime.datetime.strptime(buff, "%Y-%m-%d %H:%M:%S.%f\n")
            now = datetime.datetime.now()
            delta = now - start
            interval = delta.days * 1440 + delta.seconds // 60 + 10
            #print("push stats for the last %d minutes since last push" % interval)
        except Exception as e:
            interval = 1450
            #print("can not determine last push date. push stats for the last %d minutes" % interval)
        if interval < 21:
            interval = 21

        if self.skip_action("pushstats"):
            return
        self.task_pushstats(interval)

    @scheduler_fork
    def task_pushstats(self, interval):
        if self.config.has_option("stats", "disable"):
            disable = self.config.get("stats", "disable")
        else:
            disable = []

        if isinstance(disable, str):
            try:
                disable = json.loads(disable)
            except:
                if ',' in disable:
                    disable = disable.replace(' ','').split(',')
                else:
                    disable = disable.split(' ')
        else:
            disable = []

        return self.collector.call('push_stats',
                                stats_dir=self.options.stats_dir,
                                stats_start=self.options.begin,
                                stats_end=self.options.end,
                                interval=interval,
                                disable=disable)

    def pushpkg(self):
        if self.skip_action("pushpkg"):
            return
        self.task_pushpkg()

    @scheduler_fork
    def task_pushpkg(self):
        self.collector.call('push_pkg')

    def pushpatch(self):
        if self.skip_action("pushpatch"):
            return
        self.task_pushpatch()

    @scheduler_fork
    def task_pushpatch(self):
        self.collector.call('push_patch')

    def pushasset(self):
        if self.skip_action("pushasset"):
            return
        self.task_pushasset()

    @scheduler_fork
    def task_pushasset(self):
        self.collector.call('push_asset', self)

    def pushnsr(self):
        if self.skip_action("pushnsr"):
            return
        self.task_pushnsr()

    @scheduler_fork
    def task_pushnsr(self):
        self.collector.call('push_nsr')

    def pushhp3par(self):
        if self.skip_action("pushhp3par"):
            return
        self.task_pushhp3par()

    @scheduler_fork
    def task_pushhp3par(self):
        self.collector.call('push_hp3par', self.options.objects)

    def pushnetapp(self):
        if self.skip_action("pushnetapp"):
            return
        self.task_pushnetapp()

    @scheduler_fork
    def task_pushnetapp(self):
        self.collector.call('push_netapp', self.options.objects)

    def pushcentera(self):
        if self.skip_action("pushcentera"):
            return
        self.task_pushcentera()

    @scheduler_fork
    def task_pushcentera(self):
        self.collector.call('push_centera', self.options.objects)

    def pushemcvnx(self):
        if self.skip_action("pushemcvnx"):
            return
        self.task_pushemcvnx()

    @scheduler_fork
    def task_pushemcvnx(self):
        self.collector.call('push_emcvnx', self.options.objects)

    def pushibmds(self):
        if self.skip_action("pushibmds"):
            return
        self.task_pushibmds()

    @scheduler_fork
    def task_pushibmds(self):
        self.collector.call('push_ibmds', self.options.objects)

    def pushgcedisks(self):
        if self.skip_action("pushgcedisks"):
            return
        self.task_pushgcedisks()

    @scheduler_fork
    def task_pushgcedisks(self):
        self.collector.call('push_gcedisks', self.options.objects)

    def pushfreenas(self):
        if self.skip_action("pushfreenas"):
            return
        self.task_pushfreenas()

    @scheduler_fork
    def task_pushfreenas(self):
        self.collector.call('push_freenas', self.options.objects)

    def pushdcs(self):
        if self.skip_action("pushdcs"):
            return
        self.task_pushdcs()

    @scheduler_fork
    def task_pushdcs(self):
        self.collector.call('push_dcs', self.options.objects)

    def pushhds(self):
        if self.skip_action("pushhds"):
            return
        self.task_pushhds()

    @scheduler_fork
    def task_pushhds(self):
        self.collector.call('push_hds', self.options.objects)

    def pushnecism(self):
        if self.skip_action("pushnecism"):
            return
        self.task_pushnecism()

    @scheduler_fork
    def task_pushnecism(self):
        self.collector.call('push_necism', self.options.objects)

    def pusheva(self):
        if self.skip_action("pusheva"):
            return
        self.task_pusheva()

    @scheduler_fork
    def task_pusheva(self):
        self.collector.call('push_eva', self.options.objects)

    def pushibmsvc(self):
        if self.skip_action("pushibmsvc"):
            return
        self.task_pushibmsvc()

    @scheduler_fork
    def task_pushibmsvc(self):
        self.collector.call('push_ibmsvc', self.options.objects)

    def pushvioserver(self):
        if self.skip_action("pushvioserver"):
            return
        self.task_pushvioserver()

    @scheduler_fork
    def task_pushvioserver(self):
        self.collector.call('push_vioserver', self.options.objects)

    def pushsym(self):
        if self.skip_action("pushsym"):
            return
        self.task_pushsym()

    @scheduler_fork
    def task_pushsym(self):
        objects = self.get_push_objects("sym")
        self.collector.call('push_sym', objects)

    def pushbrocade(self):
        if self.skip_action("pushbrocade"):
            return
        self.task_pushbrocade()

    @scheduler_fork
    def task_pushbrocade(self):
        self.collector.call('push_brocade', self.options.objects)

    def auto_rotate_root_pw(self):
        if self.skip_action("auto_rotate_root_pw"):
            return
        self.task_auto_rotate_root_pw()

    @scheduler_fork
    def task_auto_rotate_root_pw(self):
        self.rotate_root_pw()

    def unschedule_reboot(self):
        if not os.path.exists(self.reboot_flag):
            print("reboot already not scheduled")
            return
        os.unlink(self.reboot_flag)
        print("reboot unscheduled")

    def schedule_reboot(self):
        if not os.path.exists(self.reboot_flag):
            with open(self.reboot_flag, "w") as f: f.write("")
        import stat
        s = os.stat(self.reboot_flag)
        if s.st_uid != 0:
            os.chown(self.reboot_flag, 0, -1)
            print("set %s root ownership"%self.reboot_flag)
        if s.st_mode & stat.S_IWOTH:
            mode = s.st_mode ^ stat.S_IWOTH
            os.chmod(self.reboot_flag, mode)
            print("set %s not world-writable"%self.reboot_flag)
        print("reboot scheduled")

    def schedule_reboot_status(self):
        import stat
        if os.path.exists(self.reboot_flag):
            s = os.stat(self.reboot_flag)
        else:
            s = None

        if s is None or s.st_uid != 0 or s.st_mode & stat.S_IWOTH:
            print("reboot is not scheduled")
        else:
            sch = self.scheduler_actions["auto_reboot"]
            schedule = self.sched_get_schedule_raw(sch.section, sch.schedule_option)
            print("reboot is scheduled")
            print("reboot schedule: %s" % schedule)

        d, _max = self.get_next_schedule("auto_reboot")
        if d:
            print("next reboot slot:", d.strftime("%a %Y-%m-%d %H:%M"))
        else:
            print("next reboot slot: none in the next %d days" % (_max/144))

    def auto_reboot(self):
        if self.skip_action("auto_reboot"):
            return
        self.task_auto_reboot()

    @scheduler_fork
    def task_auto_reboot(self):
        if not os.path.exists(self.reboot_flag):
            print("%s is not present. no reboot scheduled" % self.reboot_flag)
            return
        import stat
        s = os.stat(self.reboot_flag)
        if s.st_uid != 0:
            print("%s does not belong to root. abort scheduled reboot" % self.reboot_flag)
            return
        if s.st_mode & stat.S_IWOTH:
            print("%s is world writable. abort scheduled reboot" % self.reboot_flag)
            return
        print("remove %s and reboot" % self.reboot_flag)
        os.unlink(self.reboot_flag)
        self.reboot()

    def pushdisks(self):
        if self.skip_action("pushdisks"):
            return
        self.task_pushdisks()

    @scheduler_fork
    def task_pushdisks(self):
        if self.svcs is None:
            self.build_services()
        self.collector.call('push_disks', self)

    def shutdown(self):
        print("TODO")

    def reboot(self):
        self.do_triggers("reboot", "pre")
        self.log.info("reboot")
        self._reboot()

    def do_triggers(self, action, when):
        trigger = None
        blocking_trigger = None
        try:
            trigger = self.config.get(action, when)
        except:
            pass
        try:
            blocking_trigger = self.config.get(action, "blocking_"+when)
        except:
            pass
        if trigger:
            self.log.info("execute trigger %s" % trigger)
            try:
                self.do_trigger(trigger)
            except ex.excError:
                pass
        if blocking_trigger:
            self.log.info("execute blocking trigger %s" % trigger)
            try:
                self.do_trigger(blocking_trigger)
            except ex.excError:
                if when == "pre":
                    self.log.error("blocking pre trigger error: abort %s" % action)
                raise

    def do_trigger(self, cmd, err_to_warn=False):
        import shlex
        _cmd = shlex.split(cmd)
        ret, out, err = self.vcall(_cmd, err_to_warn)
        if ret != 0:
            raise ex.excError
 
    def _reboot(self):
        print("TODO")

    def sysreport(self):
        if self.skip_action("sysreport"):
            return
        try:
            self.task_sysreport()
        except Exception as e:
            print(e)
            return 1

    @scheduler_fork
    def task_sysreport(self):
        from rcGlobalEnv import rcEnv
        try:
            m = __import__('rcSysReport'+rcEnv.sysname)
        except ImportError:
            print("sysreport is not supported on this os")
            return
        m.SysReport(node=self).sysreport(force=self.options.force)

    def get_prkey(self):
        if self.config.has_option("node", "prkey"):
            hostid = self.config.get("node", "prkey")
            if len(hostid) > 18 or not hostid.startswith("0x") or \
               len(set(hostid[2:]) - set("0123456789abcdefABCDEF")) > 0:
                raise ex.excError("prkey in node.conf must have 16 significant hex digits max (ex: 0x90520a45138e85)")
            return hostid
        self.log.info("can't find a prkey forced in node.conf. generate one.")
        hostid = "0x"+self.hostid()
        self.config.set('node', 'prkey', hostid)
        self.write_config()
        return hostid

    def prkey(self):
        print(self.get_prkey())

    def hostid(self):
        from rcGlobalEnv import rcEnv
        m = __import__('hostid'+rcEnv.sysname)
        return m.hostid()

    def checks(self):
        if self.skip_action("checks"):
            return
        self.task_checks()

    @scheduler_fork
    def task_checks(self):
        import checks
        if self.svcs is None:
            self.build_services()
        c = checks.checks(self.svcs)
        c.node = self
        c.do_checks()

    def wol(self):
        import rcWakeOnLan
        if self.options.mac is None:
            print("missing parameter. set --mac argument. multiple mac addresses must be separated by comma", file=sys.stderr)
            print("example 1 : --mac 00:11:22:33:44:55", file=sys.stderr)
            print("example 2 : --mac 00:11:22:33:44:55,66:77:88:99:AA:BB", file=sys.stderr)
            return 1
        if self.options.broadcast is None:
            print("missing parameter. set --broadcast argument. needed to identify accurate network to use", file=sys.stderr)
            print("example 1 : --broadcast 10.25.107.255", file=sys.stderr)
            print("example 2 : --broadcast 192.168.1.5,10.25.107.255", file=sys.stderr)
            return 1
        macs = self.options.mac.split(',')
        broadcasts = self.options.broadcast.split(',')
        for brdcast in broadcasts:
            for mac in macs:
                req = rcWakeOnLan.wolrequest(macaddress=mac, broadcast=brdcast)
                if not req.check_broadcast():
                    print("Error : skipping broadcast address <%s>, not in the expected format 123.123.123.123"%req.broadcast, file=sys.stderr)
                    break
                if not req.check_mac():
                    print("Error : skipping mac address <%s>, not in the expected format 00:11:22:33:44:55"%req.mac, file=sys.stderr)
                    continue
                if req.send():
                    print("Sent Wake On Lan packet to mac address <%s>"%req.mac)
                else:
                    print("Error while trying to send Wake On Lan packet to mac address <%s>"%req.mac, file=sys.stderr)

    def unset(self):
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if not self.config.has_section(section):
            print("section '%s' not found"%section, file=sys.stderr)
            return 1
        if not self.config.has_option(section, option):
            print("option '%s' not found in section '%s'"%(option, section), file=sys.stderr)
            return 1
        try:
            self.config.remove_option(section, option)
            self.write_config()
        except:
            return 1
        return 0

    def get(self):
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l

        if not self.config.has_section(section):
            self.config.add_section(section)

        if self.config.has_option(section, option):
            print(self.config.get(section, option))
            return 0
        else:
            if self.options.param in deprecated_keywords:
                newkw = deprecated_keywords[self.options.param]
                if self.config.has_option(section, newkw):
                    print(self.config.get(section, newkw))
                    return 0
            if self.options.param in reverse_deprecated_keywords:
                for oldkw in reverse_deprecated_keywords[self.options.param]:
                    if self.config.has_option(section, oldkw):
                        print(self.config.get(section, oldkw))
                        return 0
            print("option '%s' not found in section '%s'"%(option, section), file=sys.stderr)
            return 1

    def set(self):
        if self.options.param is None:
            print("no parameter. set --param", file=sys.stderr)
            return 1
        if self.options.value is None:
            print("no value. set --value", file=sys.stderr)
            return 1
        l = self.options.param.split('.')
        if len(l) != 2:
            print("malformed parameter. format as 'section.key'", file=sys.stderr)
            return 1
        section, option = l
        if not self.config.has_section(section):
            try:
                self.config.add_section(section)
            except ValueError as e:
                print(e)
                return 1
        self.config.set(section, option, self.options.value)
        try:
            self.write_config()
        except:
            return 1
        return 0

    def register(self):
        if self.options.user is None:
            u = self.collector.call('register_node')
            if u is None:
                print("failed to obtain a registration number", file=sys.stderr)
                return 1
            elif isinstance(u, dict) and "ret" in u and u["ret"] != 0:
                print("failed to obtain a registration number", file=sys.stderr)
                try:
                    print(u["msg"])
                except:
                    pass
                return 1
            elif isinstance(u, list):
                print(u[0], file=sys.stderr)
                return 1
        else:
            try:
                data = self.collector_rest_post("/register", {
                  "nodename": rcEnv.nodename,
                  "app": self.options.app
                })
            except Exception as e:
                print(e, file=sys.stderr)
                return 1
            if "error" in data:
                print(data["error"], file=sys.stderr)
                return 1
            u = data["data"]["uuid"]
        try:
            if not self.config.has_section('node'):
                self.config.add_section('node')
            self.config.set('node', 'uuid', u)
            self.write_config()
        except:
            print("failed to write registration number: %s"%u, file=sys.stderr)
            return 1
        print("registered")
        rcEnv.uuid = u
        self.pushasset()
        self.pushdisks()
        self.pushpkg()
        self.pushpatch()
        self.sysreport()
        self.checks()
        return 0

    def service_action_worker(self, s, **kwargs):
        try:
            r = s.action(**kwargs)
        except s.exMonitorAction:
            self.close()
            sys.exit(self.ex_monitor_action_exit_code)
        except:
            self.close()
            sys.exit(1)
        self.close()
        sys.exit(r)

    def devlist(self, tree=None):
        if tree is None:
            try:
                m = __import__("rcDevTree"+rcEnv.sysname)
            except ImportError:
                return
            tree = m.DevTree()
            tree.load()
        l = []
        for dev in tree.get_top_devs():
            if len(dev.devpath) > 0:
                l.append(dev.devpath[0])
        return l

    def updatecomp(self):
        if self.config.has_option('node', 'repocomp'):
            pkg_name = self.config.get('node', 'repocomp').strip('/') + "/current"
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + "/compliance/current"
        else:
            if self.options.cron:
                return 0
            print("node.repo or node.repocomp must be set in node.conf", file=sys.stderr)
            return 1
        import tempfile
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        print("get %s (%s)"%(pkg_name, tmpf))
        try:
            self.urlretrieve(pkg_name, tmpf)
        except IOError as e:
            print("download failed", ":", e, file=sys.stderr)
            try:
                os.unlink(tmpf)
            except:
                pass
            if self.options.cron:
                return 0
            return 1
        tmpp = os.path.join(rcEnv.pathtmp, 'compliance')
        backp = os.path.join(rcEnv.pathtmp, 'compliance.bck')
        compp = os.path.join(rcEnv.pathvar, 'compliance')
        if not os.path.exists(compp):
            os.makedirs(compp, 0o755)
        import shutil
        try:
            shutil.rmtree(backp)
        except:
            pass
        print("extract compliance in", rcEnv.pathtmp)
        import tarfile
        tar = tarfile.open(f.name)
        os.chdir(rcEnv.pathtmp)
        try:
            tar.extractall()
            tar.close()
        except:
            try:
                os.unlink(tmpf)
            except:
                pass
            print("failed to unpack", file=sys.stderr)
            return 1
        try:
            os.unlink(tmpf)
        except:
            pass
        print("install new compliance")
        shutil.move(compp, backp)
        shutil.move(tmpp, compp)

    def updatepkg(self):
        if not os.path.exists(os.path.join(rcEnv.pathlib, 'rcUpdatePkg'+rcEnv.sysname+'.py')):
            print("updatepkg not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        m = __import__('rcUpdatePkg'+rcEnv.sysname)
        if self.config.has_option('node', 'repopkg'):
            pkg_name = self.config.get('node', 'repopkg').strip('/') + "/" + m.repo_subdir + '/current'
        elif self.config.has_option('node', 'repo'):
            pkg_name = self.config.get('node', 'repo').strip('/') + "/packages/" + m.repo_subdir + '/current'
        else:
            print("node.repo or node.repopkg must be set in node.conf", file=sys.stderr)
            return 1
        import tempfile
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        print("get %s (%s)"%(pkg_name, tmpf))
        try:
            self.urlretrieve(pkg_name, tmpf)
        except IOError as e:
            print("download failed", ":", e, file=sys.stderr)
            try:
                os.unlink(tmpf)
            except:
                pass
            return 1
        print("updating opensvc")
        m.update(tmpf)
        print("clean up")
        try:
            os.unlink(tmpf)
        except:
            pass
        return 0

    def provision(self):
        self.provision_resource = []
        for rs in self.options.resource:
            try:
                d = json.loads(rs)
            except:
                print("JSON read error: %s", rs, file=sys.stderr)
                return 1
            if 'rtype' not in d:
                print("'rtype' key must be set in resource dictionary: %s", rs, file=sys.stderr)
                return 1

            rtype = d['rtype']
            if len(rtype) < 2:
                print("invalid 'rtype' value: %s", rs, file=sys.stderr)
                return 1
            rtype = rtype[0].upper() + rtype[1:].lower()

            if 'type' in d:
                rtype +=  d['type'][0].upper() + d['type'][1:].lower()
            modname = 'prov' + rtype
            try:
                m = __import__(modname)
            except ImportError:
                print("provisioning is not available for resource type:", d['rtype'], "(%s)"%modname, file=sys.stderr)
                return 1
            if not hasattr(m, "d_provisioner"):
                print("provisioning with nodemgr is not available for this resource type:", d['rtype'], file=sys.stderr)
                return 1

            self.provision_resource.append((m, d))

        for o, d in self.provision_resource:
            getattr(o, "d_provisioner")(d)

    def get_ruser(self, node):
        default = "root"
        if not self.config.has_option('node', "ruser"):
            return default
        h = {}
        s = self.config.get('node', 'ruser').split()
        for e in s:
            l = e.split("@")
            if len(l) == 1:
                default = e
            elif len(l) == 2:
                _ruser, _node = l
                h[_node] = _ruser
            else:
                continue
        if node in h:
            return h[node]
        return default

    def dequeue_actions(self):
        if self.skip_action("dequeue_actions"):
            return
        self.task_dequeue_actions()

    @scheduler_fork
    def task_dequeue_actions(self):
        actions = self.collector.call('collector_get_action_queue')
        if actions is None:
            return "unable to fetch actions scheduled by the collector"
        import re
        regex = re.compile("\x1b\[([0-9]{1,3}(;[0-9]{1,3})*)?[m|K|G]", re.UNICODE)
        data = []
        for action in actions:
            ret, out, err = self.dequeue_action(action)
            out = regex.sub('', out)
            err = regex.sub('', err)
            data.append((action.get('id'), ret, out, err))
        if len(actions) > 0:
            self.collector.call('collector_update_action_queue', data)

    def dequeue_action(self, action):
        if action.get("svcname") is None or action.get("svcname") == "":
            cmd = [rcEnv.nodemgr]
        else:
            cmd = [rcEnv.svcmgr, "-s", action.get("svcname")]
        import shlex
        from rcUtilities import justcall
        cmd += shlex.split(action.get("command", ""))
        print("dequeue action %s" % " ".join(cmd))
        out, err, ret = justcall(cmd)
        return ret, out, err

    def rotate_root_pw(self):
        pw = self.genpw()

        from collector import Collector
        o = Collector(self.options, self)
        try:
            data = getattr(o, 'rotate_root_pw')(pw)
        except Exception as e:
            print("unexpected error sending the new password to the collector (%s). Abording password change."%str(e), file=sys.stderr)
            return 1

        try:
            rc = __import__('rcPasswd'+rcEnv.sysname)
        except ImportError:
            print("not implemented")
            return 1
        except Exception as e:
            print(e)
            return 1
        r = rc.change_root_pw(pw)
        if r == 0:
            print("root password changed")
        else:
            print("failed to change root password")
        return r

    def genpw(self):
        import string
        chars = string.letters + string.digits + r'+/'
        assert 256 % len(chars) == 0  # non-biased later modulo
        PWD_LEN = 16
        return ''.join(chars[ord(c) % len(chars)] for c in os.urandom(PWD_LEN))

    def scanscsi(self):
        try:
            m = __import__("rcDiskInfo"+rcEnv.sysname)
        except ImportError:
            print("scanscsi is not supported on", rcEnv.sysname, file=sys.stderr)
            return 1
        o = m.diskInfo()
        if not hasattr(o, 'scanscsi'):
            print("scanscsi is not implemented on", rcEnv.sysname, file=sys.stderr)
            return 1
        return o.scanscsi()

    def discover(self):
        self.cloud_init()

    def cloud_init(self):
        r = 0
        for s in self.config.sections():
            try:
                self.cloud_init_section(s)
            except ex.excInitError as e:
                print(str(e), file=sys.stderr)
                r |= 1
        return r

    def cloud_get(self, s):
        if not s.startswith("cloud"):
            return

        if not s.startswith("cloud#"):
            raise ex.excInitError("cloud sections must have a unique name in the form '[cloud#n] in %s"%rcEnv.nodeconf)

        if hasattr(self, "clouds") and s in self.clouds:
            return self.clouds[s]

        try:
            cloud_type = self.config.get(s, 'type')
        except:
            raise ex.excInitError("type option is mandatory in cloud section in %s"%rcEnv.nodeconf)

        # noop if already loaded
        self.load_auth_config()
        try:
            auth_dict = {}
            for key, val in self.auth_config.items(s):
                auth_dict[key] = val
        except:
            raise ex.excInitError("%s must have a '%s' section"%(rcEnv.authconf, s))

        if len(cloud_type) == 0:
            raise ex.excInitError("invalid cloud type in %s"%rcEnv.nodeconf)

        mod_name = "rcCloud" + cloud_type[0].upper() + cloud_type[1:].lower()

        try:
            m = __import__(mod_name)
        except ImportError:
            raise ex.excInitError("cloud type '%s' is not supported"%cloud_type)

        if not hasattr(self, "clouds"):
            self.clouds = {}
        c = m.Cloud(s, auth_dict)
        self.clouds[s] = c
        return c

    def cloud_init_section(self, s):
        c = self.cloud_get(s)

        if c is None:
            return

        cloud_id = c.cloud_id()
        svcnames = c.list_svcnames()

        self.cloud_purge_services(cloud_id, map(lambda x: x[1], svcnames))

        for vmname, svcname in svcnames:
            self.cloud_init_service(c, vmname, svcname)

    def cloud_purge_services(self, suffix, svcnames):
        import glob
        cfs = glob.glob(os.path.join(rcEnv.pathetc, '*.conf'))
        for cf in cfs:
            svcname = os.path.basename(cf)[:-5]
            if svcname.endswith(suffix) and svcname not in svcnames:
                print("purge_service(svcname)", svcname)

    def cloud_init_service(self, c, vmname, svcname):
        import glob
        cfs = glob.glob(os.path.join(rcEnv.pathetc, '*.conf'))
        cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
        if cf in cfs:
            print(svcname, "is already defined")
            return
        print("initialize", svcname)

        defaults = {
          'app': c.app_id(svcname),
          'mode': c.mode,
          'nodes': rcEnv.nodename,
          'service_type': 'TST',
          'vm_name': vmname,
          'cloud_id': c.cid,
        }
        config = RawConfigParser(defaults)

        try:
            fp = open(cf, 'w')
            config.write(fp)
            fp.close()
        except:
            print("failed to write %s"%cf, file=sys.stderr)
            raise Exception()

        b = cf[:-5]
        d = b + '.dir'
        s = b + '.d'
        x = rcEnv.svcmgr
        try:
            os.makedirs(d)
        except:
            pass
        try:
            os.symlink(d, s)
        except:
            pass
        try:
            os.symlink(x, b)
        except:
            pass

    def can_parallel(self, action):
        if self.options.parallel and action not in actions_no_parallel:
            return True
        return False

    def action_need_aggregate(self, action):
        if action.startswith("print_"):
            return True
        if action.startswith("json_"):
            return True
        if action.startswith("collector_"):
            return True
        if "_json_" in action:
            return True
        return False

    @formatter
    def print_aggregate(self, data):
        return data

    def do_svcs_action(self, action, rid=None, tags=None, subsets=None):
        err = 0
        outs = {}
        need_aggregate = self.action_need_aggregate(action)

        # generic cache janitoring
        rcUtilities.purge_cache()
        self.log.debug("session uuid: %s" % rcEnv.session_uuid)

        if action in actions_no_multiple_services and len(self.svcs) > 1:
            print("action '%s' is not allowed on multiple services" % action, file=sys.stderr)
            return 1
        if self.can_parallel(action):
            from multiprocessing import Process
            if rcEnv.sysname == "Windows":
                from multiprocessing import set_executable
                set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
            p = {}
            svcs = {}
        for s in self.svcs:
            if self.can_parallel(action):
                d = {
                  'action': action,
                  'rid': rid,
                  'tags': tags,
                  'subsets': subsets,
                  'waitlock': self.options.waitlock
                }
                svcs[s.svcname] = s
                p[s.svcname] = Process(target=self.service_action_worker,
                                       name='worker_'+s.svcname,
                                       args=[s],
                                       kwargs=d)
                p[s.svcname].start()
            else:
                try:
                    ret = s.action(action, rid=rid, tags=tags, subsets=subsets, waitlock=self.options.waitlock)
                    if need_aggregate:
                        if ret is not None:
                            outs[s.svcname] = ret
                    else:
                        err += ret
                except s.exMonitorAction:
                    s.action('toc')
                except ex.excSignal:
                    break

        if self.can_parallel(action):
            for svcname in p:
                p[svcname].join()
                r = p[svcname].exitcode
                if r == self.ex_monitor_action_exit_code:
                    svcs[svcname].action('toc')
                elif r > 0:
                    # r is negative when p[svcname] is killed by signal.
                    # in this case, we don't want to decrement the err counter.
                    err += r

        if need_aggregate:
            if len(self.svcs) == 1:
                svcname = self.svcs[0].svcname
                if svcname not in outs:
                    return
                return self.print_aggregate(outs[self.svcs[0].svcname])
            else:
                return self.print_aggregate(outs)

        return err

    def collector_cli(self):
        data = {}

        if os.getuid() == 0:
            if not hasattr(self.options, "user") or self.options.user is None:
                user, password = self.collector_auth_node()
                data["user"] = user
                data["password"] = password
            if not hasattr(self.options, "api") or self.options.api is None:
                data["api"] = rcEnv.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        from rcCollectorCli import Cli
        cli = Cli(**data)
        return cli.run()

    def collector_api(self, svcname=None):
        if hasattr(self, "collector_api_cache"):
            return self.collector_api_cache
        data = {}
        if not hasattr(self.options, "user") or self.options.user is None:
            username, password = self.collector_auth_node()
            if svcname:
                username = svcname+"@"+username
        else:
            username, password = self.collector_auth_user()
        data["username"] = username
        data["password"] = password
        data["url"] = rcEnv.dbopensvc.replace("/feed/default/call/xmlrpc", "/init/rest/api")
        self.collector_api_cache = data
        return self.collector_api_cache

    def collector_auth_node(self):
        import platform
        sysname, username, x, x, machine, x = platform.uname()
        config = RawConfigParser({})
        config.read(os.path.join(rcEnv.pathetc, "node.conf"))
        password = config.get("node", "uuid")
        return username, password

    def collector_auth_user(self):
        username = self.options.user
        import getpass
        try:
            password = getpass.getpass()
        except EOFError:
            raise KeyboardInterrupt()
        return username, password

    def collector_url(self):
        api = self.collector_api()
        s = "%s:%s@" % (api["username"], api["password"])
        url = api["url"].replace("https://", "https://"+s)
        url = url.replace("http://", "http://"+s)
        return url

    def collector_request(self, path, svcname=None):
        api = self.collector_api(svcname=svcname)
        url = api["url"]
        request = Request(url+path)
        auth_string = '%s:%s' % (api["username"], api["password"])
        if sys.version_info[0] >= 3:
            base64string = base64.encodestring(auth_string.encode()).decode()
        else:
            base64string = base64.encodestring(auth_string)
        base64string = base64string.replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        return request

    def collector_rest_get(self, path, svcname=None):
        return self.collector_rest_request(path, svcname=svcname)

    def collector_rest_post(self, path, data=None, svcname=None):
        return self.collector_rest_request(path, data, svcname=svcname)

    def urlretrieve(self, url, fpath):
        request = Request(url)
        kwargs = {}
        try:
            import ssl
            kwargs["context"] = ssl._create_unverified_context()
        except:
            pass
        f = urlopen(request, **kwargs)
        with open(fpath, 'wb') as df:
            for chunk in iter(lambda: f.read(4096), b""):
                df.write(chunk)

    def collector_rest_request(self, path, data=None, svcname=None):
        api = self.collector_api(svcname=svcname)
        request = self.collector_request(path)
        if not api["url"].startswith("https"):
            raise ex.excError("refuse to submit auth tokens through a non-encrypted transport")
        if data:
            import urllib
            request.add_data(urlencode(data))
        kwargs = {}
        try:
            import ssl
            kwargs["context"] = ssl._create_unverified_context()
        except:
            pass
        try:
            f = urlopen(request, **kwargs)
        except HTTPError as e:
            try:
                err = json.loads(e.read())["error"]
                e = ex.excError(err)
            except:
                pass
            raise e
        import json
        data = json.loads(f.read().decode("utf-8"))
        f.close()
        return data

    def collector_rest_get_to_file(self, path, fpath):
        api = self.collector_api()
        request = self.collector_request(path)
        if api["url"].startswith("https"):
            raise ex.excError("refuse to submit auth tokens through a non-encrypted transport")
        kwargs = {}
        try:
            import ssl
            kwargs["context"] = ssl._create_unverified_context()
        except:
            pass
        try:
            f = urlopen(request, **kwargs)
        except HTTPError as e:
            try:
                err = json.loads(e.read())["error"]
                e = ex.excError(err)
            except:
                pass
            raise e
        with open(fpath, 'wb') as df:
            for chunk in iter(lambda: f.read(4096), b""):
                df.write(chunk)
        f.close()

    def install_service_cf_from_template(self, svcname, template):
        cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
        data = self.collector_rest_get("/provisioning_templates/"+template+"?props=tpl_definition&meta=0")
        if "error" in data:
            raise ex.excError(data["error"])
        if len(data["data"]) == 0:
            raise ex.excError("service not found on the collector")
        if len(data["data"][0]["tpl_definition"]) == 0:
            raise ex.excError("service has an empty configuration")
        with open(cf, "w") as f:
            f.write(data["data"][0]["tpl_definition"].replace("\\n", "\n").replace("\\t", "\t"))
        self.install_service_cf_from_file(svcname, cf)

    def install_service_cf_from_uri(self, svcname, cf):
        import tempfile
        f = tempfile.NamedTemporaryFile()
        tmpf = f.name
        f.close()
        print("get %s (%s)"%(cf, tmpf))
        try:
            self.urlretrieve(cf, tmpf)
        except IOError as e:
            print("download failed", ":", e, file=sys.stderr)
            try:
                os.unlink(tmpf)
            except:
                pass
        self.install_service_cf_from_file(svcname, tmpf)

    def install_service_cf_from_file(self, svcname, cf):
        if not os.path.exists(cf):
            raise ex.excError("%s does not exists" % cf)

        import shutil

        # install the configuration file in etc/
        src_cf = os.path.realpath(cf)
        dst_cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
        if dst_cf != src_cf:
            shutil.copy2(src_cf, dst_cf)

    def install_service(self, svcname, cf=None, template=None):
        if type(svcname) == list:
            if len(svcname) != 1:
                raise ex.excError("only one service must be specified")
            svcname = svcname[0]

        if cf is None and template is None:
            return

        if cf is not None and template is not None:
            raise ex.excError("--config and --template can't both be specified")

        if template is not None:
            if "://" in template:
                self.install_service_cf_from_uri(svcname, template)
            elif os.path.exists(template):
                self.install_service_cf_from_file(svcname, template)
            else:
                self.install_service_cf_from_template(svcname, template)
        else:
            if "://" in cf:
                self.install_service_cf_from_uri(svcname, cf)
            else:
                self.install_service_cf_from_file(svcname, cf)

        # install .dir
        d = os.path.join(rcEnv.pathetc, svcname+'.dir')
        if not os.path.exists(d):
            os.makedirs(d)

        if rcEnv.sysname == 'Windows':
            return

        # install .d
        ld = os.path.join(rcEnv.pathetc, svcname+'.d')
        if not os.path.exists(ld):
            os.symlink(d, ld)
        elif not os.path.exists(ld+os.sep):
            # repair broken symlink
            os.unlink(ld)
            os.symlink(d, ld)

        # install svcmgr link
        ls = os.path.join(rcEnv.pathetc, svcname)
        s = os.path.join(rcEnv.pathbin, 'svcmgr')
        if not os.path.exists(ls):
            os.symlink(s, ls)
        elif os.path.realpath(s) != os.path.realpath(ls):
            os.unlink(ls)
            os.symlink(s, ls)

    def install_service_files(self, svcname):
        if rcEnv.sysname == 'Windows':
            return

        # install svcmgr link
        ls = os.path.join(rcEnv.pathetc, svcname)
        s = rcEnv.svcmgr
        if not os.path.exists(ls):
            os.symlink(s, ls)
        elif os.path.realpath(s) != os.path.realpath(ls):
            os.unlink(ls)
            os.symlink(s, ls)

    def pull_services(self, svcnames):
        for svcname in svcnames:
            self.pull_service(svcname)

    def pull_service(self, svcname):
        cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
        data = self.collector_rest_get("/services/"+svcname+"?props=svc_config&meta=0")
        if "error" in data:
            self.log.error(data["error"])
            return 1
        if len(data["data"]) == 0:
            self.log.error("service not found on the collector")
            return 1
        if len(data["data"][0]["svc_config"]) == 0:
            self.log.error("service has an empty configuration")
            return 1
        with open(cf, "w") as f:
            f.write(data["data"][0]["svc_config"].replace("\\n", "\n").replace("\\t", "\t"))
        self.log.info("%s pulled" % cf)
        self.install_service_files(svcname)

    def set_rlimit(self):
        try:
            n = 64 * len(self.svcs)
        except:
            n = 4096
        try:
            import resource
            (vs, vg) = resource.getrlimit(resource.RLIMIT_NOFILE)
            if vs < n:
                self.log.debug("raise nofile resource from %d limit to %d" % (vs, n))
                resource.setrlimit(resource.RLIMIT_NOFILE, (n, vg))
        except:
            pass

    def schedulers(self):
        self.scheduler()

        self.build_services()
        for s in self.svcs:
            s.scheduler()

    def logs(self):
        if not os.path.exists(rcEnv.logfile):
            return
        from rcColor import color, colorize
        class shared:
            skip = False
        def c(line):
            line = line.rstrip("\n")
            l = line.split(" - ")

            if len(l) < 3 or l[2] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
                # this is a log line continuation (command output for ex.)
                if shared.skip:
                    return
                else:
                    return line

            if not self.options.debug and l[2] == "DEBUG":
                shared.skip = True
                return
            else:
                shared.skip = False

            if len(l[1]) > rcLogger.namelen:
                l[1] = "*"+l[1][-(rcLogger.namelen-1):]
            l[1] = rcLogger.namefmt % l[1]
            l[1] = colorize(l[1], color.BOLD)
            l[2] = "%-7s" % l[2]
            l[2] = l[2].replace("ERROR", colorize("ERROR", color.RED))
            l[2] = l[2].replace("WARNING", colorize("WARNING", color.BROWN))
            l[2] = l[2].replace("INFO", colorize("INFO", color.LIGHTBLUE))
            return " ".join(l)

        try:
            with open(rcEnv.logfile, "r") as f:
                for line in f.readlines():
                    s = c(line)
                    if s:
                         print(s)
        except BrokenPipeError:
            try:
                sys.stdout = os.fdopen(1)
            except:
                pass

    def _print_config(self, cf):
        from rcColor import colorize, color
        import re
        def c(line):
            line = line.rstrip("\n")
            if re.match(r'\[.+\]', line):
                return colorize(line, color.BROWN)
            line = re.sub("({.+})", colorize(r"\1", color.GREEN), line)
            line = re.sub("^(\s*\w+\s*)=", colorize(r"\1", color.LIGHTBLUE)+"=", line)
            line = re.sub("^(\s*\w+)(@\w+\s*)=", colorize(r"\1", color.LIGHTBLUE)+colorize(r"\2", color.RED)+"=", line)
            return line
        try:
            with open(cf, 'r') as f:
                for line in f.readlines():
                    print(c(line))
        except Exception as e:
            raise ex.excError(e)

    def print_config(self):
        self._print_config(rcEnv.nodeconf)

    def print_authconfig(self):
        self._print_config(rcEnv.authconf)

if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
