#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
# To change this template, choose Tools | Templates
# and open the template in the editor.

from resources import Resource, ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv
import rcExceptions as ex
import xmlrpcClient
import os
import signal
import lock

def signal_handler(signum, frame):
    raise ex.excSignal

def fork_dblogger(self, action, begin, end, actionlogfile):
    kwargs = {'self': self,
              'action': action,
              'begin': begin,
              'end': end,
              'actionlogfile': actionlogfile}
    fork(dblogger, kwargs)

def dblogger(self, action, begin, end, actionlogfile):
    import logging
    for rs in self.resSets:
        for r in rs.resources:
            r.log.setLevel(logging.CRITICAL)

    xmlrpcClient.end_action(self, action, begin, end, actionlogfile)
    gs = self.group_status()
    xmlrpcClient.svcmon_update(self, self.group_status())
    os.unlink(actionlogfile)

def fork(fn, kwargs):
    try:
        if os.fork() > 0:
            """ return to parent execution
            """
            return
    except:
        """ no dblogging will be done. too bad.
        """
        return

    """ separate the son from the father
    """
    os.chdir('/')
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except:
        os._exit(1)

    fn(**kwargs)
    os._exit(0)


class Svc(Resource, Freezer):
    """Service class define a Service Resource
    It contain list of ResourceSet where each ResourceSets contain same resource
    type
    """

    def __init__(self, svcname=None, type="hosted", optional=False, disabled=False, tags=set([])):
        """usage : aSvc=Svc(type)"""
        self.svcname = svcname
        self.vmname = ""
        self.hostid = rcEnv.nodename
        self.resSets = []
        self.type2resSets = {}
        self.disks = set([])
        self.force = False
        self.status_types = ["container.hpvm",
                             "container.kvm",
                             "container.xen",
                             "container.lxc",
                             "container.zone",
                             "container.jail",
                             "container.ldom",
                             "disk.drbd",
                             "disk.loop",
                             "disk.scsireserv",
                             "disk.vg",
                             "disk.zpool",
                             "fs",
                             "ip",
                             "sync.rsync",
                             "sync.symclone",
                             "sync.dds",
                             "sync.zfs",
                             "sync.netapp",
                             "app"]
        Resource.__init__(self, type=type, optional=optional, disabled=disabled, tags=tags)
        Freezer.__init__(self, svcname)
        self.scsirelease = self.prstop
        self.scsireserv = self.prstart
        self.scsicheckreserv = self.prstatus
        self.runmethod = []
        self.resources_by_id = {}
        self.rset_status_cache = None
        self.print_status_fmt = "%-8s %-8s %s"
        self.presync_done = False
        self.lockfd = None

    def __cmp__(self, other):
        """order by service name
        """
        return cmp(self.svcname, other.svcname)

    def __iadd__(self, r):
        """svc+=aResourceSet
        svc+=aResource
        """
        if r.type in self.type2resSets:
            self.type2resSets[r.type] += r

        elif isinstance(r, ResourceSet):
            self.resSets.append(r)
            self.type2resSets[r.type] = r

        elif isinstance(r, Resource):
            R = ResourceSet(r.type, [r])
            self.__iadd__(R)

        else:
            # Error
            pass

        self.resources_by_id[r.rid] = r
        if r.rid in rcEnv.vt_supported:
            self.resources_by_id["container"] = r
        r.svc = self
        import logging
        r.log = logging.getLogger(str(self.svcname+'.'+str(r.rid)).upper())

        return self

    def svclock(self, timeout=30, delay=5):
        lockfile = os.path.join(rcEnv.pathlock, self.svcname)
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            self.log.error("timed out waiting for lock")
            raise ex.excError
        except lock.lockNoLockFile:
            self.log.error("lock_nowait: set the 'lockfile' param")
            raise ex.excError
        except lock.lockCreateError:
            self.log.error("can not create lock file %s"%lockfile)
            raise ex.excError
        except lock.lockAcquire as e:
            self.log.warn("another action is currently running (pid=%s)"%e.pid)
            raise ex.excError
        except:
            self.log.error("unexpected locking error")
            import traceback
            traceback.print_exc()
            raise ex.excError
        if lockfd is not None:
            self.lockfd = lockfd

    def svcunlock(self):
        lock.unlock(self.lockfd)

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_res_sets(self, type):
         return [ r for r in self.resSets if r.type == type ]

    def has_res_set(self, type):
        if len(get_res_sets(type)) > 0: return True
        else: return False

    def all_set_action(self, action=None, tags=set([])):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.resSets, action=action, tags=tags)

    def sub_set_action(self, type=None, action=None, tags=set([])):
        """Call action on each member of the subset of specified type
        """
        self.set_action(self.get_res_sets(type), action=action, tags=tags)

    def set_action(self, sets=[], action=None, tags=set([])):
        """ TODO: r.is_optional() not doing what's expected if r is a rset
        """
        for r in sets:
            try:
                r.action(action, tags=tags)
            except ex.excError:
                if r.is_optional():
                    pass
                else:
                    raise ex.excError
            except ex.excAbortAction:
                if r.is_optional():
                    pass
                else:
                    break

    def __str__(self):
        output="Service %s available resources:" % (Resource.__str__(self))
        for k in self.type2resSets.keys() : output += " %s" % k
        output+="\n"
        for r in self.resSets:  output+= "  [%s]" % (r.__str__())
        return output

    def status(self):
        """aggregate status a service
        """
        ss = rcStatus.Status()
        for t in self.status_types:
            for r in self.get_res_sets(t):
                if "sync." not in r.type:
                     ss += r.status()
                else:
                    """ sync are expected to be up
                    """
                    s = r.status()
                    if s == rcStatus.UP:
                        ss += rcStatus.UNDEF
                    elif s in [rcStatus.NA, rcStatus.UNDEF, rcStatus.TODO]:
                        ss += s
                    else:
                        ss += rcStatus.WARN
        if ss.status == rcStatus.STDBY_UP_WITH_UP:
            ss.status = rcStatus.UP
        elif ss.status == rcStatus.STDBY_UP_WITH_DOWN:
            ss.status = rcStatus.STDBY_UP
        return ss.status

    def print_status(self):
        """print each resource status for a service
        """
        print self.print_status_fmt%("rid", "status", "label")
        print self.print_status_fmt%("---", "------", "-----")

        s = rcStatus.Status(rcStatus.UNDEF)
        for t in self.status_types:
            for rs in self.get_res_sets(t):
                for r in rs.resources:
                    r.print_status()
        s = self.group_status()['overall']

        print self.print_status_fmt%("overall",
                                     str(s),
                                     ""),

    def get_rset_status(self, groups):
        self.setup_environ()
        rset_status = {}
        for t in self.status_types:
            g = t.split('.')[0]
            if g not in groups:
                continue
            for rs in self.get_res_sets(t):
                rset_status[rs.type] = rs.status()
        return rset_status

    def group_status(self,
                     groups=set(["container", "ip", "disk", "fs", "sync", "app"]),
                     excluded_groups=set([])):
        """print each resource status for a service
        """
        status = {}
        groups = groups.copy() - excluded_groups
        rset_status = self.get_rset_status(groups)
        moregroups = groups | set(["overall"])
        for group in moregroups:
            status[group] = rcStatus.Status(rcStatus.NA)
        for t in self.status_types:
            group = t.split('.')[0]
            if group not in groups:
                continue
            for r in self.get_res_sets(t):
                s = rset_status[r.type]
                status[group] += s
                if group != "sync":
                    status["overall"] += s
                else:
                    """ sync are expected to be up
                    """
                    if s == rcStatus.UP:
                        status["overall"] += rcStatus.UNDEF
                    elif s in [rcStatus.NA, rcStatus.UNDEF, rcStatus.TODO]:
                        status["overall"] += s
                    else:
                        status["overall"] += rcStatus.WARN
        if status["overall"].status == rcStatus.STDBY_UP_WITH_UP:
            status["overall"].status = rcStatus.UP
        elif status["overall"].status == rcStatus.STDBY_UP_WITH_DOWN:
            status["overall"].status = rcStatus.STDBY_UP
        self.group_status_cache = status
        return status

    def disklist(self):
        """List all disks held by all resources of this service
        """
        disks = set()
        for rs in self.resSets:
            for r in rs.resources:
                if r.is_disabled():
                    continue
                disks |= r.disklist()
        self.log.debug("found disks %s held by service" % disks)
        return disks

    def start(self):
        self.startip()
        self.mount()
        self.startcontainer()
        self.startapp()

    def stop(self):
        try:
            self.stopapp()
        except ex.excError:
            pass
        self.stopcontainer()
        self.umount()
        self.stopip()

    def startdrbd(self):
        self.sub_set_action("disk.drbd", "start")

    def stopdrbd(self):
        self.sub_set_action("disk.drbd", "stop")

    def startloop(self):
        self.sub_set_action("disk.loop", "start")

    def stoploop(self):
        self.sub_set_action("disk.loop", "stop")

    def startvg(self):
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.vg", "start")

    def startpool(self):
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.zpool", "start")

    def stoppool(self):
        self.sub_set_action("disk.zpool", "stop")
        self.sub_set_action("disk.scsireserv", "stop")

    def startdisk(self):
        self.sub_set_action("sync.netapp", "start")
        self.sub_set_action("sync.symclone", "start")
        self.sub_set_action("sync.dds", "start")
        self.sub_set_action("disk.loop", "start")
        self.sub_set_action("disk.scsireserv", "start")
        self.sub_set_action("disk.drbd", "start", tags=set(['prevg']))
        self.sub_set_action("disk.zpool", "start")
        self.sub_set_action("disk.vg", "start")
        self.sub_set_action("disk.drbd", "start", tags=set(['postvg']))

    def stopdisk(self):
        self.sub_set_action("disk.drbd", "stop", tags=set(['postvg']))
        self.sub_set_action("disk.vg", "stop")
        self.sub_set_action("disk.zpool", "stop")
        self.sub_set_action("disk.drbd", "stop", tags=set(['prevg']))
        self.sub_set_action("disk.scsireserv", "stop")
        self.sub_set_action("disk.loop", "stop")

    def startip(self):
        self.sub_set_action("ip", "start")

    def stopip(self):
        self.sub_set_action("ip", "stop")

    def mount(self):
        self.startdisk()
        self.sub_set_action("fs", "start")

    def umount(self):
        self.sub_set_action("fs", "stop")
        self.stopdisk()

    def startcontainer(self):
        self.sub_set_action("container.lxc", "start")
        self.sub_set_action("container.jail", "start")
        self.sub_set_action("container.kvm", "start")
        self.sub_set_action("container.xen", "start")
        self.sub_set_action("container.hpvm", "start")
        self.sub_set_action("container.ldom", "start")

    def stopcontainer(self):
        self.sub_set_action("container.ldom", "stop")
        self.sub_set_action("container.hpvm", "stop")
        self.sub_set_action("container.xen", "stop")
        self.sub_set_action("container.kvm", "stop")
        self.sub_set_action("container.jail", "stop")
        self.sub_set_action("container.lxc", "stop")

    def startapp(self):
        self.sub_set_action("app", "start")

    def stopapp(self):
        self.sub_set_action("app", "stop")

    def prstop(self):
        self.sub_set_action("disk.scsireserv", "scsirelease")

    def prstart(self):
        self.sub_set_action("disk.scsireserv", "scsireserv")

    def prstatus(self):
        self.sub_set_action("disk.scsireserv", "scsicheckreserv")

    def startstandby(self):
        self.sub_set_action("ip", "startstandby")
        self.sub_set_action("disk.loop", "startstandby")
        self.sub_set_action("disk.scsireserv", "startstandby")
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['prevg']))
        self.sub_set_action("disk.vg", "startstandby")
        self.sub_set_action("disk.zpool", "startstandby")
        self.sub_set_action("disk.drbd", "startstandby", tags=set(['postvg']))
        self.sub_set_action("fs", "startstandby")
        self.sub_set_action("app", "startstandby")

    def postsync(self):
        """ action triggered by a remote master node after
            syncnodes and syncdrp. Typically make use of files
            received in var/
        """
        self.all_set_action("postsync")

    def remote_postsync(self):
	""" detach the remote exec of postsync because the
            waitlock timeout is long, and we ourselves still
            hold the service lock we want to release early.
	"""
	kwargs = {}
	fork(self._remote_postsync, kwargs)

    def _remote_postsync(self):
        """ action triggered by a remote master node after
            syncnodes and syncdrp. Typically make use of files
            received in var/.
            use a long waitlock timeout to give a chance to
            remote syncs to finish
        """
        for n in self.need_postsync:
            self.remote_action(n, 'postsync', waitlock=3600)

        self.need_postsync = set([])

    def remote_action(self, node, action, waitlock=60):
        rcmd = [os.path.join(rcEnv.pathetc, self.svcname),
                '--waitlock', str(waitlock), action]
        self.log.info("exec '%s' on node %s"%(' '.join(rcmd), node))
        cmd = rcEnv.rsh.split() + [node] + rcmd
        self.call(cmd)

    def presync(self):
        """ prepare files to send to slave nodes in var/.
            Each resource can prepare its own set of files.
        """
        self.need_postsync = set([])
        if self.presync_done:
            return
        self.all_set_action("presync")
        self.presync_done = True

    def syncnodes(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncnodes")
        self.sub_set_action("sync.zfs", "syncnodes")
        self.remote_postsync()

    def syncdrp(self):
        self.presync()
        self.sub_set_action("sync.rsync", "syncdrp")
        self.remote_postsync()

    def syncswap(self):
        self.sub_set_action("sync.netapp", "syncswap")

    def syncresume(self):
        self.sub_set_action("sync.netapp", "syncresume")

    def syncquiesce(self):
        self.sub_set_action("sync.netapp", "syncquiesce")

    def syncresync(self):
        self.sub_set_action("sync.netapp", "syncresync")
        self.sub_set_action("sync.symclone", "syncresync")
        self.sub_set_action("sync.dds", "syncresync")

    def syncbreak(self):
        self.sub_set_action("sync.netapp", "syncbreak")
        self.sub_set_action("sync.symclone", "syncbreak")

    def syncupdate(self):
        self.sub_set_action("sync.netapp", "syncupdate")
        self.sub_set_action("sync.dds", "syncupdate")
        self.sub_set_action("sync.zfs", "syncnodes")

    def syncfullsync(self):
        self.sub_set_action("sync.dds", "syncfullsync")
        self.sub_set_action("sync.zfs", "syncnodes")

    def syncverify(self):
        self.sub_set_action("sync.dds", "syncverify")

    def printsvc(self):
        print str(self)

    def syncall(self):
        try: self.syncnodes()
        except: pass
        try: self.syncdrp()
        except: pass
        try: self.syncupdate()
        except: pass

    def push(self):
        xmlrpcClient.push_all([self])

    def tag_match(self, rtags, keeptags):
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def disable_resources(self, keeprid=[], keeptags=set([])):
        if len(keeprid) > 0:
            ridfilter = True
        else:
            ridfilter = False

        if len(keeptags) > 0:
            tagsfilter = True
        else:
            tagsfilter = False

        if not tagsfilter and not ridfilter:
            return

        for rs in self.resSets:
            for r in rs.resources:
                if ridfilter and r.rid in keeprid:
                    continue
                if tagsfilter and self.tag_match(r.tags, keeptags):
                    continue
                r.disable()

    def setup_environ(self):
        os.environ['OPENSVC_SVCNAME'] = self.svcname

    def action(self, action, rid=[], tags=set([]), waitlock=60):
        if self.frozen() and action not in ['thaw', 'status', 'frozen', 'push', 'print_status']:
            self.log.info("Abort action on frozen service")
            return
        self.setup_environ()
        self.setup_signal_handlers()
        self.disable_resources(keeprid=rid, keeptags=tags)
        if action in ["print_status", "status", "group_status"]:
            self.do_action(action, waitlock=waitlock)
        else:
            self.do_logged_action(action, waitlock=waitlock)

    def do_action(self, action, waitlock=60):
        """Trigger action
        """
        err = 0
        try:
            self.svclock(timeout=waitlock)
        except:
            return 1

        try:
            getattr(self, action)()
        except ex.excError:
            err = 1
        except ex.excSignal:
            self.log.error("interrupted by signal")
            err = 1
        except:
            err = 1
            self.save_exc()

        self.svcunlock()
        return err

    def save_exc(self):
        import traceback
        try:
            import tempfile
            import datetime
            now = str(datetime.datetime.now()).replace(' ', '-')
            f = tempfile.NamedTemporaryFile(dir=rcEnv.pathtmp,
                                            prefix='exc-'+now+'-')
            f.close()
            f = open(f.name, 'w')
            traceback.print_exc(file=f)
            self.log.error("unexpected error. stack saved in %s"%f.name)
            f.close()
        except:
            self.log.error("unexpected error")
            traceback.print_exc()

    def do_logged_action(self, action, waitlock=60):
        from datetime import datetime
        import tempfile
        import logging
        begin = datetime.now()

        """Provision a database entry to store action log later
        """
        xmlrpcClient.begin_action(self, action, begin)

        """Per action logfile to push to database at the end of the action
        """
        f = tempfile.NamedTemporaryFile(delete=False, dir='/var/tmp', prefix=self.svcname+'.'+action)
        actionlogfile = f.name
        f.close()
        log = logging.getLogger()
        actionlogformatter = logging.Formatter("%(asctime)s;%(name)s;%(levelname)s;%(message)s;%(process)d;EOL")
        actionlogfilehandler = logging.FileHandler(actionlogfile)
        actionlogfilehandler.setFormatter(actionlogformatter)
        log.addHandler(actionlogfilehandler)

        err = self.do_action(action, waitlock=waitlock)

        """ Push result and logs to database
        """
        actionlogfilehandler.close()
        log.removeHandler(actionlogfilehandler)
        end = datetime.now()
        fork_dblogger(self, action, begin, end, actionlogfile)
        return err

    def restart(self):
	""" stop then start service
        """
        self.stop()
        self.start()

    def switch(self):
	""" stop then start service
        """
        if not hasattr(self, "destination_node"):
            self.log.error("a destination node must be provided for the switch action")
            raise ex.excError
        if self.destination_node not in self.nodes:
            self.log.error("destination node %s is not in service node list"%self.destination_node)
            raise ex.excError
        self.stop()
	kwargs = {'node': self.destination_node, 'action': 'start'}
	fork(self.remote_action, kwargs)


if __name__ == "__main__" :
    for c in (Svc,) :
        help(c)
    print """s1=Svc("Zone")"""
    s1=Svc("Zone")
    print "s1=",s1
    print """s2=Svc("basic")"""
    s2=Svc("basic")
    print "s2=",s2
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print "s1=",s1
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print """s1+=Resource("fs")"""
    s1+=Resource("fs")
    print """s1+=Resource("fs")"""
    s1+=Resource("fs")
    print "s1=",s1

    print """s1.action("status")"""
    s1.action("status")
