import os
import rcExceptions as ex
import subprocess
import resSync
from rcGlobalEnv import rcEnv
from rcUtilities import justcall

class SyncDcs(resSync.Sync):
    def wait_for_devs_ready(self):
        pass

    def get_active_dcs(self):
        if self.active_dcs is not None:
            return
        for d in self.dcs:
            try:
                self.log.debug("try dcs", d)
                self.dcscmd("get-dcsserver", dcs=d)
                self.active_dcs = d
                self.log.debug("set active dcs", self.active_dcs)
                return
            except:
                pass
        if self.active_dcs is None:
            self.log.error("no usable dcs server")
            raise ex.excError

    def get_auth(self):
        if self.username is not None and \
           self.password is not None:
            return
        self.get_active_manager()
        if self.active_manager is None:
            raise ex.excError("no active manager")
        import ConfigParser
        if not os.path.exists(self.conf):
            raise ex.excError("missing %s"%self.conf)
        self.config = ConfigParser.RawConfigParser()
        self.config.read(self.conf)
        if not self.config.has_section(self.active_manager):
            raise ex.excError("no credentials for manager %s in %s"%(self.active_manager, self.conf))
        if not self.config.has_option(self.active_manager, "username"):
            raise ex.excError("no username set for manager %s in %s"%(self.active_manager, self.conf))
        if not self.config.has_option(self.active_manager, "password"):
            raise ex.excError("no password set for manager %s in %s"%(self.active_manager, self.conf))
        self.username = self.config.get(self.active_manager, "username")
        self.password = self.config.get(self.active_manager, "password")
 
    def dcscmd(self, cmd="", verbose=False, check=True, dcs=None):
        if len(cmd) == 0:
            return

        self.get_active_manager()
        if dcs is None:
            self.get_active_dcs()
            dcs = self.active_dcs
        self.get_auth()
        cmd = self.ssh + [self.active_manager,
               "connect-dcsserver -server %s -username %s -password %s -connection %s ; "%(dcs, self.username, self.password, self.conn)+\
               cmd+\
               " ; disconnect-dcsserver -connection %s"%self.conn]
        if verbose:
            import re
            from copy import copy
            _cmd = copy(cmd)
            _cmd[2] = re.sub(r'password \S+', 'password xxxxx', _cmd[2])
            self.log.info(subprocess.list2cmdline(_cmd))
            ret, out, err = self.call(cmd)
        else:
            ret, out, err = self.call(cmd, errlog=False)
        if check and "ErrorId" in err:
            raise ex.excError("dcscmd command execution error")
        return ret, out, err

    def get_active_manager(self):
        if self.active_manager is not None:
            return
        for manager in self.manager:
            cmd = self.ssh + [manager, 'id']
            out, err, ret = justcall(cmd)
            if ret != 0:
                continue
            self.active_manager = manager
            self.log.debug("set active manager", self.active_manager)
            return
        if self.active_manager is None:
            self.log.error("no usable manager")
            raise ex.excError

    def __init__(self,
                 rid=None,
                 manager=set([]),
                 dcs=set([]),
                 type="sync.dcsunknown",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 internal=False):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type=type,
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              subset=subset,
                              tags=tags)
        self.ssh = rcEnv.rsh.split()
        self.active_dcs = None
        self.active_manager = None
        self.username = None
        self.password = None
        self.dcs = dcs
        self.manager = manager
        self.conf = os.path.join(rcEnv.pathetc, 'auth.conf')

    def on_add(self):
        self.get_conn()

    def get_conn(self):
        from hashlib import md5
        import uuid
        o = md5()
        o.update(uuid.uuid1().hex)
        o.update(self.svc.svcname)
        self.conn = o.hexdigest()

    def __str__(self):
        return "%s dcs=%s manager=%s" % (
                 resSync.Sync.__str__(self),
                 ' '.join(self.dcs),
                 ' '.join(self.manager))

