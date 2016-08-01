import os
import resources as Res
import rcStatus
import rcExceptions as ex
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv

class Drbd(Res.Resource):
    """ Drbd device resource

        The tricky part is that drbd devices can be used as PV
        and LV can be used as drbd base devices. Beware of the
        the ordering deduced from rids and subsets.

        Start 'ups' and promotes the drbd devices to primary.
        Stop 'downs' the drbd devices.
    """
    def __init__(self,
                 rid=None,
                 res=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 subset=None,
                 tags=set([]),
                 monitor=False,
                 restart=0):
        Res.Resource.__init__(self,
                              rid,
                              "disk.drbd",
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset,
                              monitor=monitor,
                              restart=restart)
        self.res = res
        self.label = "drbd "+res
        self.drbdadm = None
        self.always_on = always_on
        self.disks = set()

    def __str__(self):
        return "%s resource=%s" % (Res.Resource.__str__(self),\
                                 self.res)

    def files_to_sync(self):
        cf = os.path.join(os.sep, 'etc', 'drbd.d', self.res+'.res')
        if os.path.exists(cf):
            return [cf]
        self.log.error("%s does not exist"%cf)
        return []

    def drbdadm_cmd(self, cmd):
        if self.drbdadm is None:
            if which('drbdadm'):
                self.drbdadm = 'drbdadm'
            else:
                self.log("drbdadm command not found")
                raise exc.excError
        return [self.drbdadm] + cmd.split() + [self.res]

    def devlist(self):
        devps = set()

        (ret, out, err) = self.call(self.drbdadm_cmd('dump-xml'))
        if ret != 0:
            raise ex.excError

        from xml.etree.ElementTree import XML, fromstring
        tree = fromstring(out)
        
        for res in tree.getiterator('resource'):
            if res.attrib['name'] != self.res:
                continue
            for host in res.getiterator('host'):
                if host.attrib['name'] != rcEnv.nodename:
                    continue
                d = host.find('device')
                if d is None:
                    d = host.find('volume/device')
                if d is None:
                    continue
                devps |= set([d.text])
        return devps

    def disklist(self):
        if self.disks != set():
            return self.disks

        self.disks = set()
        devps = set()

        (ret, out, err) = self.call(self.drbdadm_cmd('dump-xml'))
        if ret != 0:
            raise ex.excError

        from xml.etree.ElementTree import XML, fromstring
        tree = fromstring(out)
        
        for res in tree.getiterator('resource'):
            if res.attrib['name'] != self.res:
                continue
            for host in res.getiterator('host'):
                if host.attrib['name'] != rcEnv.nodename:
                    continue
                d = host.find('disk')
                if d is None:
                    d = host.find('volume/disk')
                if d is None:
                    continue
                devps |= set([d.text])

        try:
            u = __import__('rcUtilities'+rcEnv.sysname)
            self.disks = u.devs_to_disks(self, devps)
        except:
            self.disks = devps

        return self.disks

    def drbdadm_down(self):
        (ret, out, err) = self.vcall(self.drbdadm_cmd('down'))
        if ret != 0:
            raise ex.excError

    def drbdadm_up(self):
        (ret, out, err) = self.vcall(self.drbdadm_cmd('up'))
        if ret != 0:
            raise ex.excError

    def get_cstate(self):
        self.prereq()
        (out, err, ret) = justcall(self.drbdadm_cmd('cstate'))
        if ret != 0:
            if "Device minor not allocated" in err:
                return "Unattached"
            else:
                raise ex.excError
        return out.strip()

    def prereq(self):
        if not os.path.exists("/proc/drbd"):
            (ret, out, err) = self.vcall(['modprobe', 'drbd'])
            if ret != 0: raise ex.excError

    def start_connection(self):
        cstate = self.get_cstate()
        if cstate == "Connected":
            self.log.info("drbd resource %s is already up"%self.res)
        elif cstate == "StandAlone":
            self.drbdadm_down()
            self.drbdadm_up()
        elif cstate == "WFConnection":
            self.log.info("drbd resource %s peer node is not listening"%self.res)
            pass
        else:
            self.drbdadm_up()

    def get_roles(self):
        out, err, ret = justcall(self.drbdadm_cmd('role'))
        if ret != 0:
            raise ex.excError(err)
        out = out.strip().split('/')
        if len(out) != 2:
            raise ex.excError(out)
        return out

    def start_role(self, role):
        roles = self.get_roles()
        if roles[0] != role:
            (ret, out, err) = self.vcall(self.drbdadm_cmd(role.lower()))
            if ret != 0:
                raise ex.excError
        else:
            self.log.info("drbd resource %s is already %s"%(self.res, role))

    def startstandby(self):
        self.start_connection()
        roles = self.get_roles()
        if roles[0] == "Primary":
            return
        self.start_role('Secondary')
        self.can_rollback = True

    def stopstandby(self):
        self.start_connection()
        roles = self.get_roles()
        if roles[0] == "Secondary":
            return
        self.start_role('Secondary')

    def start(self):
        self.start_connection()
        self.start_role('Primary')
        self.can_rollback = True

    def stop(self):
        self.drbdadm_down()

    def _status(self, verbose=False):
        try:
            roles = self.get_roles()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        self.status_log(str(roles[0]))
        (ret, out, err) = self.call(self.drbdadm_cmd('dstate'))
        if ret != 0:
            self.status_log("drbdadm dstate %s failed"%self.res)
            return rcStatus.WARN
        out = out.strip()
        if out == "UpToDate/UpToDate":
            return self.status_stdby(rcStatus.UP)
        elif out == "Unconfigured":
            return self.status_stdby(rcStatus.DOWN)
        self.status_log("unexpected drbd resource %s state: %s"%(self.res, out))
        return rcStatus.WARN

if __name__ == "__main__":
    help(Drbd)
    v = Drbd(res='test')
    print(v)

