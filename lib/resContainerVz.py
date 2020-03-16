import os

import rcStatus
import resources as Res
from rcUtilities import which, qcall, justcall
import resContainer
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from svcBuilder import init_kwargs, container_kwargs

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s))
    r = Vz(**kwargs)
    svc += r

class Vz(resContainer.Container):
    def files_to_sync(self):
        return [self._cf]

    def get_cf_value(self, param):
        value = None
        try:
            cf = self.cf()
        except:
            return value
        with open(cf, 'r') as f:
            for line in f.readlines():
                if param not in line:
                    continue
                if line.strip()[0] == '#':
                    continue
                l = line.replace('\n', '').split('=')
                if len(l) < 2:
                    continue
                if l[0].strip() != param:
                    continue
                value = ' '.join(l[1:]).strip().rstrip('/')
                break
        return value

    def get_rootfs(self):
        with open(self.cf(), 'r') as f:
            for line in f:
                if 'VE_PRIVATE' in line:
                    return line.strip('\n').split('=')[1].strip('"').replace('$VEID', self.name)
        self.log.error("could not determine lxc container rootfs")
        return ex.excError

    def rcp_from(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def vzctl(self, action, options=[]):
        cmd = ['vzctl', action, self.name] + options
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        return out

    def container_start(self):
        self.vzctl('start')

    def container_stop(self):
        self.vzctl('stop')

    def container_forcestop(self):
        raise ex.excError

    def operational(self):
        cmd = self.runmethod + ['/sbin/ifconfig', '-a']
        ret = qcall(cmd)
        if ret == 0:
            return True
        return False

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        """ CTID 101 exist mounted running
        """
        cmd = ['vzctl', 'status', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        ret, out, err = self.call(cmd)
        if ret != 0:
            return False
        l = out.split()
        if len(l) != 5:
            return False
        if l[2] != 'exist' or \
           l[3] != 'mounted' or \
           l[4] != 'running':
            return False
        return True

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        try:
            cf = self.cf()
        except:
            return True
        with open(self.cf(), 'r') as f:
            for line in f:
                if 'ONBOOT' in line and 'yes' in line:
                    return False
        return True

    def check_capabilities(self):
        if not which('vzctl'):
            self.log.debug("vzctl is not in PATH")
            return False
        return True

    def cf(self):
        if not os.path.exists(self._cf):
            self.log.error("%s does not exist"%self._cf)
            raise ex.excError
        return self._cf

    def __init__(self,
                 rid,
                 name,
                 guestos="Linux",
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.vz",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self._cf = os.path.join(os.sep, 'etc', 'vz', 'conf', name+'.conf')
        self.runmethod = ['vzctl', 'exec', name]

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

