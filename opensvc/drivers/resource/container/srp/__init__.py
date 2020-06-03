import os
import socket

import core.exceptions as ex

from .. import \
    BaseContainer, \
    KW_START_TIMEOUT, \
    KW_STOP_TIMEOUT, \
    KW_NO_PREEMPT_ABORT, \
    KW_NAME, \
    KW_HOSTNAME, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV
from env import Env
from utilities.lazy import lazy
from core.capabilities import capabilities
from core.resource import Resource
from core.objects.svcdict import KEYS
from utilities.lazy import lazy
from utilities.proc import justcall, qcall

DRIVER_GROUP = "container"
DRIVER_BASENAME = "srp"
KEYWORDS = [
    {
        "keyword": "prm_cores",
        "default": 1,
        "convert": "integer",
        "provisioning": True,
        "text": "The number of core to bind the SRP container to."
    },
    {
        "keyword": "ip",
        "at": True,
        "provisioning": True,
        "text": "The ip name or addr used to create the SRP container."
    },
    KW_START_TIMEOUT,
    KW_STOP_TIMEOUT,
    KW_NO_PREEMPT_ABORT,
    KW_NAME,
    KW_HOSTNAME,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("srp") and which("srp_su"):
        data.append("container.srp")
    return data


class ContainerSrp(BaseContainer):
    def __init__(self, guestos="HP-UX", prm_cores=1, ip=None, **kwargs):
        super(ContainerSrp, self).__init__(type="container.srp", guestos=guestos, **kwargs)
        self.prm_cores = prm_cores
        self.raw_ip = ip
        self.need_start = []

    @lazy
    def rootpath(self):
        return os.path.join(os.sep, 'var', 'hpsrp', self.name)

    @lazy
    def runmethod(self):
        return ['srp_su', self.name, 'root', '-c']

    def files_to_sync(self):
        return [self.export_file]

    def get_rootfs(self):
        return self.get_status()['state']

    def rcp_from(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.Error()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.Error()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def install_drp_flag(self):
        rootfs = self.get_rootfs()
        flag = os.path.join(rootfs, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def container_start(self):
        cmd = ['srp', '-start', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def container_stop(self):
        cmd = ['srp', '-stop', self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def container_forcestop(self):
        raise ex.Error

    def operational(self):
        cmd = self.runmethod + ['pwd']
        ret = qcall(cmd)
        if ret == 0:
            return True
        return False

    def get_verbose_list(self):
        """
        Name: iisdevs1  Template: system Service: provision ID: 1
        ----------------------------------------------------------------------

        autostart=0
        srp_name=iisdevs1
        ...
        """
        cmd = ['srp', '-list', self.name, '-v']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("srp -list returned %d:\n%s"%(ret, err))

        data = {}

        words = out.split()
        for i, w in enumerate(words):
            if w == "Service:":
                service = words[i+1]
            if '=' in w:
                key = service + '.' + w[:w.index('=')]
                val = w[w.index('=')+1:]
                data[key] = val
        return data

    def get_verbose_status(self):
        """
        SRP Status:

        ----------------- Status for SRP:iisdevs1 ----------------------
            Status:MAINTENANCE

            Type:system    Subtype:private    Rootpath:/var/hpsrp/iisdevs1

            IP:10.102.184.12      Interface:lan0:1 (DOWN)   id: 1
            MEM Entitle:50.00%    MEM Max:(none)    Usage:0.00%
            CPU Entitle:9.09%    CPU Max:(none)    Usage:0.00%
        """
        cmd = ['srp', '-status', self.name, '-v']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("srp -status returned %d:\n%s"%(ret, err))

        data = {'ip': [], 'mem': {}, 'cpu': {}}

        words = out.split()
        for i, w in enumerate(words):
            if w.startswith('IP'):
                ip = w.replace('IP:','')
                intf = words[i+1].replace('Interface:','')
                state = words[i+2].strip('(').strip(')')
                id = words[i+4]
                data['ip'].append({
                  'ip': ip,
                  'intf': intf,
                  'state': state,
                  'id': id,
                })
            elif w == "MEM" and words[i+1].startswith("Entitle"):
                entitle = words[i+1].replace('Entitle:','')
                max = words[i+3].replace('Max:','')
                usage = words[i+4].replace('Usage:','')
                data['mem'] = {
                  'entitle': entitle,
                  'max': max,
                  'usage': usage,
                }
            elif w == "CPU" and words[i+1].startswith("Entitle"):
                entitle = words[i+1].replace('Entitle:','')
                max = words[i+3].replace('Max:','')
                usage = words[i+4].replace('Usage:','')
                data['cpu'] = {
                  'entitle': entitle,
                  'max': max,
                  'usage': usage,
                }
        return data

    def get_status(self, nodename=None):
        """
        NAME         TYPE      STATE       SUBTYPE    ROOTPATH
        iisdevs1     system    maintenance private    /var/hpsrp/iisdevs1
        """
        cmd = ['srp', '-status', self.name]
        if nodename is not None:
            cmd = Env.rsh.split() + [nodename] + cmd

        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("srp -status returned %d:\n%s"%(ret, err))
        lines = out.split('\n')
        if len(lines) < 2:
            raise ex.Error("srp -status output too short:\n%s"%out)
        l = lines[1].split()
        if l[0] != self.name:
            raise ex.Error("srp -status second line, first entry does not match container name")
        if len(l) != 5:
            raise ex.Error("unexpected number of entries in %s"%str(l))
        _type, _state, _subtype, _rootpath = l[1:]
        return {
          'type': l[1],
          'state': l[2],
          'subtype': l[3],
          'rootpath': l[4],
        }

    def is_down(self):
        d = self.get_status()
        if d['state'] == 'stopped':
            return True
        return False

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        d = self.get_status(nodename)
        if d['state'] == 'started':
            return True
        return False

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        try:
            val = self.get_verbose_list()['init.autostart']
        except ex.Error:
            return False
        if val == 'yes' or val == '1':
            return False
        return True

    def check_capabilities(self):
        if "node.x.srp" not in capabilities:
            self.log.debug("srp is not in PATH")
            return False
        return True

    def presync(self):
        self.container_export()

    def postsync(self):
        self.container_import()

    def container_import(self):
        if not os.path.exists(self.export_file):
            raise ex.Error("%s does not exist"%self.export_file)
        cmd = ['srp', '-batch', '-import', '-xfile', self.export_file, 'allow_sw_mismatch=yes', 'autostart=no']
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()

    def container_export(self):
        cmd = ['srp', '-batch', '-export', self.name, '-xfile', self.export_file]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()


    @lazy
    def export_file(self):
        return os.path.join(self.var_d, self.name + '.xml')

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)


    @lazy
    def ip(self):
        return self.lookup(self.raw_ip)

    def lookup(self, ip):
        if ip is None:
            raise ex.Error("the ip provisioning keyword is not set")

        try:
            int(ip[0])
            # already in cidr form
            return
        except:
            pass

        try:
            a = socket.getaddrinfo(ip, None)
            if len(a) == 0:
                raise Exception
            ip = a[0][-1][0]
            return ip
        except:
            raise ex.Error("could not resolve %s to an ip address"%ip)

    def validate(self):
        # False triggers provisioner, True skip provisioner
        if "node.x.srp" not in capabilities:
            self.log.error("this node is not srp capable")
            return True

        if self.check_srp():
            self.log.error("container is already created")
            return True

        return False

    def check_srp(self):
        try:
            self.get_status()
        except:
            return False
        return True

    def cleanup(self):
        rs = self.svc.get_resources('fs')
        rs.sort(key=lambda x: x.mount_point, reverse=True)
        for r in rs:
            if r.mount_point == self.rootpath:
                continue
            if not r.mount_point.startswith(self.rootpath):
                continue
            r.stop()
            self.need_start.append(r)
            os.unlink(r.mount_point)
            p = r.mount_point
            while True:
                p = os.path.realpath(os.path.join(p, '..'))
                if p == self.rootpath:
                    break
                try:
                    self.log.info("unlink %s"%p)
                    os.unlink(p)
                except:
                    break

    def restart_fs(self):
        for r in self.need_start:
            r.start()

    def add_srp(self):
        self.cleanup()
        cmd = ['srp', '-batch',
               '-a', self.name,
               '-t', 'system',
               '-s', 'admin,cmpt,init,prm,network',
               'ip_address='+self.ip, 'assign_ip=no',
               'autostart=no',
               'delete_files_ok=no',
               'root_password=""',
               'prm_group_type=PSET',
               'prm_cores='+str(self.prm_cores)]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.Error()
        self.restart_fs()

    def provisioner(self):
        self.add_srp()
        self.start()
        self.log.info("provisioned")
        return True
