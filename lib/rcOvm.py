import os
import subprocess
from rcUtilities import which, call
import rcExceptions as ex
from rcGlobalEnv import rcEnv

class Ovm(object):
    def __init__(self, log=None):
        self.ovmcli = 'ovm'
        if log is not None:
            self.log = log
        else:
            import logging
            self.log = logging.getLogger('OVM')

        import ConfigParser
        cf = rcEnv.authconf
        self.conf = ConfigParser.RawConfigParser()
        self.conf.read(cf)
        if not self.conf.has_section("ovm"):
            raise ex.excError("no auth information for OVM manager")
        if not self.conf.has_option("ovm", "username"):
            raise ex.excError("no username information for OVM manager")
        if not self.conf.has_option("ovm", "password"):
            raise ex.excError("no password information for OVM manager")
        self.username = self.conf.get("ovm", "username")
        self.password = self.conf.get("ovm", "password")

    def test(self):
        if which(self.ovmcli):
            self.log.error("ovm CLI is not installed")
            return False
        if self.username is None:
            self.log.error("manager username is not set")
            return False
        if self.password is None:
            self.log.error("manager password is not set")
            return False
        return True

    def ovm(self, args, check=True, verbose=True):
        if not self.test:
            raise ex.excError
        cmd = [self.ovmcli, '-u', self.username, '-p', self.password, '-S";"'] + args
        if verbose:
            _cmd = [self.ovmcli, '-u', self.username, '-p', 'XXXXXX'] + args
            self.log.info(subprocess.list2cmdline(_cmd))
            ret, out, err = call(cmd, log=self.log)
            if 'Error:' in out > 0:
                self.log.error(out)
            else:
                self.log.info(out)
        else:
            ret, out, err = call(cmd, log=self.log)
        if check and ret != 0:
            raise ex.excError("ovm command execution error")
        return ret, out, err

    def get_pool(self):
        cmd = ['svr', 'ls']
        ret, out, err = self.ovm(cmd, verbose=False)
        for line in out.split('\n'):
            l = line.split(';')
            if len(l) != 4:
                continue
            if l[1].strip('"') == rcEnv.nodename:
                return l[3].strip('"')
        raise ex.excError("can't find node's pool name")

    def vm_enable_ha(self, vm):
        pool = self.get_pool()
        cmd = ['vm', 'conf', '-n', vm, '-s', pool, '-e']
        self.ovm(cmd)

    def vm_disable_ha(self, vm):
        pool = self.get_pool()
        cmd = ['vm', 'conf', '-n', vm, '-s', pool, '-d']
        self.ovm(cmd)

    def vm_info(self, vm):
        pool = self.get_pool()
        cmd = ['vm', 'info', '-n', vm, '-s', pool]
        ret, out, err = self.ovm(cmd, verbose=False)
        if ret != 0:
            raise ex.excError("failed to fetch VM information from manager")
        h = {}
        for line in out.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            h[l[0].strip()] = l[1].strip()
        return h

    def vm_ha_enabled(self, vm):
        info = self.vm_info(vm)
        if 'Hign Availability' in info and info['Hign Availability'] == 'Enabled':
            return True
        if 'High Availability' in info and info['High Availability'] == 'Enabled':
            return True
        return False

if __name__ == "__main__":
    o = Ovm()
    #o.vm_disable_ha("ovmguest1")
    #o.vm_enable_ha("ovmguest1")
