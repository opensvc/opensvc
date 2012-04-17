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
import re
import os
import rcExceptions as ex
import resDg
from subprocess import *
from rcUtilities import qcall
from rcGlobalEnv import rcEnv

class Vg(resDg.Dg):
    def __init__(self, rid=None, name=None, type=None,
                 always_on=set([]), dsf=True,
                 disabled=False, tags=set([]), optional=False,
                 monitor=False):
        self.label = name
        self.dsf = dsf
        resDg.Dg.__init__(self, rid=rid, name=name,
                          type='disk.vg',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled, tags=tags,
                          monitor=monitor)

    def files_to_sync(self):
        return [self.mapfile_name(), self.mkfsfile_name()]

    def mapfile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.map')

    def mkfsfile_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.mksf')

    def has_it(self):
        """ returns True if the volume is present
        """
        if self.is_active():
            return True
        if not os.path.exists(self.mapfile_name()):
            return False
        if self.is_imported():
            return True
        return False

    def dev2char(self, dev):
        dev = dev.replace("/dev/disk", "/dev/rdisk")
        dev = dev.replace("/dev/dsk", "/dev/rdsk")
        return dev

    def dsf_name(self, dev):
        cmd = ['scsimgr', 'get_attr', '-D', self.dev2char(dev), '-a', 'device_file', '-p']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        return out.split()[0]

    def write_mksf(self):
        cmd = ['ioscan', '-F', '-m', 'dsf']
        (ret, buff, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError
        if len(buff) == 0:
            return
        mksf = {}
        if len(self.disks) == 0:
            self.disks = self.disklist()
        dsf_names = map(self.dsf_name, self.disks)
        with open(self.mkfsfile_name(), 'w') as f:
            for line in buff.split('\n'):
                if len(line) == 0:
                    return
                a = line.split(':')[0]
                if '/dev/pt/pt' not in a and '/dev/rdisk/disk' not in a and self.dsf_name(a) in dsf_names:
                    cmd = ['scsimgr', 'get_attr', '-D', self.dev2char(a), '-a', 'wwid', '-p']
                    (ret, out, err) = self.call(cmd)
                    if ret != 0:
                        raise ex.excError
                    f.write(":".join([a, out.split()[0].replace('0x', '')])+'\n')

    def do_mksf(self):
        if not os.path.exists(self.mkfsfile_name()):
            return

        instance = {}
        cmd = ['scsimgr', 'get_attr', 'all_lun', '-a', 'wwid', '-a', 'instance', '-p']
        (ret, buff, err) = self.call(cmd)
        for line in buff.split('\n'):
            l = line.split(':')
            if len(l) != 2:
                continue
            instance[l[0].replace('0x', '')] = l[1]

        r = 0
        with open(self.mkfsfile_name(), 'r') as f:
            for line in f.readlines():
                a = line.replace('\n', '').split(':')
                if len(a) == 0:
                    continue
                if os.path.exists(a[0]):
                    continue
                if a[1] not in instance.keys():
                    self.log.error("expected lun %s not present on node %s"%(a[1], rcEnv.nodename))
                    r += 1
                    continue
                cmd = ['mksf', '-r', '-C', 'disk', '-I', instance[a[1]], a[0]]
                (ret, buff, err) = self.vcall(cmd)
                if ret != 0:
                    r += 1
                    continue
        if r > 0:
            raise ex.excError

    def presync(self):
        """ this one is exported as a service command line arg
        """
        cmd = [ 'vgexport', '-m', self.mapfile_name(), '-p', '-s', self.name ]
        ret = qcall(cmd)
        if ret != 0:
            raise ex.excError
        self.write_mksf()

    def is_active(self):
        cmd = [ 'vgdisplay', self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "available" in buff[0]:
            return False
        return True

    def is_imported(self):
        if not os.path.exists(self.mapfile_name()):
            return False
        if self.dsf:
            dsfflag = '-N'
        else:
            dsfflag = ''
        cmd = [ 'vgimport', '-m', self.mapfile_name(), '-s', '-p', dsfflag, self.name ]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        if not "already exists" in buff[1]:
            return False
        return True

    def is_up(self):
        """Returns True if the volume group is present and activated
        """
        if not os.path.exists(self.mapfile_name()):
            self.do_export()
        if not self.is_imported():
            return False
        if not self.is_active():
            return False
        return True

    def do_import(self):
        if self.is_imported():
            self.log.info("%s is already imported" % self.name)
            return
        if self.dsf:
            dsfflag = '-N'
        else:
            dsfflag = ''
        cmd = [ 'vgimport', '-m', self.mapfile_name(), '-s', dsfflag, self.name ]
        self.log.info(' '.join(cmd))
        self.lock()
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = process.communicate()
        self.unlock()

        # we will modify buff[1], so convert from tuple to list
        buff = list(buff)

        # test string for warnings
        #buff[1] = """Warning: Cannot determine block size of Physical Volume "/dev/rdisk/disk394".
        #Assuming a default value of 1024 bytes. Continuing.
        #Warning: Cannot determine block size of Physical Volume "/dev/rdisk/disk395".
        #Assuming a default value of 1024 bytes. Continuing.
        #vgimport:"""

        if len(buff[1]) > 0:
            import re
            regex = re.compile("Warning:.*\n.*Continuing.\n", re.MULTILINE)
            w = regex.findall(buff[1])
            if len(w) > 0:
                warnings = '\n'.join(w)
                self.log.warning(warnings)
                buff[1] = regex.sub('', buff[1])
            if buff[1] != "vgimport: " and buff[1] != "vgimport:":
                self.log.error('error:\n' + buff[1])

        if len(buff[0]) > 0:
            self.log.debug('output:\n' + buff[0])

        if process.returncode != 0:
            raise ex.excError

    def do_export(self):
        preview = False
        if os.path.exists(self.mapfile_name()):
            if not self.is_imported():
                self.log.info("%s is already exported" % self.name)
                return
        elif self.is_active():
            preview = True
        if preview:
            cmd = [ 'vgexport', '-p', '-m', self.mapfile_name(), '-s', self.name ]
        else:
            cmd = [ 'vgexport', '-m', self.mapfile_name(), '-s', self.name ]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_activate(self):
        if self.is_active():
            self.log.info("%s is already available" % self.name)
            return
        cmd = ['vgchange', '-c', 'n', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError
        cmd = ['vgchange', '-a', 'y', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_deactivate(self):
        if not self.is_active():
            self.log.info("%s is already unavailable" % self.name)
            return
        cmd = ['vgchange', '-a', 'n', self.name]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def do_start(self):
        self.do_import()
        self.do_activate()

    def do_stop(self):
        self.do_deactivate()
        self.do_export()

    def start(self):
        self.do_mksf()
        self.do_start()

    def disklist(self):
        need_export = False
        if not self.is_active() and not self.is_imported():
            self.do_import()
            need_export = True
        cmd = ['strings', '/etc/lvmtab']
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        tab = out.split('\n')
        insection = False
        self.disks = set([])
        for e in tab:
            """ move to the first disk of the vg
            """
            if e == '/dev/'+self.name:
                 insection = True
                 continue
            if not insection:
                 continue
            if e == "_KDI":
                 continue
            if "/dev/dsk" not in e and "/dev/disk" not in e:
                 break
            self.disks |= set([e])

        if need_export:
            self.do_export()
        return self.disks

    def lock(self, timeout=30, delay=1):
        import lock
        lockfile = os.path.join(rcEnv.pathlock, 'vgimport')
        lockfd = None
        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile)
        except lock.lockTimeout:
            self.log.error("timed out waiting for lock (%s)"%lockfile)
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
        except ex.excSignal:
            self.log.error("interrupted by signal")
            raise ex.excError
        except:
            self.log.error("unexpected locking error")
            import traceback
            traceback.print_exc()
            raise ex.excError
        self.lockfd = lockfd

    def unlock(self):
        import lock
        lock.unlock(self.lockfd)


