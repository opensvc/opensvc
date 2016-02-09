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
import rcStatus
import resources as Res
import time
import os
import rcExceptions as ex
from rcUtilities import qcall
from rcGlobalEnv import rcEnv
import resContainer
u = __import__('rcUtilitiesHP-UX')
import resVgHpVm

class HpVm(resContainer.Container):
    def __init__(self,
                 rid,
                 name,
                 guestos="HP-UX",
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 subset=None,
                 tags=set([]),
                 always_on=set([])):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.hpvm",
                                        guestos=guestos,
                                        optional=optional,
                                        disabled=disabled,
                                        monitor=monitor,
                                        restart=restart,
                                        subset=subset,
                                        tags=tags,
                                        always_on=always_on)
        self.vg = resVgHpVm.Vg(
          rid = 'vmdg#'+self.rid,
          name = 'vmdg_'+self.name,
          container_name = self.name
        )

    def on_add(self):
        self.vg.svc = self.svc

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def files_to_sync(self):
        import glob
        a = self.vg.files_to_sync()
        guest = os.path.join(os.sep, 'var', 'opt', 'hpvm', 'guests', self.name)
        uuid = os.path.realpath(guest)
        share = os.path.join(rcEnv.pathvar, 'vg_'+self.name+'_*.share')
        if os.path.exists(guest):
            a.append(guest)
        if os.path.exists(uuid):
            a.append(uuid)
        files = glob.glob(share)
        if len(files) > 0:
            a += files
        return a

    def ping(self):
        return u.check_ping(self.addr, timeout=1, count=1)

    def container_start(self):
        cmd = ['/opt/hpvm/bin/hpvmstart', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_stop(self):
        cmd = ['/opt/hpvm/bin/hpvmstop', '-g', '-F', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_forcestop(self):
        cmd = ['/opt/hpvm/bin/hpvmstop', '-F', '-P', self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def check_manual_boot(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        (ret, out, err) = self.call(cmd, cache=True)
        if ret != 0:
            return False
        if out.split(":")[11] == "Manual":
            return True
        self.log.info("Auto boot should be turned off")
        return False

    def get_container_info(self):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        (ret, out, err) = self.call(cmd, cache=True)
        self.info = {'vcpus': '0', 'vmem': '0'}
        if ret != 0:
            return self.info
        self.info['vcpus'] = out.split(':')[19].split(';')[0]
        self.info['vmem'] = out.split(':')[20].split(';')[0]
        if 'GB' in self.info['vmem']:
            self.info['vmem'] = str(1024*1024*int(self.info['vmem'].replace('GB','')))
        return self.info

    def is_up_on(self, nodename):
        return self.is_up(nodename)

    def is_up(self, nodename=None):
        cmd = ['/opt/hpvm/bin/hpvmstatus', '-M', '-P', self.name]
        if nodename is not None:
            cmd = rcEnv.rsh.split() + [nodename] + cmd
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            return False
        if out.split(":")[10] == "On":
            return True
        return False

    def check_capabilities(self):
        if os.path.exists('/opt/hpvm/bin/hpvmstatus'):
            return True
        return False

    def _migrate(self):
        cmd = ['hpvmmigrate', '-o', '-P', self.name, '-h', self.svc.destination_node]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def disklist(self):
        return self.vg.disklist()

    def devlist(self):
        return self.vg.devlist()

    def presync(self):
        return self.vg.presync()

    def postsync(self):
        return self.vg.postsync()
