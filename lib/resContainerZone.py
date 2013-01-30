#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@free.fr>'
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
from datetime import datetime
import rcStatus
import resources as Res
import time
import os
from rcUtilities import justcall, qcall
from stat import *
import resContainer
from rcExceptions import excError
from rcZfs import zfs_setprop
from rcGlobalEnv import rcEnv

ZONECFG="/usr/sbin/zonecfg"
PGREP="/usr/bin/pgrep"
PWAIT="/usr/bin/pwait"
INIT="/sbin/init"
SVCS="/usr/bin/svcs"

MULTI_USER_SMF="svc:/milestone/multi-user:default"

class Zone(resContainer.Container):
    def __init__(self, rid, name, guestos="SunOS", optional=False, disabled=False, monitor=False,
                 tags=set([]), always_on=set([])):
        """define Zone object attribute :
                name
                label
                state
                zonepath
        """
        resContainer.Container.__init__(self, rid=rid, name=name, type="container.zone", guestos=guestos,
                              optional=optional, disabled=disabled,
                              monitor=monitor, tags=tags, always_on=always_on)
        self.label = name
        self.state = None
        self.zonepath = os.path.realpath(os.path.join(os.sep, 'zones', self.name))
        self.zone_refresh()
        self.runmethod = [ '/usr/sbin/zlogin', '-S', name ]

    def zonecfg(self, zonecfg_args=[]):
        cmd = [ZONECFG, '-z', self.name] + zonecfg_args
        (ret, out, err) = self.vcall(cmd,err_to_info=True)
        if ret != 0:
            msg = '%s failed status: %i\n%s' % (" ".join(cmd), ret, out)
            self.log.error(msg)
            raise excError(msg)
        else:
            msg = '%s done status: %i\n%s' % (" ".join(cmd), ret, out)
            self.log.info(msg)
        self.zone_refresh()
        return ret

    def zoneadm(self, action, option=None):
        if action in [ 'ready' , 'boot' ,'shutdown' , 'halt' ,'attach', 'detach', 'install', 'clone' ] :
            cmd = ['zoneadm', '-z', self.name, action ]
        else:
            self.log.error("unsupported zone action: %s" % action)
            return 1
        if option is not None:
            cmd += option

        t = datetime.now()
        (ret, out, err) = self.vcall(cmd,err_to_info=True)
        len = datetime.now() - t
        if ret != 0:
            msg = '%s failed status: %i in %s logs in %s' % (' '.join(cmd), ret, len, out)
            self.log.error(msg)
            raise excError(msg)
        else:
            self.log.info('%s done in %s - ret %i - logs in %s'
                            % (' '.join(cmd), len, ret, out))
        self.zone_refresh()
        return ret

    def set_zonepath_perms(self):
        if not os.path.exists(self.zonepath):
            os.makedirs(self.zonepath)
        s = os.stat(self.zonepath)
        if s.st_uid != 0 or s.st_gid != 0:
            self.log.info("set %s ownership to uid 0 gid 0"%self.zonepath)
            os.chown(self.zonepath, 0, 0)
        mode = s[ST_MODE]
        if (S_IWOTH&mode) or (S_IXOTH&mode) or (S_IROTH&mode) or \
           (S_IWGRP&mode) or (S_IXGRP&mode) or (S_IRGRP&mode):
            self.vcall(['chmod', '700', self.zonepath])

    def rcp(self, src, dst):
        dst = os.path.realpath(self.zonepath + '/root/' + dst)
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def attach(self):
        self.zone_refresh()
        if self.state in ('installed' , 'ready', 'running'):
            self.log.info("zone container %s already installed" % self.name)
            return 0
        try:
            self.umount_fs_in_zonepath()
            self.zoneadm('attach')
        except excError:
            self.zoneadm('attach', ['-F'] )
        self.can_rollback = True

    def detach(self):
        self.zone_refresh()
        if self.state == "configured" :
            self.log.info("zone container %s already detached/configured" % self.name)
            return 0
        return self.zoneadm('detach')

    def ready(self):
        self.zone_refresh()
        if self.state == 'ready' or self.state == "running" :
            self.log.info("zone container %s already ready" % self.name)
            return 0
        self.set_zonepath_perms()
        return self.zoneadm('ready')

    def install_drp_flag(self):
        flag = os.path.join(self.zonepath, ".drp_flag")
        self.log.info("install drp flag in container : %s"%flag)
        with open(flag, 'w') as f:
            f.write(' ')
            f.close()

    def get_smf_state(self, smf=None):
        cmd = self.runmethod + [SVCS, '-H', '-o', 'state', smf]
        (out, err, status) = justcall(cmd)
        if status == 0:
            return out.split('\n')[0]
        else:
            return False

    def is_smf_state(self, smf=None, value=None):
        current_value = self.get_smf_state(smf)
        if current_value is False:
            return False
        elif current_value == value:
            return True
        else:
            return False

    def is_multi_user(self):
        return self.is_smf_state(MULTI_USER_SMF, "online")

    def wait_multi_user(self):
        self.log.info("wait for smf state on on %s", MULTI_USER_SMF)
        self.wait_for_fn(self.is_multi_user, self.startup_timeout, 2)
        
    def boot(self):
        "return 0 if zone is running else return self.zoneadm('boot')"
        self.zone_refresh()
        if self.state == "running" :
            self.log.info("zone container %s already running" % self.name)
            return 0
        self.zoneadm('boot')
        if self.state == "running":
            return(0)
        else:
            raise(excError("zone should be running"))
        self.log.info("wait for zone operational")
        self.wait_for_fn(self.operational, self.startup_timeout, 2)

    def halt(self):
        """ Need wait poststat after returning to installed state on ipkg
            example : /bin/ksh -p /usr/lib/brand/ipkg/poststate zonename zonepath 5 4
        """
        self.zone_refresh()
        if self.state in [ 'installed', 'configured'] :
            self.log.info("zone container %s already stopped" % self.name)
            return 0
        if self.state == 'running':
            (ret, out, err) = self.vcall(['zlogin' , self.name , '/sbin/init' , '0'])
            for t in range(self.shutdown_timeout):
                self.zone_refresh()
                if self.state == 'installed':
                    for t2 in range(self.shutdown_timeout):
                        time.sleep(1)
                        (out,err,st) = justcall([ 'pgrep', '-fl', 'ipkg/poststate.*'+ self.name])
                        if st == 0 : 
                            self.log.info("Waiting for ipkg poststate complete: %s" % out)
                        else:
                            break
                    return 0
                time.sleep(1)
            self.log.info("timeout out waiting for %s shutdown", self.name)
        ret = self.zoneadm('halt')
        if ret != 0:
            return ret
        return self.detach()

    def container_start(self):
        return self.boot()

    def _status(self, verbose=False):
        self.zone_refresh()
        if self.state == 'running' :
            return rcStatus.UP
        else:
            return rcStatus.DOWN

    def zone_refresh(self):
        """ refresh Zone object attributes:
                state
                zonepath
                brand
            from zoneadm -z zonename list -p
            zoneid:zonename:state:zonepath:uuid:brand:ip-type
        """

        (out,err,st) = justcall([ 'zoneadm', '-z', self.name, 'list', '-p' ])

        if st == 0 :
            l = out.split(':')
            n_fields = len(l)
            if n_fields == 9:
                (zoneid,zonename,state,zonepath,uuid,brand,iptype,rw,macp) = l
            elif n_fields == 7:
                (zoneid,zonename,state,zonepath,uuid,brand,iptype) = l
            else:
                raise excError("Unexpected zoneadm list output: %s"%out)
            if zonename == self.name :
                self.state = state
                self.zonepath = zonepath
                self.brand = brand
                return True
            else:
                return False
        else:
            return False

    def is_running(self):
        "return True if zone is running else False"
        self.zone_refresh()
        if self.state == 'running' :
            return True
        else:
            return False

    def is_up(self):
        "return self.is_running status"
        return self.is_running()

    def operational(self):
        "return status of: zlogin zone pwd"
        cmd = self.runmethod + ['pwd']
        if qcall(cmd) == 0:
            return True
        else:
            return False

    def boot_and_wait_reboot(self):
        """boot zone, then wait for automatic zone reboot
            boot zone
            wait for zone init process end
            wait for zone running
            wait for zone operational
        """
        self.log.info("wait for zone boot and reboot...")
        self.boot()
        if self.is_running is False:
            raise(excError("zone is not running"))
        cmd = [PGREP, "-z", self.name, "-f", INIT]
        (out, err, st) = justcall(cmd)
        if st != 0:
            raise(excError("fail to detect zone init process"))
        pids = " ".join(out.split("\n")).rstrip()
        cmd = [PWAIT, pids]
        self.log.info("wait for zone init process %s termination" % (pids))
        if qcall(cmd) != 0:
            raise(excError("failed " + " ".join(cmd)))
        self.log.info("wait for zone running again")
        self.wait_for_fn(self.is_up, self.startup_timeout, 2)
        self.log.info("wait for zone operational")
        self.wait_for_fn(self.operational, self.startup_timeout, 2)
 
    def umount_fs_in_zonepath(self):
        """zone boot will fail if some fs linger under the zonepath.
           those fs might be datasets automounted upon zpool import.
           umount them.
           if they are needed, them still may be mounted by opensvc
           if declared as zoned fs or encap fs.
        """
        if self.zonepath == "/":
            # sanity check
            return

        m = __import__('rcMounts'+rcEnv.sysname)
        mounts = m.Mounts()
        mounts.sort(reverse=True)
        mntpts = []
        for resource in self.svc.get_resources('fs'):
            mntpts.append(resource.mountPoint)
        for mount in mounts.mounts:
            if not mount.mnt.startswith(self.zonepath):
                continue
            # don't umount fs not handled by the service
            if mount.mnt not in mntpts:
                continue
            self.vcall(['umount', mount.mnt])
            self.vcall(['rmdir', mount.mnt])
            if mount.type == 'zfs':
                zfs_setprop(mount.dev, 'canmount', 'noauto')

    def start(self):
        self.attach()
        self.ready()
        self.svc.sub_set_action("ip", "start", tags=set([self.name]))
        self.boot()
        self.svc.sub_set_action("disk.scsireserv", "start", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "start", tags=set([self.name]))
        self.svc.sub_set_action("fs", "start", tags=set([self.name]))

    def stop(self):
        self.svc.sub_set_action("fs", "stop", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "stop", tags=set([self.name]))
        self.svc.sub_set_action("disk.scsireserv", "stop", tags=set([self.name]))
        self.svc.sub_set_action("ip", "stop", tags=set([self.name]))
        self.halt()
        self.detach()

    def provision(self):
        self._provision()
        self.svc.sub_set_action("disk.scsireserv", "provision", tags=set([self.name]))
        self.svc.sub_set_action("disk.zpool", "provision", tags=set([self.name]))
        self.svc.sub_set_action("fs", "provision", tags=set([self.name]))

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def _provision(self):
        m = __import__("provZone")
        m.ProvisioningZone(self).provisioner()

    def get_container_info(self):
        vcpus = "0"
        vmem = "0"
        cmd = [ZONECFG, "-z", self.name, "info", "rctl", "name=zone.cpu-cap"]
        (out, err, status) = justcall(cmd)
        if status == 0:
            lines = out.split('\n')
            for line in lines:
                if "value:" not in line:
                    continue
                l = line.split("limit=")
                if len(l) == 2:
                    vcpus = l[-1][:l[-1].index(',')]
                    vcpus = str(float(vcpus)/100)
                    break
                
        cmd = [ZONECFG, "-z", self.name, "info", "capped-memory"]
        (out, err, status) = justcall(cmd)
        if status == 0:
            lines = out.split('\n')
            for line in lines:
                if "physical:" not in line:
                    continue
                l = line.split(": ")
                if len(l) == 2:
                    vmem = l[-1].strip()
                    if vmem.endswith('T'):
                        vmem = str(float(vmem[:-1])*1024*1024)
                    elif vmem.endswith('G'):
                        vmem = str(float(vmem[:-1])*1024)
                    elif vmem.endswith('M'):
                        vmem = vmem[:-1]
                    elif vmem.endswith('K'):
                        vmem = str(float(vmem[:-1])/1024)
                    break
                
        return {'vcpus': vcpus, 'vmem': vmem}


if __name__ == "__main__":
    for c in (Zone,) :
        help(c)
