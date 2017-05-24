from rcGlobalEnv import rcEnv
import resDisk
from rcUtilities import justcall
import os
import rcExceptions as ex

import re

class Disk(resDisk.Disk):
    def __init__(self,
                 rid=None,
                 name=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                          rid=rid,
                          name=name,
                          type='disk.vg',
                          **kwargs)
        self.label = 'fdmn ' + name

    def disklist_name(self):
        return os.path.join(rcEnv.paths.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.disklist')

    def files_to_sync(self):
        return [self.disklist_name()]

    def presync(self):
        """ this one is exported as a service command line arg
        """
        dl = self._disklist()
        import json
        with open(self.disklist_name(), 'w') as f:
            f.write(json.dumps(list(dl)))

    def has_it(self):
        """Returns True if the pool is present
        """
        if os.path.exists("/etc/fdmns/"+self.name):
            return True
        return False

    def is_up(self):
        """Returns True if the fdmn is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'showfdmn', self.name ]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if len(err) > 0:
                self.status_log(err)
            return False
        if 'not active' in out:
            return False
        return True

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def disklist(self):
        if not os.path.exists(self.disklist_name()):
            s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
            import rcStatus
            if s['overall'].status == rcStatus.UP:
                self.log.debug("no disklist cache file and service up ... refresh disklist cache")
                self.presync()
            else:
                self.log.debug("no disklist cache file and service not up ... unable to evaluate disklist")
                return set([])
        with open(self.disklist_name(), 'r') as f:
            buff = f.read()
        import json
        try:
            dl = set(json.loads(buff))
        except:
            self.log.error("corrupted disklist cache file %s"%self.disklist_name())
            raise ex.excError
        return dl


    def _disklist(self):
        # return cache if initialized
        if len(self.disks) > 0 :
            return self.disks

        disks = set([])
        if not os.path.exists("/etc/fdmns/"+self.name):
            return disks

        import glob
        dl = glob.glob("/etc/fdmns/"+self.name+"/*")
        dl = map(lambda x: os.readlink(x), dl)
        self.disks = set(dl)

        self.log.debug("found disks %s held by pool %s" % (disks, self.name))
        return self.disks

if __name__ == "__main__":
    p=Disk(name="dom1")
    print p._disklist()
