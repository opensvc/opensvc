from rcGlobalEnv import rcEnv
import resDisk
from rcUtilities import justcall
import os
import rcExceptions as ex
import json

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

    def sub_devs_name(self):
        return os.path.join(rcEnv.paths.pathvar, self.svc.svcname, self.rid + '.sub_devs')

    def files_to_sync(self):
        return [self.sub_devs_name()]

    def presync(self):
        """ this one is exported as a service command line arg
        """
        dl = self._sub_devs()
        with open(self.sub_devs_name(), 'w') as f:
            json.dump(list(dl), f)

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

    def sub_devs(self):
        if not os.path.exists(self.sub_devs_name()):
            s = self.svc.group_status(excluded_groups=set(["sync", "hb"]))
            import rcStatus
            if s['overall'].status == rcStatus.UP:
                self.log.debug("no sub_devs cache file and service up ... refresh sub_devs cache")
                self.presync()
            else:
                self.log.debug("no sub_devs cache file and service not up ... unable to evaluate sub_devs")
                return set([])
        with open(self.sub_devs_name(), 'r') as f:
            return set(json.load(f))
        except:
            self.log.error("corrupted sub_devs cache file %s"%self.sub_devs_name())
            raise ex.excError

    def _sub_devs(self):
        # return cache if initialized
        if len(self.sub_devs_cache) > 0 :
            return self.sub_devs_cache

        if not os.path.exists("/etc/fdmns/"+self.name):
            return set()

        import glob
        dl = glob.glob("/etc/fdmns/"+self.name+"/*")
        dl = map(lambda x: os.readlink(x), dl)
        self.sub_devs_cache = set(dl)

        self.log.debug("found sub devs %s held by fdmn %s" % (dl, self.name))
        return self.sub_devs_cache

if __name__ == "__main__":
    p=Disk(name="dom1")
    print p._sub_devs()
