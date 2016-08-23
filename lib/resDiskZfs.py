from rcGlobalEnv import rcEnv
import resDisk
from rcUtilities import qcall, which
import os
import rcExceptions as ex
import glob

import re

class Disk(resDisk.Disk):
    """ basic pool resource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 type=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 always_on=set([]),
                 monitor=False,
                 restart=0,
                 subset=None):
        self.label = 'pool ' + name
        resDisk.Disk.__init__(self,
                          rid=rid,
                          name=name,
                          type='disk.zpool',
                          always_on=always_on,
                          optional=optional,
                          disabled=disabled,
                          tags=tags,
                          monitor=monitor,
                          restart=restart,
                          subset=subset)

    def disklist_name(self):
        return os.path.join(rcEnv.pathvar, 'vg_' + self.svc.svcname + '_' + self.name + '.disklist')

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
        if not which("zpool"):
            raise ex.excError("zpool command not found")
        ret = qcall( [ 'zpool', 'list', self.name ] )
        if ret == 0 :
            return True
        return False

    def is_up(self):
        """Returns True if the pool is present and activated
        """
        if not self.has_it():
            return False
        cmd = [ 'zpool', 'list', '-H', '-o', 'health', self.name ]
        (ret, out, err) = self.call(cmd)
        if out.strip() == "ONLINE" :
            return True
        return False

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            return 0
        devzp = os.path.join(rcEnv.pathvar, self.svc.svcname, 'dev', 'dsk')
        if os.path.isdir(devzp):
            cmd = [ 'zpool', 'import', '-f', '-o', 'cachefile='+os.path.join(rcEnv.pathvar, 'zpool.cache'), '-d', devzp, self.name ]
            (ret, out, err) = self.vcall(cmd)
            if ret == 0:
                return ret
            else:
                self.log.info("import %s: FallBack Long Way" %self.name)
        cmd = [ 'zpool', 'import', '-f', '-o', 'cachefile='+os.path.join(rcEnv.pathvar, 'zpool.cache'), self.name ]
        (ret, out, err) = self.vcall(cmd)
        self.can_rollback = True
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        cmd = [ 'zpool', 'export', self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def disklist(self):
        if not os.path.exists(self.disklist_name()):
            if self.is_up():
                self.log.debug("no disklist cache file and resource up ... refresh disklist cache")
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
        vdl = []
        for d in dl:
            if os.path.exists(d):
                vdl.append(d)
                continue
            if len(d) < 36 or not d.endswith("s2"):
                self.log.debug("no remapping possible for disk %s. keep as is." % d)
                vdl.append(d)
                continue
            wwid = d[-35:-2]
            l = glob.glob("/dev/rdsk/*"+wwid+"s2")
            if len(l) != 1:
                self.log.warning("discard disk %s from disklist cache: not found" % wwid)
                continue
            self.log.debug("remapped device %s to %s" % (d, l[0]))
            vdl.append(l[0])
        return set(vdl)

    def _disklist(self):
        """disklist() search zpool vdevs from
        output of : zpool status poolname if status cmd == 0
        else
        output of : zpool import output if status cmd == 0

        disklist(self) update self.disks[]
        """

        # return cache if initialized
        if len(self.disks) > 0 :
            return self.disks

        disks = set([])
        cmd = [ 'zpool', 'status', self.name ]
        (ret, out, err) = self.call(cmd)
        if ret != 0:
            raise ex.excError

        for line in out.split('\n'):
            if re.match('^\t  ', line) is not None:
                if re.match('^\t  mirror', line) is not None:
                    continue
                if re.match('^\t  raid', line) is not None:
                    continue
                # vdev entry
                disk = line.split()[0]
                if disk.startswith(rcEnv.pathvar):
                    disk = disk.split('/')[-1]
                if re.match("^.*", disk) is None:
                    continue
                if not disk.startswith("/dev/rdsk/"):
                    disk = "/dev/rdsk/" + disk
                disks.add(disk)

        self.log.debug("found disks %s held by pool %s" % (disks, self.name))
        for d in disks:
            if "emcpower" in d:
                regex = re.compile('[a-g]$', re.UNICODE)
                d = regex.sub('c', d)
            elif re.match('^.*s[0-9]*$', d) is None:
                d += "s2"
            else:
                regex = re.compile('s[0-9]*$', re.UNICODE)
                d = regex.sub('s2', d)
            self.disks.add(d)

        return self.disks

if __name__ == "__main__":
    help(Disk)

    # return cache if initialized
    print("""p=Disk("svczfs1")""")
    p=Disk("svczfs1")
    print("show p", p)
    print("""p.do_action("start")""")
    p.do_action("start")
    print("""p.do_action("stop")""")
    p.do_action("stop")
