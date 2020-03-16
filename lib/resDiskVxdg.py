import resDisk
import re
from collections import namedtuple
from rcGlobalEnv import rcEnv

import rcExceptions as ex
from rcUtilities import qcall, which, justcall
from svcBuilder import init_kwargs


def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["name"] = svc.oget(s, "name")
    r = Disk(**kwargs)
    svc += r


class Disk(resDisk.Disk):
    """ basic Veritas Volume group resource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                              rid=rid,
                              name=name,
                              type='disk.vxdg',
                              **kwargs)
        self.label = "vxdg "+str(name)
        self.sub_devs_cache = set()

    def vxprint(self):
        cmd = ["vxprint", "-g", self.name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        data = {}
        for line in out.splitlines():
            words = line.split()
            if len(words) < 7:
                continue
            if words[0] == "TY":
                headers = list(words)
                continue
            line = namedtuple("line", headers)._make(words)
            data[(line.TY, line.NAME)] = line
        return data

    def has_it(self):
        """
        Return True if the vg is present
        """
        if not which("vxdg"):
            raise ex.excError("vxdg command not found")
        ret = qcall(["vxdg", "list", self.name])
        if ret == 0 :
            return True
        else:
            return False

    def is_up(self):
        """Returns True if the vg is present and not disabled
        """
        if not which("vxdg"):
            self.status_log("vxdg command not found")
            return False
        if not self.has_it():
            return False
        cmd = ["vxprint", "-ng", self.name]
        ret = qcall(cmd)
        if ret == 0 :
            return True
        else:
            return False

    def defects(self):
        try:
            data = self.vxprint()
        except ex.excError:
            # dg does not exist
            return []
        errs = ["%s:%s:%s" % (key[0], key[1], val.STATE) for key, val in data.items() if val.STATE not in ("-", "ACTIVE")]
        errs += ["%s:%s:%s" % (key[0], key[1], val.KSTATE) for key, val in data.items() if val.KSTATE not in ("-", "ENABLED")]
        return sorted(errs)

    def _status(self, **kwargs):
        for defect in self.defects():
             self.status_log(defect, "warn")
        return resDisk.Disk._status(self, **kwargs)

    def has_vxvol_resources(self):
        for res in self.svc.get_resources("disk.vxvol"):
            if res.vg == self.name:
                return True
        return False

    def do_startvol(self):
        if self.has_vxvol_resources():
            return 0
        cmd = ['vxvol', '-g', self.name, '-f', 'startall']
        ret, out, err = self.vcall(cmd)
        return ret

    def do_stopvol(self):
        cmd = [ 'vxvol', '-g', self.name, '-f', 'stopall' ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            ret = self.do_startvol()
            if ret == 0 :
                return 0
            else:
                return ret
        self.can_rollback = True
        for flag in [ '-t', '-tC', '-tCf']:
            cmd = [ 'vxdg', flag, 'import', self.name ]
            (ret, out, err) = self.vcall(cmd)
            if ret == 0 :
                ret = self.do_startvol()
                return ret
        return ret

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        ret = self.do_stopvol()
        cmd = [ 'vxdg', 'deport', self.name ]
        (ret, out, err) = self.vcall(cmd)
        return ret

    def vxdisk_list(self):
        if not which("vxdisk"):
            return {}
        cmd = ["vxdisk", "list"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        data = {}
        for line in out.splitlines():
            words = line.split(None, 4)
            if len(words) < 5:
                continue
            if words[0] == "DEVICE":
                headers = list(words)
                continue
            dev = namedtuple("dev", headers)._make(words)
            if dev.GROUP != self.name and dev.GROUP != "(%s)"%self.name:
                continue
            data[dev.DEVICE] = dev
        return data

    def sub_devs(self):
        """
        Return the set of devices used by the dg.
        """
        if hasattr(self, "sub_devs_cache") and len(self.sub_devs_cache) > 0:
            return self.sub_devs_cache

        devs = ["/dev/vx/dsk/"+dev for dev in self.vxdisk_list()]
        if rcEnv.sysname == "SunOS":
            for idx, dev in enumerate(devs):
                if re.match('^.*s[0-9]$', dev) is None:
                    devs[idx] += "s2"

        self.log.debug("found devs %s held by vg %s" % (devs, self.name))
        self.sub_devs_cache = devs

        return devs

