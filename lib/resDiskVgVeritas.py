import resDisk
import re
from rcUtilities import qcall

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
                              type='disk.vg',
                              **kwargs)
        self.label = "vg "+str(name)

    def has_it(self):
        """Returns True if the vg is present
        """
        ret = qcall( [ 'vxdg', 'list', self.name ] )
        if ret == 0 :
            return True
        else:
            return False

    def is_up(self):
        """Returns True if the vg is present and not disabled
        """
        if not self.has_it():
            return False
        cmd = [ 'vxprint', '-ng', self.name ]
        ret = qcall(cmd)
        if ret == 0 :
                return True
        else:
                return False

    def do_startvol(self):
        cmd = [ 'vxvol', '-g', self.name, '-f', 'startall' ]
        (ret, out, err) = self.vcall(cmd)
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

    def sub_devs(self):
        """
        parse "vxdisk -g <vgname> -q path"
        """
        if len(self.sub_devs_cache) > 0 :
            return self.sub_devs_cache

        devs = set([])
        cmd = [ 'vxdisk', '-g', self.name, '-q', 'list' ]
        (ret, out, err) = self.call(cmd, errlog=False)
        if ret != 0 :
            self.sub_devs_cache = devs
            return devs
        for line in out.split('\n'):
            dev = line.split(" ")[0]
            if dev != '' :
                if re.match('^.*s[0-9]$', dev) is None:
                    dev += "s2"
                devs.add("/dev/rdsk/" + dev)

        self.log.debug("found devs %s held by vg %s" % (devs, self.name))
        self.sub_devs_cache = devs

        return devs

