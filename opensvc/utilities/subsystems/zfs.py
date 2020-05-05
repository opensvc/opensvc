import logging
import sys

from env import Env
import core.exceptions as ex
from utilities.cache import cache
from utilities.proc import call, vcall, justcall

def dataset_exists(device, dstype):
    """
    Return True if the data exists.
    """
    return Dataset(device).exists(dstype)

def zpool_getprop(pool='undef_pool', propname='undef_prop'):
    """
    Return the zpool property <propname> value
    """
    cmd = [Env.syspaths.zpool, 'get', '-Hp', '-o', 'value', propname, pool]
    out, _, ret = justcall(cmd)
    if ret == 0:
        return out.split("\n")[0]
    else:
        return ""

def zpool_setprop(pool='undef_pool', propname='undef_prop', propval='undef_val', log=None):
    """
    Set the dataset property <propname> to value <propval>.
    """
    current = zpool_getprop(pool, propname)
    if current == "":
        # pool does not exist
        return False
    if current == propval:
        return True
    cmd = [Env.syspaths.zpool, 'set', propname + '='+ propval, pool]
    ret, _, _ = vcall(cmd, log=log)
    if ret == 0:
        return True
    else:
        return False

def zfs_getprop(dataset='undef_ds', propname='undef_prop'):
    """
    Return the dataset property <propname> value
    """
    cmd = [Env.syspaths.zfs, 'get', '-Hp', '-o', 'value', propname, dataset]
    out, _, ret = justcall(cmd)
    if ret == 0:
        return out.split("\n")[0]
    else:
        return ""

def zfs_setprop(dataset='undef_ds', propname='undef_prop', propval='undef_val', log=None):
    """
    Set the dataset property <propname> to value <propval>.
    """
    current = zfs_getprop(dataset, propname)
    if current == "":
        # dataset does not exist
        return False
    if current == propval:
        return True
    cmd = [Env.syspaths.zfs, 'set', propname + '='+ propval, dataset]
    ret, _, _ = vcall(cmd, log=log)
    if ret == 0:
        return True
    else:
        return False

def a2pool_dataset(s):
    """
    Return the (pool, dataset) tuple from a mount point.

    Examples:
    * / => ('rpool','rpool/ROOT/opensolaris-b134')
    * rpool/ROOT/opensolaris-b134 => ('rpool','rpool/ROOT/opensolaris-b134')
    """
    if len(s) == 0:
        return ("", "")
    ss = s
    if s[0] == '/':
        cmd = [Env.syspaths.zfs, 'list', '-H',  '-o', 'name', s]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return ("", "")
        ss = out.split('\n')[0]
    x = ss.split('/')
    if len(x) < 2:
        return (ss, ss)
    return (x[0], ss)

class Dataset(object):
    """
    A class exposing usual ops on a zfs dataset.
    """
    log = None
    def __init__(self, name, log=None):
        self.name = name
        if log is None:
            if Dataset.log is None:
                Dataset.log = logging.getLogger("DATASET".upper())
                Dataset.log.addHandler(logging.StreamHandler(sys.stdout))
                Dataset.log.setLevel(logging.INFO)
            self.log = Dataset.log
        else:
            self.log = log
    def __str__(self, option=None):
        if option is None:
            cmd = [Env.syspaths.zfs, 'list', self.name]
        else:
            cmd = [Env.syspaths.zfs, 'list'] + option + [self.name]
        (retcode, stdout, stderr) = call(cmd, log=self.log)
        if retcode == 0:
            return stdout
        else:
            return "Failed to list info for dataset: %s" % (self.name)

    def exists(self, type="all"):
        """
        Return True if the dataset exists else return False.
        If type is set, also verify the dataset type.
        """
        out, err, ret = justcall(["zfs", "get", "-H", "-o", "value", "type", self.name])
        if ret == 0 and type == "all":
            return True
        elif ret == 0 and out.split('\n')[0] == type:
            return True
        else:
            return False

    def rename(self, name=None, option=None):
        """
        Rename the dataset.
        """
        if option is None:
            cmd = [Env.syspaths.zfs, 'rename', self.name, name]
        else:
            cmd = [Env.syspaths.zfs, 'rename'] + option + [self.name, name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return True
        else:
            return False

    def create(self, option=None):
        """
        Create the dataset with options.
        """
        if option is None:
            cmd = [Env.syspaths.zfs, 'create', self.name]
        else:
            cmd = [Env.syspaths.zfs, 'create'] + option + [self.name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return True
        else:
            return False

    def destroy(self, options=None):
        """
        Destroy the dataset.
        """
        if options is None:
            options = []
        if not self.exists():
            return True
        cmd = [Env.syspaths.zfs, 'destroy'] + options + [self.name]
        if self.log:
            self.log.info(" ".join(cmd))
        _, err, ret = justcall(cmd)
        if ret == 0:
            return True
        elif "could not find any snapshot" in err:
            return True
        elif "dataset does not exist" in err:
            return True
        else:
            return False

    def getprop(self, propname):
        """
        Return a dataset propertie value or '' on error.
        """
        cmd = [Env.syspaths.zfs, 'get', '-Hp', '-o', 'value', propname, self.name]
        out, _, ret = justcall(cmd)
        if ret == 0 :
            return out.rstrip('\n')
        else:
            return ""

    def setprop(self, propname, propval, err_to_warn=False, err_to_info=False):
        """
        Set Dataset property value.
        Return True is success else return False.
        """
        cmd = [Env.syspaths.zfs, 'set', propname + '='+ propval, self.name]
        ret, out, err = vcall(cmd, log=self.log,
                              err_to_warn=err_to_warn,
                              err_to_info=err_to_info)
        if ret == 0 :
            return True
        else:
            return False

    def verify_prop(self, nv_pairs=None, err_to_warn=False, err_to_info=False):
        """
        For name, val from nv_pairs dict,
        if zfs name property value of dataset differ from val
        then zfs set name=value on dataset object.
        """
        if nv_pairs is None:
            nv_pairs = {}
        for name in nv_pairs.keys():
            if self.getprop(name) != nv_pairs[name]:
                self.setprop(propname=name, propval=nv_pairs[name],
                            err_to_warn=err_to_warn,
                            err_to_info=err_to_info)

    def snapshot(self, snapname=None, recursive=False):
        """
        Snapshot the dataset.
        Return the snapshot dataset object.
        Return False on error.
        """
        if snapname is None:
            raise ex.Error("snapname should be defined")
        snapdataset = self.name + "@" + snapname
        cmd = [Env.syspaths.zfs, 'snapshot']
        if recursive:
            cmd.append("-r")
        cmd.append(snapdataset)
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return Dataset(snapdataset)
        else:
            return False

    def clone(self, name, option=None):
        """
        Clone the dataset with options
        Return the clone dataset object.
        Return False on failure.
        """
        if option is None:
            cmd = [Env.syspaths.zfs, 'clone', self.name, name]
        else:
            cmd = [Env.syspaths.zfs, 'clone'] + option + [self.name, name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return Dataset(name)
        else:
            return False

@cache("zpool.devs.{args[0]}")
def zpool_devs(poolname, node=None):
    """
    Search zpool vdevs from the output of "zpool status <poolname>" if
    imported.
    """
    devs = set()
    cmd = ['zpool', 'status']
    if Env.sysname == "Linux":
        cmd += ["-L", "-P"]
    cmd += [poolname]
    out, err, ret = justcall(cmd)
    if ret != 0:
        return []

    import re

    for line in out.split('\n'):
        if re.match('^\t  ', line) is not None:
            if re.match('^\t  mirror', line) is not None:
                continue
            if re.match('^\t  raid', line) is not None:
                continue
            # vdev entry
            disk = line.split()[0]
            if Env.sysname == "SunOS":
                if disk.startswith(Env.paths.pathvar):
                    disk = disk.split('/')[-1]
                if re.match("^.*", disk) is None:
                    continue
                if not disk.startswith("/dev/rdsk/"):
                    disk = "/dev/rdsk/" + disk
            devs.add(disk)

    vdevs = set()
    for d in devs:
        if "emcpower" in d:
            regex = re.compile('[a-g]$', re.UNICODE)
            d = regex.sub('c', d)
        elif Env.sysname == "SunOS":
            if re.match('^.*s[0-9]*$', d) is None:
                d += "s2"
            else:
                regex = re.compile('s[0-9]*$', re.UNICODE)
                d = regex.sub('s2', d)
        elif Env.sysname == "Linux" and node:
            tdev = node.devtree.get_dev_by_devpath(d)
            if tdev is None:
                continue
            for path in tdev.devpath:
                if "/dev/mapper/" in path or "by-id" in path:
                    d = path
                    break
        vdevs.add(d)

    return list(vdevs)

