from rcUtilities import justcall, call, vcall
from rcGlobalEnv import rcEnv
import rcExceptions as ex
import logging
import sys
"""
"""

def dataset_exists(device, dstype):
    """
    Return True if the data exists.
    """
    return Dataset(device).exists(dstype)

def zfs_getprop(dataset='undef_ds', propname='undef_prop'):
    """
    Return the dataset property <propname> value
    """
    cmd = [rcEnv.syspaths.zfs, 'get', '-Hp', '-o', 'value', propname, dataset]
    (stdout, stderr, retcode) = justcall(cmd)
    if retcode == 0 :
        return stdout.split("\n")[0]
    else:
        return ""

def zfs_setprop(dataset='undef_ds', propname='undef_prop', propval='undef_val'):
    """
    Set the dataset property <propname> to value <propval>.
    """
    if zfs_getprop(dataset, propname) == propval :
        return True
    cmd = [rcEnv.syspaths.zfs, 'set', propname + '='+ propval, dataset]
    print(' '.join(cmd))
    (retcode, stdout, stderr) = vcall(cmd)
    if retcode == 0 :
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
        cmd = [rcEnv.syspaths.zfs, 'list', '-H',  '-o', 'name', s]
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
            cmd = [rcEnv.syspaths.zfs, 'list', self.name]
        else:
            cmd = [rcEnv.syspaths.zfs, 'list'] + option + [self.name]
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
            cmd = [rcEnv.syspaths.zfs, 'rename', self.name, name]
        else:
            cmd = [rcEnv.syspaths.zfs, 'rename'] + option + [self.name, name]
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
            cmd = [rcEnv.syspaths.zfs, 'create', self.name]
        else:
            cmd = [rcEnv.syspaths.zfs, 'create'] + option + [self.name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return True
        else:
            return False

    def destroy(self, options=[]):
        """
        Destroy the dataset.
        """
        if not self.exists():
            return True
        cmd = [rcEnv.syspaths.zfs, 'destroy'] + options + [self.name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return True
        else:
            return False

    def getprop(self, propname):
        """
        Return a dataset propertie value or '' on error.
        """
        cmd = [rcEnv.syspaths.zfs, 'get', '-Hp', '-o', 'value', propname, self.name]
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
        cmd = [rcEnv.syspaths.zfs, 'set', propname + '='+ propval, self.name]
        ret, out, err = vcall(cmd, log=self.log,
                              err_to_warn=err_to_warn,
                              err_to_info=err_to_info)
        if ret == 0 :
            return True
        else:
            return False

    def verify_prop(self, nv_pairs={}, err_to_warn=False, err_to_info=False):
        """
        For name, val from nv_pairs dict,
        if zfs name property value of dataset differ from val
        then zfs set name=value on dataset object.
        """
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
            raise ex.excError("snapname should be defined")
        snapdataset = self.name + "@" + snapname
        cmd = [rcEnv.syspaths.zfs, 'snapshot']
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
            cmd = [rcEnv.syspaths.zfs, 'clone', self.name, name]
        else:
            cmd = [rcEnv.syspaths.zfs, 'clone'] + option + [self.name, name]
        ret, _, _ = vcall(cmd, log=self.log)
        if ret == 0:
            return Dataset(name)
        else:
            return False

