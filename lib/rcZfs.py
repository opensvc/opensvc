#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
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
from rcUtilities import justcall, call
"""
"""

def dataset_exists(device, type):
    """return Dataset(device).exists(type)"""
    return Dataset(device).exists(type)

def zfs_getprop(dataset='undef_ds', propname='undef_prop'):
    "return zfs dataset property propname value"
    cmd = [ 'zfs', 'get', '-Hp', '-o', 'value', propname, dataset ]
    (stdout, stderr, retcode) = justcall(cmd)
    if retcode == 0 :
        return stdout.split("\n")[0]
    else:
        return ""

def zfs_setprop(dataset='undef_ds', propname='undef_prop', propval='undef_val'):
    "set zfs dataset property propname to value propval"
    if zfs_getprop(dataset, propname) == propval :
        return True
    cmd = [ 'zfs', 'set', propname + '='+ propval, dataset ]
    print ' '.join(cmd)
    (retcode, stdout, stderr) = call(cmd)
    if retcode == 0 :
        return True
    else:
        return False

def a2pool_dataset(s):
    """return (pool,dataset) from mount point
       example: a2pool_dataset('/') => ('rpool','rpool/ROOT/opensolaris-b134')
                same with a2pool_dataset('rpool/ROOT/opensolaris-b134')
    """
    if len(s) == 0:
        return ("", "")
    ss = s
    if s[0] == '/':
        cmd = ['zfs', 'list', '-H',  '-o', 'name', s]
        (ret, out, err) = call(cmd)
        if ret != 0:
            return ("", "")
        ss = out.split('\n')[0]
    x = ss.split('/')
    if len(x) < 2:
        return (ss, ss)
    return (x[0], ss)

class Dataset(object):
    """Define Dataset Class"""
    def __init__(self, name):
        self.name = name

    def __str__(self, option=None):
        if option is None:
            cmd = ['zfs', 'list', self.name ]
        else:
            cmd = ['zfs', 'list'] + option + [ self.name ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0:
            return stdout
        else:
            return "Failed to list info for dataset: %s" % (self.name)

    def destroy(self):
        "destroy dataset"
        cmd = ['zfs', 'destroy', self.name ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0:
            return True
        else:
            return False

    def exists(self, type="all"):
        "return True if dataset exists else return False"
        (out, err, ret) = justcall('zfs get -H -o value type'.split()+[self.name])
        if ret == 0 and out.split('\n')[0] == type :
            return True
        else:
            return False

    def create(self, option = None):
        "create dataset with options"
        if option is None:
            cmd = ['zfs', 'create', self.name ]
        else:
            cmd = ['zfs', 'create'] + option + [ self.name ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0:
            return True
        else:
            return False

    def getprop(self, propname):
        """get a dataset propertie value of dataset
        If success return propperty value
        else return ''
        """
        cmd = [ 'zfs', 'get', '-Hp', '-o', 'value', propname, self.name ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0 :
            return stdout.rstrip('\n')
        else:
            return ""

    def setprop(self, propname, propval):
        """set Dataset property value
        Return True is sucess else return False
        """
        cmd = [ 'zfs', 'set', propname + '='+ propval, self.name ]
        print ' '.join(cmd)
        (retcode, stdout, stderr) = call(cmd)
        if retcode == 0 :
            return True
        else:
            return False

    def snapshot(self, snapname=None):
        """snapshot dataset
        return snapshot dataset object
        Return False if failure
        """
        if snapname is None:
            raise(rcExceptions.excBug("snapname should be defined"))
        snapdataset = self.name + "@" + snapname
        cmd = ['zfs', 'snapshot', snapdataset ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0:
            return Dataset(snapdataset)
        else:
            return False

    def clone(self, name, option=None):
        """clone dataset with options
        return clone object
        return False if failure
        """
        if option is None:
            cmd = ['zfs', 'clone', self.name, name]
        else:
            cmd = ['zfs', 'clone'] + option + [ self.name, name ]
        (stdout, stderr, retcode) = justcall(cmd)
        if retcode == 0:
            return Dataset(name)
        else:
            return False

if __name__ == "__main__":
    dsname="rpool/toto"
    ds = Dataset(dsname)
    print "create dataset", ds.name
    if ds.create(option=[ "-o", "mountpoint=none"]) is False:
        print "========== Failed"
    else:
        print ds
    val = ds.setprop('opensvc:name', 'Example')
    print ds.__str__(["-Ho", "opensvc:name"])

    val = ds.getprop('opensvc:name')
    print "val Value=",val

    for sname in ["foo" , "bar"]:
        print "create snapshot for", sname
        s = ds.snapshot(sname)
        if s is False:
            print "========== Failed"
        else:
            print s
            print "create clone"
            c=s.clone(dsname + "/clone_"+ sname)
            if c is False:
                print "========== Failed"
            else:
                print c
                print "Destroy clone:", c.name
                c.destroy()

            print "Destroy snapshot for", sname
            if s.destroy() is False:
                print "========== Failed"

    if ds.exists:
        print "Destroy dataset", ds.name
        if ds.destroy() is False:
            print "Failed to create snapshot"

