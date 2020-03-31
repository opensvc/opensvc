import sys
import os
import logging

import core.exceptions as ex
import utilities.devices.linux

from env import Env
from utilities.proc import justcall, vcall

class InitError(Exception):
     pass

class ExecError(Exception):
     pass

class ExistError(Exception):
     pass

def btrfs_devs(mnt):
    out, err, ret = justcall(["btrfs", "fi", "show", mnt])
    if ret != 0:
        return []
    devs = []
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith("devid"):
            continue
        dev = line.split(" path ")[-1]
        devs.append(dev)
    return devs

class Btrfs(object):
    log = None
    #snapvol = ".osvcsnap"
    snapvol = ""

    def __init__(self, path=None, label=None, node=None, resource=None):
        self.path = path
        self.label = label
        self.node = node
        self.resource = resource

        if self.resource is None:
            if Btrfs.log is None:
                Btrfs.log = logging.getLogger("BTRFS")
                Btrfs.log.addHandler(logging.StreamHandler(sys.stdout))
                Btrfs.log.setLevel(logging.INFO)
            self.log = Btrfs.log
        else:
            self.log = self.resource.log

        if path is not None:
            if not self.dir_exists(path):
                raise InitError("path %s does not exist"%path)
            self.get_label_from_path(path)

        if self.label is None:
            raise InitError("failed to determine btrfs label")

        self.setup_rootvol()
        self.path = self.rootdir
        self.snapdir = os.path.join(self.rootdir, self.snapvol)
        self.snapdir = os.path.normpath(self.snapdir)


    def get_dev(self):
        if hasattr(self, "dev"):
            return
        if self.node is None:
            try:
                self.dev = utilities.devices.linux.label_to_dev(
                    "LABEL="+self.label,
                    tree=self.resource.svc.node.devtree
                )
            except ex.Error as exc:
                self.dev = None
        else:
            return
        if self.dev is None:
            self.dev = "LABEL="+self.label

    def rmdir(self, path):
        cmd = ['rmdir', path]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error removing dir %s:\n%s"%(path,err))

    def dir_exists(self, path):
        cmd = ['test', '-d', path]
        out, err, ret = self.justcall(cmd)
        if ret > 1:
            raise ExecError("error joining remote node %s:\n%s"%(self.node, err))
        if ret == 1:
            return False
        return True

    def get_subvols(self, refresh=False):
        """
        ID 256 parent 5 top level 5 path btrfssvc
        ID 259 parent 256 top level 5 path btrfssvc/child
        ID 260 parent 5 top level 5 path btrfssvc@sent
        ID 261 parent 256 top level 5 path btrfssvc/child@sent
        ID 262 parent 5 top level 5 path btrfssvc@tosend
        ID 263 parent 256 top level 5 path btrfssvc/child@tosend
        ID 264 parent 5 top level 5 path subdir/vol
        ID 265 parent 256 top level 5 path btrfssvc/cross_mnt_snap
        """
        if not refresh and hasattr(self, "subvols"):
            return
        self.subvols = {}
        cmd = ['btrfs', 'subvol', 'list', '-p', self.path]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise InitError("error running btrfs subvol list %s:\n"%self.path+err)

        for line in out.split("\n"):
            if len(line) == 0:
                continue
            l = line.split()
            subvol = {}
            subvol['id'] = l[1]
            subvol['parent_id'] = l[3]
            subvol['top'] = l[6]
            subvol['path'] = line[line.index(" path ")+6:]
            self.subvols[subvol['id']] = subvol

    def subvol_delete(self, subvol=None, recursive=False):
        if subvol is None:
            subvol = []
        opts = []
        if recursive:
            opts.append('-R')

        if isinstance(subvol, list):
            subvols = subvol
        else:
            subvols = [subvol]

        # delete in descending depth order
        subvols.sort(reverse=True)

        cmd = []
        for subvol in subvols:
            if not self.has_subvol(subvol):
                continue
            cmd += ['btrfs', 'subvolume', 'delete'] + opts + [subvol, '&&']

        if len(cmd) == 0:
            return

        cmd = cmd[:-1]

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError()

    def get_subvols_in_path(self, path):
        self.get_subvols(refresh=True)
        head = self.path_to_subvol(path)
        subvols = [path]
        for subvol in self.subvols.values():
            if subvol['path'].startswith(head+'/'):
                subvols.append(self.rootdir+'/'+subvol['path'])
        return subvols

    def snapshot(self, origin, snap, readonly=False, recursive=False):
        if self.has_subvol(snap):
            raise ExistError("snapshot %s already exists"%snap)

        opts = []
        if recursive:
            opts.append('-R')
        if readonly:
            opts.append('-r')

        cmd = ['btrfs', 'subvolume', 'snapshot'] + opts + [origin, snap]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError(err)

    def has_snapvol(self):
        return self.has_subvol(self.snapvol)

    def path_to_subvol(self, path):
        if path.startswith('/'):
            return path.replace(self.rootdir+'/', "")
        return path

    def has_subvol(self, subvol):
        # refresh subvol list
        self.get_subvols(refresh=True)

        subvol = self.path_to_subvol(subvol)
        for sub in self.subvols.values():
            if sub['path'] == subvol:
                return True
        return False

    def mount_snapvol(self):
        self.get_dev()
        cmd = ['mount', '-t', 'btrfs', '-o', 'subvol='+self.snapvol, self.dev, self.snapdir]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError("error mounting %s subvol:\ncmd: %s\n%s"%(self.label,' '.join(cmd),err))

    def mount_rootvol(self):
        if self.node:
            return
        self.get_dev()
        if self.is_mounted_subvol(self.rootdir):
            return
        cmd = ['mount', '-t', 'btrfs', '-o', 'subvolid=0', self.dev, self.rootdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error mounting %s btrfs:\ncmd: %s\n%s"%(self.label,' '.join(cmd),err))

    def create_snapvol(self):
        self.get_dev()
        error = False

        import tempfile
        tmpdir = tempfile.mktemp()
        cmd = ['mkdir', '-p', tmpdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error creating dir %s:\n"%tmpdir+err)

        cmd = ['mount', '-t', 'btrfs', '-o', 'subvolid=0', self.dev, tmpdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            self.rmdir(tmpdir)
            raise ExecError("error mounting %s btrfs:\ncmd: %s\n%s"%(self.label,' '.join(cmd),err))

        try:
            self.create_subvol(os.path.join(tmpdir, self.snapvol))
        except:
            error = True

        cmd = ['umount', tmpdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error umounting %s btrfs:\n"%self.label+err)
        self.rmdir(tmpdir)
        if error:
            raise ExecError("failed to create %s"%self.snapvol)

    def vcall(self, cmd):
        if self.node is not None:
            cmd = [' '.join(cmd)]
            cmd = Env.rsh.split() + [self.node] + cmd

        return vcall(cmd, log=self.log)

    def justcall(self, cmd):
        if self.node is not None:
            cmd = [' '.join(cmd)]
            cmd = Env.rsh.split() + [self.node] + cmd
        return justcall(cmd)

    def create_subvol(self, path):
        cmd = ['btrfs', 'subvol', 'create', path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError("error creating %s subvol"%path)

    def setup_snap_subvol(self):
        # unused for now


        if not self.dir_exists(self.snapdir):
            cmd = ['mkdir', '-p', self.snapdir]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ExecError("error creating dir %s:\n"%self.snapdir+err)

        if not self.has_snapvol():
            self.create_snapvol()
            self.mount_snapvol()

        try:
            o = Btrfs(self.snapdir)
        except InitError:
            self.mount_snapvol()
            o = Btrfs(self.snapdir)

        if o.label != self.label:
            raise ExecError("wrong fs mounted in %s: %s"%(self.snapdir, o.label))

        # verify this is the right subvol (missing: path->subvol name fn)

    def setup_rootvol(self):
        self.rootdir = os.path.join(Env.paths.pathvar, 'btrfs', self.label)

        if not self.dir_exists(self.rootdir):
            cmd = ['mkdir', '-p', self.rootdir]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ExecError("error creating dir %s:\n"%self.rootdir+err)

        self.mount_rootvol()

    def setup_snap(self):
        if not self.has_snapvol():
            self.create_subvol(self.snapdir)
        try:
            o = Btrfs(self.snapdir)
        except InitError:
            self.mount_snapvol()
            o = Btrfs(self.snapdir)

        if o.label != self.label:
            raise ExecError("wrong fs mounted in %s: %s"%(self.snapdir, o.label))

        # verify this is the right subvol (missing: path->subvol name fn)

    def get_mounts(self):
        """
        /dev/vdb on /data type btrfs (rw) [data]
        """
        cmd = ['mount', '-t', 'btrfs', '-l']
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise InitError("error running %s:\n"%' '.join(cmd)+err)
        mounts = {}
        for line in out.split("\n"):
            if len(line) == 0 or " on " not in line or " type btrfs " not in line:
                continue
            mntpt = line[line.index(" on ")+4:line.index(" type btrfs ")]
            if '[' in line:
                l = line.split('[')
                label = l[-1].strip(']')
            else:
                label = self.get_label(mntpt)
            mounts[mntpt] = label
        return mounts

    def get_label(self, mntpt):
        cmd = ['btrfs', 'fi', 'label', mntpt]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ex.Error("error running %s:\n"%' '.join(cmd)+err)
        return out.strip('\n')

    def is_mounted_subvol(self, path):
        path = path.rstrip('/')
        for mntpt, label in self.get_mounts().items():
            if mntpt == path and label == self.label:
                return True
        return False

    def get_label_from_path(self, path):
        path = path.rstrip('/')

        mounts = self.get_mounts()
        l = path.split('/')
        while len(l) > 0:
            m = '/'.join(l)
            if m in mounts:
                self.label = mounts[m]
                return
            l = l[:-1]

        raise InitError("could not get label from path %s"%path)

    def parse_fi_show(self):
        """
        Label: 'data'  uuid: 0d05d0b9-ffab-4ab8-b923-15a38ec806d5
                Total devices 2 FS bytes used 48.92MB
                devid    2 size 5.00GB used 1.51GB path /dev/vdc
                devid    1 size 5.00GB used 1.53GB path /dev/vdb
        """
        cmd = ['btrfs', 'fi', 'show', self.path]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise InitError("error running btrfs fi show:\n"+err)

        self.devs = {}
        for line in out.split("\n"):
            if "Label:" in line:
                l = line.split("'")
                if len(l) != 2:
                    raise InitError("unexpected line format: "+line)
                label = l[1]

                l = line.split()
                uuid = l[-1]
            elif line.strip().startswith("devid"):
                l = line.split()
                self.devs[l[-1]] = (label, uuid)

    def get_transid(self, path):
        """
        /opt/opensvc/var/btrfs/win2/win2@sent
                Name:                   win2@sent
                uuid:                   167af15f-7d5a-2745-966c-dde4aaa056b7
                Parent uuid:            30121b33-a10f-a642-8caf-0184f5d8e5b0
                Creation time:          2015-09-02 04:01:20
                Object ID:              549
                Generation (Gen):       591564
                Gen at creation:        591564
                Parent:                 5
                Top Level:              5
                Flags:                  readonly
                Snapshot(s):
        """
        cmd = ['btrfs', 'subvolume', 'show', path]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ExecError("can't fetch %s transid:\n%s"%(path, err))
        for line in out.split("\n"):
            if "Generation" in line:
                return line.split()[-1]
        raise ExecError("can't find %s transid\n"%path)

    def __str__(self):
        self.get_subvols()
        s = "label: %s\n" % self.label
        s += "subvolumes:\n"
        for sub in self.subvols.values():
            s += "id: %s parent_id: %s top: %s path: %s\n"%(sub['id'], sub['parent_id'], sub['top'], sub['path'])
        return s

if __name__ == "__main__":
    o = Btrfs(label=sys.argv[1])
    print(o.get_transid("/opt/opensvc/var/btrfs/deb1/deb1@sent"))
    #o.setup_snap()

