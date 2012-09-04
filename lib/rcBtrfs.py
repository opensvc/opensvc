#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
from rcUtilities import justcall, vcall
import sys
import os
import logging
from rcGlobalEnv import rcEnv

class InitError(Exception):
     pass

class ExecError(Exception):
     pass

class ExistError(Exception):
     pass

class Btrfs(object):
    log = None
    #snapvol = ".osvcsnap"
    snapvol = ""

    def __init__(self, path=None, label=None, node=None, log=None):
        self.path = path
        self.label = label
        self.node = node

        if log is None:
            if Btrfs.log is None:
                Btrfs.log = logging.getLogger("BTRFS")
                Btrfs.log.addHandler(logging.StreamHandler(sys.stdout))
                Btrfs.log.setLevel(logging.INFO)
            self.log = Btrfs.log
        else:
            self.log = log

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

        self.get_subvols()

    def rmdir(self, path):
        cmd = ['rmdir', path]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error removing dir %s:\n"%path+err)

    def dir_exists(self, path):
        cmd = ['test', '-d', path]
        out, err, ret = self.justcall(cmd)
        if ret > 1:
            raise ExecError("error joining remote node %s\n"%(self.node,err))
        if ret == 1:
            return False
        return True

    def get_subvols(self):
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

    def subvol_delete(self, subvol=[], recursive=False):
        opts = []
        if recursive:
            opts.appendi('-R')

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
        self.get_subvols()
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
            raise ExecError()

    def has_snapvol(self):
        return self.has_subvol(self.snapvol)

    def path_to_subvol(self, path):
        if path.startswith('/'):
            return path.replace(self.rootdir+'/', "")
        return path

    def has_subvol(self, subvol):
        # refresh subvol list
        self.get_subvols()

        subvol = self.path_to_subvol(subvol)
        for sub in self.subvols.values():
            if sub['path'] == subvol:
                return True
        return False

    def mount_snapvol(self):
        cmd = ['mount', '-t', 'btrfs', '-o', 'subvol='+self.snapvol, 'LABEL='+self.label, self.snapdir]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError("error mounting %s subvol:\ncmd: %s\n%s"%(self.label,' '.join(cmd),err))

    def mount_rootvol(self):
        if self.is_mounted_subvol(self.rootdir):
            return
        cmd = ['mount', '-t', 'btrfs', '-o', 'subvolid=0', 'LABEL="%s"'%self.label, self.rootdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error mounting %s btrfs:\ncmd: %s\n%s"%(self.label,' '.join(cmd),err))

    def create_snapvol(self):
        error = False

        import tempfile
        tmpdir = tempfile.mktemp()
        cmd = ['mkdir', '-p', tmpdir]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            raise ExecError("error creating dir %s:\n"%tmpdir+err)

        cmd = ['mount', '-t', 'btrfs', '-o', 'subvolid=0', 'LABEL="%s"'%self.label, tmpdir]
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
            cmd = rcEnv.rsh.split() + [self.node] + cmd

        return vcall(cmd, log=self.log)

    def justcall(self, cmd):
        if self.node is not None:
            cmd = [' '.join(cmd)]
            cmd = rcEnv.rsh.split() + [self.node] + cmd
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
        pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
        pathvar = os.path.join(pathsvc, 'var')
        self.rootdir = os.path.join(pathvar, 'btrfs', self.label)

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
            l = line.split('[')
            label = l[-1].strip(']')
            mntpt = line[line.index(" on ")+4:line.index(" type btrfs ")]
            mounts[mntpt] = label
        return mounts

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
        cmd = ['btrfs', 'fi', 'show', path]
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
        cmd = ['btrfs', 'subvolume', 'find-new', path, '-1']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ExecError("can't fetch %s transid:\n%s"%(path, err))
        return out.split()[-1]

    def __str__(self):
        s = "label: %s\n" % self.label
        s += "subvolumes:\n"
        for sub in self.subvols.values():
            s += "id: %s parent_id: %s top: %s path: %s\n"%(sub['id'], sub['parent_id'], sub['top'], sub['path'])
        return s

if __name__ == "__main__":
    o = Btrfs(label=sys.argv[1], node="deb2")
    print o
    #o.setup_snap()

