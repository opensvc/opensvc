import sys
import os
import logging
import subprocess

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

    def __init__(self, path=None, label=None, node=None, resource=None):
        self.path = path
        self.label = label
        self.node = node
        self.resource = resource
        self.subvols = None

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

        self.rootdir = os.path.join(Env.paths.pathvar, 'btrfs', self.label)
        self.path = self.rootdir

        self.setup_rootvol()

    def get_dev(self):
        if hasattr(self, "dev"):
            return
        if self.node is None:
            if self.resource is None:
                tree = None
            else:
                tree = self.resource.svc.node.devtree
            try:
                self.dev = utilities.devices.linux.label_to_dev(
                    "LABEL="+self.label,
                    tree=tree,
                )
            except ex.Error as exc:
                self.dev = None
        else:
            return
        if self.dev is not None:
            return
        if self.label is not None:
            self.dev = "LABEL="+self.label
            return
        raise ex.Error("no dev nor label")

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

    def get_snaps_of(self, path, refresh=False):
        """
        Snaps are subvols with the same top as the subvol with <path>
        """
        self.get_subvols(refresh=refresh)
        l = []
        ref = None
        for sv in self.subvols.values():
            if sv["path"] == path:
                ref = sv
                break
        if ref is None:
            return []
        for sv in self.subvols.values():
            if sv["parent_uuid"] == ref["uuid"]:
                l.append(sv)
        return l

    def get_subvols(self, refresh=False):
        """
        ID 9203 gen 19446 parent 5 top level 5 parent_uuid 9a272ac8-b089-d540-8777-87535ee8f4be received_uuid d77d16ad-a1ad-f84f-8392-5d2a66fc6cbf uuid 23a3e8aa-8996-1e41-b810-017d4afc4c8a path .opensvc/snapshots/bt1@sent

           1        3            5           8             10                                                 12                                   13   14                                        16
        """
        if not refresh and self.subvols is not None:
            return self.subvols
        self.subvols = {}
        cmd = ['btrfs', 'subvol', 'list', '-qupR', self.path]
        out, err, ret = self.justcall(cmd)
        if ret != 0:
            cmd_string = subprocess.list2cmdline(cmd)
            if self.node is not None:
                self.log.warning("command failed on %s: %s", self.node, cmd_string)
            raise InitError("error running '%s': %s\n" % (cmd_string, err))

        for line in out.split("\n"):
            if len(line) == 0:
                continue
            l = line.split()
            subvol = {}
            subvol['id'] = int(l[1])
            subvol['gen'] = int(l[3])
            subvol['parent'] = int(l[5])
            subvol['top'] = int(l[8])
            subvol['parent_uuid'] = l[10]
            subvol['received_uuid'] = l[12]
            subvol['uuid'] = l[14]
            subvol['path'] = line[line.index(" path ")+6:]
            self.subvols[subvol['id']] = subvol
        return self.subvols

    def subvol_delete_cmd(self, subvol=None):
        if subvol is None:
            subvol = []
        opts = []

        if isinstance(subvol, list):
            subvols = subvol
        else:
            subvols = [subvol]

        # discard already deleted subvols
        subvols = [subvol for subvol in subvols if self.has_subvol(subvol)]
        if len(subvols) == 0:
            return

        # delete in descending depth order
        subvols.sort(reverse=True)

        cmd = ["btrfs", "subvolume", "delete"] + opts + subvols
        return cmd

    def subvol_delete(self, subvol=None):
        cmd = self.subvol_delete_cmd(subvol)
        if not cmd:
            return
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError()

    def fsfreeze(self):
        cmd = ["fsfreeze", "--freeze", self.rootdir]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError()

    def fsunfreeze(self):
        cmd = ["fsfreeze", "--unfreeze", self.rootdir]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError()

    def get_subvols_in_path(self, path, refresh=False):
        self.get_subvols(refresh=refresh)
        head = self.path_to_subvol(path)
        subvols = [path]
        for subvol in self.subvols.values():
            if subvol['path'].startswith(head+'/'):
                subvols.append(self.rootdir+'/'+subvol['path'])
        return subvols

    def snapshots(self, snaps, readonly=False):
        opts = []
        if readonly:
            opts.append('-r')

        cmds = []
        for s in snaps:
            origin = s[0]
            snap = s[1]
            cmd = ['btrfs', 'subvolume', 'snapshot'] + opts + [origin, snap]
            cmds += [subprocess.list2cmdline(cmd)]
        ret, out, err = self.vcall(" && ".join(cmds), shell=True)
        if ret != 0:
            raise ExecError(err)

    def snapshot_cmd(self, origin, snap, readonly=False):
        opts = []
        if readonly:
            opts.append('-r')
        cmd = ['btrfs', 'subvolume', 'snapshot'] + opts + [origin, snap]
        return cmd

    def snapshot(self, origin, snap, readonly=False):
        if self.has_subvol(snap):
            raise ExistError("snapshot %s already exists"%snap)
        cmd = self.snapshot_cmd(origin, snap, readonly)
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError(err)

    def path_to_subvol(self, path):
        if path.startswith('/'):
            return path.replace(self.rootdir+'/', "")
        return path

    def get_subvol(self, subvol, refresh=False):
        self.get_subvols(refresh=refresh)

        subvol = self.path_to_subvol(subvol)
        for sub in self.subvols.values():
            if sub['path'] == subvol:
                return sub
        return None

    def has_subvol(self, subvol, refresh=False):
        # refresh subvol list
        self.get_subvols(refresh=refresh)

        if isinstance(subvol, str):
            subvol = self.path_to_subvol(subvol)
        for sub in self.subvols.values():
            if sub['path'] == subvol:
                return True
        return False

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

    def vcall(self, cmd, shell=False):
        if self.node is not None:
            lcmd = Env.rsh.split() + [self.node]
            if shell:
                rcmd = cmd
                cmd = lcmd + [rcmd]
                cmd = subprocess.list2cmdline(cmd)
            else:
                rcmd = subprocess.list2cmdline(cmd)
                cmd = lcmd + [rcmd]

        return vcall(cmd, log=self.log, shell=shell)

    def justcall(self, cmd):
        if self.node is not None:
            cmd = subprocess.list2cmdline(cmd)
            cmd = Env.rsh.split() + [self.node, cmd]
        return justcall(cmd)

    def create_subvol(self, path):
        cmd = ['btrfs', 'subvol', 'create', path]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ExecError("error creating %s subvol"%path)

    def setup_rootvol(self):
        if not self.dir_exists(self.rootdir):
            cmd = ['mkdir', '-p', self.rootdir]
            ret, out, err = self.vcall(cmd)
            if ret != 0:
                raise ExecError("error creating dir %s:\n"%self.rootdir+err)
        self.mount_rootvol()

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

    def get_subvol_path(self, path):
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
            raise ExecError("get_subvol_path: %s\n%s"%(path, err))
        for line in out.split("\n"):
            return line
        raise ExecError("can't find %s path relative to the btrfs root\n" % path)

    def __str__(self):
        self.get_subvols()
        s = "label: %s\n" % self.label
        s += "subvolumes:\n"
        for sub in self.subvols.values():
            s += "uuid: %s parent: %s top: %s path: %s\n"%(sub['uuid'], sub['parent_uuid'], sub['top'], sub['path'])
        return s

if __name__ == "__main__":
    o = Btrfs(label=sys.argv[1])
    #print(o)
    for sub in o.get_snaps_of("bt1/child1/1a"):
        print(sub)
    #print(o.get_transid("/opt/opensvc/var/btrfs/deb1/deb1@sent"))

