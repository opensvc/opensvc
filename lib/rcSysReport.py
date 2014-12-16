from __future__ import print_function
import os
import shutil
import glob
from rcGlobalEnv import rcEnv
from rcUtilities import which, cmdline2list
from stat import *
from subprocess import *

class SysReport(object):
    def __init__(self, node=None):
        self.todo = [
          ('FILE', '/opt/opensvc/etc/node.conf'),
          ('GLOB', '/opt/opensvc/etc/*env'),
          ('DIR', '/opt/opensvc/etc/sysreport.conf.d'),
        ]

        self.changed = []
        self.full = []
        self.sysreport_conf_d = os.path.join(rcEnv.pathetc, "sysreport.conf.d")
        self.sysreport_d = os.path.join(rcEnv.pathvar, "sysreport")
        self.collect_d = os.path.join(self.sysreport_d, rcEnv.nodename)
        self.collect_cmd_d = os.path.join(self.collect_d, "cmd")
        self.collect_file_d = os.path.join(self.collect_d, "file")
        self.root_uid = 0
        self.root_gid = 0
        self.node = node
        self.archive_extension = '.tar'
        self.send_rpc = "send_sysreport"

    def init(self):
        self.init_dir(self.collect_d)
        self.init_dir(self.collect_cmd_d)
        self.init_dir(self.collect_file_d)
        self.init_dir(self.sysreport_conf_d)
        self.merge_todo()

    def init_dir(self, fpath):
        self.init_collect_d(fpath)
        self.init_collect_d_ownership(fpath)
        self.init_collect_d_perms(fpath)

    def init_collect_d(self, fpath):
        if not os.path.exists(fpath):
            print("create dir", fpath)
            os.makedirs(fpath)

    def init_collect_d_perms(self, fpath):
        s = os.stat(fpath)
        mode = s[ST_MODE]
        if mode != 16768:
            print("set dir", fpath, "mode to 0600")
            os.chmod(fpath, 0600)

    def init_collect_d_ownership(self, fpath):
        s = os.stat(fpath)
        if s.st_uid != self.root_uid or s.st_gid != self.root_gid:
            print("set dir", self.collect_d, "ownership to", self.root_uid, self.root_gid)
            os.chown(self.collect_d, self.root_uid, self.root_gid)

    def merge_todo(self):
        for root, dnames, fnames in os.walk(self.sysreport_conf_d):
            for fname in fnames:
                fpath = os.path.join(self.sysreport_conf_d, fname)
                s = os.stat(fpath)
                mode = s[ST_MODE]
                if mode & S_IWOTH:
                    print("skip %s config file: file mode is insecure ('other' has write permission)" % fpath)
                    continue
                if s.st_uid != self.root_uid or s.st_gid != self.root_gid:
                    print("skip %s config file: file ownership is insecure (must be owned by root)" % fpath)
                    continue

                with open(fpath, 'r') as f:
                    buff = f.read()
                for line in buff.split("\n"):
                    if line.startswith("FILE"):
                        t = ("FILE", line[4:].strip())
                    elif line.startswith("CMD"):
                        t = ("CMD", line[3:].strip())
                    elif line.startswith("DIR"):
                        t = ("DIR", line[3:].strip())
                    elif line.startswith("GLOB"):
                        t = ("GLOB", line[4:].strip())
                    else:
                        continue
                    if t not in self.todo:
                        self.todo.append(t)

    def cmdlist2fname(self, l):
        fname = '_'.join(l)
        fname = fname.replace('|','(pipe)')
        fname = fname.replace('&','(amp)')
        fname = fname.replace('$','(dollar)')
        fname = fname.replace('^','(caret)')
        fname = fname.replace('/','(slash)')
        fname = fname.replace(':','(colon)')
        fname = fname.replace(';','(semicolon)')
        fname = fname.replace('<','(lt)')
        fname = fname.replace('>','(gt)')
        fname = fname.replace('=','(eq)')
        fname = fname.replace('?','(question)')
        fname = fname.replace('@','(at)')
        fname = fname.replace('!','(excl)')
        fname = fname.replace('#','(num)')
        fname = fname.replace('%','(pct)')
        fname = fname.replace('"','(dquote)')
        fname = fname.replace("'",'(squote)')
        return fname

    def write(self, fpath, buff):
        try:
            with open(fpath, 'r') as f:
                pbuff = f.read()
            if buff != pbuff:
                self.changed.append(fpath)
        except IOError:
            self.changed.append(fpath)
        with open(fpath, 'w') as f:
            f.write(buff)
        self.full.append(fpath)

    def collect_cmd(self, cmd):
        l = cmdline2list(cmd)
        if len(l) == 0:
            print(" err: syntax error")
            return
        if not os.path.exists(l[0]):
            return
        if which(l[0]) is None:
            print(" err: not executable")
            return
        fname = self.cmdlist2fname(l)
        cmd_d = os.path.join(self.collect_cmd_d, fname)
        p = Popen(l, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        self.write(os.path.join(cmd_d), out)

    def collect_file(self, fpath):
        if not os.path.exists(fpath):
            return
        dst_d = os.path.join(self.collect_file_d) + os.path.dirname(fpath)
        fname = os.path.basename(fpath)
        dst_f = os.path.join(dst_d, fname)
        if not os.path.exists(dst_d):
            os.makedirs(dst_d)
        try:
            with open(fpath, 'r') as f:
                buff = f.read()
            with open(dst_f, 'r') as f:
                pbuff = f.read()
            if buff != pbuff:
                self.changed.append(dst_f)
        except IOError:
            # in doubt, send ... git will know better on the collector
            self.changed.append(dst_f)
        shutil.copy2(fpath, dst_f)
        self.full.append(dst_f)

    def collect_glob(self, fpath):
        for fp in glob.glob(fpath):
            self.collect_file(fp)

    def collect_dir(self, fpath):
        if not os.path.exists(fpath):
            return
        if not os.path.isdir(fpath):
            print(" err: not a directory")
            return
        self.recurse_dir(fpath)

    def recurse_dir(self, fpath):
        for item in os.listdir(fpath):
            _fpath = os.path.join(fpath, item)
            if os.path.isdir(_fpath):
                self.recurse_dir(_fpath)
            elif not os.path.islink(_fpath):
                self.collect_file(_fpath)

    def collect(self, item):
        i_type, i_ref = item
        print("  collecting", i_type, i_ref)
        if i_type == 'CMD':
            self.collect_cmd(i_ref)
        elif i_type == 'FILE':
            self.collect_file(i_ref)
        elif i_type == 'DIR':
            self.collect_dir(i_ref)
        elif i_type == 'GLOB':
            self.collect_glob(i_ref)
        else:
            print("unsupported item type", i_type, "for ref", i_ref)

    def sysreport(self, force=False):
        self.node.collector.init(self.send_rpc)
        if self.node.collector.proxy is None:
            print("no collector connexion. abort sysreport")
            return 1
        self.init()
        print("collect directory is", self.collect_d)
        for item in self.todo:
            self.collect(item)
        self.send(force)

    def send(self, force=False):
        if force:
            to_send = self.full
        else:
            to_send = self.changed
            self.changed_report()
        if len(to_send) == 0:
            print("no change to report")
            return
        tmpf = self.archive(to_send)
        print("sending archive", tmpf)
        self.node.collector.call(self.send_rpc, tmpf)
        print("deleting archive", tmpf)
        os.unlink(tmpf)

    def archive(self, l):
        import tarfile
        import tempfile
        f = tempfile.NamedTemporaryFile(prefix="sysreport.", suffix=self.archive_extension, dir=self.collect_d)
        tmpf = f.name
        f.close()
        cwd = os.getcwd()
        os.chdir(self.sysreport_d)
        n = len(self.sysreport_d) + 1
        print("creating tarball", tmpf)
        tar = tarfile.open(tmpf, mode="w")
        for fpath in l:
            if len(fpath) < n:
                print(" err: can not archive", fpath, "(fpath too short)")
                continue
            tar.add(fpath[n:])
        tar.close()
        os.chdir(cwd)
        return tmpf

    def changed_report(self):
        if len(self.changed) > 0:
            print("changed files:")
            for fpath in self.changed:
                print(" ", fpath)

