from __future__ import print_function

import hashlib
import os
import sys
import shutil
import shlex
import glob
import json
from stat import *
from subprocess import *

import foreign.six as six
import core.exceptions as ex
from env import Env
from utilities.proc import which

POSIX = os.name == "posix"


class BaseSysReport(object):
    def __init__(self, node=None, collect_d=None, compress=False):
        self.todo = [
            ("INC", os.path.join(Env.paths.pathetc, "*.conf")),
            ("INC", os.path.join(Env.paths.pathetc, "namespaces", "*", "*", "*.conf")),
            ("INC", os.path.join(Env.paths.pathetc, "sysreport.conf.d")),
        ]

        self.changed = []
        self.deleted = []
        self.sysreport_conf_d = os.path.join(Env.paths.pathetc, "sysreport.conf.d")
        if collect_d is None:
            self.sysreport_d = os.path.join(Env.paths.pathvar, "sysreport")
            self.collect_d = os.path.join(self.sysreport_d, Env.nodename)
        else:
            self.sysreport_d = collect_d
            self.collect_d = collect_d
        self.collect_cmd_d = os.path.join(self.collect_d, "cmd")
        self.collect_file_d = os.path.join(self.collect_d, "file")

        if collect_d is None:
            self.collect_stat = os.path.join(self.collect_file_d, "stat")
            self.full = [self.collect_stat]
        else:
            self.collect_stat = None
            self.full = []
        self.stat_changed = False
        self.root_uid = 0
        self.root_gid = 0
        self.node = node
        if compress:
            self.archive_extension = '.tar.gz'
            self.tarmode = "w:gz"
        else:
            self.archive_extension = '.tar'
            self.tarmode = "w"
        self.send_rpc = "send_sysreport"
        self.lstree_rpc = "sysreport_lstree"

    def init(self):
        self.init_dir(self.collect_d)
        self.init_dir(self.collect_cmd_d)
        self.init_dir(self.collect_file_d)
        self.init_dir(self.sysreport_conf_d)
        self.load_stat()
        self.merge_todo()

    def init_dir(self, fpath):
        self.init_collect_d(fpath)
        self.init_collect_d_ownership(fpath)
        self.init_collect_d_perms(fpath)

    def init_collect_d(self, fpath):
        if not os.path.exists(fpath):
            # print("create dir", fpath)
            os.makedirs(fpath)

    def init_collect_d_perms(self, fpath):
        s = os.stat(fpath)
        mode = s[ST_MODE]
        if mode != 16768:
            # print("set dir", fpath, "mode to 0600")
            os.chmod(fpath, 0o0600)

    def init_collect_d_ownership(self, fpath):
        s = os.stat(fpath)
        if s.st_uid != self.root_uid or s.st_gid != self.root_gid:
            # print("set dir", self.collect_d, "ownership to", self.root_uid, self.root_gid)
            os.chown(self.collect_d, self.root_uid, self.root_gid)

    def load_stat(self):
        try:
            self.stat = self._load_stat()
        except:
            self.stat = {}

    def _load_stat(self):
        with open(self.collect_stat, "r") as f:
            buff = f.read()
        l = json.loads(buff)
        stat = {}
        for e in l:
            try:
                stat[e["fpath"]] = e
            except:
                pass
        return stat

    def write_stat(self, force=False):
        if not force and not self.stat_changed:
            return
        self._write_stat()

    def _write_stat(self):
        l = []
        for fpath in sorted(self.stat.keys()):
            l.append(self.stat[fpath])
        with open(self.collect_stat, "w") as f:
            f.write(json.dumps(l, sort_keys=True, separators=[", ", ": "], indent=4))

    def check_cf_perms(self, fpath):
        s = os.stat(fpath)
        mode = s[ST_MODE]
        if mode & S_IWOTH:
            raise ValueError("skip %s config file: file mode is insecure ('other' has write permission)" % fpath)
        if s.st_uid != self.root_uid or s.st_gid != self.root_gid:
            raise ValueError("skip %s config file: file ownership is insecure (must be owned by root)" % fpath)

    def merge_todo(self):
        for root, dnames, fnames in os.walk(self.sysreport_conf_d):
            for fname in fnames:
                fpath = os.path.join(self.sysreport_conf_d, fname)
                try:
                    self.check_cf_perms(fpath)
                except ValueError as exc:
                    print(exc, file=sys.stderr)
                    continue
                with open(fpath, 'r') as f:
                    buff = f.read()
                for line in buff.split("\n"):
                    line = line.strip()
                    if line.startswith("FILE"):
                        t = ("INC", line[4:].strip())
                    elif line.startswith("CMD"):
                        t = ("CMD", line[3:].strip())
                    elif line.startswith("DIR"):
                        t = ("INC", line[3:].strip())
                    elif line.startswith("GLOB"):
                        t = ("INC", line[4:].strip())
                    elif line.startswith("INC"):
                        t = ("INC", line[3:].strip())
                    elif line.startswith("EXC"):
                        t = ("EXC", line[3:].strip())
                    elif line == "":
                        continue
                    elif line.startswith("#"):
                        continue
                    elif line.startswith(";"):
                        continue
                    else:
                        print("unsupported item type:", line)
                        continue
                    if t not in self.todo:
                        self.todo.append(t)

        # expand
        inc = set()
        exc = set()
        self.cmds = set()
        for mode, s in self.todo:
            if mode == "CMD":
                self.cmds.add(s)
                continue
            l = []
            for _s in glob.glob(s):
                if os.path.isdir(_s):
                    l += self.find_files(_s)
                else:
                    l.append(_s)
            if mode == "INC":
                inc |= set(l)
            elif mode == "EXC":
                exc |= set(l)
        self.files = inc - exc

        if self.collect_stat is None:
            return

        # find deleted
        dst_files = self.find_files(self.collect_file_d)
        dst_files = self.rel_paths(self.collect_file_d, dst_files, posix=False)
        files = self.rel_paths("", self.files, posix=False)
        self.deleted = set(dst_files) - set(files) - set([os.sep + "stat"])

        # order file lists
        self.files = sorted(list(self.files))
        self.deleted = sorted(list(self.deleted))

        # purge stat info of deleted files
        for fpath in self.deleted:
            if fpath in self.stat:
                del (self.stat[fpath])
                self.stat_changed = True
                if self.collect_stat not in self.changed:
                    self.changed.append(self.collect_stat)
                if self.collect_stat not in self.full:
                    self.full.append(self.collect_stat)
        self.deleted = self.rel_paths("", self.deleted, posix=True)

    def cmdlist2fname(self, l):
        fname = '(space)'.join(l)
        fname = fname.replace('|', '(pipe)')
        fname = fname.replace('&', '(amp)')
        fname = fname.replace('$', '(dollar)')
        fname = fname.replace('^', '(caret)')
        fname = fname.replace('/', '(slash)')
        fname = fname.replace(':', '(colon)')
        fname = fname.replace(';', '(semicolon)')
        fname = fname.replace('<', '(lt)')
        fname = fname.replace('>', '(gt)')
        fname = fname.replace('=', '(eq)')
        fname = fname.replace('?', '(question)')
        fname = fname.replace('@', '(at)')
        fname = fname.replace('!', '(excl)')
        fname = fname.replace('#', '(num)')
        fname = fname.replace('%', '(pct)')
        fname = fname.replace('"', '(dquote)')
        fname = fname.replace("'", '(squote)')
        fname = fname.replace("\\", '(bslash)')
        return fname

    def write(self, fpath, buff):
        try:
            with open(fpath, 'r') as f:
                pbuff = f.read()
        except IOError as exc:
            pbuff = None
        if POSIX:
            diff = pbuff != buff
        else:
            diff = pbuff != buff.replace("\r", "\n")
        if diff:
            self.changed.append(fpath)
            with open(fpath, 'w') as f:
                f.write(buff)
            self.full.append(fpath)
        elif pbuff is not None:
            self.full.append(fpath)

    def get_exe(self, fpath):
        if not os.path.exists(fpath):
            raise ValueError("not found (%s)" % fpath)
        if which(fpath) is None:
            raise ValueError("not executable (%s)" % fpath)
        return fpath

    def collect_cmd(self, cmd):
        l = shlex.split(cmd, posix=POSIX)
        if len(l) == 0:
            print(" err: syntax error", file=sys.stderr)
            return
        if os.sep not in l[0]:
            print(" err: full path required for commands (%s)" % l[0], file=sys.stderr)
        try:
            self.get_exe(l[0])
        except ValueError as exc:
            print(" err: %s" % str(exc), file=sys.stderr)
            return
        fname = self.cmdlist2fname(l)
        cmd_d = os.path.join(self.collect_cmd_d, fname)
        p = Popen(l, stdout=PIPE, stderr=STDOUT, close_fds=POSIX)
        out, err = p.communicate()
        if six.PY3:
            out = out.decode("utf-8")
        self.write(os.path.join(cmd_d), out)

    def get_stat(self, fpath):
        st = os.stat(fpath)
        stat = {
            "fpath": fpath,
            "realpath": os.path.realpath(fpath),
            "mode": oct(st[ST_MODE]),
            "uid": st[ST_UID],
            "gid": st[ST_GID],
            "dev": st[ST_DEV],
            "nlink": st[ST_NLINK],
            "mtime": st[ST_MTIME],
            "ctime": st[ST_CTIME],
        }
        return stat

    def push_stat(self, fpath):
        if self.collect_stat is None:
            return
        stat = self.get_stat(fpath)
        cached_stat = self.stat.get(fpath)
        if cached_stat is None:
            self.stat[fpath] = stat
            self.stat_changed = True
            if self.collect_stat not in self.changed:
                self.changed.append(self.collect_stat)
            if self.collect_stat not in self.full:
                self.full.append(self.collect_stat)
            # print("  add %s stat info"%fpath)
            return
        for p in ("realpath", "mode", "uid", "gid", "dev", "nlink", "mtime", "ctime"):
            if stat[p] != cached_stat[p]:
                self.stat[fpath] = stat
                self.stat_changed = True
                if self.collect_stat not in self.changed:
                    self.changed.append(self.collect_stat)
                if self.collect_stat not in self.full:
                    self.full.append(self.collect_stat)
                # print("  change %s stat info"%fpath)
                return

    def dst_d(self, base_d, fpath):
        """
        Return the full path of the collect dir that will host
        fpath
        """
        return base_d + os.path.dirname(fpath)

    def _digest_secret(self, name, data):
        obfuscate = {}
        if name in ['cluster.conf']:
            rid_secrets = ['cluster']
            rid_secrets.extend(self.node.conf_sections(cat='hb'))
            rid_secrets.extend(self.node.conf_sections(cat='arbitrator'))
            for rid in rid_secrets:
                try:
                    value = self.node.oget(rid, 'secret')
                    obfuscate[value] = 'hexdigest-%s' % hashlib.md5(value.encode()).hexdigest()
                except:
                    pass
        for secret, secret_md5 in obfuscate.items():
            data = data.replace(secret, secret_md5)
        return data

    def collect_file(self, fpath):
        if not os.path.exists(fpath):
            return
        if os.path.islink(fpath):
            return
        dst_d = self.dst_d(self.collect_file_d, fpath)
        fname = os.path.basename(fpath)
        dst_f = os.path.join(dst_d, fname)
        if os.path.isdir(dst_f):
            # the fpath has changed from dir to file. cleanup the dst tree.
            shutil.rmtree(dst_f)
        if not os.path.exists(dst_d):
            os.makedirs(dst_d)

        self.push_stat(fpath)

        try:
            with open(fpath, 'r') as f:
                buff = f.read()
                if fpath.endswith('cluster.conf'):
                    buff = self._digest_secret('cluster.conf', buff)
            with open(dst_f, 'r') as f:
                pbuff = f.read()
            if buff != pbuff:
                self.changed.append(dst_f)
        except IOError:
            # in doubt, send ... git will know better on the collector
            self.changed.append(dst_f)
        except UnicodeDecodeError:
            # binary file: skip
            pass
        shutil.copy2(fpath, dst_f)
        if dst_f.endswith('cluster.conf'):
            with open(dst_f, 'r') as src:
                cluster_conf = src.read()
            with open(dst_f, 'w') as dst:
                dst.write(self._digest_secret('cluster.conf', cluster_conf))
        self.full.append(dst_f)

    def delete_collected(self, fpaths):
        for fpath in fpaths:
            self.delete_collected_one(fpath)

    def delete_collected_one(self, fpath):
        fp = self.collect_file_d + fpath
        os.unlink(fp)

    def find_files(self, fpath):
        l = []
        if not os.path.exists(fpath):
            return l
        for item in os.listdir(fpath):
            _fpath = os.path.join(fpath, item)
            if os.path.isdir(_fpath):
                l += self.find_files(_fpath)
            elif not os.path.islink(_fpath):
                l.append(_fpath)
        return l

    def sysreport(self, force=False):
        self.node.collector.init(self.send_rpc)
        if self.node.collector.proxy is None:
            print("no collector connexion. abort sysreport")
            return 1
        self.collect()
        self.delete_collected(self.deleted)
        self.write_stat(force=force)
        self.send(force)

    def collect(self):
        self.init()
        print("collect directory is", self.collect_d)
        for fpath in self.files:
            self.collect_file(fpath)
        for cmd in self.cmds:
            self.collect_cmd(cmd)

    def deleted_report(self):
        print("files deleted:")
        for fpath in sorted(self.deleted):
            print("  " + fpath)

    def collected(self):
        return self.rel_paths(self.collect_d, self.full)

    def rel_paths(self, base, fpaths, posix=True):
        if base:
            n = len(base)
        else:
            n = 0
        return [x[n:] for x in fpaths]

    def send(self, force=False):
        if force:
            to_send = self.full
            lstree_data = self.node.collector.call(self.lstree_rpc)
            if lstree_data is None:
                raise ex.Error("can not get lstree from collector")
            lstree_data = self.rel_paths("", lstree_data, posix=True)
            collected = self.rel_paths("", self.collected(), posix=True)
            self.deleted = sorted(list(set(lstree_data) - set(os.sep + "stat") - set(collected)))
        else:
            to_send = self.changed
            self.changed_report()

        if len(self.deleted) > 0:
            self.deleted_report()

        if len(to_send) == 0 and len(self.deleted) == 0:
            print("no change to report")
            return

        if len(to_send) > 0:
            tmpf = self.archive(to_send)
        else:
            tmpf = None

        print("sending sysreport")
        self.node.collector.call(self.send_rpc, tmpf, self.deleted)

        if tmpf is not None:
            os.unlink(tmpf)

    @staticmethod
    def mangle_sep(fpath):
        return fpath

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
        tar = tarfile.open(tmpf, mode=self.tarmode)
        for fpath in l:
            if len(fpath) < n:
                print(" err: can not archive", fpath, "(fpath too short)", file=sys.stderr)
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
