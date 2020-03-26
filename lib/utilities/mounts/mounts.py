import os
from subprocess import Popen, PIPE, STDOUT

import core.exceptions as ex
from utilities.string import bdecode


class Mount(object):
    def __init__(self, dev, mnt, type, mnt_opt):
        self.dev = dev.rstrip("/")
        self.mnt = mnt.rstrip("/")
        if mnt == "/":
            self.mnt = mnt
        self.type = type
        self.mnt_opt = mnt_opt

    def __str__(self):
        return "Mount: dev[%s] mnt[%s] type[%s] options[%s]" % \
               (self.dev, self.mnt, self.type, self.mnt_opt)


class BaseMounts(object):
    src_dir_devs_cache = {}
    df_one_cmd = []

    def __init__(self):
        try:
            self.mounts = self.parse_mounts()  # pylint: disable=assignment-from-no-return
        except Exception as exc:
            self.mounts = None

    def __iter__(self):
        return iter(self.mounts or [])

    def match_mount(self, *args, **kwargs):
        """ OS dependent """
        pass

    def mount(self, dev, mnt):
        for i in self.mounts or []:
            if self.match_mount(i, dev, mnt):
                return i
        return None

    def parse_mounts(self):
        raise ex.Error("parse_mounts is not implemented")

    def has_mount(self, dev, mnt):
        if self.mounts is None:
            raise ex.Error("unable to parse mounts")
        for i in self.mounts:
            if self.match_mount(i, dev, mnt):
                return True
        return False

    def has_param(self, param, value):
        for i in self.mounts or []:
            if getattr(i, param) == value:
                return i
        return None

    def sort(self, key='mnt', reverse=False):
        if len(self.mounts or []) == 0:
            return
        if key not in ('mnt', 'dev', 'type'):
            return
        self.mounts.sort(key=lambda x: getattr(x, key), reverse=reverse)

    def get_fpath_dev(self, fpath):
        last = False
        d = fpath
        while not last:
            d = os.path.dirname(d)
            if d in ("", None):
                return
            m = self.has_param("mnt", d)
            if m:
                return m.dev
            if d == os.sep:
                last = True

    def get_src_dir_dev(self, dev):
        """Given a directory path, return its hosting device
        """
        if dev in self.src_dir_devs_cache:
            return self.src_dir_devs_cache[dev]
        p = Popen(self.df_one_cmd + [dev], stdout=PIPE, stderr=STDOUT, close_fds=True)
        out, err = p.communicate()
        if p.returncode != 0:
            return
        out = bdecode(out).lstrip()
        lines = out.splitlines()
        if len(lines) == 2:
            out = lines[1]
        self.src_dir_devs_cache[dev] = out.split()[0]
        return self.src_dir_devs_cache[dev]

    def __str__(self):
        output = "%s" % self.__class__.__name__
        for m in self.mounts or []:
            output += "\n  %s" % m.__str__()
        return output
