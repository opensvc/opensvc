import os

from core.sysreport.sysreport import BaseSysReport
from utilities.proc import which


class SysReport(BaseSysReport):
    @staticmethod
    def mangle_drive(fpath):
        try:
            if fpath[1] == ":":
                fpath = fpath[0].upper() + fpath[2:]
        except IndexError:
            pass
        return os.sep + fpath

    @staticmethod
    def mangle_sep(fpath):
        def mangle(_fp):
            return _fp.replace("\\", "/")

        if isinstance(fpath, list):
            return [mangle(_fp) for _fp in fpath]
        return mangle(fpath)

    def rel_paths(self, base, fpaths, posix=True):
        if base:
            n = len(base) + 1
        else:
            n = 0
        if posix:
            return [self.mangle_drive(x[n:]).replace("\\", "/")[1:] for x in fpaths]
        else:
            return [self.mangle_drive(x[n:]) for x in fpaths]

    def dst_d(self, base_d, fpath):
        """
        Return the full path of the collect dir that will host
        fpath
        """
        dst_d = os.path.dirname(fpath)
        return base_d + self.mangle_drive(dst_d)

    def check_cf_perms(self, fpath):
        pass

    def get_exe(self, fpath):
        for suffix in ("", ".exe", ".bat", ".cmd", ".lnk"):
            candidate = fpath + suffix
            if not os.path.exists(candidate):
                continue
            if which(candidate):
                return candidate
        raise ValueError("not found or not executable (%s)" % fpath)
