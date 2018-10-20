import os
import rcSysReport

from rcUtilities import which

class SysReport(rcSysReport.SysReport):
    def __init__(self, node=None, **kwargs):
        rcSysReport.SysReport.__init__(self, node=node, **kwargs)

    def dst_d(self, base_d, fpath):
        """
        Return the full path of the collect dir that will host
        fpath
        """
        dst_d = os.path.dirname(fpath)
        try:
            if dst_d[1] == ":":
                dst_d = dst_d[0] + dst_d[2:]
            if dst_d[0] != os.sep:
                dst_d = os.sep + dst_d
        except IndexError:
            pass
        return base_d + dst_d

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
