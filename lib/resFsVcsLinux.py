import rcStatus
import rcExceptions as ex
from rcGlobalEnv import rcEnv
Res = __import__("resFsLinux")

class Mount(Res.Mount):
    def __init__(self,
                 rid,
                 mount_point,
                 device,
                 fs_type,
                 mount_options,
                 always_on=set([]),
                 snap_size=None,
                 disabled=False,
                 tags=set([]),
                 optional=False,
                 monitor=False,
                 restart=0,
                 subset=None):
        Res.Mount.__init__(self,
                           rid,
                           mount_point,
                           device,
                           fs_type,
                           mount_options,
                           always_on=always_on,
                           snap_size=snap_size,
                           disabled=disabled,
                           tags=tags,
                           optional=optional,
                           monitor=monitor,
                           restart=restart,
                           subset=subset)

    def _status(self, verbose=False):
        try:
            s = self.svc.get_res_val(self.vcs_name, 'State')
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN

        if s == "ONLINE":
            return rcStatus.UP
        elif s == "OFFLINE":
            return rcStatus.DOWN
        else:
            self.status_log(s)
            return rcStatus.WARN

if __name__ == "__main__":
    for c in (Mount,) :
        help(c)

