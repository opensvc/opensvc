import resources as Res
import rcStatus
import rcExceptions as ex

class Disk(Res.Resource):
    def __init__(self, rid=None, vgname=None, lvname=None, **kwargs):
        Res.Resource.__init__(self, rid, "disk.lv", **kwargs)
        self.name = '@'.join((lvname, vgname))
        self.label = self.name

    def start(self):
        pass

    def stop(self):
        pass

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

