import resources as Res
from rcGlobalEnv import rcEnv
import os
import rcExceptions as ex
import rcStatus

class Hb(Res.Resource):
    """ HeartBeat ressource
    """
    def cluster_files(self):
        svcfile = os.path.join(rcEnv.pathetc, self.svc.svcname)
        svcmgr = os.path.join('..', 'bin', 'svcmgr')
        svcmgr_real = os.path.join(rcEnv.pathbin, 'svcmgr')
        cluster_f = '.'.join((svcfile, 'cluster'))
        stonith_f = '.'.join((svcfile, 'stonith'))

        for f in (cluster_f, stonith_f):
            if os.path.islink(f):
                if os.path.exists(f):
                    if os.path.realpath(f) == svcmgr_real:
                        self.log.debug("%s: symlink ok."%f)
                        pass
                    else:
                        self.log.info("%s: symlink exists but points to wrong file. fix."%f)
                        os.unlink(f)
                        os.symlink(svcmgr, f)
                else:
                    self.log.info("%s: broken link. fix."%f)
                    os.unlink(f)
                    os.symlink(svcmgr, f)
            else:
                if os.path.exists(f):
                    self.log.info("%s: regular file. fix."%f)
                    os.unlink(f)
                    os.symlink(svcmgr, f)
                else:
                    self.log.info("%s: not regular file nor symlink. fix."%f)
                    os.symlink(svcmgr, f)

    def __str__(self):
        return "%s" % (Res.Resource.__str__(self))

    def freeze(self):
        pass

    def thaw(self):
        pass

    def stop(self):
        pass

    def start(self):
        pass

    def shutdown(self):
        raise ex.excEndAction("shutdown action is not allowed on heartbeat-driven services")

    def __status(self, verbose=False):
        return rcStatus.UNDEF

    def _status(self, verbose=False):
        self.cluster_files()
        return self.__status(verbose)
