from subprocess import *
import os
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import qcall, justcall
import resApp
import rcStatus
import rcExceptions as ex

class Apps(resApp.App):
    app_d = os.path.join(os.sep, 'svc', 'etc', 'init.d')

    def set_perms(self, rc):
        (ret, out, err) = self.call(self.prefix+['/usr/bin/find',
                                            self.app_d,
                                            '-name', os.path.basename(rc),
                                            '-a', '-user', 'root',
                                            '-a', '-group', 'root'])
        if len(out) == 0 or rc != out.split()[0]:
            self.vcall(self.prefix+['chown', 'root:root', rc])
        (ret, out, err) = self.call(self.prefix+['test', '-x', rc])
        if ret != 0:
            self.vcall(self.prefix+['chmod', '+x', rc])

    def check_reachable(self, container):
        cmd = self.prefix + ['/bin/pwd']
        ret = qcall(cmd)
        if ret != 0:
            return False
        return True

    def checks(self, verbose=False):
        container = self.svc.resources_by_id["container"]
        if container.guestos == 'Windows':
            raise ex.excNotAvailable
        if container.status(refresh=True) != rcStatus.UP:
            self.log.debug("abort resApp action because container status is %s" % rcStatus.Status(container.status()))
            self.status_log("container is %s" % rcStatus.Status(container.status()))
            raise ex.excNotAvailable
        if not self.check_reachable(container):
            self.log.debug("abort resApp action because container is unreachable")
            self.status_log("container is unreachable")
            return False
        cmd = self.prefix + ['test', '-d', self.app_d]
        ret = qcall(cmd)
        if ret == 0:
            return True
        cmd = self.prefix + ['/bin/mkdir', '-p', self.app_d]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            return False
        return True

    def stop_checks(self):
        return self.checks()

    def start_checks(self):
        return self.checks()

    def status_checks(self, verbose=False):
        return self.checks(verbose=verbose)

    def sorted_app_list(self, pattern):
        cmd = self.prefix + ['/usr/bin/find', self.app_d, '-name', pattern.replace('*', '\*')]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, close_fds=True)
        buff = p.communicate()
        if p.returncode != 0:
            self.log.debug("failed to fetch container startup scripts list")
            return []

        l = buff[0].split('\n')

        # most unix find commands don't support maxdepth.
        # discard manually the startup scripts found in subdirs of app_d
        n = self.app_d.count("/")
        if not self.app_d.endswith("/"):
            n += 1
        l = [e for e in l if e.count("/") == n]

        return sorted(l)

    def app_exist(self, name):
        """ verify app_exists inside Vm
        """
        (out, err, ret) = justcall (self.prefix + ['/bin/ls', '-Ld', name ])
        if ret == 0:
            return True
        else:
            return False

if __name__ == "__main__":
    for c in (Apps,) :
        help(c)
