import time

from utilities.proc import justcall

from .node import Node as BaseNode


class Node(BaseNode):
    def sys_reboot(self, delay=0):
        if delay:
            self.log.info("reboot -q in %s seconds", delay)
            time.sleep(delay)
        justcall(['reboot', '-q'])

    def sys_crash(self, delay=0):
        if delay:
            self.log.info("toc crash in %s seconds", delay)
            time.sleep(delay)
        justcall(['toc'])

    def shutdown(self):
        cmd = ["shutdown", "-h", "-y", "0"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["shutdown", "-r", "-y", "0"]
        ret, out, err = self.vcall(cmd)
