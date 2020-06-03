from .node import Node as BaseNode


class Node(BaseNode):
    def shutdown(self):
        cmd = ["shutdown", "-h", "now"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["shutdown", "-r", "now"]
        ret, out, err = self.vcall(cmd)
