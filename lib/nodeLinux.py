import node

class Node(node.Node):
    def shutdown(self):
        cmd = ["shutdown", "-h"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["reboot"]
        ret, out, err = self.vcall(cmd)
