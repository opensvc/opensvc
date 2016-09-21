import node

class Node(node.Node):
    def shutdown(self):
        cmd = ["init", "5"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["init", "6"]
        ret, out, err = self.vcall(cmd)
