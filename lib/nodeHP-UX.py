import node

class Node(node.Node):
    def shutdown(self):
        cmd = ["shutdown", "-h", "-y", "0"]
        ret, out, err = self.vcall(cmd)

    def reboot(self):
        cmd = ["shutdown", "-r", "-y", "0"]
        ret, out, err = self.vcall(cmd)
