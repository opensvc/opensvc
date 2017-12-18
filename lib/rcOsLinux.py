import shutil
import rcOs

class Os(rcOs.Os):
    def reboot(self):
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("b")

    def crash(self):
        with open("/proc/sysrq-trigger", "w") as ofile:
            ofile.write("c")
