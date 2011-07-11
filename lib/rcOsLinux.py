import shutil
import rcOs

class Os(rcOs.Os):
    def reboot(self):
        with open("/proc/sysrq-trigger", "w") as f:
            f.write("b")

    def crash(self):
        shutil.copyfile('/dev/zero', '/dev/mem')
