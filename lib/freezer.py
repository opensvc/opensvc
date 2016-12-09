import os
import glob
from rcGlobalEnv import rcEnv

def dummy():
    return

class Freezer:
    flag_dir = rcEnv.pathvar
    base_flag = os.path.join(flag_dir, 'FROZEN')
    flag = base_flag

    def frozen(self):
        if os.path.exists(self.flag) or os.path.exists(self.base_flag):
            return True
        return False

    def freeze(self):
        open(self.flag, 'w').close()

    def thaw(self):
        if self.flag != self.base_flag and os.path.exists(self.flag):
            os.unlink(self.flag)
            return
        for name in glob.glob(self.flag_dir + '/FROZEN*'):
            os.unlink(name)

    def __init__(self, name=None):
        if name is None:
            pass
        elif not os.path.exists(os.path.join(rcEnv.pathetc, name)):
            self.freeze = dummy
            self.thaw = dummy
            self.frozen = dummy
        else:
            self.flag = self.flag + "." + name
