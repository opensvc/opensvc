import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import glob

class TestImport:
    def test_import(self):
        """
        All modules syntax check
        """
        mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        import rcExceptions as ex

        for fpath in glob.glob(mod_d+"/*.py"):
            if "win" in fpath.lower() or "wmi" in fpath or "osvcd_win" in fpath:
                continue
            if "rcMd5" in fpath:
                continue
            mod = os.path.basename(fpath).replace(".py", "")
            try:
                __import__(mod)
            except (ex.excInitError, ex.excError):
                # dependent module missing
                pass
