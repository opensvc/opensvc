import os
import glob
import pytest


@pytest.mark.ci
class TestImport:
    @staticmethod
    def test_i():
        """
        All modules syntax check
        """
        mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        import core.exceptions as ex

        for fpath in glob.glob(mod_d+"/*.py"):
            if "win" in fpath.lower() or "wmi" in fpath or "osvcd_win" in fpath:
                continue
            if "rcMd5" in fpath:
                continue
            mod = os.path.basename(fpath).replace(".py", "")
            try:
                __import__(mod)
            except (ex.InitError, ex.Error):
                # dependent module missing
                pass
