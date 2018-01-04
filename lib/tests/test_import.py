import glob
import os

def test_import():
    mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
    import rcExceptions as ex

    for fpath in glob.glob(mod_d+"/*.py"):
        if "Win" in fpath or "wmi" in fpath or "osvcd_win" in fpath:
            continue
        mod = os.path.basename(fpath).replace(".py", "")
        try:
            __import__(mod)
        except (ex.excInitError, ex.excError):
            # dependent module missing
            pass
