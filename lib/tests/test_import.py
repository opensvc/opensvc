import glob
import os

def test_import():
    mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))

    for fpath in glob.glob(mod_d+"/*.py"):
        if "Win" in fpath or "wmi" in fpath:
            continue
        mod = os.path.basename(fpath).replace(".py", "")
        __import__(mod)
