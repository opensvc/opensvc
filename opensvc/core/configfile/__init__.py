import os
import shutil
from env import Env
from utilities.lock import cmlock


def move_config_file(src, dest):
    """
    move_config_file is a wrapper to shutil.move
    after file moved, os.fsync is called to ensure write dest to disk.

    This reduces the risk of dest updates lost when crash short time after
    move_config_file returns (detected on centos 8 with xfs /).
    """
    lockfile = os.path.join(Env.paths.pathlock, "move_file_" + os.path.basename(dest))
    with cmlock(timeout=3, delay=0.2, lockfile=lockfile, intent=dest):
        _move_config_file(src, dest)


def _move_config_file(src, dest):
    if not os.path.exists(src):
        raise Exception("move_file source file %s is absent" % src)
    if os.path.exists(dest) and not os.path.isfile(dest):
        raise Exception("move_file destination %s exists and is not a regular file" % dest)
    dest_dir = os.path.dirname(dest)
    if not os.path.isdir(dest_dir):
        raise Exception("move_file destination dir %s is not a directory" % dest_dir)

    shutil.move(src, dest)

    if hasattr(os, "fsync"):
        with open(dest, "a") as fd:
            os.fsync(fd)
