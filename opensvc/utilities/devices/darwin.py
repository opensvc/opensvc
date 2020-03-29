import os
import plistlib

import core.exceptions as ex

from utilities.proc import justcall, which

def file_to_loop(f):
    """Given a file path, returns the disk device associated. For example,
    /path/to/file => /dev/disk0s1
    """
    if which('hdiutil') is None:
        return []
    if not os.path.isfile(f):
        return []
    out, err, ret = justcall(['hdiutil', 'info', '-plist'])
    if ret != 0:
        return []

    devs= []
    try:
        pl = plistlib.readPlistFromString(out)
    except AttributeError as exc:
        raise ex.Error(str(exc))
    for image in pl['images']:
        if image.get('image-path') == f:
            for se in image['system-entities']:
                diskdevice = se.get('dev-entry')
                if diskdevice is not None:
                    devs.append(diskdevice)
                else:
                    return []
    return devs
