import glob
import os
import re

import rcExceptions as ex
from rcUtilities import justcall

CONFIG = "50-KillMode.conf"

SCOPE_NONE = 0
SCOPE_TUNED = 1
SCOPE_UNTUNED = 2

def gen_systemd_scope_name(vmname):
    sdvm = "opensvc-dummy-machine"
    for fpqemu in glob.glob("/run/systemd/machines/qemu*"):
        qemu = fpqemu.split('/')[-1]
        if qemu.endswith(vmname):
            sdvm = qemu
            break
    dashname = sdvm + '.scope'
    asciiname = dashname.replace('-', '\\x2d')
    scopename = 'machine-' + asciiname
    return scopename

def gen_systemd_dirname(vmname):
    scopename = gen_systemd_scope_name(vmname)
    fullpathname = '/etc/systemd/system/' + scopename + '.d'
    return fullpathname

def deploy_scope_killmode(res):
    scope = has_scope(res.name)
    if scope == SCOPE_NONE:
        # no need to tune (no scope)
        return
    elif scope == SCOPE_TUNED:
        # no need to tune (already tuned)
        return

    # create systemd directory
    folder = gen_systemd_dirname(res.name)
    if not os.path.isdir(folder):
        try:
            os.makedirs(folder)
        except:
            raise ex.excError("failed to create missing dir %s" % folder)

    # create systemd scope config file
    fname = os.path.join(folder, CONFIG)
    if not os.path.isfile(fname):
        try:
            with open(fname, 'w') as ofile:
                ofile.write("[Scope]\n")
                ofile.write("KillMode=none\n")
            res.log.info("create systemd machine scope custom config file %s" % fname)
        except:
            raise ex.excError("failed to create systemd machine scope custom "
                              "config file %s" % fname)

    # reload systemd config
    res.log.info("reload systemd configuration")
    justcall(['systemctl', 'daemon-reload'])

def has_scope(vmname):
    scopename = gen_systemd_scope_name(vmname)
    out, _, ret = justcall(['systemctl', 'cat', scopename])
    if ret != 0:
        return SCOPE_NONE
    for line in out.splitlines():
        if re.match(r'^\s*KillMode\s*=\s*none', line):
            return SCOPE_TUNED
    return SCOPE_UNTUNED
