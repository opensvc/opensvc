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
    candidates = glob.glob("/run/systemd/machines/*-*-%s"%vmname)
    if not candidates:
        return
    machine = os.path.basename(candidates[0]).replace('-', '\\x2d')
    return 'machine-%s.scope' % machine

def gen_systemd_dirname(vmname):
    scopename = gen_systemd_scope_name(vmname)
    fullpathname = '/etc/systemd/system/%s.d' % scopename
    return fullpathname

def remove_scope_killmode(res):
    scope = has_scope(res.name)
    if scope == SCOPE_NONE:
        # no need to tune (no scope)
        return

    folder = gen_systemd_dirname(res.name)
    fname = os.path.join(folder, CONFIG)
    res.log.info("remove systemd machine scope custom config file %s" % fname)
    try:
        os.unlink(fname)
    except Exception as exc:
        res.log.warning(exc)
    try:
        os.rmdir(folder)
    except Exception:
        pass

    if res.svc.running_action != "shutdown":
        systemd_daemon_reload(res)

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
    systemd_daemon_reload(res)

def systemd_daemon_reload(res):
    res.log.info("reload systemd configuration")
    justcall(['systemctl', 'daemon-reload'])

def has_scope(vmname):
    scopename = gen_systemd_scope_name(vmname)
    if scopename is None:
        return SCOPE_NONE
    out, _, ret = justcall(['systemctl', 'cat', scopename])
    if ret != 0:
        return SCOPE_NONE
    for line in out.splitlines():
        if re.match(r'^\s*KillMode\s*=\s*none', line):
            return SCOPE_TUNED
    return SCOPE_UNTUNED
