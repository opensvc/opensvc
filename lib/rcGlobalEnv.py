"""module rcGlobalEnv module define rcEnv class
   rcEnv class attribute may be updated with rcLocalEnv module if present
   rcLocalEnv module is not provided with opensvc and allow customers to
   redefine following vars:
       o dbopensvc_host
       o dbopensvc_port
       o rsh
       o rcp
   rcLocalEnv.py may be installed into path_opensvc/lib
"""
import sys
import os
import platform
import socket

class rcEnv:
    """Class to store globals
    """

    logging_initialized = []
    allowed_svctype = ['PRD', 'PPRD', 'REC', 'INT', 'DEV', 'TST', 'TMP', 'DRP', 'FOR', 'PRA', 'PRJ', 'STG']
    _platform = sys.platform
    sysname, x, x, x, machine, x = platform.uname()
    nodename = socket.gethostname().lower()
    fqdn = socket.getfqdn().lower()
    listener_port = 1214

    """program used to execute remote command on other nodes or virtual hosts
    """
    if _platform == "sunos5" :
        if os.path.exists('/usr/local/bin/ssh'):
            rsh = "/usr/local/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
            rcp = "/usr/local/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        else:
            rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -n"
            rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
    elif os.path.exists('/etc/vmware-release'):
        rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
        rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
    elif sysname == 'OSF1':
        rsh = "ssh -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        rcp = "scp -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
    else:
        rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"

    """EZ-HA defines. EZ-HA does heartbeat, stonith, automatic service failover

    ez_last_chance == True:
        check_up_script_gen.sh will try a ping + RSH other node before stonith
    ez_startapp_bg == True:
        startapp in background if EZ-HA take-over is succesful
    """
    ez_path = "/usr/local/cluster"
    ez_path_services = ez_path + "/conf/services"
    ez_last_chance = True
    ez_startapp_bg = True

    """Directory on DRP node where to store the PRD nodes files necessary
    for takeover.
    """
    drp_path = "/opt/opensvc/var/cache/"
    drp_sync_excludes = [
        '--exclude="/spice"',
        '--exclude="/dbadm"',
        '--exclude="*.dmp"',
        '--exclude="*.dbf"',
        '--exclude="*.rdo"',
        '--exclude="*.log"',
        '--exclude="*.Z"',
        '--exclude="*.gz"',
        '--exclude="*.tgz"',
        '--exclude="*.tar"',
        '--exclude="*.tmp"',
        '--exclude="/oracle/ficimp"',
        '--exclude="/oracle/tmp"',
        '--exclude="/oracle/LOG/*"',
        '--exclude="/oracle/product/*/network/log/listener*.log"',
    ]
    drp_sync_etc_solaris = [
        "/etc/inet",
        "/etc/inetd.conf",
        "/etc/defaultdomain",
        "/etc/lp",
        "/etc/printers.conf",
        "/etc/system",
        "/etc/auto_master",
        "/etc/auto_home",
        "/etc/hosts.equiv",
        "/etc/pam.conf",
        "/etc/cron.d",
    ]
    drp_sync_etc_linux = [
        # linux
        "/etc/xinetd.d",
        "/etc/xinetd.conf",
        "/etc/sysconfig",
        "/etc/cups",
        "/etc/auto.master",
        "/etc/auto.misc",
        "/etc/listener.ora",
        "/etc/oratab",
        "/etc/sqlnet.ora",
        "/etc/tnsnames.ora",
        "/etc/yp.conf",
        "/etc/pam.d",
        "/etc/cron.allow",
        "/etc/cron.deny",
    ]
    drp_sync_etc_common = [
        # common
        "/etc/shadow",
        "/etc/passwd",
        "/etc/group",
        "/etc/syslog.conf",
        "/etc/services",
        "/etc/hosts",
        "/etc/nsswitch.conf",
        "/etc/sudoers",
        "/etc/project",
        "/etc/user_attr",
        "/etc/ssh",
        "/etc/centrifydc",
        "/etc/krb5*",
        "/etc/sudoers",
    ]
    drp_sync_misc = [
        "/var/centrifydc",
        "/var/opt/oracle",
        "/var/spool/cron",
        "/var/spool/cron/crontabs",
        "/var/yp/binding",
        "/usr/local/oraenv", "/usr/local/coraenv", "/usr/local/dbhome",
        "/usr/local/etc/sudoers",
    ]
    drp_sync_files = [
        [drp_sync_etc_solaris + drp_sync_etc_linux + drp_sync_etc_common + drp_sync_misc, []],
        [["/home/oracle", "/home/sybase", "/opt/oracle", "/opt/sybase"], drp_sync_excludes],
    ]

    vt_cloud = ['vcloud', 'openstack', 'amazon']
    vt_libvirt = ['kvm']
    vt_vm = ['ldom', 'hpvm', 'kvm', 'xen', 'vbox', 'ovm', 'esx'] + vt_cloud
    vt_container = ['zone', 'lxc', 'jail', 'vz', 'srp', 'docker']
    vt_supported = vt_vm + vt_container

    dbopensvc = "None"
    dbcompliance = "None"

    pathlib = os.path.realpath(os.path.dirname(__file__))
    if pathlib.startswith('/usr/share'):
        pathbin = '/usr/bin'
        pathetc = '/etc/opensvc'
        pathlog = '/var/log/opensvc'
        pathtmp = '/var/tmp/opensvc'
        pathvar = '/var/lib/opensvc'
        pathlock = '/var/lib/opensvc/lock'
    else:
        pathsvc = os.path.join(pathlib, '..')
        pathbin = os.path.join(pathsvc, 'bin')
        pathetc = os.path.join(pathsvc, 'etc')
        pathlog = os.path.join(pathsvc, 'log')
        pathtmp = os.path.join(pathsvc, 'tmp')
        pathvar = os.path.join(pathsvc, 'var')
        pathlock = os.path.join(pathvar, 'lock')

