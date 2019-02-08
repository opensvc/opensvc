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
import time
from uuid import uuid4
from storage import Storage

def get_osvc_paths(osvc_root_path=None, sysname=None, detect=False):
    o = Storage()

    if osvc_root_path:
        o.pathsvc = osvc_root_path
    elif detect:
        o.pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
    else:
        o.pathsvc = '/usr/share/opensvc'

    if o.pathsvc == '/usr/share/opensvc':
        o.pathlib = '/usr/share/opensvc/lib'
        o.pathbin = '/usr/bin'
        o.pathetc = '/etc/opensvc'
        o.pathetcns = '/etc/opensvc/namespaces'
        o.pathlog = '/var/log/opensvc'
        o.pathtmp = '/var/tmp/opensvc'
        o.pathvar = '/var/lib/opensvc'
        o.pathdoc = '/usr/share/doc/opensvc'
        o.pathlock = '/var/lib/opensvc/lock'
        o.pathcron = '/usr/share/opensvc'
        o.postinstall = '/usr/share/opensvc/bin/postinstall'
        o.preinstall = '/usr/share/opensvc/bin/preinstall'
    else:
        o.pathlib = os.path.join(o.pathsvc, 'lib')
        o.pathbin = os.path.join(o.pathsvc, 'bin')
        o.pathetc = os.path.join(o.pathsvc, 'etc')
        o.pathetcns = os.path.join(o.pathsvc, 'etc', 'namespaces')
        o.pathlog = os.path.join(o.pathsvc, 'log')
        o.pathtmp = os.path.join(o.pathsvc, 'tmp')
        o.pathvar = os.path.join(o.pathsvc, 'var')
        o.pathdoc = os.path.join(o.pathsvc, 'usr', 'share', 'doc')
        o.pathlock = os.path.join(o.pathvar, 'lock')
        o.pathcron = o.pathbin
        o.postinstall = os.path.join(o.pathbin, 'postinstall')
        o.preinstall = os.path.join(o.pathbin, 'preinstall')

    if os.name == "nt":
        o.svcmgr = os.path.join(o.pathsvc, "svcmgr.cmd")
        o.nodemgr = os.path.join(o.pathsvc, "nodemgr.cmd")
        o.svcmon = os.path.join(o.pathsvc, "svcmon.cmd")
        o.cron = os.path.join(o.pathsvc, "cron.cmd")
    else:
        o.svcmgr = os.path.join(o.pathbin, "svcmgr")
        o.nodemgr = os.path.join(o.pathbin, "nodemgr")
        o.svcmon = os.path.join(o.pathbin, "svcmon")
        o.cron = os.path.join(o.pathcron, "cron")

    o.nodeconf = os.path.join(o.pathetc, "node.conf")
    o.authconf = os.path.join(o.pathetc, "auth.conf")

    o.lsnruxsockd = os.path.join(o.pathvar, "lsnr")
    o.lsnruxsock = os.path.join(o.lsnruxsockd, "lsnr.sock")
    o.dnsuxsockd = os.path.join(o.pathvar, "dns")
    o.dnsuxsock = os.path.join(o.dnsuxsockd, "pdns.sock")
    o.pathcomp = os.path.join(o.pathvar, "compliance")
    o.pathcomposvc = os.path.join(o.pathcomp, "com.opensvc")
    o.safe = os.path.join(o.pathvar, "safe")
    o.drp_path = os.path.join(o.pathvar, "cache")
    o.last_shutdown = os.path.join(o.pathvar, "last_shutdown")

    o.daemon_pid = os.path.join(o.pathvar, "osvcd.pid")
    o.daemon_lock = os.path.join(o.pathlock, "osvcd.lock")

    return o

def create_or_update_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)
    else:
        # update tmpdir timestamp to avoid tmpwatch kicking-in while we run
        now = time.time()
        try:
            os.utime(d, (now, now))
        except:
            # unprivileged
            pass

class rcEnv:
    """Class to store globals
    """
    uuid = ""
    if "OSVC_PARENT_SESSION_UUID" in os.environ:
        # passed from parent forking process: share the session
        session_uuid = os.environ["OSVC_PARENT_SESSION_UUID"]
    else:
        session_uuid = str(uuid4())
    initial_env = os.environ.copy()
    os.environ["OSVC_SESSION_UUID"] = session_uuid
    node_env = ""

    allowed_svc_envs = [
        'DEV',
        'DRP',
        'FOR',
        'INT',
        'PRA',
        'PRD',
        'PRJ',
        'PPRD',
        'REC',
        'STG',
        'TMP',
        'TST',
        'UAT',
    ]
    fs_pooling = [
        "zfs",
    ]
    fs_non_pooling = [
        "ext2", "ext3", "ext4", "xfs", "btrfs", "vfat",
        "reiserfs", "jfs", "jfs2", "bfs", "msdos", "ufs",
        "ufs2", "minix", "xia", "ext", "umsdos", "hpfs",
        "ntfs", "reiserfs4", "vxfs", "hfs", "hfsplus",
        "qnx4", "ocfs", "ocfs2", "nilfs", "jffs", "jffs2",
        "tux3", "f2fs", "logfs", "gfs", "gfs2", "gpfs",
    ]
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

    vt_cloud = ['vcloud', 'openstack', 'amazon']
    vt_libvirt = ['kvm']
    vt_vm = ['ldom', 'hpvm', 'kvm', 'xen', 'vbox', 'ovm', 'esx'] + vt_cloud
    vt_container = ['zone', 'lxd', 'lxc', 'jail', 'vz', 'srp', 'docker']
    vt_supported = vt_vm + vt_container

    dbopensvc = None
    dbopensvc_host = None
    dbcompliance = None
    dbcompliance_host = None
    paths = get_osvc_paths(sysname=sysname, detect=True)

    syspaths = Storage(
        df="/bin/df",
        mount="/bin/mount",
        umount="/bin/umount",
        zfs="/sbin/zfs",
        zpool="/sbin/zpool",
    )
    if sysname == "Linux":
        syspaths.ps = "/bin/ps"
        syspaths.blkid = "/sbin/blkid"
        syspaths.dmsetup = "/sbin/dmsetup"
        syspaths.ip = "/sbin/ip"
        syspaths.losetup = "/sbin/losetup"
        syspaths.lsmod = "/sbin/lsmod"
        syspaths.lvs = "/sbin/lvs"
        syspaths.multipath = "/sbin/multipath"
        syspaths.multipathd = "/sbin/multipathd"
        syspaths.nsenter = "/usr/bin/nsenter"
        syspaths.pvs = "/sbin/pvs"
        syspaths.pvscan = "/sbin/pvscan"
        syspaths.vgscan = "/sbin/vgscan"
        syspaths.vgs = "/sbin/vgs"
    elif sysname == "SunOS":
        syspaths.ps = "/usr/bin/ps"
        syspaths.df = "/usr/sbin/df"
        syspaths.ipadm = "/usr/sbin/ipadm"
        syspaths.mount = "/usr/sbin/mount"
        syspaths.umount = "/usr/sbin/umount"
        syspaths.zfs = "/usr/sbin/zfs"
        syspaths.zpool = "/usr/sbin/zpool"
    elif sysname == "AIX":
        syspaths.ps = "/usr/sbin/ps"
        syspaths.df = "/usr/sbin/df"
        syspaths.mount = "/usr/sbin/mount"
        syspaths.umount = "/usr/sbin/umount"

    create_or_update_dir(paths.pathtmp)

    if "LD_PRELOAD" in os.environ:
        ld_preload = os.environ["LD_PRELOAD"]
        del os.environ["LD_PRELOAD"]
    else:
        ld_preload = None

    if "OSVC_PYTHON_ARGS" in os.environ:
        pyargs = os.environ["OSVC_PYTHON_ARGS"].split()
        del os.environ["OSVC_PYTHON_ARGS"]
    else:
        pyargs = None

    python_cmd = []
    if ld_preload:
        python_cmd.append("LD_PRELOAD="+ld_preload)
    if os.name == "nt":
        python_cmd.append(sys.executable.replace("lib\site-packages\win32\PythonService.exe", "Python.exe"))
    else:
        python_cmd.append(sys.executable)
    if pyargs:
        python_cmd += pyargs
