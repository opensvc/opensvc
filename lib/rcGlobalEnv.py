#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import sys

class rcEnv:
    """Class to store globals
    """

    platform = sys.platform

    """program used to execute remote command on other nodes or virtual hosts
    """
    if platform == "sunos5" :
        rsh = "/usr/bin/ssh -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -n"
        rcp = "/usr/bin/scp -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -n"
    else :
        rsh = "/usr/bin/ssh -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        rcp = "/usr/bin/scp -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"

    """Database sink for node and service configurations and status collection.
    """
    dbopensvc = "http://dbopensvc:80/init/default/call/xmlrpc"

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

    vt_libvirt = ['kvm', 'lxc', 'xen']
    vt_vm = ['ldom', 'hpvm', 'kvm', 'xen']
    vt_container = ['zone', 'lxc']
    vt_supported = vt_vm + vt_container




