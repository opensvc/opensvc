#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import resHb
from rcGlobalEnv import rcEnv
import os
import rcStatus
import rcExceptions as ex
from rcUtilities import justcall, which
import time

class excDoLocalAfterRemote(Exception):
    pass

class Hb(resHb.Hb):
    """ HeartBeat ressource
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 restart=0,
                 subset=None,
                 tags=set([])):
        resHb.Hb.__init__(self,
                          rid,
                          "hb.openha",
                          optional=optional,
                          disabled=disabled,
                          restart=restart,
                          subset=subset,
                          tags=tags,
                          always_on=always_on)
        self.basedir = os.path.join(os.sep, 'usr', 'local', 'cluster')
        self.bindir = os.path.join(self.basedir, 'bin')
        self.logdir = os.path.join(self.basedir, 'log')
        self.svcdir = os.path.join(self.basedir, 'services')
        self.cfsvc = os.path.join(self.basedir, 'conf', 'services')
        self.cfnod = os.path.join(self.basedir, 'conf', 'nodes')
        self.cfmon = os.path.join(self.basedir, 'conf', 'monitor')
        os.environ['EZ'] = self.basedir
        os.environ['EZ_BIN'] = self.bindir
        os.environ['EZ_SERVICES'] = self.cfsvc
        os.environ['EZ_NODES'] = self.cfnod
        os.environ['EZ_MONITOR'] = self.cfmon
        os.environ['EZ_LOG'] = self.logdir
        self.service_cmd = os.path.join(self.bindir, 'service')
        self.heartc = os.path.join(self.bindir, 'heartc')
        self.heartd = os.path.join(self.bindir, 'heartd')
        self.nmond = os.path.join(self.bindir, 'nmond')
        self.name = name
        self.state = {'0': 'stopped',
                      '1': 'stopping',
                      '2': 'started',
                      '3': 'starting',
                      '4': 'start_failed',
                      '5': 'stop_failed',
                      '6': 'frozen_stop',
                      '7': 'start_ready',
                      '8': 'unknown',
                      '9': 'force_stop'}

    def cluster_name(self):
        if self.name is None:
            return self.svc.svcname
        else:
            return self.name

    def service_local_status(self):
        return self.service_status(rcEnv.nodename)

    def get_peer(self):
        if len(self.svc.nodes) != 2:
            self.log.error("HA cluster must have 2 nodes")
            raise ex.excError
        nodes = [n for n in self.svc.nodes if n != rcEnv.nodename]
        if len(nodes) != 1:
            self.log.error("local node is not in cluster")
            raise ex.excError
        peer = nodes[0]
        return peer

    def service_remote_status(self):
        peer = self.get_peer()
        return self.service_status(peer)

    def service_status(self, nodename):
        if not self.process_running():
            self.log.error("open-ha daemons are not running")
            return 'unknown'
        service_state = os.path.join(self.svcdir, self.cluster_name(), 'STATE.'+nodename)
        if not os.path.exists(service_state):
            self.status_log("%s does not exist"%service_state)
            return 'unknown'
        try:
            f = open(service_state, 'r')
            buff = f.read().strip(' \n')
            f.close()
        except Exception as e:
            self.status_log(str(e))
            return 'unknown'
        if len(buff) < 1:
            self.status_log("%s is corrupted"%service_state)
            return 'unknown'
        if buff[0] in self.state:
            return self.state[buff[0]]
        else:
            return 'unknown'

    def service_action(self, command):
        cmd = [self.service_cmd, '-A', self.cluster_name(), command]
        (ret, out, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def freezestop(self):
        cmd = ['env']
        vars = ('EZ', 'EZ_BIN', 'EZ_SERVICES', 'EZ_NODES', 'EZ_MONITOR', 'EZ_LOG')
        for var in vars:
            cmd.append(var+'='+os.environ[var])
        cmd += [self.service_cmd, '-A', self.cluster_name(), 'freeze-stop']
        self.svc.node.cmdworker.enqueue(cmd)

    def process_running(self):
        # self.cfmon exist if OpenHA setup is done
        # os.path.exists(self.cfmon) return true if OpenHA setup is done
        # _not_ os.path.exists(self.cfmon) = True mean that setup is _not_ done
        # in this case, self.process_running() have to return _False_
        if not os.path.exists(self.cfmon):
            return False
        buff = ""
        with open(self.cfmon) as f:
            buff = f.read()
        daemons = []
        for line in buff.split('\n'):
            l = line.split()
            if len(l) < 4:
                continue
            if '#' in l[0]:
                continue
            if l[1] == 'net':
                suffix = ''
            elif l[1] == 'disk':
                suffix = '_raw'
            else:
                suffix = '_dio'
            if rcEnv.nodename == l[0]:
                daemon = self.heartd + suffix
                string = daemon + ' ' + ' '.join(l[2:-1])
                daemons.append(string)
            else:
                daemon = self.heartc + suffix
                string = daemon + ' ' + ' '.join(l[2:])
                daemons.append(string)
        daemons.append(self.nmond)
        (out, err, ret) = justcall(['ps', '-ef'])
        if ret != 0:
            return False
        h = {}
        for d in daemons:
            h[d] = 1
        # ckecking running daemons 
        lines = [ l for l in out.split('\n') if "heart" in l or "nmond" in l ]
        if which("pargs"):
            # solaris ps command truncates long lines
            # disk-based hb tend to have long args
            import re
            regex = re.compile('argv.*: ')
            for line in lines:
                v = line.split()
                if len(v) < 3:
                    continue
                pid = v[1]
                cmd = ['pargs', pid]
                out, err, ret = justcall(cmd)
                if ret != 0:
                    continue
                s = ' '.join([ regex.sub('', l) for l in out.split('\n') if l.startswith('argv') ])
		if s in h:
                    h[s] = 0
        else:
            for line in lines:
                for d in daemons:
                    if line.endswith(d):
                        h[d] = 0
        # now counting daemons not found as running
        total = 0
        for d in daemons:
            total += h[d]
        if total > 0:
            return False
        return True

    def need_stonith(self):
        status = self.service_remote_status()
        if status == 'unknown':
            return True
        return False

    def print_remote(self, out, err):
        peer = self.get_peer()
        s = out+err
        s = s.replace(self.svc.svcname.upper(), peer.upper()+"."+self.svc.svcname.upper())
        print(s)

    def freeze(self):
        """
          --force disables the remote node status check
          it is set on remote actions
        """
        do_local = True
        do_remote = True
        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        peer = self.get_peer()

        if local_status in ['frozen_stop', 'start_ready']:
            self.log.info("local already frozen")
            do_local = False

        if not self.svc.force and remote_status in ['frozen_stop', 'start_ready']:
            self.log.info("remote already frozen")
            do_remote = False
        if self.svc.force:
            do_remote = False

        if not do_local and not do_remote:
            return

        if not self.svc.force and remote_status == 'stopped' and local_status == 'started':
            out, err, ret = self.svc.remote_action(peer, "freeze --force", sync=True)
            self.print_remote(out, err)
            do_remote = False
            if ret != 0:
                raise ex.excError(err)

        if local_status in ['stopped', 'start_failed', 'stop_failed']:
            self.service_action('freeze-stop')
            time.sleep(2)
        elif local_status in ['started']:
            self.service_action('freeze-start')
            time.sleep(2)

        if do_remote:
            out, err, ret = self.svc.remote_action(peer, "freeze --force", sync=True)
            self.print_remote(out, err)
            if ret != 0:
                raise ex.excError(err)


    def thaw(self):
        """
          --force disables the remote node status check
          it is set on remote actions
        """
        do_local = True
        do_remote = True
        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        peer = self.get_peer()

        if local_status not in ['frozen_stop', 'start_ready']:
            self.log.info("local already unfrozen")
            do_local = False

        if not self.svc.force and remote_status not in ['frozen_stop', 'start_ready']:
            self.log.info("remote already unfrozen")
            do_remote = False
        if self.svc.force:
            do_remote = False

        if not do_local and not do_remote:
            return

        if not self.svc.force and remote_status == 'start_ready' and local_status == 'frozen_stop':
            out, err, ret = self.svc.remote_action(peer, "thaw --force", sync=True)
            self.print_remote(out, err)
            do_remote = False
            if ret != 0:
                raise ex.excError(err)

        self.service_action('unfreeze')
        time.sleep(2)

        if do_remote:
            out, err, ret = self.svc.remote_action(peer, "thaw --force", sync=True)
            self.print_remote(out, err)
            if ret != 0:
                raise ex.excError(err)



    def __status(self, verbose=False):
        if not os.path.exists(self.service_cmd):
            self.status_log("open-ha is not installed")
            return rcStatus.WARN
        if not self.process_running():
            self.status_log("open-ha daemons are not running")
            return rcStatus.WARN
        status = self.service_local_status()
        if status == 'unknown':
            self.status_log("unable to determine cluster service state")
            return rcStatus.WARN
        elif status in ['frozen_stop', 'start_failed', 'stop_failed', 'starting', 'start_ready', 'stopping', 'force_stop']:
            self.status_log("cluster service status is %s"%status)
            return rcStatus.WARN
        elif status in ['stopped']:
            return rcStatus.DOWN
        elif status in ['started']:
            return rcStatus.UP
        else:
            self.status_log("unknown cluster service status: %s"%status)
            return rcStatus.WARN
