import resHb
from rcGlobalEnv import rcEnv
import os
import logging
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
        if which("openha"):
            self.need_env = False
            self.basedir = os.path.join(os.sep, 'usr', 'lib', 'openha')
            self.bindir = os.path.join(self.basedir, 'bin')
            self.svcdir = os.path.join(os.sep, 'var', 'lib', 'openha', 'services')
            self.confdir = os.path.join(os.sep, 'etc', 'openha')
            self.service_cmd = ['openha', 'service']
        else:
            self.need_env = True
            self.basedir = os.path.join(os.sep, 'usr', 'local', 'cluster')
            self.bindir = os.path.join(self.basedir, 'bin')
            self.logdir = os.path.join(self.basedir, 'log')
            self.svcdir = os.path.join(self.basedir, 'services')
            self.confdir = os.path.join(self.confdir, 'conf')
            os.environ['EZ'] = self.basedir
            os.environ['EZ_BIN'] = self.bindir
            os.environ['EZ_SERVICES'] = self.cfsvc
            os.environ['EZ_NODES'] = self.cfnod
            os.environ['EZ_MONITOR'] = self.cfmon
            os.environ['EZ_LOG'] = self.logdir
            self.service_cmd = [os.path.join(self.bindir, 'service')]

        self.cfsvc = os.path.join(self.confdir, 'services')
        self.cfnod = os.path.join(self.confdir, 'nodes')
        self.cfmon = os.path.join(self.confdir, 'monitor')

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
        from subprocess import Popen
        cmd = self.service_cmd + ['-A', self.cluster_name(), command]
        p = Popen(cmd)
        out, err = p.communicate()
        if p.returncode != 0:
            raise ex.excError

    def freezestop(self):
        cmd = []
        if self.need_env:
            cmd += ['env']
            vars = ('EZ', 'EZ_BIN', 'EZ_SERVICES', 'EZ_NODES', 'EZ_MONITOR', 'EZ_LOG')
            for var in vars:
                cmd.append(var+'='+os.environ[var])
        cmd += self.service_cmd + ['-A', self.cluster_name(), 'freeze-stop']
        self.svc.node.cmdworker.enqueue(cmd)

    def process_running(self):
        # self.cfmon exist if OpenHA setup is done
        # os.path.exists(self.cfmon) return true if OpenHA setup is done
        # _not_ os.path.exists(self.cfmon) = True mean that setup is _not_ done
        # in this case, self.process_running() have to return _False_
        if not os.path.exists(self.cfmon):
            self.log.debug('openha configuration is not complete')
            return False
        buff = ""
        with open(self.cfmon) as f:
            buff = f.read()
        daemons = []
        for line in buff.split('\n'):
            self.log.debug('monitor file [%s]'%line)
            l = line.split()
            if len(l) < 4:
                continue
            if '#' in l[0]:
                continue
            if l[1] == 'net':
                suffix = ''
            elif l[1] == 'unicast':
                suffix = '_unicast'
            elif l[1] == 'disk':
                suffix = '_raw'
            else:
                suffix = '_dio'
            self.log.debug('nodename [%s]  monitor nodename [%s]'%(rcEnv.nodename,l[0].lower()))
            if rcEnv.nodename == l[0].lower():
                daemon = self.heartd + suffix
                string = daemon + ' ' + ' '.join(l[2:-1])
                self.log.debug('append heartd daemon [%s]'%string)
                daemons.append(string)
            else:
                daemon = self.heartc + suffix
                string = daemon + ' ' + ' '.join(l[2:])
                self.log.debug('append heartc daemon [%s]'%string)
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
        if self.log.isEnabledFor(logging.DEBUG):
            for line in lines:
                self.log.debug('ps daemons [%s]'%line)
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
            self.log.debug('daemon [%s] [%s]'%(d,h[d]))
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
          --remote disables the remote node status check
          it is set on remote actions
        """
        do_local = True
        do_remote = True
        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        peer = self.get_peer()
        self.log.debug('freeze: local_status=%s remote_status=%s'%(local_status, remote_status))

        if local_status in ['frozen_stop', 'start_ready']:
            self.log.info("local already frozen")
            do_local = False

        if not self.svc.remote and remote_status in ['frozen_stop', 'start_ready']:
            self.log.info("remote already frozen")
            do_remote = False
        if self.svc.remote:
            do_remote = False

        if not do_local and not do_remote:
            return

        if not self.svc.remote and remote_status == 'stopped' and local_status == 'started':
            out, err, ret = self.svc.remote_action(peer, "freeze --remote", sync=True)
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
            out, err, ret = self.svc.remote_action(peer, "freeze --remote", sync=True)
            self.print_remote(out, err)
            if ret != 0:
                raise ex.excError(err)

        self.status(refresh=True)


    def thaw(self):
        """
          --remote disables the remote node status check
          it is set on remote actions
        """
        do_local = True
        do_remote = True
        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        self.log.debug('thaw: local_status=%s remote_status=%s'%(local_status, remote_status))
        peer = self.get_peer()

        if local_status not in ['frozen_stop', 'start_ready']:
            self.log.info("local already unfrozen")
            do_local = False

        if not self.svc.remote and remote_status not in ['frozen_stop', 'start_ready']:
            self.log.info("remote already unfrozen")
            do_remote = False
        if self.svc.remote:
            do_remote = False

        if not do_local and not do_remote:
            return

        if not self.svc.remote and remote_status == 'start_ready' and local_status == 'frozen_stop':
            out, err, ret = self.svc.remote_action(peer, "thaw --remote", sync=True)
            self.print_remote(out, err)
            do_remote = False
            if ret != 0:
                raise ex.excError(err)

        if local_status in ['frozen_stop', 'start_ready']:
            self.service_action('unfreeze')
            time.sleep(2)

        if do_remote:
            out, err, ret = self.svc.remote_action(peer, "thaw --remote", sync=True)
            self.print_remote(out, err)
            if ret != 0:
                raise ex.excError(err)

        self.status(refresh=True)


    def wait_for_state(self, states, timeout=10, remote=False):
        if remote:
            node = "remote"
        else:
            node = "local"
        self.log.info("waiting for %s state to become either %s"%(node, ' or '.join(states)))
        for i in range(timeout):
            if remote:
                status = self.service_remote_status()
            else:
                status = self.service_local_status()
            if status in states:
                return
            time.sleep(1)
        raise ex.excError("waited %d seconds for %s state to become either %s. current state: %s"%(timeout, node, ' or '.join(states), status))

    def switch(self):
        if self.svc.cluster:
            """
            if called by the heartbeat daemon, don't drive the hb service
            """
            self.log.debug('switch : called by heartbeat daemon, returning.')
            return

        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        self.log.debug('switch: local_status=%s remote_status=%s'%(local_status, remote_status))
        peer = self.get_peer()
        if local_status == "started":
            if remote_status == "frozen_stop":
                raise ex.excError("remote state is frozen_stop, can't relocate service")
            try:
                self.stop()
            except ex.excEndAction:
                pass
            self.wait_for_state(["stopped", "stop_failed", "stopping", "frozen_stop"])
            local_status = self.service_local_status()
            self.log.debug('switch [local was started]: local_status=%s'%(local_status))
            if local_status == "stop_failed":
                raise ex.excError("local state is stop_failed")
            remote_status = self.service_remote_status()
            self.log.debug('switch [local was started]: remote_status=%s'%(remote_status))
            if not self.svc.remote and remote_status in ['stopped', 'frozen_stop']:
                out, err, ret = self.svc.remote_action(peer, "start --remote", sync=True)
                self.print_remote(out, err)
                if ret != 0:
                    raise ex.excError(err)
            self.wait_for_state(["started", "start_failed", "starting"], remote=True)
        elif remote_status == "started":
            if not self.svc.remote:
                out, err, ret = self.svc.remote_action(peer, "stop --remote", sync=True)
                self.print_remote(out, err)
                if ret != 0:
                    raise ex.excError(err)
            self.wait_for_state(["stopped", "stop_failed", "stopping", "frozen_stop"], remote=True)
            remote_status = self.service_remote_status()
            self.log.debug('switch [remote was started]: remote_status=%s'%(remote_status))
            if remote_status == "stop_failed":
                raise ex.excError("remote state is stop_failed")
            self.start()
            self.wait_for_state(["started", "start_failed", "starting"])
        else:
            raise ex.excError("cannot switch in current state: %s/%s"%(local_status,remote_status))

        self.thaw()
        raise ex.excEndAction("heartbeat actions done")

    def start(self):
        if self.svc.cluster:
            """
            if called by the heartbeat daemon, don't drive the hb service
            """
            self.log.debug('start: called by heartbeat daemon, returning.')
            return

        if self.svc.options.parm_rid is not None or \
           self.svc.options.parm_tags is not None or \
           self.svc.options.parm_subsets is not None:
            self.log.debug('start: called with --rid, --tags or --subset, returning.')
            return

        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        self.log.debug('start: local_status=%s remote_status=%s'%(local_status, remote_status))
        peer = self.get_peer()

        if remote_status == 'started':
            raise ex.excError("already started on peer node")

        if remote_status == 'start_failed':
            raise ex.excError("start_failed on peer node. please investigate the reason and freeze-stop the service on peer node before trying to start.")

        if remote_status == 'start_ready':
            raise ex.excError("start_ready on peer node. please stop the service on the peer node before trying to start.")

        if local_status == 'started':
            raise ex.excEndAction("already started")

        if local_status == 'start_failed':
            raise ex.excError("start_failed on local node. please investigate the reason and freeze-stop the service on local node before trying to start.")

        if not self.svc.remote and remote_status == 'stopped':
            out, err, ret = self.svc.remote_action(peer, "freeze --remote", sync=True)
            self.print_remote(out, err)
            do_remote = False
            if ret != 0:
                raise ex.excError(err)

        if local_status in ('frozen_stop', 'start_ready'):
            self.service_action("unfreeze")
        elif local_status == 'stopped':
            pass
        else:
            raise ex.excError("%s on local node. unexpected state"%local_status)

        self.wait_for_state(['starting', 'started', 'start_failed'])
        self.thaw()

        raise ex.excEndAction("heartbeat actions done")

    def stop(self):
        if self.svc.cluster:
            """
            if called by the heartbeat daemon, don't drive the hb service
            """
            self.log.debug('stop: called by heartbeat daemon, returning.')
            return

        if self.svc.options.parm_rid is not None or \
           self.svc.options.parm_tags is not None or \
           self.svc.options.parm_subsets is not None:
            self.log.debug('stop: called with --rid, --tags or --subset, returning.')
            return

        local_status = self.service_local_status()
        remote_status = self.service_remote_status()
        self.log.debug('stop: local_status=%s remote_status=%s'%(local_status, remote_status))
        peer = self.get_peer()

        if not self.svc.remote and remote_status != 'frozen_stop':
            out, err, ret = self.svc.remote_action(peer, "stop --remote", sync=True)
            if ret != 0:
                raise ex.excError(err)

        if local_status == 'frozen_stop':
            raise ex.excEndAction("already frozen stop")

        self.svc.svcunlock()
        self.service_action('freeze-stop')

        raise ex.excEndAction("heartbeat actions done")

    def __status(self, verbose=False):
        if which(self.service_cmd[0]) is None:
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
