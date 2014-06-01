#
# Copyright (c) 2014 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2014 Arnaud Veron <arnaud.veron@opensvc.com>'
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
import os
from datetime import datetime
from subprocess import *

import sys
import rcStatus
import json
import re
import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv
import resContainer
import rcExceptions as ex

from svcBuilder import conf_get_string_scope

os.environ['LANG'] = 'C'

class Docker(resContainer.Container):

    def files_to_sync(self):
        return []

    def operational(self):
        return True

    def rcp_from(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def docker_data_dir_resource(self):
        mntpts = []
        mntpt_res = {}
        for resource in self.svc.get_resources('fs'):
            mntpts.append(resource.mountPoint)
            mntpt_res[resource.mountPoint] = resource
        for mntpt in sorted(mntpts, reverse=True):
            if mntpt.startswith(self.docker_data_dir):
                return mntpt_res[mntpt]

    def add_run_args(self):
        if self.run_args is None:
            return []
        l = self.run_args.split()
        for e, i in enumerate(l):
            if e != '-p':
                continue
            if len(l) < i + 2:
                # bad
                break
            volarg = l[i+1]
            if ':' in volarg:
                # mapping ... check source dir presence
                v = volarg.split(':')
                if len(v) != 3:
                    raise ex.excError("mapping %s should be formatted as <src>:<dst>:<ro|rw>" % (volarg))
                if not os.path.exists(v[0]):
                    raise ex.excError("source dir of mapping %s does not exist" % (volarg))
        return l

    def get_container_id_by_name(self):
        cmd = self.docker_cmd + ['ps', '-a']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        lines = out.split('\n')
        if len(lines) < 2:
            return
        try:
            start = lines[0].index('NAMES')
        except:
            return
        for line in lines[1:]:
            name = line[start:].strip()
            if name == self.container_name:
                return line.split()[0]

    def docker(self, action):
        cmd = self.docker_cmd + []
        if action == 'start':
            if self.container_id is None:
                cmd += ['run', '-t', '-i', '-d', '--name='+self.container_name]
                cmd += self.add_run_args()
                cmd += [self.run_image]
                if self.run_command is not None and self.run_command != "":
                    cmd += [self.run_command]
            else:
                cmd += ['start', self.container_id]
        elif action == 'stop':
            cmd += ['stop', self.container_id]
        elif action == 'kill':
            cmd += ['kill', self.container_id]
        else:
            self.log.error("unsupported docker action: %s" % action)
            return 1

        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

        if action == 'start':
            self.container_id = self.get_container_id_by_name()

    def container_start(self):
        self.docker_start()
        self.docker('start')

    def container_stop(self):
        self.docker('stop')

    def stop(self):
        resContainer.Container.stop(self)
        self.docker_stop()
 
    def get_run_image_id(self):
        if len(self.run_image) == 12 and re.match('[a-z0-9]*', self.run_image):
            return self.run_image
        try:
            image_name, image_tag = self.run_image.split(':')
        except:
            image_name, image_tag = [self.run_image, "latest"]

        cmd = self.docker_cmd + ['images', image_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.run_image
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 3:
                continue
            if l[0] == image_name and l[1] == image_tag:
                return l[2]
        return self.run_image


    def _status(self, verbose=False):
        s = resContainer.Container._status(self, verbose)
        try:
            inspect = self.docker_inspect(self.container_id)
        except Exception as e:
            return s
        running_image_id = str(inspect['Image'][:12])
        run_image_id = self.get_run_image_id()

        if run_image_id != running_image_id:
            self.status_log("the running container is based on image '%s' instead of '%s'"%(running_image_id, run_image_id))
            s = rcStatus._merge(s, rcStatus.WARN)

        return s

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self, nodename=None):
        if self.docker_data_dir is None:
            self.status_log("DEFAULT.docker_data_dir must be defined")

        if not self.docker_running():
            self.status_log("docker daemon is not running")
            return False

        if self.container_id is None:
            self.status_log("can not find container id")
            return False

        cmd = self.docker_cmd + ['ps', '-q']
        out, err, ret = justcall(cmd)

        if self.container_id in out.replace('\n', ' ').split():
            return True
        return False

    def get_container_info(self):
        cpu_set = self.get_cf_value("lxc.cgroup.cpuset.cpus")
        #d = json.loads(response)
        if cpu_set is None:
            vcpus = 0
        else:
            vcpus = len(cpu_set.split(','))
        return {'vcpus': str(vcpus), 'vmem': '0'}

    def image_userfriendly_name(self):
        if ':' in self.run_image:
            return self.run_image
        cmd = self.docker_cmd + ['images']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return self.run_image
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 3:
                continue
            if l[2] == self.run_image:
                return l[0]+':'+l[1]
        return self.run_image
        
    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        return True

    def docker_inspect(self, id):
        cmd = self.docker_cmd + ['inspect', id]
        out, err, ret = justcall(cmd)
        data = json.loads(out)
        return data[0]

    def docker_stop(self):
        if not self.docker_running():
            return
        if self.docker_data_dir is None:
            return
        if not os.path.exists(self.docker_pid_file):
            return

        cmd = self.docker_cmd + ['ps', '-q']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return

        if len(out) > 0:
            # skip stop ... daemon still handles containers
            return

        try:
            with open(self.docker_pid_file, 'r') as f:
                pid = int(f.read())
        except:
            self.log.warning("can't read %s. skip docker daemon kill" % self.docker_pid_file)
            return

        self.log.info("no more container handled by docker daemon. shut it down")
        import signal
        os.kill(pid, signal.SIGTERM)

    def docker_start(self, verbose=True):
        # Sanity checks before deciding to start the daemon
        if self.docker_running():
            return

        if self.docker_data_dir is None:
            return

        resource = self.docker_data_dir_resource()
        if resource is not None and resource._status() != rcStatus.UP:
            state= rcStatus.status_str(resource._status())
            self.log.warning("the docker daemon data dir is handled by the %s resource in %s state. can't start the docker daemon" % (resource.rid, state))
            return

        # Now we can start the daemon, creating its data dir if necessary
        cmd = self.docker_cmd + ['-r=false', '-d',
               '-g', self.docker_data_dir,
               '-p', self.docker_pid_file]
        cmd += self.docker_daemon_args

        if verbose:
            self.log.info("starting docker daemon")
            self.log.info(" ".join(cmd))
        import subprocess
        subprocess.Popen(['nohup'] + cmd,
                 stdout=open('/dev/null', 'w'),
                 stderr=open('/dev/null', 'a'),
                 preexec_fn=os.setpgrp
                 )

        import time
        for i in range(self.max_wait_for_dockerd):
            if self.docker_running():
                self.container_id = self.get_container_id_by_name()
                return
            time.sleep(1)

    def docker_running(self):
        cmd = self.docker_cmd + ['info']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def __init__(self,
                 rid,
                 run_image,
                 run_command=None,
                 run_args=None,
                 guestos="Linux",
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 subset=None,
                 tags=set([]),
                 always_on=set([])):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name="127.0.0.1",
                                        type="container.docker",
                                        guestos=guestos,
                                        optional=optional,
                                        disabled=disabled,
                                        monitor=monitor,
                                        restart=restart,
                                        subset=subset,
                                        tags=tags,
                                        always_on=always_on)

        self.run_image = run_image
        self.run_command = run_command
        self.run_args = run_args
        self.max_wait_for_dockerd = 5

    def on_add(self):
        self.container_name = self.svc.svcname+'.'+self.rid
        self.container_name = self.container_name.replace('#', '.')
        self.docker_var_d = os.path.join(rcEnv.pathvar, self.svc.svcname)
        if not os.path.exists(self.docker_var_d):
            os.makedirs(self.docker_var_d)
        self.docker_pid_file = os.path.join(self.docker_var_d, 'docker.pid')
        self.docker_socket = os.path.join(self.docker_var_d, 'docker.sock')
        self.docker_socket_uri = 'unix://' + self.docker_socket
        try:
            self.docker_data_dir = conf_get_string_scope(self.svc, self.svc.config, 'DEFAULT', 'docker_data_dir')
        except ex.OptNotFound:
            self.docker_data_dir = None
        try:
            self.docker_daemon_args = conf_get_string_scope(self.svc, self.svc.config, 'DEFAULT', 'docker_daemon_args').split()
        except ex.OptNotFound:
            self.docker_daemon_args = []
        self.docker_cmd = [self.docker_exe(), '-H', self.docker_socket_uri]
        self.label = ""
        try:
            self.container_id = self.get_container_id_by_name()
            self.label += self.container_id + "@"
        except Exception as e:
            self.container_id = None
        self.label += self.image_userfriendly_name()

    def docker_exe(self):
        if which("docker.io"):
            return "docker.io"
        elif which("docker"):
            return "docker"
        else:
            raise ex.excInit("docker executable not found")

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def provision(self):
        # docker resources are naturally provisioned
        self.start()
