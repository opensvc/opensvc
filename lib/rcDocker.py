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

import rcStatus
import json
import re
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv
import rcExceptions as ex

from svcBuilder import conf_get_string_scope

os.environ['LANG'] = 'C'

class DockerLib(object):

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
            names = line[start:].strip().split(',')
            if self.container_name in names:
                return line.split()[0]

    def docker(self, action):
        cmd = self.docker_cmd + []
        if action == 'start':
            if self.container_id is None:
                self.container_id = self.get_container_id_by_name()
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

    def get_run_image_id(self, run_image=None):
        if run_image is None and hasattr(self, "run_image"):
            run_image = self.run_image
        if len(run_image) == 12 and re.match('^[a-f0-9]*$', run_image):
            return run_image
        try:
            image_name, image_tag = run_image.split(':')
        except:
            image_name, image_tag = [run_image, "latest"]

        cmd = self.docker_cmd + ['images', image_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return run_image
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 3:
                continue
            if l[0] == image_name and l[1] == image_tag:
                return l[2]
        return run_image

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

    def dockerd_cmd(self):
        cmd = self.docker_cmd + ['-r=false', '-d',
               '-g', self.docker_data_dir,
               '-p', self.docker_pid_file]
        cmd += self.docker_daemon_args
        return cmd

    def docker_start(self, verbose=True):
        # Sanity checks before deciding to start the daemon
        if self.docker_running():
            return

        if self.docker_data_dir is None:
            return

        resource = self.docker_data_dir_resource()
        if resource is not None and resource._status() not in (rcStatus.UP, rcStatus.STDBY_UP):
            state= rcStatus.status_str(resource._status())
            self.log.warning("the docker daemon data dir is handled by the %s resource in %s state. can't start the docker daemon" % (resource.rid, state))
            return

        if os.path.exists(self.docker_pid_file):
            self.log.warning("removing leftover pid file %s" % self.docker_pid_file)
            os.unlink(self.docker_pid_file)

        # Now we can start the daemon, creating its data dir if necessary
        cmd = self.dockerd_cmd()

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
            if self.docker_working():
                self.container_id = self.get_container_id_by_name()
                return
            time.sleep(1)

    def docker_running(self):
        if not os.path.exists(self.docker_pid_file):
            self.log.debug("docker_running: no pid file %s" % self.docker_pid_file)
            return False
        with open(self.docker_pid_file, "r") as f:
            buff = f.read()
        self.log.debug("docker_running: pid found in pid file %s" % buff)
        if not os.path.exists("/proc/%s"%buff):
            self.log.debug("docker_running: no proc info in %s" % "/proc/%s"%buff)
            return False
        exe = os.path.join(os.sep, "proc", buff, "exe")
        exe = os.path.realpath(exe)
        if "docker" not in exe:
            self.log.debug("docker_running: pid found but owned by a process that is not a docker (%s)" % exe)
            os.unlink(self.docker_pid_file)
            return False
        return True

    def docker_working(self):
        cmd = self.docker_cmd + ['info']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def on_add(self):
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
        if "--exec-opt" not in self.docker_daemon_args:
            self.docker_daemon_args += ["--exec-opt", "native.cgroupdriver=cgroupfs"]
        self.docker_cmd = [self.docker_exe(), '-H', self.docker_socket_uri]

    def docker_exe(self):
        if which("docker.io"):
            return "docker.io"
        elif which("docker"):
            return "docker"
        else:
            raise ex.excInitError("docker executable not found")


