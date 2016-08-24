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
from distutils.version import LooseVersion as V

os.environ['LANG'] = 'C'

class DockerLib(object):
    def __init__(self, docker_exe=None):
        if docker_exe:
            self.docker_exe_init = docker_exe

    def get_ps(self, refresh=False):
        if not refresh and hasattr(self.svc, "cache_docker_ps"):
            return self.svc.cache_docker_ps
        cmd = self.docker_cmd + ['ps', '-a', '--no-trunc']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.svc.cache_docker_ps = out
        return out

    def get_container_id_by_name_hash(self, refresh=False):
        if not refresh and hasattr(self.svc, "cache_docker_container_id_by_name_hash"):
            return self.svc.cache_docker_container_id_by_name_hash
        out = self.get_ps(refresh=refresh)
        lines = out.split('\n')
        if len(lines) < 2:
            return
        try:
            start = lines[0].index('NAMES')
        except:
            return
        data = {}
        for line in lines[1:]:
            if len(line.strip()) == 0:
                continue
            try:
                names = line[start:].strip().split(',')
            except:
                continue
            for name in names:
                # swarm names are preffixed by <nodename>/
                v = name.split("/")
                container_name = v[-1]
                if len(v) == 2:
                    swarm_node = v[0]
                else:
                    swarm_node = None
                data[container_name] = {
                  "id": line.split()[0],
                  "swarm_node": swarm_node,
                }
        self.svc.cache_docker_container_id_by_name_hash = data
        return data

    def get_container_id_by_name(self, refresh=False):
        data = self.get_container_id_by_name_hash(refresh=refresh)
        if data is None or not self.container_name in data:
            return
        d = data[self.container_name]
        if d["swarm_node"]:
            self.swarm_node = d["swarm_node"]
        return d["id"]

    def get_docker_info(self):
        if not hasattr(self, "docker_info_cache"):
            cmd = [self.docker_exe(), "info"]
            out, err, ret = justcall(cmd)
            self.docker_info_cache = out
        return self.docker_info_cache

    def get_docker_version(self):
        if not hasattr(self, "docker_version"):
            cmd = [self.docker_exe(), "--version"]
            out, err, ret = justcall(cmd)
            v = out.split()
            if len(v) < 3:
                return False
            self.docker_version = v[2].rstrip(",")
        return self.docker_version

    def docker_min_version(self, version):
        if V(self.get_docker_version()) >= V(version):
            return True
        return False

    def docker(self, action):
        cmd = self.docker_cmd + []
        if action == 'start':
            if self.container_id is None:
                self.container_id = self.get_container_id_by_name()
            if self.container_id is None:
                cmd += ['run', '-d', '--name='+self.container_name]
                cmd += self.add_run_args()
                cmd += [self.run_image]
                if self.run_command is not None and self.run_command != "":
                    cmd += self.run_command.split()
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
            self.container_id = self.get_container_id_by_name(refresh=True)
        self.get_running_instance_ids(refresh=True)

    def get_run_image_id(self, run_image=None):
        if run_image is None and hasattr(self, "run_image"):
            run_image = self.run_image
        if len(run_image) == 12 and re.match('^[a-f0-9]*$', run_image):
            return run_image
        try:
            image_name, image_tag = run_image.split(':')
        except:
            image_name, image_tag = [run_image, "latest"]

        cmd = self.docker_cmd + ['images', '--no-trunc', image_name]
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

    def get_images(self):
        if hasattr(self.svc, "docker_images_cache"):
            return self.svc.docker_images_cache
        cmd = self.docker_cmd + ['images', '--no-trunc']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        data = {}
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 3:
                continue
            if l[2] == "IMAGE":
                continue
            data[l[2]] = l[0]+':'+l[1]
        self.svc.docker_images_cache = data
        return data

    def docker_info(self):
        if hasattr(self.svc, "docker_info_done"):
            return []
        data = []
        data += self.docker_info_version()
        data += self.docker_info_drivers()
        data += self.docker_info_images()
        return data

    def docker_info_version(self):
        return [[
          "",
          "docker_version",
          self.get_docker_version()
        ]]

    def docker_info_drivers(self):
        data = []
        lines = self.get_docker_info().split("\n")
        for line in lines:
             l = line.split(": ")
             if len(l) < 2:
                 continue
             if l[0] == "Storage Driver":
                 data.append(["", "storage_driver", l[1]])
             if l[0] == "Execution Driver":
                 data.append(["", "exec_driver", l[1]])
        return data

    def docker_info_images(self):
        data = []
        images = self.get_images()
        h = {}
        for r in self.svc.get_resources("container.docker"):
            image_id = r.get_run_image_id()
            d = {"rid": r.rid, "instance_id": r.container_id}
            if image_id in h:
                h[image_id].append(d)
            else:
                h[image_id] = [d]
        for image_id in images:
            if image_id in h:
                for d in h[image_id]:
                    data.append([
                      d["rid"],
                      "docker_image",
                      image_id+":"+d["instance_id"]
                    ])
            else:
                data.append([
                  "",
                  "docker_image",
                  image_id
                ])
        self.svc.docker_info_done = True
        return data

    def image_userfriendly_name(self):
        if ':' in self.run_image:
            return self.run_image
        images = self.get_images()
        if images is None:
            return self.run_image
        if self.run_image in images:
            return images[self.run_image]
        return self.run_image

    def docker_inspect(self, id):
        cmd = self.docker_cmd + ['inspect', id]
        out, err, ret = justcall(cmd)
        data = json.loads(out)
        return data[0]

    def docker_stop(self):
        if not self.svc.docker_daemon_private:
            return
        if not self.docker_running():
            return
        if self.docker_data_dir is None:
            return
        if not os.path.exists(self.docker_pid_file):
            return

        cmd = self.docker_cmd + ['ps', '-q', '--no-trunc']
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
        if self.docker_min_version("1.8"):
            cmd = [self.docker_exe(), 'daemon',
                   '-H', self.docker_socket,
                   '-g', self.docker_data_dir,
                   '-p', self.docker_pid_file]
        else:
            cmd = self.docker_cmd + ['-r=false', '-d',
                   '-g', self.docker_data_dir,
                   '-p', self.docker_pid_file]
        if self.docker_min_version("1.9") and '--exec-root' not in str(self.docker_daemon_args):
            cmd += ["--exec-root", self.docker_data_dir]
        cmd += self.docker_daemon_args
        return cmd

    def docker_start(self, verbose=True):
        if not self.svc.docker_daemon_private:
            return
        import lock
        lockfile = os.path.join(rcEnv.pathlock, 'docker_start')
        try:
            lockfd = lock.lock(timeout=15, delay=1, lockfile=lockfile)
        except Exception as e:
            self.log.error("dockerd start lock acquire failed: %s"%str(e))
            return

        # Sanity checks before deciding to start the daemon
        if self.docker_running():
            lock.unlock(lockfd)
            return

        if self.docker_data_dir is None:
            lock.unlock(lockfd)
            return

        resource = self.docker_data_dir_resource()
        if resource is not None and resource._status() not in (rcStatus.UP, rcStatus.STDBY_UP):
            state= rcStatus.status_str(resource._status())
            self.log.warning("the docker daemon data dir is handled by the %s resource in %s state. can't start the docker daemon" % (resource.rid, state))
            lock.unlock(lockfd)
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
                lock.unlock(lockfd)
                return
            time.sleep(1)
        lock.unlock(lockfd)

    def docker_running(self):
        if self.svc.docker_daemon_private:
            return self.docker_running_private()
        else:
            return self.docker_running_shared()

    def docker_running_shared(self):
        out = self.get_docker_info()
        if out == "":
            return False
        return True

    def docker_running_private(self):
        if not os.path.exists(self.docker_pid_file):
            self.log.debug("docker_running: no pid file %s" % self.docker_pid_file)
            return False
        with open(self.docker_pid_file, "r") as f:
            buff = f.read()
        self.log.debug("docker_running: pid found in pid file %s" % buff)
        exe = os.path.join(os.sep, "proc", buff, "exe")
        try:
            exe = os.path.realpath(exe)
        except OSError as e:
            self.log.debug("docker_running: no proc info in %s" % "/proc/%s"%buff)
            os.unlink(self.docker_pid_file)
            return False
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
        if hasattr(self, "run_swarm") and self.run_swarm is not None:
            if "://" not in self.run_swarm:
                proto = "tcp://"
            else:
                proto = ""
            self.docker_socket = proto + self.run_swarm
        elif self.svc.docker_daemon_private:
            self.docker_socket = "unix://"+os.path.join(self.docker_var_d, 'docker.sock')
        else:
            self.docker_socket = None
        if self.svc.docker_daemon_private:
            self.docker_pid_file = os.path.join(self.docker_var_d, 'docker.pid')
            self.docker_data_dir = self.svc.docker_data_dir
        else:
            self.docker_pid_file = None
            l = [line for line in self.get_docker_info().split("\n") if "Root Dir" in line]
            try:
                self.docker_data_dir = l[0].split(":")[-1].strip()
            except:
                self.docker_data_dir = None
        self.docker_daemon_args = self.svc.docker_daemon_args
        self.docker_cmd = [self.docker_exe()]
        if self.docker_socket:
            self.docker_cmd += ['-H', self.docker_socket]

    def docker_exe(self):
        if hasattr(self, "docker_exe_init") and which(self.docker_exe_init):
            return self.docker_exe_init
        elif hasattr(self, "svc") and hasattr(self.svc, "docker_exe") and which(self.svc.docker_exe):
            return self.svc.docker_exe
        elif which("docker.io"):
            return "docker.io"
        elif which("docker"):
            return "docker"
        else:
            raise ex.excInitError("docker executable not found")


