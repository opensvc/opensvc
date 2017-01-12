import os
from datetime import datetime
from subprocess import *

import rcStatus
import json
import re
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv
import rcExceptions as ex

from svcBuilder import conf_get_string_scope, conf_get_boolean_scope
from distutils.version import LooseVersion as V

os.environ['LANG'] = 'C'

class DockerLib(object):
    def __init__(self, svc=None):
        self.svc = svc
        self.max_wait_for_dockerd = 5

        try:
            self.docker_daemon_private = conf_get_boolean_scope(svc, svc.config, 'DEFAULT', 'docker_daemon_private')
        except ex.OptNotFound:
            self.docker_daemon_private = True
        if rcEnv.sysname != "Linux":
            self.docker_daemon_private = False

        try:
            self.docker_exe_init = conf_get_string_scope(svc, svc.config, 'DEFAULT', 'docker_exe')
        except ex.OptNotFound:
            self.docker_exe_init = None

        try:
            self.docker_data_dir = conf_get_string_scope(svc, svc.config, 'DEFAULT', 'docker_data_dir')
        except ex.OptNotFound:
            self.docker_data_dir = None

        try:
            self.docker_daemon_args = conf_get_string_scope(svc, svc.config, 'DEFAULT', 'docker_daemon_args').split()
        except ex.OptNotFound:
            self.docker_daemon_args = []

        if self.docker_data_dir:
            if "--exec-opt" not in self.docker_daemon_args and self.docker_min_version("1.7"):
                self.docker_daemon_args += ["--exec-opt", "native.cgroupdriver=cgroupfs"]

        self.docker_var_d = os.path.join(rcEnv.pathvar, self.svc.svcname)

        if not os.path.exists(self.docker_var_d):
            os.makedirs(self.docker_var_d)
        elif self.docker_daemon_private:
            self.docker_socket = "unix://"+os.path.join(self.docker_var_d, 'docker.sock')
        else:
            self.docker_socket = None

        if self.docker_daemon_private:
            self.docker_pid_file = os.path.join(self.docker_var_d, 'docker.pid')
        else:
            self.docker_pid_file = None
            l = [line for line in self.get_docker_info().split("\n") if "Root Dir" in line]
            try:
                self.docker_data_dir = l[0].split(":")[-1].strip()
            except:
                self.docker_data_dir = None

        self.docker_cmd = [self.docker_exe()]
        if self.docker_socket:
            self.docker_cmd += ['-H', self.docker_socket]

    def get_ps(self, refresh=False):
        if not refresh and hasattr(self, "cache_docker_ps"):
            return self.cache_docker_ps
        cmd = self.docker_cmd + ['ps', '-a', '--no-trunc']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.cache_docker_ps = out
        return out

    def get_container_id_by_name_hash(self, refresh=False):
        if not refresh and hasattr(self, "cache_docker_container_id_by_name_hash"):
            return self.cache_docker_container_id_by_name_hash
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
        self.cache_docker_container_id_by_name_hash = data
        return data

    def get_container_id_by_name(self, r, refresh=False):
        data = self.get_container_id_by_name_hash(refresh=refresh)
        if data is None or not r.container_name in data:
            return
        d = data[r.container_name]
        if d["swarm_node"]:
            r.swarm_node = d["swarm_node"]
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

    def get_running_instance_ids(self, refresh=False):
        if not refresh and hasattr(self, "docker_running_instance_ids_cache"):
            return self.docker_running_instance_ids_cache
        self.docker_running_instance_ids_cache = self._get_running_instance_ids()
        return self.docker_running_instance_ids_cache

    def _get_running_instance_ids(self):
        cmd = self.docker_cmd + ['ps', '-q', '--no-trunc']
        out, err, ret = justcall(cmd)
        return out.replace('\n', ' ').split()

    def get_run_image_id(self, resource, run_image=None):
        if run_image is None and hasattr(resource, "run_image"):
            run_image = resource.run_image
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
        if hasattr(self, "docker_images_cache"):
            return self.docker_images_cache
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
        self.docker_images_cache = data
        return data

    def docker_info(self):
        if hasattr(self, "docker_info_done"):
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
        images_done = []
        h = {}

        # referenced images
        for resource in self.svc.get_resources("container.docker"):
            image_id = self.get_run_image_id(resource)
            images_done.append(image_id)
            data.append([resource.rid, "run_image", resource.run_image])
            data.append([resource.rid, "docker_image_id", image_id])
            data.append([resource.rid, "docker_instance_id", resource.container_id])

        # unreferenced images
        for image_id in images:
            if image_id in images_done:
                continue
            data.append(["", "docker_image_id", image_id])
        self.docker_info_done = True

        return data

    def image_userfriendly_name(self, resource):
        if ':' in resource.run_image:
            return resource.run_image
        images = self.get_images()
        if images is None:
            return resource.run_image
        if resource.run_image in images:
            return images[resource.run_image]
        return resource.run_image

    def docker_inspect(self, id):
        cmd = self.docker_cmd + ['inspect', id]
        out, err, ret = justcall(cmd)
        data = json.loads(out)
        return data[0]

    def docker_stop(self):
        if not self.docker_daemon_private:
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
            self.svc.log.warning("can't read %s. skip docker daemon kill" % self.docker_pid_file)
            return

        self.svc.log.info("no more container handled by docker daemon (pid %d). shut it down" % pid)
        import signal
        import time
        tries = 10
        os.kill(pid, signal.SIGTERM)
        while self.docker_running() and tries > 0:
            tries -= 1
            time.sleep(1)
        if tries == 0:
            self.svc.log.warning("dockerd did not stop properly. send a kill signal")
            os.kill(pid, signal.SIGKILL)
   

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

    def docker_data_dir_resource(self):
        mntpts = []
        mntpt_res = {}
        for resource in self.svc.get_resources('fs'):
            mntpts.append(resource.mount_point)
            mntpt_res[resource.mount_point] = resource
        for mntpt in sorted(mntpts, reverse=True):
            if mntpt.startswith(self.docker_data_dir):
                return mntpt_res[mntpt]

    def docker_start(self, verbose=True):
        if not self.docker_daemon_private:
            return
        import lock
        lockfile = os.path.join(rcEnv.pathlock, 'docker_start')
        try:
            lockfd = lock.lock(timeout=15, delay=1, lockfile=lockfile)
        except Exception as e:
            self.svc.log.error("dockerd start lock acquire failed: %s"%str(e))
            return

        # Sanity checks before deciding to start the daemon
        if self.docker_running():
            lock.unlock(lockfd)
            return

        if self.docker_data_dir is None:
            lock.unlock(lockfd)
            return

        resource = self.docker_data_dir_resource()
        if resource is not None:
            state = resource._status()
            if state not in (rcStatus.UP, rcStatus.STDBY_UP):
                self.svc.log.warning("the docker daemon data dir is handled by the %s "
                                 "resource in %s state. can't start the docker "
                                 "daemon" % (resource.rid, rcStatus.Status(state)))
                lock.unlock(lockfd)
                return

        if os.path.exists(self.docker_pid_file):
            self.svc.log.warning("removing leftover pid file %s" % self.docker_pid_file)
            os.unlink(self.docker_pid_file)

        # Now we can start the daemon, creating its data dir if necessary
        cmd = self.dockerd_cmd()

        if verbose:
            self.svc.log.info("starting docker daemon")
            self.svc.log.info(" ".join(cmd))
        import subprocess
        subprocess.Popen(['nohup'] + cmd,
                 stdout=open('/dev/null', 'w'),
                 stderr=open('/dev/null', 'a'),
                 preexec_fn=os.setpgrp
                 )

        import time
        try:
            for _ in range(self.max_wait_for_dockerd):
                if self.docker_working():
                    return
                time.sleep(1)
        finally:
            lock.unlock(lockfd)

    def docker_running(self):
        if self.docker_daemon_private:
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
            self.svc.log.debug("docker_running: no pid file %s" % self.docker_pid_file)
            return False
        try:
            with open(self.docker_pid_file, "r") as f:
                buff = f.read()
        except IOError as exc:
            if exc.errno == 2:
                return False
            return ex.excError("docker_running: "+str(exc))
        self.svc.log.debug("docker_running: pid found in pid file %s" % buff)
        exe = os.path.join(os.sep, "proc", buff, "exe")
        try:
            exe = os.path.realpath(exe)
        except OSError as e:
            self.svc.log.debug("docker_running: no proc info in %s" % "/proc/%s"%buff)
            try:
                os.unlink(self.docker_pid_file)
            except OSError:
                pass
            return False
        if "docker" not in exe:
            self.svc.log.debug("docker_running: pid found but owned by a process that is not a docker (%s)" % exe)
            try:
                os.unlink(self.docker_pid_file)
            except OSError:
                pass
            return False
        return True

    def docker_working(self):
        cmd = self.docker_cmd + ['info']
        out, err, ret = justcall(cmd)
        if ret != 0:
            return False
        return True

    def docker_exe(self):
        if self.docker_exe_init and which(self.docker_exe_init):
            return self.docker_exe_init
        elif which("docker.io"):
            return "docker.io"
        elif which("docker"):
            return "docker"
        else:
            raise ex.excInitError("docker executable not found")


