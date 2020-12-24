# -*- coding: utf8 -*-

"""
The module implementing the DockerLib class.
"""
import errno
import json
import os
import re
import time
from distutils.version import LooseVersion as V # pylint: disable=no-name-in-module,import-error

import utilities.lock
import core.status
import core.exceptions as ex

from env import Env
from utilities.lazy import threadsafe_lazy as lazy, unset_lazy, set_lazy
from utilities.proc import justcall

def has_docker(program_list):
    for exe in program_list:
        try:
            out = justcall([exe, "--version"])[0]
        except:
            continue
        if out.startswith("Docker"):
            return True
    return False

class ContainerLib(object):
    """
    Instanciated as the 'dockerlib' Svc lazy attribute, this class abstracts
    docker daemon ops.
    """
    docker_cmd = ["/bin/true"]
    json_opt = ["--format", "{{json .}}"]
    container_type = "none"
    config_args_position_head = True

    def __init__(self, svc=None):
        self.svc = svc
        self.docker_info_done = False
        self.raw_container_data_dir = self.svc.oget("DEFAULT", "container_data_dir")

    def client_config_args(self, path):
        return ["--config", os.path.dirname(path)]

    @lazy
    def container_data_dir(self):
        if not self.raw_container_data_dir:
            return
        return self.svc.replace_volname(self.raw_container_data_dir, errors="ignore")[0]

    @lazy
    def docker_exe(self):
        return "/bin/true"

    def get_ps(self, refresh=False):
        """
        Return the "docker ps" output from cache or from the command
        execution depending on <refresh>.
        """
        if refresh:
            unset_lazy(self, "container_ps")
        return self.container_ps

    @lazy
    def container_ps(self):
        """
        The "docker ps -a --no-trunc" json loaded dicts assembled in a list.
        """
        cmd = self.docker_cmd + ["ps", "-a", "--no-trunc"] + self.json_opt
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        data = []
        for line in out.splitlines():
            data.append(json.loads(line))
        return data

    @lazy
    def container_by_label(self):
        """
        A hash of instances data as found in "docker ps", indexed by
        instance label.
        """
        data = {}
        for container in self.container_ps:  # pylint: disable=not-an-iterable
            labels = container.get("Labels", "")
            if isinstance(labels, dict):
                # podman
                for lname, lvalue in labels.items():
                    label = "%s=%s" % (lname, lvalue)
                    try:
                        data[label].append(container)
                    except KeyError:
                        data[label] = [container]
            else:
                # docker
                for label in labels.split(","):
                    try:
                        data[label].append(container)
                    except KeyError:
                        data[label] = [container]
        return data

    @lazy
    def container_by_name(self):
        """
        A hash of instances data as found in "docker ps", indexed by
        instance id.
        """
        data = {}
        for container in self.container_ps:  # pylint: disable=not-an-iterable
            for name in container.get("Names", "").split(","):
                try:
                    data[name].append(container)
                except KeyError:
                    data[name] = [container]
        return data

    def get_container_id_by_name(self, resource, refresh=False):
        """
        Return the container id for the <resource> container resource.
        Lookup in docker ps by docker name <namepace>..<name>.container.<n>
        where <n> is the identifier part of the resource id.
        """
        if refresh:
            unset_lazy(self, "container_ps")
            unset_lazy(self, "container_by_name")
        try:
            return self.container_by_name[resource.name][0]["ID"]  # pylint: disable=unsubscriptable-object
        except Exception:
            return

    def get_container_id_by_label(self, resource, refresh=False):
        """
        Return the container id for the <resource> container resource.
        Lookup in docker ps by docker label com.opensvc.id=<...>.
        """
        if refresh:
            unset_lazy(self, "container_ps")
            unset_lazy(self, "container_by_label")
        try:
            # pylint: disable=unsubscriptable-object
            return self.container_by_label[resource.container_label_id][0]["ID"]
        except Exception as exc:
            return

    def get_container_id(self, resource, refresh=False):
        if refresh:
            unset_lazy(self, "container_ps")
            unset_lazy(self, "container_by_label")
            unset_lazy(self, "container_by_name")
        cid = self.get_container_id_by_label(resource, refresh=False)
        if cid:
            return cid
        try:
            return self.get_container_id_by_name(resource, refresh=False)
        except:
            return

    @lazy
    def docker_info(self):
        """
        The output of "docker info".
        """
        try:
            self.docker_exe
        except ex.InitError:
            return ""
        cmd = [self.docker_exe, "info"] + self.json_opt
        try:
            data = json.loads(justcall(cmd)[0])
        except ValueError:
            data = {}
        return data

    @lazy
    def docker_version(self):
        """
        The docker version.
        """
        try:
            cmd = [self.docker_exe, "--version"]
        except ex.InitError:
            return "0"
        out = justcall(cmd)[0]
        elements = out.split()
        if len(elements) < 3:
            return False
        return elements[2].rstrip(",")

    def docker_min_version(self, version):
        """
        Return True if the docker version is at least <version>.
        """
        try:
            cmd = [self.docker_exe, "--version"]
        except ex.InitError:
            return False
        if V(self.docker_version) >= V(version):
            return True
        return False

    def get_running_instance_ids(self, refresh=False):
        """
        Return the list of running docker instances id.
        """
        if refresh:
            unset_lazy(self, "container_ps")
            unset_lazy(self, "running_instance_ids")
        return self.running_instance_ids

    @lazy
    def running_instance_ids(self):
        """
        The list of running docker instances id.
        """
        if self.docker_cmd is None:
            return []
        return [ps["ID"] for ps in self.container_ps  # pylint: disable=not-an-iterable
                if ps["Status"].startswith("Up ")]

    def get_image_id(self, name):
        """
        Return the full image id
        """
        if name.startswith("sha256:"):
            return name[7:]
        image = self.get_image(name)
        if not image:
            return
        return image["id"]

    def get_image(self, name):
        """
        Lookup the image data by name
        """
        try:
            image_name, image_tag = name.rsplit(":", 1)
        except ValueError:
            image_name = name
            image_tag = "latest"
        if "/" in image_tag:
            image_name = name
            image_tag = "latest"

        for data in self.images:  # pylint: disable=not-an-iterable
            if data["tag"] != image_tag:
                continue
            if data["name"] == image_name:
                return data
            if data["name"] == "docker.io/library/"+image_name:
                return data
            if data["name"] == "docker.io/"+image_name:
                return data
            if data["name"].replace("docker.io/library/", "docker.io/") == image_name:
                return data
            if data["name"].replace("docker.io/", "docker.io/library/") == image_name:
                return data

    def login_as_service_args(self):
        uuid = self.svc.node.conf_get("node", "uuid")
        args = ["-u", self.svc.path+"@"+Env.nodename]
        args += ["-p", uuid]
        if self.docker_min_version("1.12"):
            pass
        elif self.docker_min_version("1.10"):
            args += ["--email", self.svc.path+"@"+Env.nodename]
        return args

    def docker_login(self, ref):
        if "/" not in ref:
            return
        reg = ref.split("/")[0]
        if reg == "docker.io":
            return
        try:
            cmd = self.docker_cmd + ["login", reg] + self.login_as_service_args()
        except Exception:
            self.svc.log.debug("skip registry login as service: node not registered")
            return
        justcall(cmd)

    def image_pull(self, ref, config=None):
        if config:
            extra = self.client_config_args(config)
        else:
            extra = []
            self.docker_login(ref)
        self.svc.log.info("pulling image %s" % ref)
        if self.config_args_position_head:
            cmd = self.docker_cmd + extra + ["pull", ref]
        else:
            cmd = self.docker_cmd + ["pull"] + extra + [ref]
        results = justcall(cmd)
        if results[2] != 0:
            raise ex.Error(results[1])

    @lazy
    def images(self):
        """
        The hash of docker images, indexed by image id.
        """
        if self.docker_cmd is None:
            return {}
        cmd = self.docker_cmd + ["images", "--no-trunc"]
        results = justcall(cmd)
        if results[2] != 0:
            return {}
        data = []
        for line in results[0].splitlines():
            elements = line.split()
            if len(elements) < 3:
                continue
            if elements[2] == "IMAGE":
                continue
            image_id = re.sub("^sha256:", "", elements[2])
            data.append({
                "name": elements[0],
                "tag": elements[1],
                "id": image_id,
            })
        return data

    def _info(self):
        """
        Return the keys contributed to resinfo.
        """
        if self.docker_info_done:
            return []
        data = []
        data += self._docker_info_version()
        data += self._docker_info_drivers()
        data += self._docker_info_images()
        return data

    def _docker_info_version(self):
        """
        Return the docker version key conttributed to resinfo.
        """
        return [[
            "",
            "docker_version",
            self.docker_version
        ]]

    def _docker_info_drivers(self):
        """
        Return the docker drivers keys conttributed to resinfo.
        """
        data = []
        if "Driver" in self.docker_info:  # pylint: disable=unsupported-membership-test
            # pylint: disable=unsubscriptable-object
            data.append(["", "storage_driver", self.docker_info["Driver"]])
        if "ExecutionDriver" in self.docker_info:  # pylint: disable=unsupported-membership-test
            # pylint: disable=unsubscriptable-object
            data.append(["", "exec_driver", self.docker_info["ExecutionDriver"]])
        return data

    def _docker_info_images(self):
        """
        Return the per-container resource resinfo keys.
        """
        data = []
        images_done = []

        # referenced images
        for resource in self.svc.get_resources(self.container_type):
            image = self.get_image(resource.image)
            if image is None:
                continue
            images_done.append(image["id"])
            data.append([resource.rid, "image", resource.image])
            data.append([resource.rid, "image_id", image["id"]])
            data.append([resource.rid, "instance_id", resource.container_id])

        # unreferenced images
        for image in self.images:  # pylint: disable=not-an-iterable
            if image["id"] in images_done:
                continue
            data.append(["", "image_id", image["id"]])
        self.docker_info_done = True

        return data

    def image_userfriendly_name(self, resource):
        """
        Return the container resource docker image name if possible,
        else return the image id.
        """
        if ":" in resource.image:
            return resource.image
        if self.images is None:
            return resource.image
        image = self.get_image(resource.image)
        if image:
            return image["name"]
        return resource.image

    def docker_inspect(self, container_id):
        """
        Return the "docker inspect" data dict.
        """
        if container_id is None:
            raise IndexError("container id is None")
        for data in self.containers_inspect:  # pylint: disable=not-an-iterable
            if data and data.get("Id", data.get("ID")) == container_id:
                return data
        return {}

    @lazy
    def containers_inspect(self):
        ids = []
        for resource in self.svc.get_resources(self.container_type):
            if not resource.container_id:
                continue
            ids.append(resource.container_id)
        if not ids:
            return []
        try:
            self.docker_exe
        except ex.InitError:
            return []
        cmd = self.docker_cmd + ["inspect"] + ids
        out = justcall(cmd)[0]
        try:
            data = json.loads(out)
        except Exception:
            data = []
        return data

    def docker_volume_inspect(self, vol_id):
        """
        Return the "docker volume inspect" data dict.
        """
        try:
            self.docker_exe
        except ex.InitError:
            return {}
        if vol_id is None:
            raise IndexError("vol id is None")
        elif isinstance(vol_id, list):
            cmd = self.docker_cmd + ["volume", "inspect"] + vol_id
            out = justcall(cmd)[0]
            data = json.loads(out)
            return data
        else:
            cmd = self.docker_cmd + ["volume", "inspect", vol_id]
            out = justcall(cmd)[0]
            data = json.loads(out)
            return data[0]

    def _container_data_dir_resource(self):
        """
        Return the service fs resource handling the docker data dir, or
        None if any.
        """
        mntpts = []
        mntpt_res = {}
        for resource in self.svc.get_resources("fs"):
            if resource.type == "fs.docker":
                continue
            if not hasattr(resource, "mount_point"):
                continue
            mntpts.append(resource.mount_point)
            mntpt_res[resource.mount_point] = resource
        for mntpt in sorted(mntpts, reverse=True):
            if self.container_data_dir.startswith(mntpt):  # pylint: disable=no-member
                return mntpt_res[mntpt]



class DockerLib(ContainerLib):
    sock_tmo = 5.0
    container_type = ["container.docker", "task.docker"]

    def __init__(self, svc=None):
        ContainerLib.__init__(self, svc=svc)
        self.max_wait_for_dockerd = 5

        try:
            self.docker_daemon_private = \
                self.svc.conf_get("DEFAULT", "docker_daemon_private")
        except ex.OptNotFound:
            if self.raw_container_data_dir:
                self.docker_daemon_private = True
            else:
                self.docker_daemon_private = False
        if Env.sysname != "Linux":
            self.docker_daemon_private = False

        try:
            self.docker_exe_init = \
                self.svc.conf_get("DEFAULT", "docker_exe")
        except ex.OptNotFound as exc:
            self.docker_exe_init = exc.default

        try:
            self.dockerd_exe_init = \
                self.svc.conf_get("DEFAULT", "dockerd_exe")
        except ex.OptNotFound as exc:
            self.dockerd_exe_init = exc.default

        try:
            self.docker_daemon_args = \
                self.svc.conf_get("DEFAULT", "docker_daemon_args")
        except ex.OptNotFound as exc:
            self.docker_daemon_args = exc.default

        if self.raw_container_data_dir:
            if "--exec-opt" not in self.docker_daemon_args and self.docker_min_version("1.7"):
                self.docker_daemon_args += ["--exec-opt", "native.cgroupdriver=cgroupfs"]

        if self.docker_daemon_private:
            self.docker_socket = os.path.join(self.svc.var_d, "docker.sock")
            self.compat_docker_socket = os.path.join(Env.paths.pathvar, self.svc.name, "docker.sock")
        else:
            self.docker_socket = None

        if self.docker_daemon_private:
            self.docker_pid_file = os.path.join(self.svc.var_d, "docker.pid")
            self.compat_docker_pid_file = os.path.join(Env.paths.pathvar, self.svc.name, "docker.pid")
        else:
            self.docker_pid_file = None
            try:
                # pylint: disable=unsubscriptable-object
                set_lazy(self, "container_data_dir", self.docker_info["DockerRootDir"])
            except (KeyError, TypeError):
                set_lazy(self, "container_data_dir", None)

        try:
            self.docker_cmd = [self.docker_exe]
            if self.docker_socket:
                if self.test_sock(self.docker_socket) or not self.test_sock(self.compat_docker_socket):
                    sock = self.docker_socket
                else:
                    sock = self.compat_docker_socket
                self.docker_cmd += ["-H", "unix://"+sock]
        except:
            self.docker_cmd = None

    @lazy
    def docker_exe(self):
        """
        Return the docker executable to use, using the service configuration
        docker_exe as the first choice, and a docker.io or docker exe found
        in PATH as a fallback.
        """
        from core.capabilities import capabilities
        if capabilities.has("node.x.docker.io"):
            return "docker.io"
        if capabilities.has("node.x.docker"):
            return "docker"
        raise ex.InitError("docker executable not found")

    @lazy
    def dockerd_exe(self):
        from core.capabilities import capabilities
        if capabilities.has("node.x.dockerd"):
            return "dockerd"
        else:
            raise ex.InitError("dockerd executable not found")

    def test_sock(self, path):
        import socket
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.sock_tmo)
        try:
            sock.connect(path)
        except Exception as exc:
            return False
        finally:
            sock.close()
        return True

    @lazy
    def dockerd_cmd(self):
        """
        The docker daemon startup command, adapted to the docker version.
        """
        if self.docker_cmd is None:
            return []

        if self.docker_min_version("17.05"):
            cmd = [
                self.dockerd_exe,
                "-H", "unix://"+self.docker_socket,
                "--data-root", self.container_data_dir,
                "-p", self.docker_pid_file
            ]
        elif self.docker_min_version("1.13"):
            cmd = [
                self.dockerd_exe,
                "-H", "unix://"+self.docker_socket,
                "-g", self.container_data_dir,
                "-p", self.docker_pid_file
            ]
        elif self.docker_min_version("1.8"):
            cmd = [
                self.docker_exe, "daemon",
                "-H", "unix://"+self.docker_socket,
                "-g", self.container_data_dir,
                "-p", self.docker_pid_file
            ]
        else:
            cmd = self.docker_cmd + [
                "-r=false", "-d",
                "-g", self.container_data_dir,
                "-p", self.docker_pid_file
            ]
        if self.docker_min_version("1.9") and "--exec-root" not in str(self.docker_daemon_args):
            # keep <104 length to please dockerd
            cmd += ["--exec-root", os.path.join(Env.paths.pathvar, "dockerx", self.svc.id)]
        cmd += self.docker_daemon_args
        return cmd

    def docker_stop(self):
        """
        Stop the docker daemon if possible.
        """
        def can_stop():
            """
            Return True if the docker daemon can be stopped.
            """
            if not self.docker_daemon_private:
                return False
            if not self.docker_running():
                return False
            if self.container_data_dir is None:
                return False
            if not os.path.exists(self.docker_pid_file):
                return False
            if len(self.get_running_instance_ids(refresh=True)) > 0:
                return False
            return True

        if not can_stop():
            return

        try:
            with open(self.docker_pid_file, "r") as ofile:
                pid = int(ofile.read())
        except (OSError, IOError):
            self.svc.log.warning("can't read %s. skip docker daemon kill",
                                 self.docker_pid_file)
            return

        self.svc.log.info("no more container handled by docker daemon (pid %d)."
                          " shut it down", pid)
        import signal
        import time
        tries = 15
        os.kill(pid, signal.SIGTERM)
        unset_lazy(self, "docker_info")

        while self.docker_running() and tries > 0:
            unset_lazy(self, "docker_info")
            tries -= 1
            time.sleep(1)
        if self.docker_running() and tries == 0:
            self.svc.log.warning("dockerd did not stop properly. send a kill "
                                 "signal")
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError as exc:
                if exc.errno == errno.ESRCH:
                    # already dead
                    pass
                else:
                    raise ex.Error("failed to kill docker daemon: %s" % str(exc))

    def docker_start(self, verbose=True):
        """
        Start the docker daemon if in private mode and not already running.
        """
        if not self.docker_daemon_private:
            return
        if self.docker_cmd is None:
            raise ex.Error("docker executable not found")
        lockfile = os.path.join(self.svc.var_d, "lock.docker_start")
        try:
            lockfd = utilities.lock.lock(timeout=15, delay=1, lockfile=lockfile)
        except utilities.lock.LOCK_EXCEPTIONS as exc:
            self.svc.log.error("dockerd start lock acquire failed: %s",
                               str(exc))
            return

        # Sanity checks before deciding to start the daemon
        if self.docker_running():
            utilities.lock.unlock(lockfd)
            return

        if self.container_data_dir is None:
            utilities.lock.unlock(lockfd)
            return

        resource = self._container_data_dir_resource()
        if resource is not None:
            state = resource._status()
            if state not in (core.status.UP, core.status.STDBY_UP):
                self.svc.log.warning("the docker daemon data dir is handled by the %s "
                                     "resource in %s state. can't start the docker "
                                     "daemon", resource.rid, core.status.Status(state))
                utilities.lock.unlock(lockfd)
                return

        if os.path.exists(self.docker_pid_file):
            self.svc.log.warning("removing leftover pid file %s", self.docker_pid_file)
            os.unlink(self.docker_pid_file)

        # Now we can start the daemon, creating its data dir if necessary
        cmd = self.dockerd_cmd

        if verbose:
            self.svc.log.info("starting docker daemon")
            self.svc.log.info(" ".join(cmd))
        import subprocess
        subprocess.Popen(
            ["nohup"] + cmd,
            stdout=open("/dev/null", "w"),
            stderr=open("/dev/null", "a"),
            preexec_fn=os.setpgrp,
            close_fds=True,
        )

        try:
            for _ in range(self.max_wait_for_dockerd):
                if self._docker_working():
                    self.clear_daemon_caches()
                    return
                time.sleep(1)
        finally:
            utilities.lock.unlock(lockfd)

    def clear_daemon_caches(self):
        unset_lazy(self, "container_ps")
        unset_lazy(self, "container_by_name")
        unset_lazy(self, "container_by_label")
        unset_lazy(self, "docker_info")
        unset_lazy(self, "running_instance_ids")
        unset_lazy(self, "images")
        unset_lazy(self, "containers_inspect")

    def docker_running(self):
        """
        Return True if the docker daemon is running.
        """
        if self.docker_daemon_private:
            return self._docker_running_private()
        else:
            return self._docker_running_shared()

    def _docker_running_shared(self):
        """
        Return True if the docker daemon is running.
        """
        if self.docker_info == {}:
            return False
        return True

    def _docker_running_private(self):
        """
        Return True if the docker daemon is running.
        """
        if os.path.exists(self.docker_pid_file):
            pid_file = self.docker_pid_file
        elif os.path.exists(self.compat_docker_pid_file):
            pid_file = self.compat_docker_pid_file
        else:
            self.svc.log.debug("docker_running: no pid file %s", self.docker_pid_file)
            return False
        try:
            with open(pid_file, "r") as ofile:
                buff = ofile.read()
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                return False
            return ex.Error("docker_running: "+str(exc))
        self.svc.log.debug("docker_running: pid found in pid file %s", buff)
        exe = os.path.join(os.sep, "proc", buff, "exe")
        try:
            exe = os.path.realpath(exe)
        except OSError:
            self.svc.log.debug("docker_running: no proc info in /proc/%s", buff)
            try:
                os.unlink(self.docker_pid_file)
            except OSError:
                pass
            return False
        if "docker" not in exe:
            self.svc.log.debug("docker_running: pid found but owned by a "
                               "process that is not a docker (%s)", exe)
            try:
                os.unlink(self.docker_pid_file)
            except OSError:
                pass
            return False
        return True

    def _docker_working(self):
        """
        Return True if the docker daemon responds to a simple 'info' request.
        """
        cmd = self.docker_cmd + ["info"]
        ret = justcall(cmd)[2]
        if ret != 0:
            return False
        return True


class PodmanLib(ContainerLib):
    json_opt = ["--format=json"]
    container_type = ["container.podman", "task.podman"]
    config_args_position_head = False

    def __init__(self, svc=None):
        ContainerLib.__init__(self, svc=svc)

        self.docker_daemon_args = []
        self.docker_daemon_args += [
            "--cgroup-manager", "cgroupfs",
            "--namespace", self.svc.path,
            "--cni-config-dir", self.svc.node.cni_config,
        ]

        if self.container_data_dir:
            self.docker_daemon_private = True
            self.docker_daemon_args += ["--root", self.container_data_dir]
            self.docker_daemon_args += ["--runroot", self.container_data_dir+"/run"]
        else:
            self.docker_daemon_private = False
        if Env.sysname != "Linux":
            self.docker_daemon_private = False

        self.docker_cmd = [self.docker_exe] + self.docker_daemon_args

    @lazy
    def docker_exe(self):
        return "/usr/bin/podman"

    @lazy
    def container_ps(self):
        """
        The "docker ps -a --no-trunc" json loaded dicts assembled in a list.
        """
        cmd = self.docker_cmd + ["ps", "-a", "--no-trunc"] + self.json_opt
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error(err)
        return json.loads(out)

    def docker_stop(self):
        pass

    def docker_start(self):
        pass

    def login_as_service_args(self):
        uuid = self.svc.node.oget("node", "uuid")
        if not uuid:
            return []
        args = ["-u", self.svc.path+"@"+Env.nodename]
        args += ["-p", uuid]
        return args

    def client_config_args(self, path):
        return ["--authfile", path]


