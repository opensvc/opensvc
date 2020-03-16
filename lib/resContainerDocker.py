"""
Docker container resource driver module.
"""
import os
import re
import shlex
import signal
from itertools import chain

import resources
import resContainer
import rcContainer
import rcExceptions as ex
import rcStatus
from rcUtilitiesLinux import check_ping
from rcUtilities import justcall, unset_lazy, lazy, drop_option, has_option, get_option, get_options
from converters import print_duration
from svcBuilder import init_kwargs, container_kwargs

ATTR_MAP = {
    "hostname": {
        "attr": "vm_hostname",
        "path": ["Config", "Hostname"],
    },
    "privileged": {
        "path": ["HostConfig", "Privileged"],
    },
    "interactive": {
        "path": ["Config", "OpenStdin"],
    },
    "entrypoint": {
        "path": ["Config", "Entrypoint"],
        "cmp": "cmp_entrypoint",
    },
    "tty": {
        "path": ["Config", "Tty"],
    },
    #    "rm": {
    #        "path": ["HostConfig", "AutoRemove"],
    #    },
    "netns": {
        "path": ["HostConfig", "NetworkMode"],
        "cmp": "cmp_ns",
    },
    "pidns": {
        "path": ["HostConfig", "PidMode"],
        "cmp": "cmp_ns",
    },
    "ipcns": {
        "path": ["HostConfig", "IpcMode"],
        "cmp": "cmp_ns",
    },
    "utsns": {
        "path": ["HostConfig", "UTSMode"],
        "cmp": "cmp_ns",
    },
    "userns": {
        "path": ["HostConfig", "UsernsMode"],
        "cmp": "cmp_ns",
    },
}


def adder(svc, s, drv=None):
    drv = drv or Container
    kwargs = init_kwargs(svc, s)
    kwargs.update(container_kwargs(svc, s, default_name=None))
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
    kwargs["detach"] = svc.oget(s, "detach")
    kwargs["entrypoint"] = svc.oget(s, "entrypoint")
    kwargs["netns"] = svc.oget(s, "netns")
    kwargs["userns"] = svc.oget(s, "userns")
    kwargs["pidns"] = svc.oget(s, "pidns")
    kwargs["ipcns"] = svc.oget(s, "ipcns")
    kwargs["utsns"] = svc.oget(s, "utsns")
    kwargs["privileged"] = svc.oget(s, "privileged")
    kwargs["interactive"] = svc.oget(s, "interactive")
    kwargs["tty"] = svc.oget(s, "tty")
    kwargs["volume_mounts"] = svc.oget(s, "volume_mounts")
    kwargs["environment"] = svc.oget(s, "environment")
    kwargs["secrets_environment"] = svc.oget(s, "secrets_environment")
    kwargs["configs_environment"] = svc.oget(s, "configs_environment")
    kwargs["devices"] = svc.oget(s, "devices")
    r = drv(**kwargs)
    svc += r


def alarm_handler(signum, frame):
    raise KeyboardInterrupt


class Container(resContainer.Container):
    """
    Docker container resource driver.
    """
    default_start_timeout = 2
    default_net = "none"
    dns_option_option = "--dns-option"

    def __init__(self,
                 rid,
                 name="",
                 type="container.docker",
                 image=None,
                 image_pull_policy="once",
                 run_command=None,
                 run_args=None,
                 detach=True,
                 entrypoint=None,
                 rm=None,
                 netns=None,
                 userns=None,
                 pidns=None,
                 utsns=None,
                 ipcns=None,
                 privileged=None,
                 interactive=None,
                 tty=None,
                 volume_mounts=None,
                 devices=None,
                 environment=None,
                 secrets_environment=None,
                 configs_environment=None,
                 guestos="Linux",
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name="",
                                        type=type,
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.user_defined_name = name
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.run_command = run_command
        self.run_args = run_args
        self.detach = detach
        self.entrypoint = entrypoint
        self.rm = rm
        self.netns = netns
        self.userns = userns
        self.pidns = pidns
        self.utsns = utsns
        self.ipcns = ipcns
        self.privileged = privileged
        self.interactive = interactive
        self.tty = tty
        self.volume_mounts = volume_mounts if volume_mounts else []
        self.devices = devices if devices else []
        self.volumes = {}
        self.environment = environment
        self.secrets_environment = secrets_environment
        self.configs_environment = configs_environment
        if not self.detach:
            self.rm = True

    @lazy
    def lib(self):
        """
        Lazy allocator for the dockerlib object.
        """
        try:
            return self.svc.dockerlib
        except AttributeError:
            self.svc.dockerlib = rcContainer.DockerLib(self.svc)
            return self.svc.dockerlib

    def on_add(self):
        try:
            self.volume_options()
        except ex.excError:
            # volume not created yet
            pass
        try:
            self.device_options()
        except ex.excError:
            # volume not created yet
            pass

    def abort_start_ping(self, *args, **kwargs):
        return False

    def getaddr(self, *args, **kwargs):
        return

    @lazy
    def container_name(self):
        """
        Format a docker container name
        """
        if self.user_defined_name:
            return self.user_defined_name
        if self.svc.namespace:
            container_name = self.svc.namespace + ".."
        else:
            container_name = ""
        container_name += self.svc.name + '.' + self.rid
        return container_name.replace('#', '.')

    @lazy
    def container_label_id(self):
        """
        Format a docker container name
        """
        return "com.opensvc.id=%s.%s" % (self.svc.id, self.rid)

    @lazy
    def name(self):  # pylint: disable=method-hidden
        return self.container_name

    @lazy
    def container_id(self):
        return self.lib.get_container_id(self)

    @lazy
    def label(self):  # pylint: disable=method-hidden
        return "docker " + self.lib.image_userfriendly_name(self)

    def __str__(self):
        return "%s name=%s" % (resources.Resource.__str__(self), self.name)

    def rcmd(self, cmd):
        cmd = self.lib.docker_cmd + ['exec', '-t', self.container_name] + cmd
        return justcall(cmd)

    def enter(self):
        for cmd in [["/bin/bash"], ["/bin/sh"]]:
            try:
                self.execute(interactive=True, tty=True, cmd=cmd)
                return
            except ValueError:
                continue
            else:
                return

    def execute(self, interactive=False, tty=False, cmd=None):
        import subprocess
        xcmd = self.lib.docker_cmd + ["exec"]
        if interactive:
            xcmd.append("-i")
        if tty:
            xcmd.append("-t")
        xcmd.append(self.container_name)
        xcmd += cmd
        proc = subprocess.Popen(xcmd)
        proc.communicate()
        if proc.returncode in [126, 127]:
            # executable not found
            raise ValueError

    def rcp_from(self, src, dst):
        """
        Copy <src> from the container's rootfs to <dst> in the host's fs.
        """
        cmd = self.lib.docker_cmd + ["cp", self.container_name + ":" + src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s" % (" ".join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        """
        Copy <src> from the host's fs to the container's rootfs.
        """
        cmd = self.lib.docker_cmd + ["cp", src, self.container_name + ":" + dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s" % (" ".join(cmd), err))
        return out, err, ret

    def files_to_sync(self):
        """
        Files to contribute to sync#i0.
        """
        return []

    def operational(self):
        """
        Always return True for docker containers.
        """
        return True

    @lazy
    def vm_hostname(self):
        """
        The container hostname
        """
        try:
            hostname = self.conf_get("hostname")
        except ex.OptNotFound:
            hostname = ""
        return hostname

    def wait_for_startup(self):
        if not self.detach:
            return
        resContainer.Container.wait_for_startup(self)

    def wait_for_removed(self):
        def removed():
            self.is_up_clear_cache()
            if self.container_id:
                return False
            return True

        self.wait_for_fn(removed, 10, 1, errmsg="waited too long for container removal")

    def container_rm(self):
        """
        Remove the resource docker instance.
        """
        if not self.lib.docker_running():
            return
        cmd = self.lib.docker_cmd + ['rm', self.container_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if "No such container" in err:
                pass
            elif "no such file" in err:
                pass
            elif "removal" in err and "already in progress" in err:
                self.wait_for_removed()
            else:
                self.log.info(" ".join(cmd))
                raise ex.excError(err)
        else:
            self.log.info(" ".join(cmd))
        self.is_up_clear_cache()

    def docker(self, action):
        """
        Wrap docker commands to honor <action>.
        """
        if self.lib.docker_cmd is None:
            raise ex.excError("docker executable not found")
        sec_env = {}
        cfg_env = {}
        cmd = self.lib.docker_cmd + []
        if action == "start":
            if not self.detach:
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(self.start_timeout)
            if self.rm:
                self.container_rm()
            if self.container_id is None:
                self.is_up_clear_cache()
            if self.container_id is None:
                try:
                    image_id = self.lib.get_image_id(self.image)
                except ValueError as exc:
                    raise ex.excError(str(exc))
                if image_id is None:
                    self.lib.docker_login(self.image)
                sec_env = self.kind_environment_env("sec", self.secrets_environment)
                cfg_env = self.kind_environment_env("cfg", self.configs_environment)
                cmd += ["run"]
                cmd += self._add_run_args()
                for var in sec_env:
                    cmd += ["-e", var]
                for var in cfg_env:
                    cmd += ["-e", var]
                cmd += [self.image]
                if self.run_command:
                    cmd += self.run_command
            else:
                cmd += ["start", self.container_id]
        elif action == "stop":
            cmd += ["stop", self.container_id]
        elif action == "kill":
            cmd += ["kill", self.container_id]
        else:
            self.log.error("unsupported docker action: %s", action)
            return 1

        env = {}
        env.update(os.environ)
        env.update(sec_env)
        env.update(cfg_env)
        try:
            ret = self.vcall(cmd, warn_to_info=True, env=env)[0]
        except KeyboardInterrupt:
            self.log.error("%s timeout exceeded", print_duration(self.start_timeout))
            if action == "start":
                cmd = self.lib.docker_cmd + ["kill", self.container_name]
                self.vcall(cmd, warn_to_info=True, env=env)
            ret = 1
        if not self.detach:
            signal.alarm(0)
        if ret != 0:
            raise ex.excError

        if action == "start":
            self.is_up_clear_cache()
        elif action in ("stop", "kill"):
            if self.rm:
                self.container_rm()
            self.is_up_clear_cache()
            self.lib.docker_stop()

    def device_options(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args
        devices = []
        for arg in chain(get_options("--device", args), iter(self.devices)):
            elements = arg.split(":")
            if not elements or len(elements) not in (2, 3):
                continue
            if not elements[0].startswith(os.sep):
                # vol service
                elements[0], vol = self.replace_volname(elements[0], mode="blk", strict=False, errors=errors)
                devices.append(":".join(elements))
            elif not os.path.exists(elements[0]):
                # host path
                raise ex.excError("source dir of mapping %s does not "
                                  "exist" % arg)
            else:
                devices.append(arg)
        return devices

    def volume_options(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args
        volumes = []
        dsts = []
        for volarg in chain(get_options("-v", args), get_options("--volume", args), iter(self.volume_mounts)):
            elements = volarg.split(":")
            if not elements or len(elements) not in (2, 3):
                continue
            if elements[1] in dsts:
                raise ex.excError("different volume mounts use the same destination "
                                  "mount point: %s" % elements[1])
            if not elements[0].startswith(os.sep):
                # vol service
                wants_ro = False
                elements[0], vol = self.replace_volname(elements[0], strict=False, errors=errors)
                try:
                    options = elements[2].split(",")
                    if 'ro' in options:
                        wants_ro = True
                    options = drop_option("ro", options)
                    options = drop_option("rw", options)
                    del elements[2]
                except Exception:
                    options = []
                if wants_ro or (vol and vol.volsvc.access.startswith("ro")):
                    options.insert(0, "ro")
                else:
                    options.insert(0, "rw")
                elements.append(",".join(options))
                volumes.append(":".join(elements))
            elif not os.path.exists(elements[0]):
                # host path
                raise ex.excError("source dir of mapping %s does not "
                                  "exist" % volarg)
            else:
                volumes.append(volarg)
            dsts.append(elements[1])
        return volumes

    def environment_options(self):
        if self.environment is None:
            return []
        options = []
        for mapping in self.environment:
            try:
                var, val = mapping.split("=", 1)
            except Exception as exc:
                self.log.info("ignored environment mapping %s: %s", mapping, exc)
                continue
            var = var.upper()
            options += ["-e", "%s=%s" % (var, val)]
        return options

    def cgroup_options(self):
        if not self.lib.docker_min_version("1.7"):
            return []
        if self.lib.docker_info.get("CgroupDriver") != "cgroupfs":
            return []
        return ["--cgroup-parent", self.cgroup_dir]

    def _add_run_args(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args

        args = drop_option("-d", args, drop_value=False)
        args = drop_option("--detach", args, drop_value=False)
        if self.detach:
            args += ["--detach"]

        # drop user specified --name. we set ours already
        args = drop_option("--name", args, drop_value=True)
        args = drop_option("-n", args, drop_value=True)
        args += ['--name=' + self.container_name]
        args += ['--label=' + self.container_label_id]

        args = drop_option("--hostname", args, drop_value=True)
        args = drop_option("-h", args, drop_value=True)
        if not self.netns or (self.netns != "host" and not self.netns.startswith("container#")):
            # only allow hostname setting if the container has a private netns
            if self.vm_hostname:
                args += ["--hostname", self.vm_hostname]
            elif not self.run_args:
                pass
            else:
                hostname = get_option("--hostname", self.run_args, boolean=False)
                if not hostname:
                    hostname = get_option("-h", self.run_args, boolean=False)
                if hostname:
                    args += ["--hostname", hostname]

        if self.entrypoint:
            args = drop_option("--entrypoint", args, drop_value=True)
            args += ["--entrypoint", self.entrypoint]

        if self.netns:
            args = drop_option("--net", args, drop_value=True)
            args = drop_option("--network", args, drop_value=True)
            if self.netns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--net=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.excError("resource %s, referenced in %s.netns, does not exist" % (self.netns, self.rid))
            else:
                args += ["--net=" + self.netns]
        elif not has_option("--net", args):
            args += ["--net=" + self.default_net]

        if self.pidns:
            args = drop_option("--pid", args, drop_value=True)
            if self.pidns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--pid=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.excError("resource %s, referenced in %s.pidns, does not exist" % (self.pidns, self.rid))
            else:
                args += ["--pid=" + self.pidns]

        if self.ipcns:
            args = drop_option("--ipc", args, drop_value=True)
            if self.ipcns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--ipc=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.excError("resource %s, referenced in %s.ipcns, does not exist" % (self.ipcns, self.rid))
            else:
                args += ["--ipc=" + self.ipcns]

        if self.utsns == "host":
            args = drop_option("--uts", args, drop_value=True)
            args += ["--uts=host"]

        if self.userns is not None:
            args = drop_option("--userns", args, drop_value=True)
        if self.userns == "host":
            args += ["--userns=host"]

        if self.privileged is not None:
            args = drop_option("--privileged", args, drop_value=False)
        if self.privileged:
            args += ["--privileged"]

        if self.interactive is not None:
            args = drop_option("--interactive", args, drop_value=False)
            args = drop_option("-i", args, drop_value=False)
        if self.interactive:
            args += ["--interactive"]

        if self.tty is not None:
            args = drop_option("--tty", args, drop_value=False)
            args = drop_option("-t", args, drop_value=False)
        if self.tty:
            args += ["--tty"]

        drop_option("--rm", args, drop_value=False)

        drop_option("-v", args, drop_value=True)
        drop_option("--volume", args, drop_value=True)
        for vol in self.volume_options(errors=errors):
            args.append("--volume=%s" % vol)

        drop_option("--device", args, drop_value=True)
        for dev in self.device_options(errors=errors):
            args.append("--device=%s" % dev)

        args += self.cgroup_options()

        def dns_opts(args):
            if not self.svc.node.dns or "--dns" in args:
                return []
            net = get_option("--net", args, boolean=False)
            if net and net.startswith("container:"):
                return []
            if net == "host":
                return []
            l = []
            for dns in self.svc.node.dns:
                l += ["--dns", dns]
            for search in self.dns_search():
                l += ["--dns-search", search]
            dns_options = [o for o in get_options(self.dns_option_option, args)]
            args = drop_option(self.dns_option_option, args, drop_value=True)
            for o in self.dns_options(dns_options):
                l += [self.dns_option_option, o]
            return l

        args += dns_opts(args)
        args += self.environment_options()
        return args

    @lazy
    def cgroup_dir(self):
        return os.sep + self.svc.pg.get_cgroup_relpath(self)

    def image_pull(self):
        self.lib.image_pull(self.image)

    def container_start(self):
        self.docker('start')

    def _start(self):
        self.lib.docker_start()
        resContainer.Container.start(self)

    def provision(self):
        resContainer.Container.provision(self)
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        resContainer.Container.unprovision(self)
        self.container_rm()

    def start(self):
        if self.image_pull_policy == "always":
            self.image_pull()
        try:
            self._start()
        except KeyboardInterrupt:
            if not self.detach:
                self.is_up_clear_cache()
                self.container_forcestop()
                self.container_rm()
                raise ex.excError("timeout")
            else:
                raise ex.excAbortAction
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def container_stop(self):
        self.docker('stop')

    def stop(self):
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        self._stop()

    def _stop(self):
        if not self.lib.docker_running():
            return
        resContainer.Container.stop(self)
        if self.rm:
            self.container_rm()
        self.is_up_clear_cache()
        self.lib.docker_stop()

    def _info(self):
        """
        Return keys to contribute to resinfo.
        """
        data = self.lib._info()
        data.append([self.rid, "run_args", " ".join(self._add_run_args(errors="ignore"))])
        data.append([self.rid, "rm", str(self.rm)])
        data.append([self.rid, "container_name", str(self.container_name)])
        if self.netns:
            data.append([self.rid, "netns", str(self.netns)])
        if self.run_command:
            data.append([self.rid, "run_command", " ".join(self.run_command)])
        return data

    def _status_container_image(self, inspect_data):
        try:
            image_id = self.lib.get_image_id(self.image)
        except ValueError as exc:
            self.status_log(str(exc))
            return
        try:
            running_image_id = re.sub("^sha256:", "", inspect_data["Image"])
        except KeyError:
            return
        if image_id is None:
            self.status_log("image '%s' is not pulled yet." % self.image)
        elif image_id != running_image_id:
            self.status_log("the current container is based on image '%s' "
                            "instead of '%s'" % (running_image_id, image_id))

    def cmp_entrypoint(self, current, target, data):
        try:
            alt_target = shlex.split(target)
            if current == target or current == alt_target:
                return
            self.status_log("%s=%s, but %s=%s" % \
                            (".".join(data["path"]), current, data["attr"], target))
        except Exception:
            pass

    def cmp_ns(self, current, target, data):
        try:
            res = self.svc.get_resource(self.netns)
            target = "container:" + res.container_name
            if current == target:
                return
            target = "container:" + res.container_id
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, data["attr"], target))
        except Exception:
            pass

    def _status_inspect(self):
        try:
            inspect_data = self.lib.docker_inspect(self.container_id)
        except Exception:
            return
        self._status_container_image(inspect_data)

        def get(path, data=None):
            try:
                return get(path[1:], data[path[0]])
            except IndexError:
                return data[path[0]]

        def validate(attr, data):
            try:
                current = get(data["path"], inspect_data)
            except KeyError:
                return
            _attr = data.get("attr", attr)
            _fn = data.get("cmp")
            target = getattr(self, _attr)
            if not target:
                return
            if _fn:
                _fn = getattr(self, _fn)
                return _fn(current, target, data)
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, attr, target))

        if get(["State", "Status"], inspect_data) != "running":
            return
        for attr, data in ATTR_MAP.items():
            validate(attr, data)

    def _status(self, verbose=False):
        if not self.detach:
            return rcStatus.NA
        try:
            self.lib.docker_exe
        except ex.excInitError as exc:
            self.status_log(str(exc), "warn")
            return rcStatus.DOWN
        if not self.lib.docker_running():
            self.status_log("docker daemon is not running", "info")
            return rcStatus.DOWN
        sta = resContainer.Container._status(self, verbose)
        self._status_inspect()
        return sta

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return check_ping(self.addr, timeout=1)

    def is_down(self):
        return not self.is_up()

    def is_up_clear_cache(self):
        self.unset_lazy("container_id")
        unset_lazy(self.lib, "container_ps")
        unset_lazy(self.lib, "running_instance_ids")
        unset_lazy(self.lib, "container_by_label")
        unset_lazy(self.lib, "container_by_name")
        unset_lazy(self.lib, "containers_inspect")

    def is_up(self):
        if self.lib.docker_daemon_private and \
                self.lib.container_data_dir is None:
            self.status_log("DEFAULT.container_data_dir must be defined")

        if not self.lib.docker_running():
            return False

        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False
        if self.container_id in self.lib.get_running_instance_ids():
            return True
        return False

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        return True

    def container_pid(self):
        try:
            data = self.lib.docker_inspect(self.container_id)
            return data["State"]["Pid"]
        except (IndexError, KeyError):
            return

    def container_sandboxkey(self):
        try:
            data = self.lib.docker_inspect(self.container_id)
            return data["NetworkSettings"]["SandboxKey"]
        except (AttributeError, IndexError, KeyError):
            return

    def cni_containerid(self):
        """
        Used by ip.cni
        """
        return self.container_pid()

    def cni_netns(self):
        """
        Used by ip.cni
        """
        return self.container_sandboxkey()
