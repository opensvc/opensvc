"""
Docker container resource driver module.
"""
import os
import shlex
import signal

import resources
import resContainer
import rcExceptions as ex
import rcStatus
from rcUtilitiesLinux import check_ping
from rcUtilities import justcall, lazy, drop_option, has_option, get_option
from rcGlobalEnv import rcEnv

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
        "mangle_attr": lambda x: shlex.split(x),
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

def alarm_handler(signum, frame):
    raise KeyboardInterrupt

class Docker(resContainer.Container):
    """
    Docker container resource driver.
    """
    default_start_timeout = 2

    def __init__(self,
                 rid,
                 name="",
                 image=None,
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
                 guestos="Linux",
                 osvc_root_path=None,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name="",
                                        type="container.docker",
                                        guestos=guestos,
                                        osvc_root_path=osvc_root_path,
                                        **kwargs)
        self.user_defined_name = name
        self.image = image
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
        if not self.detach:
            self.rm = True
            self.tags.add("nostatus")

    @lazy
    def container_name(self):
        """
        Format a docker container name
        """
        if self.user_defined_name:
            return self.user_defined_name
        if self.svc.namespace:
            container_name = self.svc.namespace+".."
        else:
            container_name = ""
        container_name += self.svc.svcname+'.'+self.rid
        return container_name.replace('#', '.')

    @lazy
    def container_label_id(self):
        """
        Format a docker container name
        """
        return "com.opensvc.id=%s.%s" % (self.svc.id, self.rid)

    @lazy
    def name(self): # pylint: disable=method-hidden
        return self.container_name

    @lazy
    def container_id(self):
        return self.svc.dockerlib.get_container_id(self, refresh=True)

    @lazy
    def label(self): # pylint: disable=method-hidden
        return "docker container " + "@".join((
            self.container_name,
            self.svc.dockerlib.image_userfriendly_name(self)
        ))

    def __str__(self):
        return "%s name=%s" % (resources.Resource.__str__(self), self.name)

    def rcmd(self, cmd):
        cmd = self.svc.dockerlib.docker_cmd + ['exec', '-t', self.container_name] + cmd
        return justcall(cmd)

    def rcp_from(self, src, dst):
        """
        Copy <src> from the container's rootfs to <dst> in the host's fs.
        """
        cmd = self.svc.dockerlib.docker_cmd + ['cp', self.container_name+":"+src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        """
        Copy <src> from the host's fs to the container's rootfs.
        """
        cmd = self.svc.dockerlib.docker_cmd + ['cp', src, self.container_name+":"+dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
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

    def get_rootfs(self):
        """
        Return the rootgs layer path.
        """
        import glob
        inspect = self.svc.dockerlib.docker_inspect(self.container_id)
        instance_id = str(inspect['Id'])
        pattern = str(self.svc.dockerlib.docker_data_dir)+"/*/mnt/"+instance_id
        fpaths = glob.glob(pattern)
        if len(fpaths) == 0:
            raise ex.excError("no candidates rootfs paths matching %s" % pattern)
        elif len(fpaths) != 1:
            raise ex.excError("too many candidates rootfs paths: %s" % ', '.join(fpaths))
        return fpaths[0]

    def wait_for_startup(self):
        if not self.detach:
            return
        resContainer.Container.wait_for_startup(self)

    def wait_for_removed(self):
        def removed():
            self.unset_lazy("container_id")
            if self.container_id:
                return False
            return True
        self.wait_for_fn(removed, 10, 1, errmsg="waited too long for container removal")

    def container_rm(self):
        """
        Remove the resource docker instance.
        """
        if not self.svc.dockerlib.docker_running():
            return
        cmd = self.svc.dockerlib.docker_cmd + ['rm', self.container_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if "No such container" in err:
                pass
            elif "removal" in err and "already in progress" in err:
                self.wait_for_removed()
            else:
                self.log.info(" ".join(cmd))
                raise ex.excError(err)
        else:
            self.log.info(" ".join(cmd))
        self.unset_lazy("container_id")

    def docker(self, action):
        """
        Wrap docker commands to honor <action>.
        """
        if self.svc.dockerlib.docker_cmd is None:
            raise ex.excError("docker executable not found")
        cmd = self.svc.dockerlib.docker_cmd + []
        if action == 'start':
            if not self.detach:
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(self.start_timeout)
            if self.rm:
                self.container_rm()
            if self.container_id is None:
                self.unset_lazy("container_id")
            if self.container_id is None:
                try:
                    image_id = self.svc.dockerlib.get_image_id(self)
                except ValueError as exc:
                    raise ex.excError(str(exc))
                if image_id is None:
                    self.svc.dockerlib.docker_login(self.image)
                cmd += ['run']
                cmd += self._add_run_args()
                cmd += [self.image]
                if self.run_command:
                    cmd += self.run_command
            else:
                cmd += ['start', self.container_id]
        elif action == 'stop':
            cmd += ['stop', self.container_id]
        elif action == 'kill':
            cmd += ['kill', self.container_id]
        else:
            self.log.error("unsupported docker action: %s", action)
            return 1

        ret = self.vcall(cmd, warn_to_info=True)[0]
        if not self.detach:
            signal.alarm(0)
        if ret != 0:
            raise ex.excError

        if action == 'start':
            self.unset_lazy("container_id")
            self.svc.dockerlib.get_running_instance_ids(refresh=True)
        elif action in ("stop", "kill"):
            if self.rm:
                self.container_rm()
            self.unset_lazy("container_id")
            self.svc.dockerlib.docker_stop()

    def _add_run_args(self):
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
        args += ['--name='+self.container_name]
        args += ['--label='+self.container_label_id]

        args = drop_option("--hostname", args, drop_value=True)
        args = drop_option("-h", args, drop_value=True)
        if not self.netns:
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
                if res is None:
                    raise ex.excError("resource %s, referenced in %s.netns, does not exist" % (self.netns, self.rid))
                args += ["--net=container:"+res.container_name]
            else:
                args += ["--net="+self.netns]
        elif not has_option("--net", args):
            args += ["--net=none"]

        if self.pidns:
            args = drop_option("--pid", args, drop_value=True)
            if self.pidns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is None:
                    raise ex.excError("resource %s, referenced in %s.pidns, does not exist" % (self.pidns, self.rid))
                args += ["--pid=container:"+res.container_name]
            else:
                args += ["--pid="+self.pidns]

        if self.ipcns:
            args = drop_option("--ipc", args, drop_value=True)
            if self.ipcns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is None:
                    raise ex.excError("resource %s, referenced in %s.ipcns, does not exist" % (self.ipcns, self.rid))
                args += ["--ipc=container:"+res.container_name]
            else:
                args += ["--ipc="+self.ipcns]

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

        for arg, pos in enumerate(args):
            if arg != '-p':
                continue
            if len(args) < pos + 2:
                # bad
                break
            volarg = args[pos+1]
            if ':' in volarg:
                # mapping ... check source dir presence
                elements = volarg.split(':')
                if len(elements) != 3:
                    raise ex.excError("mapping %s should be formatted as "
                                      "<src>:<dst>:<ro|rw>" % (volarg))
                if not os.path.exists(elements[0]):
                    raise ex.excError("source dir of mapping %s does not "
                                      "exist" % (volarg))
        if self.svc.dockerlib.docker_min_version("1.7"):
            if self.svc.dockerlib.docker_info.get("CgroupDriver") == "cgroupfs":
                args += ["--cgroup-parent", self.cgroup_dir]
        if not self.svc.dockerlib.docker_min_version("1.13") and "--rm" in args:
            del args[args.index("--rm")]

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
            return l

        args += dns_opts(args)
        return args

    @lazy
    def cgroup_dir(self):
        return os.sep+self.svc.pg.get_cgroup_relpath(self)

    def container_start(self):
        self.docker('start')

    def _start(self):
        self.svc.dockerlib.docker_start()
        resContainer.Container.start(self)

    def provision(self):
        resContainer.Container.provision(self)
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        resContainer.Container.unprovision(self)
        self.container_rm()

    def start(self):
        try:
            self._start()
        except KeyboardInterrupt:
            if not self.detach:
                self.unset_lazy("container_id")
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
        if not self.svc.dockerlib.docker_running():
            return
        resContainer.Container.stop(self)
        self.svc.dockerlib.get_running_instance_ids(refresh=True)
        self.svc.dockerlib.docker_stop()

    def _info(self):
        """
        Return keys to contribute to resinfo.
        """
        data = self.svc.dockerlib._info()
        data.append([self.rid, "run_args", " ".join(self._add_run_args())])
        data.append([self.rid, "run_command", " ".join(self.run_command)])
        data.append([self.rid, "rm", str(self.rm)])
        data.append([self.rid, "netns", str(self.netns)])
        data.append([self.rid, "container_name", str(self.container_name)])
        return data

    def wanted_nodes_count(self):
        if rcEnv.nodename in self.svc.nodes:
            return len(self.svc.nodes)
        else:
            return len(self.svc.drpnodes)

    def run_args_replicas(self):
        elements = self._add_run_args()
        if "--mode" in elements:
            idx = elements.index("--mode")
            if "=" in elements[idx]:
                mode = elements[idx].split("=")[-1]
            else:
                mode = elements[idx+1]
            if mode == "global":
                return self.wanted_nodes_count()
        elif "--replicas" in elements:
            idx = elements.index("--replicas")
            if "=" in elements[idx]:
                return int(elements[idx].split("=")[-1])
            else:
                return int(elements[idx+1])
        else:
            return 1

    def _status_container_image(self):
        try:
            image_id = self.svc.dockerlib.get_image_id(self, pull=False)
        except ValueError as exc:
            self.status_log(str(exc))
            return
        try:
            inspect = self.svc.dockerlib.docker_inspect(self.container_id)
        except Exception:
            return
        running_image_id = inspect['Image']
        if image_id is None:
            self.status_log("image '%s' is not pulled yet."%(self.image))
        elif image_id != running_image_id:
            self.status_log("the current container is based on image '%s' "
                            "instead of '%s'"%(running_image_id, image_id))

    def cmp_ns(self, current, data):
        try:
            res = self.svc.get_resource(self.netns)
            target = "container:"+res.container_name
            if current == target:
                return
            target = "container:"+res.container_id
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, data["attr"], target))
        except Exception as exc:
            pass

    def _status_inspect(self):
        try:
            inspect_data = self.svc.dockerlib.docker_inspect(self.container_id)
        except Exception:
            return

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
                return _fn(current, data)
            if "mangle_attr" in data:
                target = data["mangle_attr"](target)
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, attr, target))

        if get(["State", "Status"], inspect_data) != "running":
            return
        for attr, data in ATTR_MAP.items():
            validate(attr, data)

    def _status(self, verbose=False):
        try:
            self.svc.dockerlib.docker_exe
        except ex.excInitError as exc:
            self.status_log(str(exc), "warn")
            return rcStatus.DOWN
        if not self.svc.dockerlib.docker_running():
            self.status_log("docker daemon is not running", "info")
            return rcStatus.DOWN
        sta = resContainer.Container._status(self, verbose)
        self._status_container_image()
        self._status_inspect()

        return sta

    @lazy
    def balance(self):
        replicas = self.run_args_replicas()
        nodes = self.wanted_nodes_count()
        balance = replicas // nodes
        if balance == 0:
            balance = 1
        if replicas % nodes == 0:
            return balance, balance
        else:
            return balance, balance+1

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return check_ping(self.addr, timeout=1)

    def is_down(self):
        return not self.is_up()

    def is_up(self):
        if self.svc.dockerlib.docker_daemon_private and \
           self.svc.dockerlib.docker_data_dir is None:
            self.status_log("DEFAULT.docker_data_dir must be defined")

        if not self.svc.dockerlib.docker_running():
            return False

        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False
        if self.container_id in self.svc.dockerlib.get_running_instance_ids(refresh=True):
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
            data = self.svc.dockerlib.docker_inspect(self.container_id)
            return data["State"]["Pid"]
        except (IndexError, KeyError):
            return

    def container_sandboxkey(self):
        try:
            data = self.svc.dockerlib.docker_inspect(self.container_id)
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
