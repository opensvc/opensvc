"""
Docker container resource driver module.
"""
import os
import shlex

import resources
import resContainer
import rcExceptions as ex
import rcStatus
from rcUtilitiesLinux import check_ping
from rcUtilities import justcall, lazy
from rcGlobalEnv import rcEnv

class Docker(resContainer.Container):
    """
    Docker container resource driver.
    """
    default_start_timeout = 2

    def __init__(self,
                 rid,
                 name="",
                 run_image=None,
                 run_command=None,
                 run_args=None,
                 docker_service=False,
                 rm=False,
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
        self.run_image = run_image
        self.run_command = run_command
        self.run_args = run_args
        self.rm = rm
        self.docker_service = docker_service

    @lazy
    def container_name(self):
        """
        Format a docker container name
        """
        if self.user_defined_name:
            return self.user_defined_name
        container_name = self.svc.svcname+'.'+self.rid
        return container_name.replace('#', '.')

    @lazy
    def name(self): # pylint: disable=method-hidden
        return self.container_name

    @lazy
    def service_name(self):
        """
        Format a docker compliant docker service name, ie without dots
        """
        return self.container_name.replace(".", "_")

    @lazy
    def container_id(self):
        try:
            return self.svc.dockerlib.get_container_id_by_name(self, refresh=True)
        except:
            return

    @lazy
    def service_id(self):
        try:
            return self.svc.dockerlib.get_service_id_by_name(self, refresh=True)
        except Exception:
            return

    @lazy
    def label(self): # pylint: disable=method-hidden
        if self.docker_service:
            return "docker service " + "@".join((
                self.service_name,
                self.svc.dockerlib.image_userfriendly_name(self)
            ))
        else:
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
        if self.docker_service:
            return self.svc.dockerlib.files_to_sync
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

    def service_create(self):
        self.unset_lazy("service_id")
        if self.service_id is not None:
            return
        if self.swarm_node_role() not in ("leader", "reachable"):
            return
        cmd = self.svc.dockerlib.docker_cmd + ['service', 'create', '--name='+self.service_name]
        cmd += self._add_run_args()
        cmd += [self.run_image]
        if self.run_command is not None and self.run_command != "":
            cmd += self.run_command.split()
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.unset_lazy("service_id")
        self.svc.dockerlib.get_running_service_ids(refresh=True)

    def container_rm(self):
        """
        Remove the resource docker instance.
        Only do if the dockerd is shared.
        """
        if self.docker_service:
            return
        self.unset_lazy("container_id")
        if self.container_id is None:
            self.log.info("container instance is already removed")
            return
        cmd = self.svc.dockerlib.docker_cmd + ['rm', self.container_name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.unset_lazy("container_id")

    def service_rm(self):
        """
        Remove the resource docker service.
        """
        self.unset_lazy("service_id")
        if self.service_id is None:
            self.log.info("skip: service already removed")
            return
        if self.swarm_node_role() not in ("leader", "reachable"):
            return
        cmd = self.svc.dockerlib.docker_cmd + ['service', 'rm', self.service_id]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        self.unset_lazy("service_id")
        self.svc.dockerlib.get_running_service_ids(refresh=True)

    def docker(self, action):
        """
        Wrap docker commands to honor <action>.
        """
        cmd = self.svc.dockerlib.docker_cmd + []
        if action == 'start':
            if self.docker_service:
                self.service_create()
                return
            else:
                if self.container_id is None:
                    self.unset_lazy("container_id")
                if self.container_id is None:
                    if self.svc.dockerlib.get_run_image_id(self) is None:
                        self.svc.dockerlib.docker_login(self.run_image)
                    cmd += ['run', '-d', '--name='+self.container_name]
                    cmd += self._add_run_args()
                    cmd += [self.run_image]
                    if self.run_command is not None and self.run_command != "":
                        cmd += self.run_command.split()
                else:
                    cmd += ['start', self.container_id]
        elif action == 'stop':
            if self.docker_service:
                self.service_stop()
                return
            else:
                cmd += ['stop', self.container_id]
        elif action == 'kill':
            if self.docker_service:
                return 0
            else:
                cmd += ['kill', self.container_id]
        else:
            self.log.error("unsupported docker action: %s", action)
            return 1

        ret = self.vcall(cmd, warn_to_info=True)[0]
        if ret != 0:
            raise ex.excError

        if action == 'start':
            self.unset_lazy("container_id")
            self.svc.dockerlib.get_running_instance_ids(refresh=True)
        elif action in ("stop", "kill"):
            self.unset_lazy("container_id")
            self.svc.dockerlib.docker_stop()

    def service_stop(self):
        if not self.svc.dockerlib.docker_daemon_private and self.swarm_node_role() == "worker":
            self.log.info("skip: worker with shared docker daemon")
            return
        role = self.swarm_node_role()
        if self.partial_action():
            if role == "worker":
                raise ex.excError("actions on a subset of docker services are not possible from a docker worker")
            elif role in ("leader", "reachable"):
                self.service_rm()
        else:
            if role == "worker":
                self.svc.dockerlib.docker_swarm_leave()
            elif role in ("leader", "reachable"):
                self.swarm_node_drain()

    def swarm_node_drain(self):
        if self.swarm_node_role() not in ("leader", "reachable"):
            return
        node_data = self.svc.dockerlib.node_data()
        if node_data["Spec"]["Availability"] == "drain":
            return
        cmd = self.svc.dockerlib.docker_cmd + ['node', 'update', '--availability=drain', rcEnv.nodename]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to update node availabilty as drain: %s" % err)

    def swarm_node_active(self):
        if self.swarm_node_role() not in ("leader", "reachable"):
            return
        node_data = self.svc.dockerlib.node_data()
        if node_data["Spec"]["Availability"] == "active":
            return
        cmd = self.svc.dockerlib.docker_cmd + ['node', 'update', '--availability=active', rcEnv.nodename]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError("failed to update node availabilty as active: %s" % err)

    def _add_run_args(self):
        if self.run_args is None:
            return []
        args = shlex.split(self.run_args)

        # drop user specified --name. we set ours already
        for aname in ("-n", "--name"):
            if aname in args:
                idx = args.index(aname)
                del args[idx]
                if len(args) >= idx and not args[idx].startswith("-"):
                    del args[idx]

        if self.vm_hostname:
            for aname in ("-h", "--hostname"):
                if aname in args:
                    idx = args.index(aname)
                    del args[idx]
                    if len(args) >= idx and not args[idx].startswith("-"):
                        del args[idx]
            args += ["--hostname", self.vm_hostname]

        if self.rm:
            if "--rm" not in args and \
               self.svc.dockerlib.docker_min_version("1.13"):
                args += ["--rm"]

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
        if self.svc.dockerlib.docker_min_version("1.7") and \
           not self.docker_service and self.svc.dockerlib.docker_daemon_private:
            args += ["--cgroup-parent", self._parent_cgroup_name()]
        if not self.svc.dockerlib.docker_min_version("1.13") and "--rm" in args:
            del args[args.index("--rm")]

        def dns_opts():
            if not self.svc.node.dns or "--dns" in self.run_args:
                return []
            if "--net=container:" in self.run_args or "--net container:" in self.run_args:
                return []
            if "--net=host" in self.run_args or "--net host" in self.run_args:
                return []
            l = []
            for dns in self.svc.node.dns:
                l += ["--dns", dns]
            for search in self.dns_search():
                l += ["--dns-search", search]
            return l

        args += dns_opts()
        return args

    def _parent_cgroup_name(self):
        """
        Return the name of the container parent cgroup.
        Ex: /<svcname>/<rset>/<rid> with invalid character replaced by dots.
        """
        return os.path.join(
            os.sep,
            "opensvc",
            self.svc.svcname,
            self.rset.rid.replace(":", "."),
            self.rid.replace("#", ".")
        )

    def container_start(self):
        self.docker('start')

    def _start(self):
        self.svc.dockerlib.docker_start()
        if self.docker_service:
            self.svc.dockerlib.init_swarm()
            self.swarm_node_active()
            if self.swarm_node_role() not in ("leader", "reachable"):
                self.log.info("skip: this docker node is not swarm manager")
                return
        resContainer.Container.start(self)

    def provision(self):
        resContainer.Container.provision(self)
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        resContainer.Container.unprovision(self)
        self.container_rm()

    def start(self):
        self._start()
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def container_stop(self):
        self.docker('stop')
        if self.rm:
            self.container_rm()

    def stop(self):
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        self._stop()

    def partial_action(self):
        if not self.svc.command_is_scoped():
            return False
        all_rids = set([res.rid for res in self.svc.get_resources("container.docker") if res.docker_service])
        if len(all_rids - set(self.svc.action_rid)) > 0:
            return True
        return False

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
        return data

    def wanted_nodes_count(self):
        if rcEnv.nodename in self.svc.nodes:
            return len(self.svc.nodes)
        else:
            return len(self.svc.drpnodes)

    def run_args_replicas(self):
        elements = self.run_args.split()
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

    @lazy
    def service_ps(self):
        return self.svc.dockerlib.service_ps_data(self.service_id)

    def running_replicas(self, refresh=False):
        return len(self.service_running_instances(refresh=refresh))

    @lazy
    def ready_nodes(self):
        return [node["ID"] for node in self.svc.dockerlib.node_ls_data() if node["Status"]["State"] == "ready"]

    def service_running_instances(self, refresh=False):
        if refresh:
            self.unset_lazy("service_ps")
        instances = []
        for inst in self.service_ps:
            if inst["Status"]["State"] != "running":
                continue
            if inst["NodeID"] not in self.ready_nodes:
                continue
            instances.append(inst)
        return instances

    def service_hosted_instances(self, refresh=False):
        out = self.svc.dockerlib.get_ps(refresh=refresh)
        return [line.split()[0] for line in out.splitlines() \
                if self.service_name in line and \
                "Exited" not in line and \
                "Failed" not in line and \
                "Created" not in line]

    def swarm_node_role(self):
        return self.svc.dockerlib.swarm_node_role

    def _status_service_replicas_state(self):
        if self.swarm_node_role() != "leader":
            return
        for inst in self.service_ps:
            if inst["NodeID"] not in self.ready_nodes:
                continue
            if inst["DesiredState"] != inst["Status"]["State"]:
                self.status_log("instance %s in state %s, desired %s" % (inst["ID"], inst["Status"]["State"], inst["DesiredState"]))

    def _status_service_replicas(self):
        if self.swarm_node_role() != "leader":
            return
        wanted = self.run_args_replicas()
        if wanted is None:
            return
        current = self.running_replicas()
        if wanted != current:
            if current == 0:
                # a pure resource 'down' state, we don't want to cause a warn
                # at the service overall status
                level = "info"
            else:
                level = "warn"
            self.status_log("%d replicas wanted, %d currently running" % (wanted, current), level)

    def _status_service_image(self):
        if self.swarm_node_role() != "leader":
            return
        try:
            run_image_id = self.svc.dockerlib.get_run_image_id(self, pull=False)
        except ValueError as exc:
            self.status_log(str(exc))
            return
        try:
            inspect = self.svc.dockerlib.docker_service_inspect(self.service_id)
        except Exception:
            return
        running_image_id = inspect['Spec']['TaskTemplate']['ContainerSpec']['Image']
        running_image_id = self.svc.dockerlib.repotag_to_image_id(running_image_id)
        if run_image_id is None:
            self.status_log("image '%s' is not pulled yet."%(self.run_image))
        elif run_image_id != running_image_id:
            self.status_log("the service is configured with image '%s' "
                            "instead of '%s'"%(running_image_id, run_image_id))

    def _status_container_image(self):
        try:
            run_image_id = self.svc.dockerlib.get_run_image_id(self, pull=False)
        except ValueError as exc:
            self.status_log(str(exc))
            return
        try:
            inspect = self.svc.dockerlib.docker_inspect(self.container_id)
        except Exception:
            return
        running_image_id = inspect['Image']
        if run_image_id is None:
            self.status_log("image '%s' is not pulled yet."%(self.run_image))
        elif run_image_id != running_image_id:
            self.status_log("the current container is based on image '%s' "
                            "instead of '%s'"%(running_image_id, run_image_id))

    def _status(self, verbose=False):
        try:
            self.svc.dockerlib.docker_exe
        except ex.excInitError as exc:
            self.status_log(str(exc), "warn")
            return rcStatus.DOWN
        if not self.svc.dockerlib.docker_running():
            self.status_log("docker daemon is not running", "info")
            return rcStatus.DOWN
        if self.docker_service:
            if self.swarm_node_role() == "none":
                self.status_log("swarm node is not joined", "info")
                return rcStatus.DOWN
            self.svc.dockerlib.nodes_purge()
            self.running_replicas(refresh=True)
            self._status_service_image()
            self._status_service_replicas()
            self._status_service_replicas_state()
            sta = resContainer.Container._status(self, verbose)
            hosted = len(self.service_hosted_instances())
            if hosted > 0:
                self.status_log("%d/%d instances hosted" % (hosted, self.run_args_replicas()), "info")
                balance_min, balance_max = self.balance
                if hosted > balance_max:
                    self.status_log("%d>%d instances imbalance" % (hosted, balance_max), "warn")
                elif hosted < balance_min:
                    self.status_log("%d<%d instances imbalance" % (hosted, balance_min), "warn")
            elif sta == rcStatus.UP:
                sta = rcStatus.STDBY_UP
        else:
            sta = resContainer.Container._status(self, verbose)
            self._status_container_image()

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
        if self.docker_service:
            hosted = len(self.service_hosted_instances(refresh=True))
            if hosted > 0:
                return False
            return True
        else:
            return not self.is_up()

    def is_up(self):
        if self.svc.dockerlib.docker_daemon_private and \
           self.svc.dockerlib.docker_data_dir is None:
            self.status_log("DEFAULT.docker_data_dir must be defined")

        if not self.svc.dockerlib.docker_running():
            return False

        if self.docker_service:
            if self.swarm_node_role() == "leader":
                if self.service_id is None:
                    self.status_log("docker service is not created", "info")
                    return False
                if self.running_replicas(refresh=True) == 0:
                    return False
                if self.service_id in self.svc.dockerlib.get_running_service_ids(refresh=True):
                    return True
            else:
                return True
        else:
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
