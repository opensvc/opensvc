"""
Docker container resource driver module.
"""
import os
import shlex

import resources
import resContainer
import rcExceptions as ex
from rcUtilitiesLinux import check_ping
from rcUtilities import justcall
from rcGlobalEnv import rcEnv

os.environ['LANG'] = 'C'

class Docker(resContainer.Container):
    """
    Docker container resource driver.
    """
    def __init__(self,
                 rid,
                 run_image,
                 run_command=None,
                 run_args=None,
                 run_swarm=None,
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

        self.run_image = run_image
        self.run_command = run_command
        self.run_args = run_args
        self.run_swarm = run_swarm
        self.container_id = None
        self.container_name = None
        self.swarm_node = None

    def on_add(self):
        """
        Init done after self.svc is set.
        """
        self.container_name = self.svc.svcname+'.'+self.rid
        self.container_name = self.container_name.replace('#', '.')
        try:
            self.container_id = self.svc.dockerlib.get_container_id_by_name(self)
        except Exception:
            self.container_id = None
        self.label = "@".join((
            self.container_name,
            self.svc.dockerlib.image_userfriendly_name(self)
        ))
        if self.swarm_node:
            self.label += " on " + self.swarm_node

    def __str__(self):
        return "%s name=%s" % (resources.Resource.__str__(self), self.name)

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

    def vm_hostname(self):
        """
        Return an empty string, as we won't need that.
        """
        return ""

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

    def rcp_from(self, src, dst):
        """
        Copy <src> from the container's rootfs to <dst> in the host's fs.
        """
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        src = rootfs + src
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s" % (' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        """
        Copy <src> from the host's fs to the container's rootfs.
        """
        rootfs = self.get_rootfs()
        if len(rootfs) == 0:
            raise ex.excError()
        dst = rootfs + dst
        cmd = ['cp', src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s" % (' '.join(cmd), err))
        return out, err, ret

    def docker(self, action):
        """
        Wrap docker commands to honor <action>.
        """
        cmd = self.svc.dockerlib.docker_cmd + []
        if action == 'start':
            if self.container_id is None:
                self.container_id = self.svc.dockerlib.get_container_id_by_name(self)
            if self.container_id is None:
                cmd += ['run', '-d', '--name='+self.container_name]
                cmd += self._add_run_args()
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
            self.log.error("unsupported docker action: %s", action)
            return 1

        ret = self.vcall(cmd, warn_to_info=True)[0]
        if ret != 0:
            raise ex.excError

        if action == 'start':
            self.container_id = self.svc.dockerlib.get_container_id_by_name(self, refresh=True)
        self.svc.dockerlib.get_running_instance_ids(refresh=True)

    def _add_run_args(self):
        if self.run_args is None:
            return []
        args = shlex.split(self.run_args)
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
            args += ["--cgroup-parent", self._parent_cgroup_name()]
        return args

    def _parent_cgroup_name(self):
        """
        Return the name of the container parent cgroup.
        Ex: /<svcname>/<rset>/<rid> with invalid character replaced by dots.
        """
        return os.path.join(
            os.sep,
            self.svc.svcname,
            self.rset.rid.replace(":", "."),
            self.rid.replace("#", ".")
        )

    def _swarm_primary(self):
        if not hasattr(self.svc, "flex_primary"):
            return False
        if self.run_swarm is None:
            return False
        if rcEnv.nodename != self.svc.flex_primary:
            return False
        return True

    def container_start(self):
        self.svc.dockerlib.docker_start()
        self.container_id = self.svc.dockerlib.get_container_id_by_name(self)
        self.docker('start')

    def _start(self):
        if self.svc.running_action == "boot" and self.run_swarm and not self._swarm_primary():
            self.log.info("skip boot: this container will be booted by the "
                          "flex primary node, or through a start action from "
                          "any flex node")
            return
        resContainer.Container.start(self)
        self.svc.dockerlib.get_running_instance_ids(refresh=True)

    def start(self):
        self._start()
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def container_stop(self):
        self.docker('stop')

    def stop(self):
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        self._stop()

    def _stop(self):
        self.status()
        if self.swarm_node is not None and self.swarm_node != rcEnv.nodename:
            self.log.info("skip stop: this container is handled by the %s node",
                          self.swarm_node)
            return
        resContainer.Container.stop(self)
        self.svc.dockerlib.get_running_instance_ids(refresh=True)
        self.svc.dockerlib.docker_stop()

    def info(self):
        """
        Return keys to contribute to resinfo.
        """
        data = self.svc.dockerlib.info()
        return self.fmt_info(data)

    def _status(self, verbose=False):
        sta = resContainer.Container._status(self, verbose)
        try:
            inspect = self.svc.dockerlib.docker_inspect(self.container_id)
        except Exception:
            return sta
        running_image_id = inspect['Image']
        run_image_id = self.svc.dockerlib.get_run_image_id(self)

        if run_image_id != running_image_id:
            self.status_log("the current container is based on image '%s' "
                            "instead of '%s'"%(running_image_id, run_image_id))

        return sta

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self):
        if self.svc.dockerlib.docker_daemon_private and \
           self.svc.dockerlib.docker_data_dir is None:
            self.status_log("DEFAULT.docker_data_dir must be defined")

        if not self.svc.dockerlib.docker_running():
            self.status_log("docker daemon is not running", "info")
            return False

        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False

        if self.container_id in self.svc.dockerlib.get_running_instance_ids():
            return True
        return False

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        return True

    def provision(self):
        # docker resources are naturally provisioned
        self._start()
        self.status(refresh=True)
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        self._stop()
        self.status(refresh=True)

