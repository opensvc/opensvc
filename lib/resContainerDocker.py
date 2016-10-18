import os
from datetime import datetime
from subprocess import *

import sys
import rcStatus
import rcDocker
import json
import re
import shlex
import resources as Res
from rcUtilitiesLinux import check_ping
from rcUtilities import which, justcall
from rcGlobalEnv import rcEnv
import resContainer
import rcExceptions as ex

from svcBuilder import conf_get_string_scope

os.environ['LANG'] = 'C'

class Docker(resContainer.Container, rcDocker.DockerLib):

    def files_to_sync(self):
        return []

    def operational(self):
        return True

    def vm_hostname(self):
        return ""

    def get_rootfs(self):
        import glob
        inspect = self.docker_inspect(self.container_id)
        instance_id = str(inspect['Id'])
        pattern = str(self.docker_data_dir)+"/*/mnt/"+instance_id
        l = glob.glob(pattern)
        if len(l) == 0:
            raise ex.excError("no candidates rootfs paths matching %s" % pattern)
        elif len(l) != 1:
            raise ex.excError("too many candidates rootfs paths: %s" % ', '.join(l))
        return l[0]

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
        l = shlex.split(self.run_args)
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
        if self.docker_min_version("1.7"):
            l += ["--cgroup-parent", os.path.join(os.sep, self.svc.svcname, self.rset.rid.replace(":", "."), self.rid.replace("#", "."))]
        return l

    def swarm_primary(self):
        if not hasattr(self.svc, "flex_primary"):
            return False
        if self.run_swarm is None:
            return False
        if rcEnv.nodename != self.svc.flex_primary:
            return False
        return True

    def container_start(self):
        self.docker_start()
        self.docker('start')

    def start(self):
        if self.svc.running_action == "boot" and self.run_swarm and not self.swarm_primary():
            self.log.info("skip boot: this container will be booted by the flex primary node, or through a start action from any flex node")
            return
        resContainer.Container.start(self)
        self.get_running_instance_ids(refresh=True)
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def container_stop(self):
        self.docker('stop')

    def stop(self):
        self.status()
        if hasattr(self, "swarm_node") and self.swarm_node != rcEnv.nodename:
            self.log.info("skip stop: this container is handled by the %s node" % self.swarm_node)
            return
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        resContainer.Container.stop(self)
        self.get_running_instance_ids(refresh=True)
        self.docker_stop()

    def info(self):
        data = self.docker_info()
        return self.fmt_info(data)

    def _status(self, verbose=False):
        s = resContainer.Container._status(self, verbose)
        try:
            inspect = self.docker_inspect(self.container_id)
        except Exception as e:
            return s
        running_image_id = inspect['Image']
        run_image_id = self.get_run_image_id()

        if run_image_id != running_image_id:
            self.status_log("the current container is based on image '%s' instead of '%s'"%(running_image_id, run_image_id))

        return s

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return check_ping(self.addr, timeout=1)

    def is_up(self, nodename=None):
        if self.svc.docker_daemon_private and self.docker_data_dir is None:
            self.status_log("DEFAULT.docker_data_dir must be defined")

        if not self.docker_running():
            self.status_log("docker daemon is not running", "info")
            return False

        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False

        if self.container_id in self.get_running_instance_ids():
            return True
        return False

    def get_running_instance_ids(self, refresh=False):
        if not refresh and hasattr(self.svc, "docker_running_instance_ids_cache"):
            return self.svc.docker_running_instance_ids_cache
        self.svc.docker_running_instance_ids_cache = self._get_running_instance_ids()
        return self.svc.docker_running_instance_ids_cache

    def _get_running_instance_ids(self):
        cmd = self.docker_cmd + ['ps', '-q', '--no-trunc']
        out, err, ret = justcall(cmd)
        return out.replace('\n', ' ').split()

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        return True

    def __init__(self,
                 rid,
                 run_image,
                 run_command=None,
                 run_args=None,
                 run_swarm=None,
                 guestos="Linux",
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 subset=None,
                 osvc_root_path=None,
                 tags=set([]),
                 always_on=set([])):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name="",
                                        type="container.docker",
                                        guestos=guestos,
                                        optional=optional,
                                        disabled=disabled,
                                        monitor=monitor,
                                        restart=restart,
                                        subset=subset,
                                        osvc_root_path=osvc_root_path,
                                        tags=tags,
                                        always_on=always_on)

        self.run_image = run_image
        self.run_command = run_command
        self.run_args = run_args
        self.run_swarm = run_swarm
        self.max_wait_for_dockerd = 5

    def on_add(self):
        self.container_name = self.svc.svcname+'.'+self.rid
        self.container_name = self.container_name.replace('#', '.')
        rcDocker.DockerLib.on_add(self)
        try:
            self.container_id = self.get_container_id_by_name()
        except Exception as e:
            self.container_id = None
        self.label = "@".join((self.container_name, self.image_userfriendly_name()))
        if hasattr(self, "swarm_node"):
            self.label += " on " + self.swarm_node

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def provision(self):
        # docker resources are naturally provisioned
        self.start()
        self.status(refresh=True)

    def unprovision(self):
        self.stop()
        self.status(refresh=True)

