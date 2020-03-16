"""
Docker container resource driver module.
"""
import os
import shlex
import signal
from itertools import chain

import resources
import resContainer
import rcContainer
import rcExceptions as ex
import rcStatus
from rcUtilitiesLinux import check_ping
from rcUtilities import justcall, lazy, drop_option, has_option, get_option, get_options
from rcGlobalEnv import rcEnv
import resContainerDocker


def adder(svc, s):
    resContainerDocker.adder(svc, s, drv=Container)


class Container(resContainerDocker.Container):
    """
    Docker container resource driver.
    """
    default_net = "lo"
    dns_option_option = "--dns-opt"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = "container.podman"
        resContainerDocker.Container.__init__(self, *args, **kwargs)

    @lazy
    def lib(self):
        """
        Lazy allocator for the podmanlib object.
        """
        try:
            return self.svc.podmanlib
        except AttributeError:
            self.svc.podmanlib = rcContainer.PodmanLib(self.svc)
            return self.svc.podmanlib

    @lazy
    def label(self): # pylint: disable=method-hidden
        return "podman " + self.lib.image_userfriendly_name(self)

    def container_rm(self):
        """
        Remove the resource podman instance.
        """
        cmd = self.lib.docker_cmd + ["rm", self.container_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if "unable to find" in err:
                pass
            elif "no such file" in err:
                pass
            elif "container has already been removed" in err:
                pass
            elif "has dependent containers which must be removed" in err:
                pass
            elif "no container with name" in err:
                pass
            elif "removal" in err and "already in progress" in err:
                self.wait_for_removed()
            else:
                self.log.info(" ".join(cmd))
                raise ex.excError(err)
        else:
            self.log.info(" ".join(cmd))
        self.is_up_clear_cache()

    def _start(self):
        resContainer.Container.start(self)
        self.is_up_clear_cache()

    def _stop(self):
        resContainer.Container.stop(self)
        self.is_up_clear_cache()
        if self.rm:
            self.container_rm()

    def _status(self, verbose=False):
        if not self.detach:
            return rcStatus.NA
        try:
            self.lib.docker_exe
        except ex.excInitError as exc:
            self.status_log(str(exc), "warn")
            return rcStatus.DOWN
        sta = resContainer.Container._status(self, verbose)
        self._status_inspect()
        return sta

    def is_up(self):
        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False
        if self.container_id in self.lib.get_running_instance_ids():
            return True
        return False

    def cgroup_options(self):
        return ["--cgroup-parent", self.cgroup_dir+"/libpod"]

