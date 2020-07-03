"""
Docker container resource driver module.
"""
import core.status
import utilities.subsystems.docker as dockerlib
import core.exceptions as ex
from .. import \
    BaseContainer
from ..docker import KEYWORDS, ContainerDocker
from utilities.lazy import lazy
from core.objects.svcdict import KEYS
from utilities.proc import justcall

DRIVER_GROUP = "container"
DRIVER_BASENAME = "podman"
DRIVER_BASENAME_ALIASES = ["oci"]
DEPRECATED_KEYWORDS = {
    "container.podman.run_image": "image",
    "container.podman.run_command": "command",
    "container.podman.net": "netns",
    "container.oci.run_image": "image",
    "container.oci.run_command": "command",
    "container.oci.net": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "container.podman.image": "run_image",
    "container.podman.command": "run_command",
    "container.podman.netns": "net",
    "container.oci.image": "run_image",
    "container.oci.command": "run_command",
    "container.oci.netns": "net",
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("podman"):
        data += [
            "container.podman",
            "container.podman.registry_creds",
            "container.podman.signal",
        ]
    return data


class ContainerPodman(ContainerDocker):
    """
    Docker container resource driver.
    """
    default_net = "lo"
    dns_option_option = "--dns-opt"

    def __init__(self, *args, **kwargs):
        kwargs["type"] = "container.podman"
        super(ContainerPodman, self).__init__(*args, **kwargs)

    @lazy
    def lib(self):
        """
        Lazy allocator for the podmanlib object.
        """
        try:
            return self.svc.podmanlib
        except AttributeError:
            self.svc.podmanlib = dockerlib.PodmanLib(self.svc)
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
                raise ex.Error(err)
        else:
            self.log.info(" ".join(cmd))
        self.is_up_clear_cache()

    def _start(self):
        BaseContainer.start(self)
        self.is_up_clear_cache()

    def _stop(self):
        BaseContainer.stop(self)
        self.is_up_clear_cache()
        if self.rm:
            self.container_rm()

    def _status(self, verbose=False):
        if not self.detach:
            return core.status.NA
        try:
            self.lib.docker_exe
        except ex.InitError as exc:
            self.status_log(str(exc), "warn")
            return core.status.DOWN
        sta = BaseContainer._status(self, verbose)
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

