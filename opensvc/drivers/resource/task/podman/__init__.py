import core.exceptions as ex

from .. import \
    BaseTask, \
    KEYWORDS as BASE_KEYWORDS
from drivers.resource.container.podman import \
    ContainerPodman, \
    KEYWORDS as PODMAN_KEYWORDS
from core.objects.svcdict import KEYS

DRIVER_GROUP = "task"
DRIVER_BASENAME = "podman"
DRIVER_BASENAME_ALIASES = ["oci"]
KEYWORDS = BASE_KEYWORDS + [kw for kw in PODMAN_KEYWORDS if kw["keyword"] != "start_timeout"]
DEPRECATED_KEYWORDS = {
    "task.podman.run_image": "image",
    "task.podman.run_command": "command",
    "task.podman.net": "netns",
    "task.oci.run_image": "image",
    "task.oci.run_command": "command",
    "task.oci.net": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "task.podman.image": "run_image",
    "task.podman.command": "run_command",
    "task.podman.netns": "net",
    "task.oci.image": "run_image",
    "task.oci.command": "run_command",
    "task.oci.netns": "net",
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("podman"):
        data += [
            "task.podman",
            "task.podman.registry_creds",
        ]
    return data

class TaskPodman(ContainerPodman, BaseTask):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.podman"
        ContainerPodman.__init__(self, *args, **kwargs)
        BaseTask.__init__(self, *args, **kwargs)
        self.start_timeout = self.timeout

    _info = ContainerPodman._info

    def start(self):
        BaseTask.start(self)

    def stop(self):
        BaseTask.stop(self)

    def _run_call(self):
        try:
            ContainerPodman.start(self)
            self.write_last_run_retcode(0)
        except ex.Error:
            self.write_last_run_retcode(1)
            raise
        finally:
            if self.rm:
                self.container_rm()

    def _status(self, *args, **kwargs):
        return BaseTask._status(self, *args, **kwargs)

    def post_provision_start(self):
        pass

    def pre_provision_stop(self):
        pass

