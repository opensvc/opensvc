from .. import \
    KEYWORDS as BASE_KEYWORDS, \
    BaseTask
from drivers.resource.container.docker import \
    KEYWORDS as DOCKER_KEYWORDS, \
    ContainerDocker
import core.exceptions as ex

from core.objects.builder import init_kwargs
from core.objects.svcdict import KEYS

DRIVER_GROUP = "task"
DRIVER_BASENAME = "docker"
DRIVER_BASENAME_ALIASES = ["oci"]
KEYWORDS = BASE_KEYWORDS + DOCKER_KEYWORDS
DEPRECATED_KEYWORDS = {
    "task.docker.run_image": "image",
    "task.docker.run_command": "command",
    "task.docker.net": "netns",
    "task.oci.run_image": "image",
    "task.oci.run_command": "command",
    "task.oci.net": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "task.docker.image": "run_image",
    "task.docker.command": "run_command",
    "task.docker.netns": "net",
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

def adder(svc, s):
    kwargs = init_kwargs(svc, s)
    kwargs["image"] = svc.oget(s, "image")
    kwargs["image_pull_policy"] = svc.oget(s, "image_pull_policy")
    kwargs["run_command"] = svc.oget(s, "command")
    kwargs["run_args"] = svc.oget(s, "run_args")
    kwargs["rm"] = svc.oget(s, "rm")
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
    kwargs["command"] = svc.oget(s, "command")
    kwargs["on_error"] = svc.oget(s, "on_error")
    kwargs["user"] = svc.oget(s, "user")
    kwargs["timeout"] = svc.oget(s, "timeout")
    kwargs["snooze"] = svc.oget(s, "snooze")
    kwargs["log"] = svc.oget(s, "log")
    kwargs["confirmation"] = svc.oget(s, "confirmation")
    kwargs["check"] = svc.oget(s, "check")
    r = TaskDocker(**kwargs)
    svc += r


class TaskDocker(ContainerDocker, BaseTask):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.docker"
        ContainerDocker.__init__(self, *args, **kwargs)
        BaseTask.__init__(self, *args, **kwargs)

    _info = ContainerDocker._info

    def start(self):
        BaseTask.start(self)

    def stop(self):
        BaseTask.stop(self)

    def _run_call(self):
        try:
            ContainerDocker.start(self)
            self.write_last_run_retcode(0)
        except ex.Error:
            self.write_last_run_retcode(1)
            raise
        if self.rm:
            self.container_rm()

    def _status(self, *args, **kwargs):
        return BaseTask._status(self, *args, **kwargs)

