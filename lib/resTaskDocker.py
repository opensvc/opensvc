import resTask
import resContainerDocker

import rcExceptions as ex

from svcBuilder import init_kwargs


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
    r = Task(**kwargs)
    svc += r


class Task(resContainerDocker.Container, resTask.Task):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.docker"
        resContainerDocker.Container.__init__(self, *args, **kwargs)
        resTask.Task.__init__(self, *args, **kwargs)

    _info = resContainerDocker.Container._info

    def start(self):
        resTask.Task.start(self)

    def stop(self):
        resTask.Task.stop(self)

    def _run_call(self):
        try:
            resContainerDocker.Container.start(self)
            self.write_last_run_retcode(0)
        except ex.excError:
            self.write_last_run_retcode(1)
            raise
        if self.rm:
            self.container_rm()

    def _status(self, *args, **kwargs):
        return resTask.Task._status(self, *args, **kwargs)
