import resTask
import resContainerDocker

class Task(resContainerDocker.Container, resTask.Task):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.docker"
        resContainerDocker.Container.__init__(self, *args, **kwargs)
        resTask.Task.__init__(self, *args, **kwargs)

    _info = resContainerDocker.Container._info

    def _run_call(self):
        resContainerDocker.Container.start(self)
        if self.rm:
            self.container_rm()

