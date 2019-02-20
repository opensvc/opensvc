import resTask
import resContainerDocker

class Task(resContainerDocker.Container, resTask.Task):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.docker"
        resContainerDocker.Container.__init__(self, *args, **kwargs)
        resTask.Task.__init__(self, *args, **kwargs)

    _info = resContainerDocker.Container._info
    _run_call = resContainerDocker.Container.start
