import resTask
import resContainerPodman

class Task(resContainerPodman.Container, resTask.Task):
    def __init__(self, *args, **kwargs):
        kwargs["detach"] = False
        kwargs["type"] = "task.podman"
        resContainerPodman.Container.__init__(self, *args, **kwargs)
        resTask.Task.__init__(self, *args, **kwargs)

    _info = resContainerPodman.Container._info
    _run_call = resContainerPodman.Container.start
