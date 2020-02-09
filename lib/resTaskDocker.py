import resTask
import resContainerDocker

import rcExceptions as ex

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

