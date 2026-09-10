import multiprocessing
import os

import pytest

from core.objects.svc import Svc
from core.resource import Resource
from utilities.lazy import set_lazy


class Marker(Resource):
    """
    A resource whose start leaves a file behind, so the parent can tell the
    child really ran the action.
    """

    def __init__(self, rid, marker):
        Resource.__init__(self, rid=rid, type="app.forking")
        self.marker = marker

    def start(self):
        with open(self.marker, "w") as ofile:
            ofile.write(str(os.getpid()))


@pytest.fixture(name="start_method")
def factory_start_method():
    """
    Force the way python starts a child process, and put back the one the
    test session had.
    """
    previous = multiprocessing.get_start_method()

    def set_start_method(method):
        multiprocessing.set_start_method(method, force=True)

    yield set_start_method
    multiprocessing.set_start_method(previous, force=True)


@pytest.mark.ci
class TestParallelResourceSetAction:
    @staticmethod
    @pytest.mark.parametrize("method", ["fork", "forkserver", "spawn"])
    def test_the_children_run_whatever_the_start_method_is(tmp_path, start_method, method):
        """
        The children of a parallel subset are forked, so they inherit the
        service instead of being handed a pickle of it.

        Python 3.14 made "forkserver" the default start method on linux, and
        the service does not pickle: its pg lazy caches the process group
        driver module, and a module is not picklable at all. Every parallel
        subset action died in proc.start() with "cannot pickle 'module'
        object" before the fork context was made explicit.
        """
        if method not in multiprocessing.get_all_start_methods():
            pytest.skip("no %s start method on this platform" % method)
        start_method(method)

        svc = Svc(name="svc")
        # a module, which is what the pg lazy caches on a real node
        set_lazy(svc, "pg", multiprocessing)
        markers = {}
        for idx in range(2):
            rid = "app#%d" % idx
            markers[rid] = str(tmp_path / rid)
            resource = Marker(rid, markers[rid])
            resource.subset = "parallel"
            svc += resource

        rset = svc.get_resourcesets(["app"])[0]
        rset.parallel = True
        rset.action("start")

        for rid, marker in markers.items():
            assert os.path.exists(marker), "%s did not run its action" % rid
