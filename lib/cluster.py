from rcUtilities import lazy
from svc import BaseSvc

DEFAULT_STATUS_GROUPS = [
]

class ClusterSvc(BaseSvc):
    def __init__(self, *args, **kwargs):
        BaseSvc.__init__(self, svcname="cluster", namespace="system", **kwargs)

    @lazy
    def kwdict(self):
        return __import__("clusterdict")


