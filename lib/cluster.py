from rcUtilities import lazy
from svc import BaseSvc

DEFAULT_STATUS_GROUPS = [
]

class ClusterSvc(BaseSvc):
    kind = "ccfg"

    def __init__(self, *args, **kwargs):
        try:
            del kwargs["namespace"]
        except KeyError:
            pass
        BaseSvc.__init__(self, name="cluster", namespace=None, **kwargs)

    @lazy
    def kwdict(self):
        return __import__("clusterdict")


