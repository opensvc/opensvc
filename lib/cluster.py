from rcUtilities import lazy
from svc import BaseSvc

DEFAULT_STATUS_GROUPS = [
]

class ClusterSvc(BaseSvc):
    kind = "ccfg"

    def __init__(self, *args, **kwargs):
        for kwarg in ("name", "namespace"):
            try:
                del kwargs[kwarg]
            except KeyError:
                pass
        BaseSvc.__init__(self, name="cluster", namespace=None, **kwargs)

    @lazy
    def kwstore(self):
        return __import__("clusterdict").KEYS


