from utilities.lazy import lazy
from core.objects.svc import BaseSvc

DEFAULT_STATUS_GROUPS = [
]

class Ccfg(BaseSvc):
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
        from .ccfgdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .ccfgdict import KEYS
        return KEYS

