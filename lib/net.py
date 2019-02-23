from __future__ import print_function

import os

import rcExceptions as ex
from rcUtilities import lazy
from svc import BaseSvc

DEFAULT_STATUS_GROUPS = [
]

class NetSvc(BaseSvc):
    @lazy
    def kwdict(self):
        return __import__("netdict")

