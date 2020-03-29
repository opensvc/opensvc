# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import json
import logging

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import nodemgr

os.environ["PYTHONHTTPSVERIFY"] = "0"
CONFIG = "--config=/root/.opensvc-cli"
UNICODE_STRING = "bêh"
#logging.disable(logging.CRITICAL)

class Test:
    def test_000(self):
        """
        cli get /users/self
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "get", "/users/self",
        ])
        assert ret == 0

    def test_001(self):
        """
        cli ls
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "ls",
        ])
        assert ret == 0

    def test_011(self):
        """
        cli POST /groups
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "post", "/groups", "--data", "role=unittest",
        ])

    def test_101(self):
        """
        cli moduleset create 1
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "moduleset", "--create", "--moduleset", "unittest1",
        ])
        assert ret == 0

    def test_102(self):
        """
        cli moduleset create 2
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "moduleset", "--create", "--moduleset", "unittest2",
        ])
        assert ret == 0

    def test_103(self):
        """
        cli moduleset recreate 1
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "moduleset", "--create", "--moduleset", "unittest1",
        ])
        assert ret == 0

    def test_104(self):
        """
        cli moduleset add responsible
        """
        ret = nodemgr.main(argv=[
            CONFIG, "collector", "cli",
            "moduleset", "--attach",
            "--moduleset", "unittest1",
            "--responsible-group", "unittest",
            "--publication-group", "unittest",
        ])
        assert ret == 0

