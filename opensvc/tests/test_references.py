# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import socket
import sys
import uuid

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from commands.svc import Mgr
from utilities.string import try_decode


UNICODE_STRING = "bêh"
NODENAME=socket.gethostname()
SVCNAME = "unittest-" + str(uuid.uuid4())

REFS = [
    ("env.ref0", "1 2 3", "1 2 3"),
    ("env.ref1", "{nodes}", NODENAME),
    ("env.ref2", "{ref1}", NODENAME),
    ("env.ref3", "$(1+2)", "3"),
    ("env.ref4", "1", "1"),
    ("env.ref5", "$({env.ref4}+2)", "3"),
    ("env.ref6", "$({env.ref0[#]}+2)", "5"),
    ("env.ref7", "{env.ref0[1]}", "2"),
]

class TestReferences:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_001_create_empty(self):
        """
        Create a trivial service
        """
        ret = Mgr()(argv=["create", "-s", SVCNAME])
        assert ret == 0

    def test_002_set_default(self):
        """
        Set references
        """
        refs = ["--kw", "nodes=%s" % NODENAME]
        for idx, (name, val, exp_val) in enumerate(REFS):
            refs += ["--kw", "%s=%s" % (name, val)]
        ret = Mgr()(argv=["-s", SVCNAME, "set"] + refs)
        assert ret == 0

    def test_003_ref_0(self): self.__get_ref(0)
    def test_003_ref_1(self): self.__get_ref(1)
    def test_003_ref_2(self): self.__get_ref(2)
    def test_003_ref_3(self): self.__get_ref(3)
    def test_003_ref_4(self): self.__get_ref(4)
    def test_003_ref_5(self): self.__get_ref(5)
    def test_003_ref_6(self): self.__get_ref(6)
    def test_003_ref_7(self): self.__get_ref(7)

    def __get_ref(self, idx):
        """
        Get ref
        """
        name, val, exp_val = REFS[idx]
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = Mgr()(argv=["-s", SVCNAME, "get", "--eval", "--kw", name])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout
        print(ret, output, exp_val)
        assert ret == 0
        assert try_decode(output) == exp_val

    def test_004_delete(self):
        """
        Delete local service instance
        """
        ret = Mgr()(argv=["delete", "-s", SVCNAME, "--local"])
        assert ret == 0


