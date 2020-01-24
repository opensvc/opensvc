import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import uuid
from freezer import Freezer

SVCNAME = "unittest-" + str(uuid.uuid4())

class TestFreezer:

    @classmethod
    def setup_class(cls):
        cls.freezer = Freezer(SVCNAME)

    @classmethod
    def teardown_class(cls):
        cls.freezer.node_thaw()
        cls.freezer.thaw()

    def test_00(self):
        """
        Freezer, node
        """
        self.freezer.node_thaw()
        ret = self.freezer.node_frozen()
        assert not ret

        self.freezer.node_freeze()
        ret = self.freezer.node_frozen()
        assert ret

    def test_01(self):
        """
        Freezer, svc, not strict
        """
        self.freezer.node_thaw()
        self.freezer.thaw()
        ret = self.freezer.frozen()
        assert not ret

        self.freezer.node_thaw()
        self.freezer.freeze()
        ret = self.freezer.frozen()
        assert ret

        self.freezer.node_freeze()
        self.freezer.thaw()
        ret = self.freezer.frozen()
        assert ret

        self.freezer.node_freeze()
        self.freezer.freeze()
        ret = self.freezer.frozen()
        assert ret

    def test_02(self):
        """
        Freezer, svc, strict
        """
        self.freezer.node_thaw()
        self.freezer.thaw()
        ret = self.freezer.frozen(strict=True)
        assert not ret

        self.freezer.node_freeze()
        self.freezer.thaw()
        ret = self.freezer.frozen(strict=True)
        assert not ret

        self.freezer.node_thaw()
        self.freezer.freeze()
        ret = self.freezer.frozen(strict=True)
        assert ret

        self.freezer.node_freeze()
        self.freezer.freeze()
        ret = self.freezer.frozen(strict=True)
        assert ret

