import os
import logging
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from node import Node
node = Node()

logging.disable(logging.CRITICAL)

class TestNode:
    def test_010_str(self):
        """
        Eval the Node::kwdict lazy attr
        """
        assert str(node) == rcEnv.nodename

    def test_011_kwdict(self):
        """
        Eval the Node::kwdict lazy attr
        """
        assert isinstance(node.kwdict, object)

    def test_012_devnull(self):
        """
        Eval the Node::devnull lazy attr
        """
        assert isinstance(node.devnull, int)
        assert node.devnull >= 0

    def test_013_var_d(self):
        """
        Eval the Node::var_d lazy attr
        """
        assert node.var_d

    def test_014_system(self):
        """
        Eval the Node::system lazy attr
        """
        assert node.system

    def test_015_compliance(self):
        """
        Eval the Node::compliance lazy attr
        """
        assert node.compliance

    def test_021_split_url(self):
        """
        split url "None"
        """
        assert node.split_url("None") == ('https', '127.0.0.1', '443', '/')

    def test_022_split_url(self):
        """
        split url "localhost"
        """
        assert node.split_url("localhost") == ('https', 'localhost', '443', None)

    def test_023_split_url(self):
        """
        split url "https://localhost"
        """
        assert node.split_url("https://localhost") == ('https', 'localhost', '443', None)

    def test_024_split_url(self):
        """
        split url "https://localhost:443"
        """
        assert node.split_url("https://localhost:443") == ('https', 'localhost', '443', None)

    def test_025_split_url(self):
        """
        split url "http://localhost:8080"
        """
        assert node.split_url("http://localhost:8080") == ('http', 'localhost', '8080', None)

    def test_026_split_url(self):
        """
        split url "http://localhost:8080/init"
        """
        assert node.split_url("http://localhost:8080/init") == ('http', 'localhost', '8080', "init")

    def test_027_split_url(self):
        """
        split url "http://localhost:8080/feed/default/xmlrpc"
        """
        assert node.split_url("http://localhost:8080/feed/default/xmlrpc") == ('http', 'localhost', '8080', "feed")

    def test_028_split_url(self):
        """
        split url "http://localhost/feed/default/xmlrpc"
        """
        assert node.split_url("http://localhost/feed/default/xmlrpc") == ('http', 'localhost', '80', "feed")

    def test_029_split_url(self):
        """
        split invalid url "http://localhost:80:80/feed/default/xmlrpc"
        """
        try:
            node.split_url("http://localhost:80:80/feed/default/xmlrpc")
            assert False
        except ex.excError:
            assert True

    def test_031_call(self):
        """
        Node::call()
        """
        ret, out, err = node.call(["pwd"])
        assert ret == 0

    def test_032_vcall(self):
        """
        Node::vcall()
        """
        ret, out, err = node.vcall(["pwd"])
        assert ret == 0

    def test_04_make_temp_config(self):
        fpath = node.make_temp_config()
        assert fpath
        os.unlink(fpath)

