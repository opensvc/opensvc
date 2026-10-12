import os
import logging
import core.exceptions as ex
from env import Env
from core.node import Node
import pytest


@pytest.fixture(scope='function')
def node():
    return Node()


class TestNode:
    @staticmethod
    def test_010_str(node):
        """
        Eval the Node::kwdict lazy attr
        """
        assert str(node) == Env.nodename

    @staticmethod
    def test_011_kwdict(node):
        """
        Eval the Node::kwdict lazy attr
        """
        assert isinstance(node.kwdict, object)

    @staticmethod
    def test_012_devnull(node):
        """
        Eval the Node::devnull lazy attr
        """
        assert isinstance(node.devnull, int)
        assert node.devnull >= 0

    @staticmethod
    def test_013_var_d(node):
        """
        Eval the Node::var_d lazy attr
        """
        assert node.var_d

    @staticmethod
    def test_014_system(node):
        """
        Eval the Node::system lazy attr
        """
        assert node.system

    @staticmethod
    def test_015_compliance(node):
        """
        Eval the Node::compliance lazy attr
        """
        assert node.compliance

    @staticmethod
    def test_021_split_url(node):
        """
        split url "None"
        """
        assert node.split_url("None") == ('https', '127.0.0.1', '443', '/')

    @staticmethod
    def test_022_split_url(node):
        """
        split url "localhost"
        """
        assert node.split_url("localhost") == ('https', 'localhost', '443', None)

    @staticmethod
    def test_023_split_url(node):
        """
        split url "https://localhost"
        """
        assert node.split_url("https://localhost") == ('https', 'localhost', '443', None)

    @staticmethod
    def test_024_split_url(node):
        """
        split url "https://localhost:443"
        """
        assert node.split_url("https://localhost:443") == ('https', 'localhost', '443', None)

    @staticmethod
    def test_025_split_url(node):
        """
        split url "http://localhost:8080"
        """
        assert node.split_url("http://localhost:8080") == ('http', 'localhost', '8080', None)

    @staticmethod
    def test_026_split_url(node):
        """
        split url "http://localhost:8080/init"
        """
        assert node.split_url("http://localhost:8080/init") == ('http', 'localhost', '8080', "init")

    @staticmethod
    def test_027_split_url(node):
        """
        split url "http://localhost:8080/feed/default/xmlrpc"
        """
        assert node.split_url("http://localhost:8080/feed/default/xmlrpc") == ('http', 'localhost', '8080', "feed")

    @staticmethod
    def test_028_split_url(node):
        """
        split url "http://localhost/feed/default/xmlrpc"
        """
        assert node.split_url("http://localhost/feed/default/xmlrpc") == ('http', 'localhost', '80', "feed")

    @staticmethod
    def test_029_split_url(node):
        """
        split invalid url "http://localhost:80:80/feed/default/xmlrpc"
        """
        try:
            node.split_url("http://localhost:80:80/feed/default/xmlrpc")
            assert False
        except ex.Error:
            assert True

    @staticmethod
    def test_031_call(node):
        """
        Node::call()
        """
        ret, out, err = node.call(["pwd"])
        assert ret == 0

    @staticmethod
    def test_032_vcall(node):
        """
        Node::vcall()
        """
        ret, out, err = node.vcall(["pwd"])
        assert ret == 0

    @staticmethod
    def test_04_make_temp_config(node):
        fpath = node.make_temp_config()
        assert fpath
        os.unlink(fpath)


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestNodeAllowsEnv:
    @staticmethod
    @pytest.mark.parametrize('node_env, allow_envs, env, expected', [
        ('PRD', None, 'PRD', True),
        ('PRD', None, 'PPRD', False),
        ('PRD', None, 'TST', False),
        ('DRP', None, 'PRD', True),
        ('DEV', None, 'TST', True),
        ('PRD', 'PRD PPRD', 'PPRD', True),
        ('PRD', 'PRD PPRD', 'PRD', True),
        ('PRD', 'PRD PPRD', 'TST', False),
        ('DEV', 'DEV', 'TST', False),
    ])
    def test_a_node_allows_the_envs_it_names_or_else_its_env_decides(node_env, allow_envs, env, expected):
        """
        A node naming no env allows every env but on a PRD node, which allows
        the PRD services only. A node naming envs allows those alone.
        """
        keywords = ['node.env=%s' % node_env]
        if allow_envs:
            keywords.append('node.allow_envs=%s' % allow_envs)
        Node().set_multi(keywords, validation=False)
        assert Node().allows_env(env) is expected
