import pytest

import commands.svc
from core.objects.ccfg import Ccfg
from env import Env


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestClusterCommand:
    @staticmethod
    def test_print_config(mock_argv):
        mock_argv(["om", "-s", "cluster", "print", "config"])
        assert commands.svc.Mgr()() == 0

    @staticmethod
    def test_a_default_cluster_config_settings_are_created(mock_argv):
        # ensure cluster config is created
        mock_argv(["om", "-s", "cluster", "print", "config"])
        assert commands.svc.Mgr()() == 0
        cluster = Ccfg()
        assert cluster.cluster_name == "default"
        assert cluster.nodes == {Env.nodename}
        assert len(cluster.cd["cluster"]["secret"]) > 0

    @staticmethod
    def test_can_add_unicast_hb(mock_argv):
        mock_argv(["om", "-s", "cluster", "set", "--kw", "hb#1.type=unicast"])
        assert commands.svc.Mgr()() == 0
        cluster = Ccfg()
        assert "hb#1" in cluster.conf_sections()
        assert cluster.cd["hb#1"]["type"] == "unicast"

    @staticmethod
    def test_can_delete_an_hb_section(mock_argv):
        mock_argv(["om", "-s", "cluster", "set", "--kw", "hb#1.type=unicast"])
        assert commands.svc.Mgr()() == 0
        mock_argv(["om", "-s", "cluster", "delete", "--rid", "hb#1"])
        assert commands.svc.Mgr()() == 0
        cluster = Ccfg()
        assert "hb#1" not in cluster.conf_sections()
