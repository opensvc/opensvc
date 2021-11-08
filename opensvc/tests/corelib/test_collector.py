import pytest

import env
from core.node import Node
from core.objects.svc import Svc
from tests.helpers import ANY


@pytest.mark.ci
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('has_node_dbopensvc_config')
@pytest.mark.usefixtures('has_service_with_fs_flag')
class TestCollector(object):
    @staticmethod
    def test_svc_push_config_call_correct_proxy_command(mocker, mock_sysname):
        mock_sysname('Linux')
        node = Node()
        proxy = mocker.Mock()
        update_service = proxy.update_service
        node.collector.proxy = proxy
        node.collector.proxy_methods = ["plops"]
        svc = Svc(name='svc', node=node, volatile=True)

        svc.push_config()

        update_service.assert_called_once_with(
            [
                'svc_name', 'cluster_id', 'svc_topology', 'svc_flex_min_nodes',
                'svc_flex_max_nodes', 'svc_flex_target',
                'svc_flex_cpu_low_threshold', 'svc_flex_cpu_high_threshold',
                'svc_env', 'svc_nodes', 'svc_drpnode', 'svc_drpnodes',
                'svc_comment', 'svc_app', 'svc_config', 'svc_ha'
            ],
            [
                'svc', ANY, 'failover', 1, 1, 1, 0, 100, 'TST', ANY,
                '', '', '', 'default',
                '\n[DEFAULT]\nid = abcd\n\n[fs#flag1]\ntype = flag\n',
                ANY
            ],
            ('"abcd"', env.Env.nodename)
        )

        assert "svc" in update_service.call_args_list[0][0][1][0], \
            "1st arg of update_service should contain svc name"

        assert "id = abcd" in update_service.call_args_list[0][0][1][14], \
            "14th arg of update_service should contain svc config file"
