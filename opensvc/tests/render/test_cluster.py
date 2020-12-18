import pytest

from env import Env
from utilities.render.cluster import format_cluster


@pytest.mark.ci
class TestFormatCluster(object):
    @staticmethod
    def test_show_monitor_state_undef_when_monitor_has_not_yet_state(mocker):
        mocker.patch.object(Env, "nodename", "node1")
        output = format_cluster(node=[Env.nodename],
                                data={"monitor": {"nodes": {}, "services": {}}})
        assert output == """Threads              node1
 daemon    running |      
 monitor   undef  

Nodes                node1
 score             |      
  load 15m         |      
  mem              | -    
  swap             | -    
 compat    warn    |      
 state             |      

*/svc/*              node1
"""
