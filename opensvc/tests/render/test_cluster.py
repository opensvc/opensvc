import pytest

from env import Env
from utilities.render.cluster import format_cluster


@pytest.mark.ci
class TestFormatCluster(object):
    @staticmethod
    def test_show_monitor_state_undef_when_monitor_has_not_yet_state(mocker):
        mocker.patch.object(Env, "nodename", "node1")
        mocker.patch("utilities.render.color.use_color", "no")
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

    @staticmethod
    def test_show_monitor_show_hash_when_service_is_a_drp_node(mocker):
        mocker.patch.object(Env, "nodename", "node1")
        mocker.patch("utilities.render.color.use_color", "no")
        output = format_cluster(selector="flg1",sections="services",
                                node=[Env.nodename],
                                data= {
                                    "monitor": {
                                        "nodes": {
                                            "node1": {
                                                "services":{
                                                    "config": {
                                                        "flg1": {
                                                            "csum": "4446e020698e58edcff4b026598f985a",
                                                            "scope": ["node-1",],
                                                            "updated": 1612259156.577508
                                                        },
                                                    },
                                                    "status": {
                                                        "flg1": {
                                                            "app": "default",
                                                            "avail": "down",
                                                            "csum": "b4439d83a1bd387bbf25e8c2b31a3f19",
                                                            "drp": True,
                                                            "env": "PRD",
                                                            "frozen": 0,
                                                            "kind": "svc",
                                                            "monitor": {
                                                                "global_expect": None,
                                                                "global_expect_updated": 1612259178.6121564,
                                                                "placement": "",
                                                                "status": "idle",
                                                                "status_updated": 1612259178.5763993
                                                            },
                                                            "optional": "n/a",
                                                            "orchestrate": "ha",
                                                            "overall": "down",
                                                            "placement": "nodes order",
                                                            "provisioned": True,
                                                            "resources": {
                                                                "fs#1": {
                                                                    "label": "fs.flag",
                                                                    "provisioned": {
                                                                        "mtime": 1612259177.338254,
                                                                        "state": True
                                                                    },
                                                                    "status": "down",
                                                                    "type": "fs.flag"
                                                                },
                                                            },
                                                            "status_group": {
                                                                "DEFAULT": "n/a",
                                                                "app": "n/a",
                                                                "certificate": "n/a",
                                                                "container": "n/a",
                                                                "disk": "n/a",
                                                                "expose": "n/a",
                                                                "fs": "down",
                                                                "hashpolicy": "n/a",
                                                                "ip": "n/a",
                                                                "route": "n/a",
                                                                "share": "n/a",
                                                                "subset": "n/a",
                                                                "sync": "n/a",
                                                                "task": "n/a",
                                                                "vhost": "n/a",
                                                                "volume": "n/a"
                                                            },
                                                            "subsets": {},
                                                            "topology": "failover",
                                                            "updated": 1612259178.0564723
                                                        },
                                                    }
                                                }
                                            }
                                        },
                                        "services": {
                                            "flg1": {
                                                "avail": "up",
                                                "frozen": "thawed",
                                                "overall": "up",
                                                "placement": "optimal",
                                                "provisioned": True
                                            },
                                        },
                                    }
                                })
        assert output == """*/svc/flg1                  node1
 flg1      up ha    0/1   | X#   
"""
