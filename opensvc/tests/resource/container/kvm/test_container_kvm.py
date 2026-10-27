import pytest

from env import Env
from core.node import Node
from core.objects.svc import Svc
from drivers.resource.container.kvm import ContainerKvm
from drivers.resource.sync.rsync import SyncRsync


@pytest.fixture(scope='function')
def get_svc(mocker, osvc_path_tests):
    def func(container_cd):
        mocker.patch.object(Env, "nodename", "node1")
        cd = {
            "DEFAULT": {"nodes": "node1 node2 node3"},
            "container#1": dict(type="kvm", **container_cd),
        }
        return Svc(name='plop', volatile=True, node=Node(), cd=cd)

    return func


SCOPED_NAME = {"name@node1": "vm1", "name@node2": "vm2", "name@node3": "vm1"}


@pytest.mark.ci
class TestIsSyncPeer:
    @staticmethod
    def test_nodes_running_a_container_of_the_same_name_are_peers(get_svc):
        container = get_svc(SCOPED_NAME).get_resource("container#1")
        assert container.name == "vm1"
        assert container.is_sync_peer("node3") is True

    @staticmethod
    def test_nodes_running_a_container_of_another_name_are_not_peers(get_svc):
        container = get_svc(SCOPED_NAME).get_resource("container#1")
        assert container.is_sync_peer("node2") is False

    @staticmethod
    def test_all_nodes_are_peers_when_the_name_is_not_scoped(get_svc):
        container = get_svc({"name": "vm1"}).get_resource("container#1")
        for nodename in ("node2", "node3"):
            assert container.is_sync_peer(nodename) is True

    @staticmethod
    def test_all_nodes_are_peers_when_the_name_is_not_set(get_svc):
        container = get_svc({}).get_resource("container#1")
        assert container.name == "plop"
        for nodename in ("node2", "node3"):
            assert container.is_sync_peer(nodename) is True


@pytest.mark.ci
class TestSyncI0:
    @staticmethod
    def test_container_files_are_sent_only_to_peer_nodes(mocker, get_svc):
        svc = get_svc(SCOPED_NAME)
        sync = svc.get_resource("sync#i0")
        mocker.patch.object(ContainerKvm, "files_to_sync",
                            return_value=["/etc/libvirt/qemu/vm1.xml"])
        mocker.patch.object(SyncRsync, "nodes_to_sync",
                            return_value=["node2", "node3"])
        for method in ("resources_presync", "sync_timestamp", "update_stats",
                       "remote_postsync", "write_stats", "parse_rsync"):
            mocker.patch.object(SyncRsync, method)
        mocker.patch.object(svc.node, "get_ruser", return_value="root")
        sync.src = ["/etc/opensvc/plop.conf"]
        call = mocker.patch.object(SyncRsync, "call", return_value=(0, "", ""))

        sync.sync("nodes")

        cmds = {c[0][0][-1]: c[0][0] for c in call.call_args_list}
        assert sorted(cmds) == ["root@node2:/", "root@node3:/"]
        assert "/etc/opensvc/plop.conf" in cmds["root@node2:/"]
        assert "/etc/libvirt/qemu/vm1.xml" not in cmds["root@node2:/"]
        assert "/etc/opensvc/plop.conf" in cmds["root@node3:/"]
        assert "/etc/libvirt/qemu/vm1.xml" in cmds["root@node3:/"]
