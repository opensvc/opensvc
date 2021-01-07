import os
import uuid

import pytest

from commands.svc import Mgr
from env import Env

NODENAME = Env.nodename
ID = str(uuid.uuid4())
SVCNAME = "svc-ref"

REFS = [  # (name, value, expected_evaluated_value),
    ("ref0", "a b 33", "a b 33"),
    ("string_index0", "{env.ref0[0]}", "a"),
    ("string_index1", "{env.ref0[1]}", "b"),
    ("string_index2", "{env.ref0[2]}", "33"),
    ("string_number_of_words", "{#env.ref0}", "3"),
    ("ref01", "{ref02}", "ref-order-safe"),
    ("ref02", "ref-order-safe", "ref-order-safe"),
    ("ref1", "{nodes}", NODENAME),
    ("ref2", "{ref1}", NODENAME),
    ("ref_add", "$(1+2)", "3"),
    ("ref4", "1", "1"),
    ("add_ref", "$({env.ref4}+2)", "3"),
    ("add_ref_len", "$({#env.ref0}+2)", "5"),
    ("nb", "3", "3"),
    ("ref8", "host{1...{#nodes}}/disk{1...{nb}}", "host{1...1}/disk{1...3}"),
    ("ref9", "{1...8}", "{1...8}"),
    ("accept_unref", "{abcd}", "{abcd}"),
    ("number_of_nodes", "{#nodes}", "1"),
    ("ref_kind", "{kind}", "svc"),
    ("ref_namespace", "{namespace}", "root"),
    ("ref_short_nodename", "{short_nodename}", NODENAME.split('.')[0]),
    ("ref_path", "{path}", SVCNAME),
    ("ref_name", "{name}", SVCNAME),
    ("ref_fqdn", "{fqdn}", "%s.root.svc.default" % SVCNAME),
    ("ref_id", "{id}", ID),
    ("ref_clustername", "{clustername}", "default"),
    ("ref_clustername", "{clustername}", "default"),
    ("ref_dns", "{dns}", ""),  # no setup yet
    ("ref_dnsnodes", "{dnsnodes}", ""),  # no setup yet
    ("ref_dnsuxsock", "{dnsuxsock}", str(Env.paths.dnsuxsock)),
    ("ref_dnsuxsockd", "{dnsuxsockd}", str(Env.paths.dnsuxsockd)),
    ("ref_collector_api", "{collector_api}", ""),  # no setup yet
    ("ref_nodemgr", "{nodemgr}", str(Env.paths.nodemgr)),
    ("ref_svcmgr", "{svcmgr}", str(Env.paths.svcmgr)),
    ("ref_etc", "{etc}", "OSVC_PATH_TESTS/etc"),
    ("ref_var", "{var}", "OSVC_PATH_TESTS/var"),
    ("ref_private_var", "{private_var}", "OSVC_PATH_TESTS/var/svc/%s" % SVCNAME),
    ("ref_initd", "{initd}", "OSVC_PATH_TESTS/etc/%s.d" % SVCNAME),
]


@pytest.fixture(scope='function')
def has_svc_with_ref(has_cluster_config):
    with open(os.path.join(Env.paths.pathetc, "%s.conf" % SVCNAME), "w") as svc_conf:
        config_lines = [
            '[DEFAULT]',
            'id = %s' % ID,
            "nodes = %s" % NODENAME,
            '[env]'
        ] + ["%s = %s" % (name, value) for name, value, _ in REFS]
        svc_conf.write("\n".join(config_lines))


@pytest.mark.ci
@pytest.mark.usefixtures("has_svc_with_ref")
@pytest.mark.usefixtures("has_euid_0")
class TestReferencesConfig(object):
    @staticmethod
    def test_validate_config():
        assert Mgr()(argv=["-s", SVCNAME, "validate", "config"]) == 0


@pytest.mark.ci
@pytest.mark.usefixtures("has_svc_with_ref")
@pytest.mark.usefixtures("has_euid_0")
class TestReferencesGet(object):
    @staticmethod
    @pytest.mark.parametrize("name, value", [[name, value] for name, value, _ in REFS])
    def test_can_get_native_value(capsys, name, value):
        assert Mgr()(argv=["-s", SVCNAME, "get", "--kw", "env.%s" % name]) == 0
        assert capsys.readouterr().out.strip() == value

    @staticmethod
    @pytest.mark.parametrize("name, expected_eval", [[name, expected_eval] for name, _, expected_eval in REFS])
    def test_can_get_evaluated_value(capsys, osvc_path_tests, name, expected_eval):
        assert Mgr()(argv=["-s", SVCNAME, "get", "--eval", "--kw", "env.%s" % name]) == 0
        if "OSVC_PATH_TESTS" in expected_eval:
            expected_eval = expected_eval.replace("OSVC_PATH_TESTS",
                                                  str(osvc_path_tests))
        assert capsys.readouterr().out.strip() == expected_eval
