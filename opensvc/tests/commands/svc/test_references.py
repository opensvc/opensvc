import json
import os
import uuid

import pytest

from commands.svc import Mgr
from core.extconfig import MAX_RECURSION
from env import Env

NODENAME = Env.nodename
ID = str(uuid.uuid4())
SVCNAME = "svc-ref"


class ErrorValueContains(object):
    def __init__(self, message):
        self.message = message


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
    ("ref_add", "$(1+2)", 3),
    ("ref4", "1", "1"),
    ("add_ref", "$({env.ref4}+2)", 3),
    ("add_ref_len", "$({#env.ref0}+2)", 5),
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
    ("no_change_mixed1", "{aBcDe}", "{aBcDe}"),
    ("no_change_mixed2", "{FOO1..2}", "{FOO1..2}"),
    ("no_change_mixed3", "{{{FOO1..2}}}", "{{{FOO1..2}}}"),
    ("repeat_item", "repeat_value", "repeat_value"),
    ("repeat_100_ref", " ".join(["{repeat_item}"]*100), " ".join(["repeat_value"]*100)),

    # modifiers
    ("mod_upper", "{upper:clustername}", "DEFAULT"),
    ("mod_capitalize", "{capitalize:clustername}", "Default"),
    ("mod_swapcase", "{swapcase:clustername}", "DEFAULT"),
    ("mod_title", "{title:clustername}", "Default"),

    # defers
    ("exposed_must_be_null", "{disk#slv1.exposed_devs[0]}", None),  # not "None"
    ("exposed_must_be_empty", "must be null: {disk#slv1.exposed_devs[0]}", None),

    # recursion
    ("foo", "{foo}", ErrorValueContains("recursion")),
    ("afoo", "a {afoo}", ErrorValueContains("recursion")),
    ("foobar", "{barfoo}", ErrorValueContains("recursion")),
    ("barfoo", "{foobar}", ErrorValueContains("recursion")),
    ("repeat_unref_exceed", " ".join(["{qq}"] * int(1 + MAX_RECURSION / 4)), ErrorValueContains("recursion exceeds")),
    ("repeat_unref_exceed_x_4", " ".join(["{qq}"] * MAX_RECURSION), ErrorValueContains("recursion exceeds")),
    ("repeat_unref_exceed_a_b", " ".join(["{a}{b}"] * (1 + int(MAX_RECURSION / 8))),
     ErrorValueContains("recursion exceeds")),

    ("repeat_unref_2", "{qq} {qq}", "{qq} {qq}"),
    ("repeat_unref_3", "{qq} {qq} {qq}", "{qq} {qq} {qq}"),

    # max repeated unresolved names is MAX_RECURSION / 4
    ("repeat_unref_max", " ".join(["{qq}"] * (int(MAX_RECURSION / 4))), " ".join(["{qq}"] * (int(MAX_RECURSION / 4)))),
    ("repeat_unref_max2", " ".join(["{a}{b}"] * int(MAX_RECURSION / 8)), " ".join(["{a}{b}"] * int(MAX_RECURSION / 8))),

    ("repeat_ref_2", "{clustername} {clustername}", "default default"),
    ("repeat_ref_100", " ".join(["{clustername}"] * MAX_RECURSION * 2), " ".join(["default"] * MAX_RECURSION * 2)),
    ("baz", "abc", "abc"),
    ("bar", "{baz}ref", "abcref"),
    ("foo_bar_ref", "a={bar} b={bar} c={bar}",  "a=abcref b=abcref c=abcref"),
    ("foo_bar_ref_max", " ".join(["{bar}"] * int(MAX_RECURSION * 40)), " ".join(["abcref"] * int(MAX_RECURSION * 40))),
]

ref_names = [key[0] for key in REFS]
dup_ref_names = set([k for k in ref_names if ref_names.count(k) > 1])
assert len(dup_ref_names) == 0, "duplicated name detected in REFS: %s" % ", ".join(dup_ref_names)


@pytest.fixture(scope='function')
def has_svc_with_ref(has_cluster_config):
    with open(os.path.join(Env.paths.pathetc, "%s.conf" % SVCNAME), "w") as svc_conf:
        config_lines = [
            '[DEFAULT]',
            'id = %s' % ID,
            "nodes = %s" % NODENAME,
            "[disk#slv1]",
            "type = drbd",
            "res = {fqdn}1",
            "disk = /tmp/{fqdn}1",
            "standby = true",
            '[env]'
        ] + ["%s = %s" % (name, value) for name, value, _ in REFS]
        svc_conf.write("\n".join(config_lines))


@pytest.fixture(scope='function')
def has_svc_with_valid_ref(has_cluster_config):
    with open(os.path.join(Env.paths.pathetc, "%s.conf" % SVCNAME), "w") as svc_conf:
        config_lines = [
                           '[DEFAULT]',
                           'id = %s' % ID,
                           "nodes = %s" % NODENAME,
                           "[disk#slv1]",
                           "type = drbd",
                           "res = {fqdn}1",
                           "disk = /tmp/{fqdn}1",
                           "standby = true",
                           '[env]'
                       ] + ["%s = %s" % (name, value) for name, value, expected in REFS
                            if isinstance(expected, str)]
        svc_conf.write("\n".join(config_lines))


@pytest.mark.ci
@pytest.mark.usefixtures("has_euid_0")
class TestReferencesConfigValidate(object):
    @staticmethod
    @pytest.mark.usefixtures("has_svc_with_valid_ref")
    def test_validate_config_result_is_0_when_confif_has_no_errors():
        assert Mgr()(argv=["-s", SVCNAME, "validate", "config"]) == 0

    @staticmethod
    @pytest.mark.usefixtures("has_svc_with_ref")
    def test_validate_config_result_is_not_0_when_corrupt():
        assert Mgr()(argv=["-s", SVCNAME, "validate", "config"]) > 0

    @staticmethod
    @pytest.mark.usefixtures("has_svc_with_valid_ref")
    @pytest.mark.parametrize("lines", (
            ["priority = "],
            ["priority = ", "comment = blah"],
            ["priority = ioio"],
            ["priority = {env.prio}", "[env]", "prio ="],
    ))
    def test_validate_config_detect_invalid_priority_config(lines):
        pytest.skip("Disable test on (not yet a spec) invalid priority invalid values")
        config_lines = ['[DEFAULT]', 'id = %s' % ID]
        config_lines += lines
        svc_conf_file = os.path.join(Env.paths.pathetc, "%s.conf" % SVCNAME)
        with open(svc_conf_file, "w") as f:
            f.write("\n".join(config_lines))
        print("---- print config")
        assert Mgr()(argv=["-s", SVCNAME, "print", "config"]) == 0
        print("---- validate config")
        assert Mgr()(argv=["-s", SVCNAME, "validate", "config"]) != 0
        print("---- print config --eval")
        assert Mgr()(argv=["-s", SVCNAME, "print", "config", "--eval"]) == 0

    @staticmethod
    @pytest.mark.usefixtures("has_svc_with_valid_ref")
    @pytest.mark.parametrize("lines", (
            ["comment = blah"],
            ["priority = 12", "comment = blah"],
            ["priority = 14"],
            ["priority = {env.prio}", "[env]", "prio = 12"],
            ["priority = {env.prio}", "[env]", "prio = 12", "[fs#1]", "type = flag"],
            ["[env]", "a = foo", "multiple = %s" % " ".join(["{a}"]*40)],
    ))
    def test_validate_config_detect_valid_priority_config(lines):
        config_lines = ['[DEFAULT]', 'id = %s' % ID]
        config_lines += lines
        svc_conf_file = os.path.join(Env.paths.pathetc, "%s.conf" % SVCNAME)
        with open(svc_conf_file, "w") as f:
            f.write("\n".join(config_lines))
        print("---- print config")
        assert Mgr()(argv=["-s", SVCNAME, "print", "config"]) == 0
        print("---- validate config")
        assert Mgr()(argv=["-s", SVCNAME, "validate", "config"]) == 0
        print("---- print config --eval")
        assert Mgr()(argv=["-s", SVCNAME, "print", "config", "--eval"]) == 0


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
        if isinstance(expected_eval, str) and "OSVC_PATH_TESTS" in expected_eval:
            expected_eval = expected_eval.replace("OSVC_PATH_TESTS",
                                                  str(osvc_path_tests))
        if isinstance(expected_eval, ErrorValueContains):
            assert Mgr()(argv=["-s", SVCNAME, "get", "--eval", "--kw", "env.%s" % name]) == 1
            assert expected_eval.message in capsys.readouterr().err
        else:
            assert Mgr()(argv=["-s", SVCNAME, "get", "--eval", "--kw", "env.%s" % name]) == 0
            assert capsys.readouterr().out.strip() == str(expected_eval)


@pytest.mark.ci
@pytest.mark.usefixtures("has_euid_0")
class TestReferencesPrintConfig(object):
    @staticmethod
    @pytest.mark.parametrize("name, value", [[name, value] for name, value, _ in REFS])
    def test_has_native_value(capsys, has_svc_with_ref, name, value):
        assert Mgr()(argv=["-s", SVCNAME, "print", "config", "--format", "json"]) == 0
        assert json.loads(capsys.readouterr().out)["env"][name] == value

    @staticmethod
    @pytest.mark.parametrize("name, expected_eval",
                             [[name, expected_eval] for name, _, expected_eval in REFS
                              if isinstance(expected_eval, str)
                              ])
    def test_has_expected_value_when_eval_is_asked(
            capsys,
            osvc_path_tests,
            has_svc_with_valid_ref,
            name,
            expected_eval):
        assert Mgr()(argv=["-s", SVCNAME, "print", "config", "--eval", "--format", "json"]) == 0
        if isinstance(expected_eval, str) and "OSVC_PATH_TESTS" in expected_eval:
            expected_eval = expected_eval.replace("OSVC_PATH_TESTS",
                                                  str(osvc_path_tests))
        json_output = json.loads(capsys.readouterr().out)
        assert json.dumps(json_output["env"][name]) == json.dumps(expected_eval)
