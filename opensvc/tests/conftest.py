import os
import sys
import uuid
from contextlib import contextmanager

import pytest  # nopep8

import env  # nopep8


# avoid computation of version from git describe
open(os.path.join(os.path.dirname(__file__), "..", "..", "opensvc", "utilities", "version", "version.py"), "w").write("version = 'dev'")

@pytest.fixture(scope='function')
def which(mocker):
    mocker.patch('utilities.proc.which')


@pytest.fixture(scope='function')
def has_euid_0(mocker):
    mocker.patch('os.geteuid', autospec=True, return_value=0)


@pytest.fixture(scope='function', name='osvc_path_tests')
def osvc_path_tests_fixture(tmpdir):
    test_dir = str(tmpdir)
    env.Env.paths.pathetc = os.path.join(test_dir, 'etc')
    env.Env.paths.pathetcns = os.path.join(test_dir, 'etc', 'namespaces')
    env.Env.paths.pathlog = os.path.join(test_dir, 'log')
    env.Env.paths.pathtmpv = os.path.join(test_dir, 'tmp')
    env.Env.paths.pathvar = os.path.join(test_dir, 'var')
    env.Env.paths.pathlock = os.path.join(test_dir, 'lock')
    env.Env.paths.nodeconf = os.path.join(test_dir, 'etc', 'node.conf')
    env.Env.paths.clusterconf = os.path.join(test_dir, 'etc', 'cluster.conf')
    env.Env.paths.lsnruxsock = os.path.join(test_dir, 'var', 'lsnr', 'lsnr.sock')
    env.Env.paths.lsnruxh2sock = os.path.join(test_dir, 'var', 'lsnr', 'h2.sock')
    env.Env.paths.daemon_pid = os.path.join(test_dir, 'var', "osvcd.pid")
    env.Env.paths.daemon_pid_args = os.path.join(test_dir, 'var', "osvcd.pid.args")
    env.Env.paths.nodes_info = os.path.join(test_dir, 'var', "nodes_info.json")
    os.makedirs(os.path.join(env.Env.paths.pathvar, 'lsnr'))
    os.makedirs(os.path.join(env.Env.paths.pathvar, 'node'))
    os.makedirs(env.Env.paths.pathtmpv)
    os.makedirs(env.Env.paths.pathlog)
    return tmpdir


@pytest.fixture(scope='function')
def mock_argv(mocker):
    def func(argv):
        mocker.patch.object(sys, 'argv', argv)

    return func


@pytest.fixture(scope='function')
def non_existing_file(tmp_path):
    assert os.path.exists(str(tmp_path))
    return os.path.join(str(tmp_path), 'foo')


@pytest.fixture(scope='function')
def file1(tmp_path):
    f = str(os.path.join(str(tmp_path), 'file1'))
    open(f, 'w').write('data')
    return f


@pytest.fixture(scope='function')
def file2(tmp_path):
    f = str(os.path.join(str(tmp_path), 'file2'))
    open(f, 'w').write('data')
    return f


@pytest.fixture(scope='function')
def tmp_dir(tmp_path):
    "return str for python < 3.6"
    return str(tmp_path)


@pytest.fixture(scope='function')
def tmp_file(tmp_path):
    return os.path.join(str(tmp_path), 'tmp-file')


@pytest.fixture(scope='function')
def capture_stdout():
    @contextmanager
    def func(filename):
        _stdout = sys.stdout
        try:
            with open(filename, 'w') as output_file:
                sys.stdout = output_file
                yield
        finally:
            sys.stdout = _stdout
    return func


@pytest.fixture(scope='function', name='mock_sysname')
def mock_sysname_fixture(mocker):
    def func(sysname):
        mocker.patch.object(env.Env, 'sysname', sysname)

    return func


@pytest.fixture(scope='function')
def create_driver_resource(mock_sysname):
    def create(sysname, scenario):
        driver_name, kwargs, expected_type = scenario
        mock_sysname(sysname)
        from utilities.drivers import driver_import, driver_class
        args = ["resource"] + driver_name.split(".")
        driver = driver_import(*args)
        return driver_class(driver)(**kwargs)

    return create


@pytest.fixture(scope='function')
def has_cluster_config(osvc_path_tests):
    pathetc = env.Env.paths.pathetc
    if not os.path.exists(pathetc):
        os.mkdir(pathetc)
    config_lines = [
        '[DEFAULT]',
        'id = ' + str(uuid.uuid4()),
        '',
        '[cluster]',
        'secret = ' + str(uuid.uuid4()),
        'nodes = ' + env.Env.nodename + " node2",
        'id = ' + str(uuid.uuid4()),
    ]
    with open(env.Env.paths.clusterconf, mode='w+') as cfg_file:
        for line in config_lines:
            cfg_file.write(line + '\n')


@pytest.fixture(scope='function')
def has_node_config(osvc_path_tests):

    pathetc = env.Env.paths.pathetc
    if not os.path.exists(pathetc):
        os.mkdir(pathetc)
    with open(os.path.join(pathetc, 'node.conf'), mode='w+') as node_config_file:
        """This fixture set non default port and tls_port for listener.
        This avoid port conflict with live daemon.
        """
        config_txt = """
[listener]
port = 1224
tls_port = 1225

[cni]
config = %s
""" % str(os.path.join(str(osvc_path_tests), 'cni.config'))
        node_config_file.write(config_txt)


@pytest.fixture(scope='function')
def has_service_lvm(osvc_path_tests, mock_sysname):
    mock_sysname('Linux')
    pathetc = env.Env.paths.pathetc
    os.mkdir(pathetc)
    with open(os.path.join(pathetc, 'svc.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = abcd

[disk#simple]
type = lvm
vgname = vgname1

[disk#optional]
type = lvm
vgname = vgname1
optional = true

[disk#scsireserv]
type = lvm
vgname = vgname1
scsireserv = true

[disk#scsireserv-optional]
type = lvm
vgname = vgname1
scsireserv = true
optional = true
"""
        svc_file.write(config_txt)


@pytest.fixture(scope='function')
def has_service_with_fs_flag(osvc_path_tests):
    pathetc = env.Env.paths.pathetc
    os.mkdir(pathetc)
    with open(os.path.join(pathetc, 'svc.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = abcd

[fs#flag1]
type = flag
"""
        svc_file.write(config_txt)


@pytest.fixture(scope='function')
def has_service_with_vol_and_cfg(osvc_path_tests):
    """
    Add config with a cfg object and a service that uses this config on a volume
    """
    pathetc = env.Env.paths.pathetc
    os.mkdir(pathetc)
    with open(os.path.join(pathetc, 'svc.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = abcd
nodes = *

[volume#0]
type = directory
name = vol-test
pool = default
access = roo
size = 1m
configs =
    cfg/cfg1/simple:simple_dest
    cfg/cfg1//simpleb:simple_b
    cfg/cfg1/a/*:star-to-dir/
    cfg/cfg1/**/only-one:double-star-to-only-one
    cfg/cfg1/a:recursive-dir
    cfg/cfg1/a:/recursive-dir-with-os-sep
    cfg/cfg1/a:/recursive-dir-with-os-sep_2/
    cfg/cfg1//e:os-sep-d
    cfg/cfg1/*:all-cfg1/
    cfg/cfg1/camelCase/Foo/baR:baR
"""
        svc_file.write(config_txt)

    os.mkdir(os.path.join(pathetc, 'cfg'))
    with open(os.path.join(pathetc, 'cfg', 'cfg1.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = abcde

[data]
simple = literal:cfg content of key simple
/simpleb = literal:cfg content of key /simpleb
a/b/c = literal:cfg content of key a/b/c
a/e/f1 = literal:cfg content of key a/e/f1
a/e/f2 = literal:cfg content of key a/e/f2
a/g = literal:cfg content of key a/g
/e/f = literal:cfg content of key /e/f
i/j/k/only-one = literal:cfg content of key i/j/k/only-one
camelCase/Foo/baR = literal:cfg content of key camelCase/Foo/baR
"""
        svc_file.write(config_txt)

    return osvc_path_tests


@pytest.fixture(scope='function')
def has_service_with_cfg(osvc_path_tests):
    pathetc = env.Env.paths.pathetc
    os.mkdir(pathetc)
    os.mkdir(os.path.join(pathetc, 'cfg'))
    with open(os.path.join(pathetc, 'cfg', 'cfg1.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = abcde
"""
        svc_file.write(config_txt)

    return osvc_path_tests


@pytest.fixture(scope='function')
def klass_has_capability(mocker):
    def func(klass, capabilities):
        def has_capability(_, cap):
            return cap in capabilities

        mocker.patch.object(klass, 'has_capability', has_capability)

    return func


@pytest.fixture(autouse=True)
def check_io_timeout(mocker):
    """Improve test time"""
    # import utilities.proc
    # mocker.patch.object(utilities.proc, 'LCALL_CHECK_IO_TIMEOUT', new=0.001)
    mocker.patch('utilities.proc.LCALL_CHECK_IO_TIMEOUT', new=0.001)


@pytest.fixture(autouse=True)
def gc_collect(mocker):
    """Improve test time"""
    mocker.patch('gc.collect')
