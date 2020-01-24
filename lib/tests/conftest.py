import sys
import os
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__),
                                                 "..")))
import rcGlobalEnv
import pytest


@pytest.fixture(scope='function')
def osvc_path_tests(tmpdir):
    test_dir = str(tmpdir)
    rcGlobalEnv.rcEnv.paths.pathetc = os.path.join(test_dir, 'etc')
    rcGlobalEnv.rcEnv.paths.pathetcns = os.path.join(test_dir, 'etc', 'namespaces')
    rcGlobalEnv.rcEnv.paths.pathlog = os.path.join(test_dir, 'log')
    rcGlobalEnv.rcEnv.paths.pathtmpv = os.path.join(test_dir, 'tmp')
    rcGlobalEnv.rcEnv.paths.pathvar = os.path.join(test_dir, 'var')
    rcGlobalEnv.rcEnv.paths.pathlock = os.path.join(test_dir, 'lock')


@pytest.fixture(scope='function')
def non_existing_file(tmp_path):
    return os.path.join(str(tmp_path), 'foo')
