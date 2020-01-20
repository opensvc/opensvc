import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import rcGlobalEnv
import pytest


@pytest.fixture(scope='function')
def osvc_path_tests(tmpdir):
    rcGlobalEnv.rcEnv.paths.pathetc = os.path.join(tmpdir, 'etc')
    rcGlobalEnv.rcEnv.paths.pathetcns = os.path.join(tmpdir, 'etc', 'namespaces')
    rcGlobalEnv.rcEnv.paths.pathlog = os.path.join(tmpdir, 'log')
    rcGlobalEnv.rcEnv.paths.pathtmpv = os.path.join(tmpdir, 'tmp')
    rcGlobalEnv.rcEnv.paths.pathvar = os.path.join(tmpdir, 'var')
    rcGlobalEnv.rcEnv.paths.pathlock = os.path.join(tmpdir, 'lock')
