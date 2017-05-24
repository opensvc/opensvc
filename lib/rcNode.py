import os
import sys
from rcGlobalEnv import rcEnv

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

def node_get_node_env():
    import codecs
    config = ConfigParser.RawConfigParser()
    if not os.path.exists(rcEnv.paths.nodeconf):
        return 'TST'
    with codecs.open(rcEnv.paths.nodeconf, "r", "utf8") as f:
        if sys.version_info[0] >= 3:
            config.read_file(f)
        else:
            config.readfp(f)
    if config.has_section('node'):
        if config.has_option('node', 'env'):
            return config.get('node', 'env')
        if config.has_option('node', 'host_mode'):
            # deprecated
            return config.get('node', 'host_mode')
    elif config.has_option('DEFAULT', 'env'):
        return config.get('DEFAULT', 'env')
    return 'TST'

def discover_node():
    """Fill rcEnv class with information from node discovery
    """
    if not hasattr(rcEnv, "env"):
        rcEnv.node_env = node_get_node_env()

