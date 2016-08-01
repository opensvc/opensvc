import os
import sys
import re
from rcGlobalEnv import *
import logging
import rcExceptions as ex

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

def node_get_hostmode():
    nodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', 'node.conf')
    config = ConfigParser.RawConfigParser()
    config.read(nodeconf)
    if config.has_section('node'):
        if config.has_option('node', 'host_mode'):
            return config.get('node', 'host_mode')
    elif config.has_option('DEFAULT', 'host_mode'):
        return config.get('DEFAULT', 'host_mode')
    return 'TST'

def discover_node():
    """Fill rcEnv class with information from node discovery
    """
    if not hasattr(rcEnv, "host_mode"):
        rcEnv.host_mode = node_get_hostmode()

