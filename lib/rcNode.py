import os
import sys
import re
from rcGlobalEnv import rcEnv
import logging
import rcExceptions as ex

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

def node_get_node_env():
    config = ConfigParser.RawConfigParser()
    config.read(rcEnv.nodeconf)
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

