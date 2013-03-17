#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
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
    rcEnv.host_mode = node_get_hostmode()

