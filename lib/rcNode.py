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

def node_get_hostmode():
    import node
    n = node.Node()
    if n.config.has_section('node'):
        return n.config.get('node', 'host_mode')
    return n.config.get('DEFAULT', 'host_mode')

def discover_node():
	"""Fill rcEnv class with information from node discovery
	"""
	rcEnv.host_mode = node_get_hostmode()

