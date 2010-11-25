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
"""Define some functions
"""

def update_cl_attr(cl=None, module=None, exclude_lstrip="__"):
    """Update class cl attributes with corresponding vars from module.
       vars from module matching exclude_lstrip ("__") are not updated
    """ 
    try:
        mod = __import__(module)
        for p in dir(mod):
            if p in dir(cl) and p.lstrip(exclude_lstrip) == p:
                setattr(cl, p, getattr(mod, p)) 
    except ImportError:
        pass
