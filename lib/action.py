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
# To change this template, choose Tools | Templates
# and open the template in the editor.

__author__="cgaliber"
__date__ ="$10 oct. 2009 09:38:20$"

class excError(Exception) :
    """ Failed action """

class excUndefined(Exception):
    """ Mandatory Undefined action exception """
    def __init__(self,action=None,className=None,func=None):
        self.action=action
        self.className=className
        self.func=func
    def __str__(self):
        return "Undefined mandatory Action %s for className %s in function %s" % \
                (self.action,self.className,self.func)


