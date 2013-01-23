#
# Copyright (c) 2012 Christophe Varoqui <christophe.varoqui@opensvc.com>
# Copyright (c) 2012 Lucien Hercaud <lucien@hercaud.com>
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
import checkRaidSmartArray
import os

sep = ';'
path_list = os.environ['PATH'].split(sep)

if 'PROGRAMFILES(X86)' in os.environ:
    path_list.append(os.path.join(os.environ.get('PROGRAMFILES'),
                                  'compaq', 'hpacucli', 'bin'))
if 'PROGRAMFILES' in os.environ:
    path_list.append(os.path.join(os.environ.get('PROGRAMFILES(X86)'),
                                  'compaq', 'hpacucli', 'bin'))

os.environ['PATH'] = sep.join(path_list)

class check(checkRaidSmartArray.check):
    pass

