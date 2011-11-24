#
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@opensvc.com>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@opensvc.com>'
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
from rcGlobalEnv import rcEnv

class check(object):
    undef = [{
              'check_svcname': '',
              'chk_instance': 'undef',
              'chk_value': '-1'
             }]
    def __init__(self, svcs=[]):
        self.svcs = svcs

    def do_check(self):
        return []

class checks(check):
    check_list = []

    def __init__(self, svcs=[]):
        self.svcs = svcs
        self.register('checkFsUsage')
        self.register('checkFsInode')
        self.register('checkVgUsage')
        self.register('checkMpath')
        self.register('checkZfsUsage')
        self.register('checkRaidSmartArray')
        self.register_local_checkers()

    def __iadd__(self, c):
        if isinstance(c, check):
            self.check_list.append(c)
        elif isinstance(c, checks):
            self.check_list += c.check_list

    def register_local_checkers(self):
        import os
        import glob
        check_d = os.path.join(rcEnv.pathvar, 'check')
        if not os.path.exists(check_d):
            return
        import sys
        sys.path.append(check_d)
        for f in glob.glob(os.path.join(check_d, 'check*.py')):
            if rcEnv.sysname not in f:
                continue
            cname = os.path.basename(f).replace('.py', '')
            try:
                m = __import__(cname)
                self += m.check(svcs=self.svcs)
            except:
                print >>sys.stderr, 'Could not import check:', cname

    def register(self, chk_name):
        try:
            m = __import__(chk_name+rcEnv.sysname)
        except:
            print '%s not implemented on %s'%(chk_name,rcEnv.sysname)
            return
        self += m.check(svcs=self.svcs)

    def do_checks(self):
        import datetime

        now = str(datetime.datetime.now())
        vars = [\
            "chk_nodename",
            "chk_svcname",
            "chk_type",
            "chk_instance",
            "chk_value",
            "chk_updated"]
        vals = []

        for chk in self.check_list:
            d = chk.do_check()
            if len(d) == 0:
                continue
            for i in d:
                if not isinstance(i, dict):
                    continue
                if 'chk_instance' not in i:
                    continue
                if i['chk_instance'] == 'undef':
                    continue
                if 'chk_value' not in i:
                    continue
                if 'chk_svcname' in i:
                    chk_svcname = i['chk_svcname']
                else:
                    chk_svcname = ""
                vals.append([\
                    rcEnv.nodename,
                    chk_svcname,
                    chk.chk_type,
                    i['chk_instance'],
                    i['chk_value'].replace("%",""),
                    now]
                )
        self.node.collector.call('push_checks', vars, vals)
