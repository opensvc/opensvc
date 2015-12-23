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
from __future__ import print_function
from rcGlobalEnv import rcEnv
import os

class check(object):
    undef = [{
              'check_svcname': '',
              'chk_instance': 'undef',
              'chk_value': '-1'
             }]
    def __init__(self, svcs=[]):
        self.svcs = svcs
        if self.svcs is None:
            self.svcs = []

    def do_check(self):
        return []

class checks(check):
    check_list = []

    def __init__(self, svcs=[]):
        self.svcs = svcs
        self.register('checkFsUsage')
        self.register('checkFsInode')
        self.register('checkVgUsage')
        self.register('checkEth')
        self.register('checkLag')
        self.register('checkMpath')
        self.register('checkMpathPowerpath')
        self.register('checkZfsUsage')
        self.register('checkRaidSmartArray')
        self.register('checkRaidMegaRaid')
        self.register('checkRaidSas2')
        self.register('checkFmFmadm')
        self.register('checkFmOpenManage')
        self.register('checkMce')
        self.register('checkZpool')
        self.register('checkBtrfsDevStats')
        self.register('checkAdvfsUsage')
        self.register('checkNuma')
        self.register_local_checkers()

    def __iadd__(self, c):
        if isinstance(c, check):
            self.check_list.append(c)
        elif isinstance(c, checks):
            self.check_list += c.check_list
        return self

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
            except Exception as e:
                print('Could not import check:', cname, file=sys.stderr)
                print(e, file=sys.stderr)

    def register(self, chk_name):
        if not os.path.exists(os.path.join(rcEnv.pathlib, chk_name+rcEnv.sysname+'.py')):
            return
        m = __import__(chk_name+rcEnv.sysname)
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
            # print header
            s = chk.chk_type
            if hasattr(chk, "chk_name"):
                s += ' (' + chk.chk_name + ')'
            print(s)

            d = chk.do_check()
            if type(d) != list or len(d) == 0:
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

                # print instance
                s = "  " + i['chk_instance']
                if len(chk_svcname) > 0:
                    s += '@' + chk_svcname
                s += ': ' + i['chk_value']
                print(s)

                vals.append([\
                    rcEnv.nodename,
                    chk_svcname,
                    chk.chk_type,
                    i['chk_instance'],
                    i['chk_value'].replace("%",""),
                    now]
                )
        self.node.collector.call('push_checks', vars, vals)
