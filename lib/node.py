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

from svc import Svc
from freezer import Freezer
import svcBuilder
import xmlrpcClient
import os
import ConfigParser
import datetime

class Options(object):
    def __init__(self):
        self.force = False
        self.debug = False
        self.stats_file = None
        self.stats_interval = None
        self.collect_date = None
        os.environ['LANG'] = 'C'

class Node(Svc, Freezer):
    """ Defines a cluster node.  It contain list of Svc.
        Implements node-level actions and checks.
    """
    def __init__(self):
        nodeconf = os.path.join(os.path.dirname(__file__), '..', 'etc', 'node.conf')
        config_defaults = {
          'host_mode': 'TST',
          'push_interval': 1439,
          'sync_interval': 1439,
        }
        self.config = ConfigParser.RawConfigParser(config_defaults)
        self.config.read(nodeconf)
        self.options = Options()
        self.svcs = None
        Freezer.__init__(self, '')
        self.action_desc = {
          'syncservices':   'send var files, config files and configured replications to other nodes for each node service',
          'updateservices': 'refresh var files associated with services',
          'pushasset':      'push asset information to collector',
          'pushservices':   'push service configuration to collector',
          'pushstats':      'push performance metrics to collector',
          'pushpkg':        'push package/version list to collector',
          'pushpatch':      'push patch/version list to collector',
          'pushsym':        'push symmetrix configuration to collector',
          'prkey':          'show persistent reservation key of this node',
          'checks':         'run node sanity checks, push results to collector',
          'compliance_check': 'run compliance checks',
          'compliance_fix':   'run compliance fixes',
          'compliance_fixable': 'verify compliance fixes prerequisites',
          'compliance_show_moduleset': 'show compliance rules applying to this node',
          'compliance_list_moduleset': 'list available compliance modulesets. --moduleset f% limit the scope to modulesets matching the f% pattern.',
          'compliance_attach_moduleset': 'attach moduleset specified by --moduleset for this node',
          'compliance_detach_moduleset': 'detach moduleset specified by --moduleset for this node',
          'compliance_list_ruleset': 'list available compliance rulesets. --ruleset f% limit the scope to rulesets matching the f% pattern.',
          'compliance_show_ruleset': 'show compliance rules applying to this node',
          'compliance_attach_ruleset': 'attach ruleset specified by --ruleset for this node',
          'compliance_detach_ruleset': 'detach ruleset specified by --ruleset for this node',
        }

    def format_desc(self):
        from textwrap import TextWrapper
        wrapper = TextWrapper(subsequent_indent="%29s"%"", width=78)
        desc = "Supported commands:\n"
        for a in sorted(self.action_desc):
            if not hasattr(self, a):
                continue
            text = "  %-26s %s\n"%(a.replace('_', ' '),
                                   self.action_desc[a])
            desc += wrapper.fill(text)
            desc += '\n'
        return desc

    def __iadd__(self, s):
        if not isinstance(s, Svc):
                pass
        self.svcs.append(s)
        return self

    def action(self, a):
        return getattr(self, a)()

    def check_timestamp(self, timestamp_f, comp='more', delay=10):
        if not os.path.exists(timestamp_f):
            return True
        try:
            with open(timestamp_f, 'r') as f:
                d = f.read()
                last = datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n")
                limit = last + datetime.timedelta(minutes=delay)
                if comp == "more" and datetime.datetime.now() < limit:
                    return False
                elif comp == "less" and datetime.datetime.now() < limit:
                    return False
                else:
                    return True
                f.close()
        except:
            return True
        return True

    def timestamp(self, timestamp_f, interval):
        if not self.check_timestamp(timestamp_f, 'more', interval):
            return False
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d ,0755)
        with open(timestamp_f, 'w') as f:
            f.write(str(datetime.datetime.now())+'\n')
            f.close()
        return True

    def pushstats(self):
        # get interval from config file
        if self.config.has_section('stats'):
            interval = self.config.getint('stats', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # override with command line
        if self.options.stats_interval is not None:
            interval = self.options.stats_interval

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_stats_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        xmlrpcClient.push_stats(force=self.options.force,
                                file=self.options.stats_file,
                                interval=2*interval,
                                collect_date=self.options.collect_date)

    def pushpkg(self):
        # get interval from config file
        if self.config.has_section('packages'):
            interval = self.config.getint('packages', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_pkg_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        xmlrpcClient.push_pkg()

    def pushpatch(self):
        # get interval from config file
        if self.config.has_section('patches'):
            interval = self.config.getint('patches', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_patch_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        xmlrpcClient.push_patch()

    def pushasset(self):
        # get interval from config file
        if self.config.has_section('asset'):
            interval = self.config.getint('asset', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_asset_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        xmlrpcClient.push_asset()

    def pushsym(self):
        # get interval from config file
        if self.config.has_section('sym'):
            interval = self.config.getint('sym', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_sym_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        xmlrpcClient.push_sym()

    def syncservices(self):
        # get interval from config file
        if self.config.has_section('sync'):
            interval = self.config.getint('sync', 'sync_interval')
        else:
            interval = self.config.getint('DEFAULT', 'sync_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_sync')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.action('syncall')

    def updateservices(self):
        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.action('presync')

    def pushservices(self):
        # get interval from config file
        if self.config.has_section('svcconf'):
            interval = self.config.getint('svcconf', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_svcconf_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        for svc in self.svcs:
            svc.action('push')

    def prkey(self):
        from rcGlobalEnv import rcEnv
        m = __import__('hostid'+rcEnv.sysname)
        print m.hostid()

    def checks(self):
        # get interval from config file
        if self.config.has_section('checks'):
            interval = self.config.getint('checks', 'push_interval')
        else:
            interval = self.config.getint('DEFAULT', 'push_interval')

        # do we need to run
        timestamp_f = os.path.join(os.path.dirname(__file__), '..', 'var', 'last_checks_push')
        if not self.options.force and not self.timestamp(timestamp_f, interval):
            return

        import checks
        if self.svcs is None:
            self.svcs = svcBuilder.build_services()
        c = checks.checks(self.svcs)
        c.do_checks()

    def compliance_check(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_checks()

    def compliance_fix(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_fix()

    def compliance_fixable(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_fixable()

    def compliance_show_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_show_moduleset()

    def compliance_attach_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_attach_moduleset()

    def compliance_detach_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_detach_moduleset()

    def compliance_show_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_show_ruleset()

    def compliance_attach_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_attach_ruleset()

    def compliance_detach_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_detach_ruleset()

    def compliance_list_ruleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_list_rulesets()

    def compliance_list_moduleset(self):
        import compliance
        c = compliance.Compliance(self.options)
        c.do_list_modulesets()

if __name__ == "__main__" :
    for n in (Node,) :
        help(n)
