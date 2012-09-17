from stat import *
import os
import sys
import re
import datetime
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import is_exe, justcall, banner
from subprocess import *
from rcPrintTable import print_table

class Collector(object):
    def __init__(self, options=None, collector=None, svcname=None):
        self.options = options
        self.collector = collector
        self.svcname = svcname
        self.options = options

    def collector_ack_unavailability(self):
        if self.svcname is None:
            return

        opts = {}
        opts['svcname'] = self.svcname
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        if self.options.author is not None:
            opts['author'] = self.options.author
        if self.options.comment is not None:
            opts['comment'] = self.options.comment
        if self.options.duration is not None:
            opts['duration'] = self.options.duration
        if self.options.account:
            opts['account'] = "1"
        else:
            opts['account'] = "0"

        d = self.collector.call('collector_ack_unavailability', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

    def collector_list_unavailability_ack(self):
        if self.svcname is None:
            return

        opts = {}
        opts['svcname'] = self.svcname
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        if self.options.author is not None:
            opts['author'] = self.options.author
        if self.options.comment is not None:
            opts['comment'] = self.options.comment
        if self.options.duration is not None:
            opts['duration'] = self.options.duration
        if self.options.account:
            opts['account'] = "1"
        else:
            opts['account'] = "0"

        d = self.collector.call('collector_list_unavailability_ack', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'])

    def collector_list_actions(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        if self.options.duration is not None:
            opts['duration'] = self.options.duration

        d = self.collector.call('collector_list_actions', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'])

    def collector_ack_action(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        if self.options.author is not None:
            opts['author'] = self.options.author
        if self.options.comment is not None:
            opts['comment'] = self.options.comment
        if self.options.id == 0:
            raise ex.excError("--id is not set")
        else:
            opts['id'] = self.options.id

        d = self.collector.call('collector_ack_action', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

    def collector_status(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_status', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'])

    def collector_checks(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_checks', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'])

    def collector_alerts(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_alerts', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'], width=30)

    def collector_events(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        d = self.collector.call('collector_events', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'], width=50)

    def collector_show_actions(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        if self.options.id != 0:
            opts['id'] = self.options.id
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        d = self.collector.call('collector_show_actions', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        print_table(d['data'], width=50)

    def collector_list_nodes(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_nodes', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        for node in d['data']:
            print node

    def collector_list_services(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_services', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        for service in d['data']:
            print service

    def collector_list_filtersets(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_filtersets', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        for fset in d['data']:
            print fset

