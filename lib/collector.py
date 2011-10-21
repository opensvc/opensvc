from stat import *
import os
import sys
import re
import datetime
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import is_exe, justcall, banner
from subprocess import *

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

        from rcPrintTable import print_table
        print_table(d['data'])

    def collector_list_actions(self):
        if self.svcname is None:
            return

        opts = {}
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

        from rcPrintTable import print_table
        print_table(d['data'])


