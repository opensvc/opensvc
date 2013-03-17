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
        d = self._collector_list_unavailability_ack()
        print_table(d)

    def collector_json_list_unavailability_ack(self):
        d = self._collector_list_unavailability_ack()
        import json
        print(json.dumps(d))

    def _collector_list_unavailability_ack(self):
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

        return d['data']

    def collector_list_actions(self):
        d = self._collector_list_actions()
        print_table(d)

    def collector_json_list_actions(self):
        d = self._collector_list_actions()
        import json
        print(json.dumps(d))

    def _collector_list_actions(self):
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

        return d['data']

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
        d = self._collector_status()
        print_table(d)

    def collector_json_status(self):
        d = self._collector_status()
        import json
        print(json.dumps(d))

    def _collector_status(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_status', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_checks(self):
        d = self._collector_checks()
        print_table(d)

    def collector_json_checks(self):
        d = self._collector_checks()
        import json
        print(json.dumps(d))

    def _collector_checks(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_checks', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_disks(self):
        d = self._collector_disks()
        print_table(d, width=64)

    def collector_json_disks(self):
        d = self._collector_disks()
        import json
        print(json.dumps(d))

    def _collector_disks(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_disks', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_alerts(self):
        d = self._collector_alerts()
        print_table(d, width=30)

    def collector_json_alerts(self):
        d = self._collector_alerts()
        import json
        print(json.dumps(d))

    def _collector_alerts(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_alerts', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_events(self):
        d = self._collector_events()
        print_table(d, width=50)
        
    def collector_json_events(self):
        d = self._collector_events()
        import json
        print(json.dumps(d))

    def _collector_events(self):
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

        return d['data']

    def collector_show_actions(self):
        d = self._collector_show_actions()
        print_table(d, width=50)
        
    def collector_json_show_actions(self):
        d = self._collector_show_actions()
        import json
        print(json.dumps(d))

    def _collector_show_actions(self):
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

        return d['data']

    def collector_list_nodes(self):
        d = self._collector_list_nodes()
        for node in d:
            print(node)
        
    def collector_json_list_nodes(self):
        d = self._collector_list_nodes()
        import json
        print(json.dumps(d))

    def _collector_list_nodes(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_nodes', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_list_services(self):
        d = self._collector_list_services()
        for service in d:
            print(service)
        
    def collector_json_list_services(self):
        d = self._collector_list_services()
        import json
        print(json.dumps(d))

    def _collector_list_services(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_services', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_list_filtersets(self):
        d = self._collector_list_filtersets()
        for fset in d['data']:
            print(fset)
        
    def collector_json_list_filtersets(self):
        d = self._collector_list_filtersets()
        import json
        print(json.dumps(d))

    def _collector_list_filtersets(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_filtersets', opts)
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

