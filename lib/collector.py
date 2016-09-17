from __future__ import print_function
from stat import *
import os
import sys
import re
import datetime
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import is_exe, justcall, banner
from subprocess import *
from rcColor import formatter

class Collector(object):
    def __init__(self, options=None, node=None, svcname=None):
        self.options = options
        self.node = node
        self.collector = node.collector
        self.svcname = svcname
        self.options = options

    def rotate_root_pw(self, pw):
        opts = {}
        opts['pw'] = pw
        d = self.collector.call('collector_update_root_pw', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

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

    @formatter
    def collector_list_unavailability_ack(self):
        return self._collector_list_unavailability_ack()

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

    @formatter
    def collector_list_actions(self):
        return self._collector_list_actions()

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

    @formatter
    def collector_status(self):
        return self._collector_status()

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

    @formatter
    def collector_networks(self, table=True):
        return self._collector_networks()

    def _collector_networks(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_networks', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    @formatter
    def collector_asset(self, table=True):
        return self._collector_asset()

    def _collector_asset(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_asset', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    @formatter
    def collector_checks(self, table=True):
        return self._collector_checks()

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

    @formatter
    def collector_disks(self):
        return self._collector_disks()

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

    @formatter
    def collector_alerts(self):
        return self._collector_alerts()

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

    @formatter
    def collector_events(self):
        return self._collector_events()

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

    @formatter
    def collector_show_actions(self):
        return self._collector_show_actions()

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

    def collector_untag(self):
        opts = {}
        opts['tag_name'] = self.options.tag
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_untag', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

    def collector_tag(self):
        opts = {}
        opts['tag_name'] = self.options.tag
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_tag', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

    def collector_create_tag(self):
        opts = {}
        opts['tag_name'] = self.options.tag
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_create_tag', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

    def collector_list_tags(self):
        d = self._collector_list_tags()
        for tag in d:
            print(tag)

    def _collector_list_tags(self):
        opts = {'pattern': self.options.like}
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_list_tags', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    @formatter
    def collector_show_tags(self):
        try:
            d = self._collector_show_tags()
        except ex.excError as e:
            print(e, file=sys.stderr)
            return
        return d

    def _collector_show_tags(self):
        opts = {}
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_show_tags', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    @formatter
    def collector_list_nodes(self):
        return self._collector_list_nodes()

    def _collector_list_nodes(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_nodes', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    @formatter
    def collector_list_services(self):
        return self._collector_list_services()

    def _collector_list_services(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_services', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    @formatter
    def collector_list_filtersets(self):
        return self._collector_list_filtersets()

    def _collector_list_filtersets(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_filtersets', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

