from __future__ import print_function
from stat import *
import os
import sys
import re
import datetime
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import justcall, banner
from converters import convert_duration
from subprocess import *

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
            opts['duration'] = convert_duration(self.options.duration, _to="m")
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
            opts['duration'] = convert_duration(self.options.duration, _to="m")
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
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        if self.options.begin is not None:
            opts['begin'] = self.options.begin
        if self.options.end is not None:
            opts['end'] = self.options.end
        if self.options.duration is not None:
            opts['duration'] = convert_duration(self.options.duration, _to="m")

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

    def collector_networks(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_networks', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_asset(self):
        opts = {}
        if self.svcname is not None:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_asset', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])

        return d['data']

    def collector_checks(self):
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
        if opts['tag_name'] is None:
            print("missing parameter: --tag", file=sys.stderr)
            return 1
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

    def collector_show_tags(self):
        opts = {}
        if self.svcname:
            opts['svcname'] = self.svcname
        d = self.collector.call('collector_show_tags', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_list_nodes(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_nodes', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_list_services(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_services', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_list_filtersets(self):
        opts = {'fset': self.options.filterset}
        d = self.collector.call('collector_list_filtersets', opts)
        if d is None:
            raise ex.excError("xmlrpc unknown failure")
        if d['ret'] != 0:
            raise ex.excError(d['msg'])
        return d['data']

    def collector_search(self):
        path = "/search?"
        if self.options.like.count(":") == 1:
            t, s = self.options.like.split(":")
            t = t.strip()
            s = s.strip()
            path += "substring=%s&in=%s" % (s, t)
        else:
            s = self.options.like
            path += "substring=%s" % s
        d = self.node.collector_rest_get(path)
        if "data" not in d:
            raise ex.excError("unexpected collector response: %s" % str(d))
        data = []
        for t, _d in d["data"].items():
            if _d["total"] == 0:
                continue
            print("%s (%d/%d)"  % (t, len(_d["data"]), _d["total"]))
            for e in d["data"][t]["data"]:
                e_name = _d["fmt"]["name"] % e
                e_id = _d["fmt"]["id"] % e
                print(" %s: %s" % (e_id, e_name))

    def collector_log(self):
        path = "/logs"
        data = {
          "log_fmt": self.options.message,
        }
        d = self.node.collector_rest_post(path, data, svcname=self.svcname)
        if "error" in d:
            raise ex.excError(d["error"])
        print("logged")

