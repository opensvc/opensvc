from __future__ import print_function

import glob
import importlib
import os
import pkgutil
import sys

from env import Env


class Check(object):
    def __init__(self, svcs=None):
        if svcs is None:
            svcs = []
        self.svcs = svcs
        if self.svcs is None:
            self.svcs = []

    @property
    def undef(self):
        return [{
            'path': '',
            'instance': 'undef',
            'value': '-1'
        }]

    def do_check(self): # pragma: no cover
        """
        To be implemented by child classes.
        """
        return []

class Checks(Check):
    def __init__(self, svcs=None, node=None, checkers=None):
        if svcs is None:
            svcs = []
        self.check_list = []
        self.svcs = svcs
        self.node = node
        self.checkers = checkers or []
        self.register_internal_checkers()
        self.register_local_checkers()

    def __iadd__(self, c):
        if isinstance(c, Check):
            self.check_list.append(c)
        elif isinstance(c, Checks):
            self.check_list += c.check_list
        return self

    def register_internal_checkers(self):
        def onerror(name):
            pass
        for modinfo in pkgutil.walk_packages(__path__, __name__ + '.', onerror=onerror):
            if hasattr(modinfo, "ispkg"):
                name = modinfo.name
                ispkg = modinfo.ispkg
            else:
                name = modinfo[1]
                ispkg = modinfo[2]
            if ispkg:
                continue
            if name.split(".")[-1] != Env.module_sysname:
                continue
            mod = importlib.import_module(name)
            o = mod.Check(svcs=self.svcs)
            if self.checkers and o.chk_type not in self.checkers:
                continue
            self += o

    def register_local_checkers(self):
        check_d = os.path.join(Env.paths.pathvar, 'check')
        if not os.path.exists(check_d):
            return
        sys.path.append(check_d)
        for f in glob.glob(os.path.join(check_d, 'check*.py')):
            if Env.sysname not in f:
                continue
            cname = os.path.basename(f).replace('.py', '')
            try:
                m = __import__(cname)
                self += m.Check(svcs=self.svcs)
            except Exception as e:
                print('Could not import check:', cname, file=sys.stderr)
                print(e, file=sys.stderr)

    def do_checks(self):
        import datetime

        now = str(datetime.datetime.now())
        data = {}

        for chk in self.check_list:
            idx = chk.chk_type
            if hasattr(chk, "chk_name"):
                driver = chk.chk_name.lower()
            else:
                driver = "generic"

            _data = chk.do_check()

            if not isinstance(_data, (list, tuple)) or len(_data) == 0:
                continue

            instances = []
            for instance in _data:
                if not isinstance(instance, dict):
                    continue
                if 'instance' not in instance:
                    continue
                if instance['instance'] == 'undef':
                    continue
                if 'value' not in instance:
                    continue
                _instance = {
                    "instance": instance.get("instance", ""),
                    "value": instance.get("value", ""),
                    "path": instance.get("path", ""),
                    "driver": driver,
                }
                instances.append(_instance)

            if len(instances) > 0:    
                if idx not in data:
                    data[idx] = instances
                else:
                    data[idx] += instances
        return data

    def print_checks(self, data):
        from utilities.render.forest import Forest
        from utilities.render.color import color
        tree = Forest()
        head_node = tree.add_node()
        head_node.add_column(Env.nodename, color.BOLD)
        for chk_type, instances in data.items():
            node = head_node.add_node()
            node.add_column(chk_type, color.BROWN)
            for instance in instances:
                _node = node.add_node()
                _node.add_column(str(instance["instance"]), color.LIGHTBLUE)
                _node.add_column(instance["path"])
                _node.add_column(str(instance["value"]))
                if instance["driver"] == "generic":
                    _node.add_column()
                else:
                    _node.add_column(instance["driver"])
        tree.out()

