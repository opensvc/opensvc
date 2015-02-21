#!/usr/bin/env /opt/opensvc/bin/python

"""
OSVC_COMP_MPATH='[{"key": "defaults.polling_interval", "op": ">=", "value": 20}, {"key": "device.HP.HSV210.prio", "op": "=", "value": "alua"}]' ./linux.mpath.py OSVC_COMP_MPATH check
"""

import os
import sys
import json
import re
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

comment_chars = "#;"
sections_tree = {
  'defaults': {},
  'blacklist': {
    'device': {},
  },
  'blacklist_exceptions': {
    'device': {},
  },
  'devices': {
    'device': {},
  },
  'multipaths': {
    'multipath': {},
  },
}

class Blacklist(object):
    def __init__(self, name=""):
        self.name = name
        self.wwid = []
        self.devnode = []
        self.devices = []

    def __str__(self):
        s = ""
        if len(self.devices) + len(self.wwid) + len(self.devnode) == 0:
            return s
        s += self.name + " {\n"
        for wwid in self.wwid:
            s += "\twwid " + str(wwid) + "\n"
        for devnode in self.devnode:
            s += "\tdevnode " + str(devnode) + "\n"
        for device in self.devices:
            s += str(device)
        s += "}\n"
        return s

class Section(object):
    def __init__(self, name="", indent=1):
        self.name = name
        self.attr = {}
        self.indent = ""
        for i in range(indent):
            self.indent += '\t'

    def __str__(self):
        s = ""
        s += self.indent + self.name + " {\n"
        for a, v in self.attr.items():
            v = str(v)
            if ' ' in v:
                v = '"' + v + '"'
            s += self.indent + "\t" + a + " " + v + "\n"
        s += self.indent + "}\n"
        return s

class Conf(object):
    def __init__(self):
        self.blacklist = Blacklist("blacklist")
        self.blacklist_exceptions = Blacklist("blacklist_exceptions")
        self.defaults = Section("defaults", indent=0)
        self.devices = []
        self.multipaths = []
        self.changed = False

    def __str__(self):
        s = ""
        s += str(self.defaults)
        s += str(self.blacklist)
        s += str(self.blacklist_exceptions)
        if len(self.devices) > 0:
            s += "devices {\n"
            for device in self.devices:
                s += str(device)
            s += "}\n"
        if len(self.multipaths) > 0:
            s += "multipaths {\n"
            for multipath in self.multipaths:
                s += str(multipath)
            s += "}\n"
        return s

    def set(self, key, value):
        index = self.parse_key(key)
        key = re.sub(r'\{([^\}]+)\}\.', '', key)
        l = key.split('.')
        if key.endswith('}'):
            a = None
        else:
            a = l[-1]
        if l[1] == "device":
            o = self.find_device(l[0], index)
            if o is None:
                o = Section("device")
                o.attr['vendor'] = index[0]
                o.attr['product'] = index[1]
                _l = self.get_device_list(l[0])
                _l.append(o)
            if a is not None:
                o.attr[a] = value
            self.changed = True
        elif l[1] == "multipath":
            o = self.find_multipath(index)
            if o is None:
                o = Section("multipath")
                o.attr['wwid'] = index
                self.multipaths.append(o)
            o.attr[a] = value
            self.changed = True
        elif l[-1] == "wwid":
            o = getattr(self, l[0])
            o.wwid.append(str(value))
            self.changed = True
        elif l[-1] == "devnode":
            o = getattr(self, l[0])
            o.devnode.append(str(value))
            self.changed = True
        elif l[0] == "defaults":
            self.defaults.attr[a] = value
            self.changed = True

    def get(self, key):
        index = self.parse_key(key)
        key = re.sub(r'\{([^\}]+)\}\.', '', key)
        l = key.split('.')
        if key.endswith('}'):
            a = None
        else:
            a = l[-1]
        if len(l) < 2:
            print >>sys.stderr, "malformed key", key
            return
        if l[1] == "device":
            o = self.find_device(l[0], index)
            if o:
                if a is None:
                    return ""
                elif a in o.attr:
                    return o.attr[a]
        elif l[1] == "multipath":
            o = self.find_multipath(index)
            if o and a in o.attr:
                return o.attr[a]
        elif l[-1] == "wwid":
            return getattr(self, l[0]).wwid
        elif l[-1] == "devnode":
            return getattr(self, l[0]).devnode
        elif l[0] == "defaults":
            if a in self.defaults.attr:
                return self.defaults.attr[a]

    def find_multipath(self, index):
        wwid = index
        for multipath in self.multipaths:
            if multipath.attr['wwid'] == wwid:
                return multipath

    def get_device_list(self, section):
        l = getattr(self, section)
        if type(l) != list and hasattr(l, "devices"):
            l = getattr(l, "devices")
        if type(l) != list:
            return
        return l
 
    def find_device(self, section, index):
        vendor, product = index
        l = self.get_device_list(section)
        if not l:
            return
        for device in l:
            if 'vendor' not in device.attr or \
               'product' not in device.attr:
                continue
            if device.attr['vendor'] == vendor and \
               device.attr['product'] == product:
                return device

    def parse_key(self, key):
        key = key.strip()
        m = re.search(r'device\.\{([^\}]+)\}\.\{([^\}]+)\}', key)
        if m:
            return m.group(1), m.group(2)

        m = re.search(r'multipath\.\{([^\}]+)\}', key)
        if m:
            return m.group(1)

class LinuxMpath(object):
    def __init__(self, prefix='OSVC_COMP_MPATH_'):
        self.prefix = prefix.upper()
        self.need_restart = False
        self.cf = os.path.join(os.sep, 'etc', 'multipath.conf')
        self.nocf = False
        self.conf = Conf()

        self.keys = []
        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            raise NotApplicable()

        self.load_file(self.cf)

    def fixable(self):
        return RET_OK

    def load_file(self, p):
        if not os.path.exists(p):
            print >>sys.stderr, p, "does not exist"
            self.nocf = True
            return
        with open(p, 'r') as f:
            buff = f.read()
        buff = self.strip_comments(buff)
        self._load_file(buff, sections_tree)

    def strip_comments(self, buff):
        lines = buff.split('\n')
        l = []
        for line in lines:
            line = line.strip()
            if len(line) == 0:
                continue
            discard = False
            for c in comment_chars:
                if line[0] == c:
                    discard = True
                    break
                try:
                    i = line.index(c)
                    line = line[:i]
                except ValueError:
                    pass
            if not discard and len(line) > 0:
                l.append(line)
        return "\n".join(l)

    def _load_file(self, buff, sections, chain=[]):
        for section, subsections in sections.items():
            _chain = chain + [section]
            _buff = buff
            while True:
                data = self.load_section(_buff, section)
                if data is None:
                    break
                _buff = data[1]
                self.load_keywords(data[0], subsections, _chain)
                self._load_file(data[0], subsections, _chain)

    def load_keywords(self, buff, subsections, chain):
        keywords = {}
        keyword = None
        for line in buff.split('\n'):
            if len(line) == 0:
                continue
            keyword = line.split()[0]
            if keyword in subsections:
                continue
            value = line[len(keyword):].strip().strip('"')
            if len(value) == 0:
                continue
            if keyword in ('wwid', 'devnode') and chain[-1].startswith('blacklist'):
                if keyword not in keywords:
                    keywords[keyword] = [value]
                else:
                    keywords[keyword] += [value]
            else:
                keywords[keyword] = value
        if chain[-1] == 'device' and chain[0] == 'devices':
            s = Section("device")
            s.attr = keywords
            self.conf.devices.append(s)
        elif chain[-1] == 'multipath':
            s = Section("multipath")
            s.attr = keywords
            self.conf.multipaths.append(s)
        elif chain[-1] == 'device' and chain[0] == 'blacklist':
            s = Section("device")
            s.attr = keywords
            self.conf.blacklist.devices.append(s)
        elif chain[-1] == 'device' and chain[0] == 'blacklist exceptions':
            s = Section("device")
            s.attr = keywords
            self.conf.blacklist_exceptions.devices.append(s)
        elif chain[-1] == 'blacklist':
            if 'wwid' in keywords:
                self.conf.blacklist.wwid = keywords['wwid']
            if 'devnode' in keywords:
                self.conf.blacklist.devnode = keywords['devnode']
        elif chain[-1] == 'blacklist_exceptions':
            if 'wwid' in keywords:
                self.conf.blacklist_exceptions.wwid = keywords['wwid']
            if 'devnode' in keywords:
                self.conf.blacklist_exceptions.devnode = keywords['devnode']
        elif chain[-1] == 'defaults':
            self.conf.defaults.attr = keywords

    def load_section(self, buff, section):
        l = []
        try:
            start = buff.index(section)
        except ValueError:
            return
        buff = buff[start:]
        try:
            buff = buff[buff.index('{')+1:]
        except ValueError:
            return
        depth = 1
        for i, c in enumerate(buff):
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
            if depth == 0:
                return buff[:i], buff[i+1:]
        return
                
    def _check_key(self, keyname, target, op, value, verbose=True):
        r = RET_OK
        if value is None:
            if verbose:
                print >>sys.stderr, "%s is not set"%keyname
            return RET_ERR

        if type(value) == list:
            if str(target) in value:
                if verbose:
                    print "%s=%s on target"%(keyname, str(value))
                return RET_OK
            else:
                if verbose:
                    print >>sys.stderr, "%s=%s is not set"%(keyname, str(target))
                return RET_ERR

        if op == '=':
            target = str(target).strip()
            if str(value) != target:
                if verbose:
                    print >>sys.stderr, "%s=%s, target: %s"%(keyname, str(value), target)
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        else:
            if type(value) != int:
                if verbose:
                    print >>sys.stderr, "%s=%s value must be integer"%(keyname, str(value))
                r |= RET_ERR
            elif op == '<=' and value > target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: <= %s"%(keyname, str(value), str(target))
                r |= RET_ERR
            elif op == '>=' and value < target:
                if verbose:
                    print >>sys.stderr, "%s=%s target: >= %s"%(keyname, str(value), str(target))
                r |= RET_ERR
            elif verbose:
                print "%s=%s on target"%(keyname, str(value))
        return r

    def check_key(self, key, verbose=True):
        if 'key' not in key:
            if verbose:
                print >>sys.stderr, "'key' not set in rule %s"%str(key)
            return RET_NA
        if 'value' not in key:
            if verbose:
                print >>sys.stderr, "'value' not set in rule %s"%str(key)
            return RET_NA
        if 'op' not in key:
            op = "="
        else:
            op = key['op']
        target = key['value']

        if op not in ('>=', '<=', '='):
            if verbose:
                print >>sys.stderr, "'op' must be either '=', '>=' or '<=': %s"%str(key)
            return RET_NA

        keyname = key['key']
        value = self.conf.get(keyname)

        if value is None:
            if verbose:
                print >>sys.stderr, "%s key is not set"%keyname
            return RET_ERR

        r = self._check_key(keyname, target, op, value, verbose=verbose)

        return r

    def fix_key(self, key):
        print "%s=%s set"%(key['key'], key['value'])
        self.conf.set(key['key'], key['value'])

    def check(self):
        r = 0
        for key in self.keys:
            r |= self.check_key(key, verbose=True)
        return r

    def fix(self):
        for key in self.keys:
            if self.check_key(key, verbose=False) == RET_ERR:
                self.fix_key(key)

        if not self.conf.changed:
            return

        if not self.nocf:
            import datetime
            backup = self.cf+'.'+str(datetime.datetime.now())
            try:
                import shutil
                shutil.copy(self.cf, backup)
            except:
                print >>sys.stderr, "failed to backup %s"%self.cf
                return RET_ERR
            print self.cf, "backed up as %s"%backup
        try:
            with open(self.cf, 'w') as f:
                f.write(str(self.conf))
            print self.cf, "rewritten"
            self.need_restart = True
        except:
            print >>sys.stderr, "failed to write %s"%self.cf
            if not self.nocf:
                shutil.copy(backup, self.cf)
                print "backup restored"
            return RET_ERR

        self.restart_daemon()

        return RET_OK

    def restart_daemon(self):
        if not self.need_restart:
            return
        candidates = [
          "/etc/init.d/multipathd",
          "/etc/init.d/multipath-tools",
        ]
        fpath = None
        for i in candidates:
            if os.path.exists(i):
                fpath = i
                break
        if fpath is None:
            print >>sys.stderr, "multipath tools startup script not found"
            return RET_ERR
        print "restarting multipath daemon"
        cmd = [fpath, "restart"]
        p = Popen(cmd, stdin=None, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if len(err) > 0:
            print >>sys.stderr, err

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = LinuxMpath(sys.argv[1])
        if sys.argv[2] == 'check':
            RET = o.check()
        elif sys.argv[2] == 'fix':
            RET = o.fix()
        elif sys.argv[2] == 'fixable':
            RET = o.fixable()
        else:
            print >>sys.stderr, "unsupported argument '%s'"%sys.argv[2]
            print >>sys.stderr, syntax
            RET = RET_ERR
    except NotApplicable:
        sys.exit(RET_NA)
    except:
        import traceback
        traceback.print_exc()
        sys.exit(RET_ERR)

    sys.exit(RET)

