#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_USER_... vars
which define {"gssftp": {"disable": "no", "server_args": "-l -a -u 022", ...}, ...}

supported dictionnary keys:
- disable
- server_args
"""

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class Sysctl(object):
    def __init__(self, prefix='OSVC_COMP_XINETD_'):
        self.prefix = prefix.upper()
        self.need_reload = False
        self.cf = os.path.join(os.sep, "etc", "sysctl.conf")
        if not os.path.exists(self.cf):
            print >>sys.stderr, self.cf, 'does not exist'
            raise NotApplicable()

        self.keys = []
        self.cache = None

        for k in [ key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.keys += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'sysctl key syntax error on var[', k, '] = ',os.environ[k]

        if len(self.keys) == 0:
            print "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        self.convert_keys()


    def fixable(self):
        return RET_OK

    def parse_val(self, val):
        val = map(lambda x: x.strip(), val.strip().split())
        for i, e in enumerate(val):
            try:
                val[i] = int(e)
            except:
                pass
        return val

    def get_keys(self):
        with open(self.cf, 'r') as f:
            buff = f.read()

        if self.cache is None:
            self.cache = {}

        for line in buff.split('\n'):
            line = line.strip()
            if line.startswith('#'):
                continue
            l = line.split('=')
            if len(l) != 2:
                continue
            key = l[0].strip()
            val = self.parse_val(l[1])
            self.cache[key] = val

    def get_live_key(self, key):
        p = Popen(['sysctl', key], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            return None
        l = out.split('=')
        if len(l) != 2:
            return None
        val = self.parse_val(l[1])
        return val
 
    def get_key(self, key):
        if self.cache is None:
            self.get_keys()
        if key not in self.cache:
            return None
        return self.cache[key]

    def fix_key(self, key):
        done = False
        target = key['value']
        index = key['index']

        with open(self.cf, 'r') as f:
            buff = f.read()

        lines = buff.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if line.startswith('#'):
                continue
            l = line.split('=')
            if len(l) != 2:
                continue
            keyname = l[0].strip()
            if key['key'] != keyname:
                continue
            if done:
                print "sysctl: remove redundant key %s"%keyname
                del lines[i]
                continue
            val = self.parse_val(l[1])
            if target == val[index]:
                continue
            print "sysctl: set %s[%d] = %s"%(keyname, index, str(target))
            val[index] = target
            lines[i] = "%s = %s"%(keyname, " ".join(map(str, val)))
            done = True

        if not done:
            # if key is not in sysctl.conf, get the value from kernel
            val = self.get_live_key(key['key'])
            if target != val[index]:
                val[index] = target
                print "sysctl: set %s = %s"%(key['key'], " ".join(map(str, val)))
                lines += ["%s = %s"%(key['key'], " ".join(map(str, val)))]

        try:
            with open(self.cf, 'w') as f:
                f.write('\n'.join(lines))
        except:
            print >>sys.stderr, "failed to write sysctl.conf"
            return RET_ERR

        return RET_OK

    def convert_keys(self):
        keys = []
        for key in self.keys:
            keyname = key['key']
            value = key['value']
            if type(value) == list:
                if len(value) > 0 and type(value[0]) != list:
                    value = [value]
                for i, v in enumerate(value):
                    keys.append({
                      "key": keyname,
                      "index": i,
                      "op": v[0],
                      "value": v[1],
                    })
            elif 'key' in key and 'index' in key and 'op' in key and 'value' in key:
               keys.append(key)

        self.keys = keys

    def check_key(self, key, verbose=False):
        r = RET_OK
        keyname = key['key']
        target = key['value']
        op = key['op']
        i = key['index']
        current_value = self.get_key(keyname)
        current_live_value = self.get_live_key(keyname)

        if current_value is None:
            if verbose:
                print >>sys.stderr, "key '%s' not found in sysctl.conf"%keyname
            return RET_ERR

        if op == "=" and str(current_value[i]) != str(target):
            if verbose:
                print >>sys.stderr, "sysctl err: %s[%d] = %s, target: %s"%(keyname, i, str(current_value[i]), str(target))
            r |= RET_ERR
        elif op == ">=" and type(target) == int and current_value[i] < target:
            if verbose:
                print >>sys.stderr, "sysctl err: %s[%d] = %s, target: >= %s"%(keyname, i, str(current_value[i]), str(target))
            r |= RET_ERR
        elif op == "<=" and type(target) == int and current_value[i] > target:
            if verbose:
                print >>sys.stderr, "sysctl err: %s[%d] = %s, target: <= %s"%(keyname, i, str(current_value[i]), str(target))
            r |= RET_ERR
        else:
            if verbose:
                print "sysctl ok: %s[%d] = %s, on target"%(keyname, i, str(current_value[i]))

        if r == RET_OK and current_live_value is not None and current_value != current_live_value:
            if verbose:
                print >>sys.stderr, "sysctl err: %s on target in sysctl.conf but kernel value is different"%(keyname)
            self.need_reload = True
            r |= RET_ERR

        return r

    def check(self):
        r = 0
        for key in self.keys:
            r |= self.check_key(key, verbose=True)
        return r

    def reload_sysctl(self):
        cmd = ['sysctl', '-p']
        print "sysctl:", " ".join(cmd)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, "reload failed"
            return RET_ERR
        return RET_OK

    def fix(self):
        r = 0
        for key in self.keys:
            if self.check_key(key, verbose=False) == RET_ERR:
                self.need_reload = True
                r |= self.fix_key(key)
        if self.need_reload:
            r |= self.reload_sysctl()
        return r

if __name__ == "__main__":
    """ test: OSVC_COMP_SYSCTL='[{"key": "net.unix.max_dgram_qlen", "value": [">=", 9]}, {"key": "kernel.ctrl-alt-del", "value": ["=", 1]}, {"key": "kernel.printk", "value": [[], [] , [], [">=", 12]]}]' ./sysctl.py OSVC_COMP_SYSCTL check
    """
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = Sysctl(sys.argv[1])
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

