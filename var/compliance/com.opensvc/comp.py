#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
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
import sys
import os
import re
import json

RET_OK = 0
RET_ERR = 1
RET_NA = 2

RET = RET_OK

class NotApplicable(Exception):
     def __init__(self, s=""):
         self.s = s
     def __str__(self):
         return self.s

class Unfixable(Exception):
     def __init__(self, s=""):
         self.s = s
     def __str__(self):
         return self.s

class ComplianceError(Exception):
     def __init__(self, s=""):
         self.s = s
     def __str__(self):
         return self.s

class CompObject(object):
    def __init__(self,
                 prefix=None,
                 data={}):
        if prefix:
            self.prefix = prefix.upper()
        elif "default_prefix" in data:
            self.prefix = data["default_prefix"].upper()
        else:
            self.prefix = "MAGIX12345"

        self.extra_syntax_parms = data.get("extra_syntax_parms")
        self.example_value = data.get("example_value", "")
        self.description = data.get("description", "(no description)")
        self.form_definition = data.get("form_definition", "(no form definition)")
        self.init_done = False

    def __getattribute__(self, s):
        if not object.__getattribute__(self, "init_done") and s in ("check", "fix", "fixable"):
            object.__setattr__(self, "init_done", True)
            object.__getattribute__(self, "init")()
        return object.__getattribute__(self, s)

    def init(self):
        pass

    def test(self):
        self.prefix = "OSVC_COMP_CO_TEST"
        os.environ[self.prefix] = self.example_value
        return self.check()

    def info(self):
        def indent(text):
            lines = text.split("\n")
            return "\n".join(["    "+line for line in lines])
        s = ""
        s += "Description\n"
        s += "===========\n"
        s += "\n"
        s += indent(self.description)+"\n"
        s += "\n"
        s += "Example rule\n"
        s += "============\n"
        s += "\n"
        s += indent(json.dumps(json.loads(self.example_value), indent=4, separators=(',', ': ')))+"\n"
        s += "\n"
        s += "Form definition\n"
        s += "===============\n"
        s += "\n"
        s += indent(self.form_definition)+"\n"
        s += "\n"
        print s

    def set_prefix(self, prefix):
        self.prefix = prefix.upper()

    def get_rules_raw(self):
        rules = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            s = self.subst(os.environ[k])
            rules += [s]
        if len(rules) == 0:
            raise NotApplicable()
        return rules

    def get_rules(self):
        rules = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            s = self.subst(os.environ[k])
            try:
                data = json.loads(s)
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to rules list'
            if type(data) == list:
                rules += data
            else:
                rules += [data]
        if len(rules) == 0:
            raise NotApplicable()
        return rules

    def subst(self, v):
        if type(v) == list:
            l = []
            for _v in v:
                l.append(self.subst(_v))
            return l
        if type(v) != str and type(v) != unicode:
            return v

        p = re.compile('%%ENV:\w+%%')
        for m in p.findall(v):
            s = m.strip("%").replace('ENV:', '')
            if s in os.environ:
                _v = os.environ[s]
            elif 'OSVC_COMP_'+s in os.environ:
                _v = os.environ['OSVC_COMP_'+s]
            else:
                print >>sys.stderr, s, 'is not an env variable'
                raise NotApplicable()
            v = v.replace(m, _v)
        return v


def main(co):
    syntax =  "syntax:\n"
    syntax += """ %s <ENV VARS PREFIX> check|fix|fixable\n"""%sys.argv[0]
    syntax += """ %s test|info"""%sys.argv[0]
    o = co()
    if o.extra_syntax_parms:
        syntax += " "+o.extra_syntax_parms

    if len(sys.argv) == 2:
        if sys.argv[1] == 'test':
            RET = o.test()
            sys.exit(RET)
        elif sys.argv[1] == 'info':
            o.info()
            sys.exit(0)

    if len(sys.argv) != 3:
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)

    try:
        o.set_prefix(sys.argv[1])
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

