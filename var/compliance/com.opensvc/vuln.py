#!/opt/opensvc/bin/python
""" 
OSVC_COMP_VULN_11_117=\
[{"pkgname": "kernel", "minver": "2.6.18-238.19.1.el5"},\
 {"pkgname": "kernel-xen", "minver": "2.6.18-238.19.1.el5"}]
"""

import os
import sys
import json
import pwd
from subprocess import *
from distutils.version import LooseVersion as V

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompVuln(object):
    def __init__(self, prefix='OSVC_COMP_VULN_'):
        self.prefix = prefix.upper()
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['Linux']:
            print >>sys.stderr, 'module not supported on', self.sysname
            raise NotApplicable()

        self.packages = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                o = json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to rules list'
            for i, d in enumerate(o):
                o[i]["rule"] = k.replace("OSVC_COMP_", "")
            self.packages += o

        if len(self.packages) == 0:
            print >>sys.stderr, "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        vendor = os.environ['OSVC_COMP_NODES_OS_VENDOR']
        if vendor in ['Debian', 'Ubuntu']:
            self.get_installed_packages = self.deb_get_installed_packages
            self.fix_pkg = self.apt_fix_pkg
        elif vendor in ['CentOS', 'Redhat', 'Red Hat']:
            self.get_installed_packages = self.rpm_get_installed_packages
            self.fix_pkg = self.yum_fix_pkg
        else:
            print >>sys.stderr, vendor, "not supported"
            raise NotApplicable()

        self.installed_packages = self.get_installed_packages()

    def rpm_get_installed_packages(self):
        p = Popen(['rpm', '-qa', '--qf', '%{n} %{v}-%{r} %{arch}\n'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return {}
        l = {}
        for line in out.split('\n'):
            v = line.split(' ')
            if len(v) != 3:
                continue
            if v[0] in l:
                l[v[0]] += [(v[1], v[2])]
            l[v[0]] = [(v[1], v[2])]
        return l

    def deb_get_installed_packages(self):
        p = Popen(['dpkg', '-l'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return {}
        l = {}
        for line in out.split('\n'):
            if not line.startswith('ii'):
                continue
            v = line.split()[1:2]
            l[v[0]] = [(v[1], "")]
        return l

    def yum_fix_pkg(self, pkg):
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        r = call(['yum', 'install', '-y', pkg["pkgname"]])
        if r != 0:
            return RET_ERR
        return RET_OK

    def apt_fix_pkg(self, pkg):
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        r = call(['apt-get', 'install', '-y', pkg["pkgname"]])
        if r != 0:
            return RET_ERR
        return RET_OK

    def fixable(self):
        return RET_NA

    def check_pkg(self, pkg, verbose=True):
        if not pkg["pkgname"] in self.installed_packages:
            return RET_OK
        r = RET_OK
        for vers, arch in self.installed_packages[pkg["pkgname"]]:
            target = V(pkg["pkgname"])
            actual = V(vers)
            if target > actual:
                name = pkg["pkgname"]
                if arch != "":
                    name += "."+arch
                print 'package', name, vers, 'is vulnerable. upgrade to', pkg["minver"], "(%s)"%pkg["rule"]
                r |= RET_ERR
        return r

    def check(self):
        r = 0
        for pkg in self.packages:
            r |= self.check_pkg(pkg)
        return r

    def fix(self):
        r = 0
        for pkg in self.packages:
            r |= self.fix_pkg(pkg)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix"""%sys.argv[0]
    if len(sys.argv) != 3:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        o = CompVuln(sys.argv[1])
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

