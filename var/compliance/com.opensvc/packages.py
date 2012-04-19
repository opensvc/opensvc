#!/opt/opensvc/bin/python
""" 
module use OSVC_COMP_PACKAGES_... vars
which define ['pkg1', 'pkg2', ...]
"""

import os
import sys
import json
import pwd
from subprocess import *

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompPackages(object):
    def __init__(self, prefix='OSVC_COMP_PACKAGES_', uri=None):
        self.uri = uri
        self.prefix = prefix.upper()
        self.sysname, self.nodename, x, x, self.machine = os.uname()
        self.known_archs = ['i386', 'i586', 'i686', 'x86_64', 'noarch']

        if self.sysname not in ['Linux', 'AIX', 'HP-UX']:
            print >>sys.stderr, __file__, 'module not supported on', self.sysname
            raise NotApplicable()

        if 'OSVC_COMP_PACKAGES_PKG_TYPE' in os.environ and \
           os.environ['OSVC_COMP_PACKAGES_PKG_TYPE'] == "bundle":
            self.pkg_type = 'bundle'
        else:
            self.pkg_type = 'product'

        self.packages = []
        for k in [key for key in os.environ if key.startswith(self.prefix)]:
            try:
                self.packages += json.loads(os.environ[k])
            except ValueError:
                print >>sys.stderr, 'failed to concatenate', os.environ[k], 'to package list'

        if len(self.packages) == 0:
            print "no applicable variable found in rulesets", self.prefix
            raise NotApplicable()

        vendor = os.environ['OSVC_COMP_NODES_OS_VENDOR']
        if vendor in ['Debian', 'Ubuntu']:
            self.get_installed_packages = self.deb_get_installed_packages
            self.fix_pkg = self.apt_fix_pkg
        elif vendor in ['CentOS', 'Redhat', 'Red Hat']:
            self.get_installed_packages = self.rpm_get_installed_packages
            self.fix_pkg = self.yum_fix_pkg
        elif vendor in ['IBM']:
            self.get_installed_packages = self.aix_get_installed_packages
            self.fix_pkg = self.aix_fix_pkg
        elif vendor in ['HP']:
            self.get_installed_packages = self.hp_get_installed_packages
            self.fix_pkg = self.hp_fix_pkg
        else:
            print >>sys.stderr, vendor, "not supported"
            raise NotApplicable()

        self.load_reloc()
        self.packages = map(lambda x: x.strip(), self.packages)
        self.expand_pkgnames()
        self.installed_packages = self.get_installed_packages()

    def load_reloc(self):
        self.reloc = {}
        for i, pkgname in enumerate(self.packages):
            l = pkgname.split(':')
            if len(l) != 2:
                continue
            self.packages[i] = l[0]
            self.reloc[l[0]] = l[1]

    def expand_pkgnames(self):
        """ Expand wildcards and implicit arch
        """
        l = []
        for pkgname in self.packages:
            l += self.expand_pkgname(pkgname)
        self.packages = l

    def expand_pkgname(self, pkgname):
        vendor = os.environ['OSVC_COMP_NODES_OS_VENDOR']
        if vendor in ['CentOS', 'Redhat', 'Red Hat']:
            return self.yum_expand_pkgname(pkgname)
        return [pkgname]

    def yum_expand_pkgname(self, pkgname):
        arch_specified = False
        for arch in self.known_archs:
            if pkgname.endswith(arch):
                arch_specified = True
        cmd = ['yum', 'list', pkgname]
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not expand', pkgname
            return []
        lines = out.split('\n')
        if len(lines) < 2:
            print >>sys.stderr, 'can not expand', pkgname
            return []
        lines = lines[1:]
        l = []
        for line in lines:
            words = line.split()
            if len(words) != 3:
                continue
            if words[0] in ("Installed", "Available", "Loaded"):
                continue
            l.append(words[0])

        if arch_specified or len(l) == 1:
            return l

        if os.environ['OSVC_COMP_NODES_OS_ARCH'] in ('i386', 'i586', 'i686', 'ia32'):
            archs = ('i386', 'i586', 'i686', 'ia32', 'noarch')
        else:
            archs = (os.environ['OSVC_COMP_NODES_OS_ARCH'], 'noarch')

        ll = []
        for pkgname in l:
            if pkgname.split('.')[-1] in archs:
                # keep only packages matching the arch
                ll.append(pkgname)

        return ll

    def hp_parse_swlist(self, out):
        l = {}
        for line in out.split('\n'):
            if line.startswith('#') or len(line) == 0:
                continue
            v = line.split()
            if len(v) < 2:
                continue
            if v[0] in l:
                l[v[0]] += [(v[1], "")]
            else:
                l[v[0]] = [(v[1], "")]
        return l

    def hp_fix_pkg(self, pkg):
        if pkg in self.reloc:
            pkg = ':'.join((pkg, self.reloc[pkg]))
        cmd = ['swinstall',
               '-x', 'allow_downdate=true',
               '-x', 'mount_all_filesystems=false',
               '-s', self.uri, pkg]
        print " ".join(cmd)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if len(out) > 0:
            print out
        if len(err) > 0:
            print >>sys.stderr, err
        if p.returncode != 0:
            return RET_ERR
        return RET_OK

    def hp_get_installed_packages(self):
        p = Popen(['swlist', '-l', self.pkg_type], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return []
        return self.hp_parse_swlist(out).keys()

    def aix_fix_pkg(self, pkg):
        print "TODO: aix_fix_pkg"

    def aix_get_installed_packages(self):
        cmd = ['lslpp', '-Lc']
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return []
        pkgs = []
        for line in out.split('\n'):
            l = line.split(':')
            if len(l) < 5:
                continue
            pkgvers = l[2]
            pkgname = l[1].replace('-'+pkgvers, '')
            pkgs.append(pkgname)
        return pkgs

    def rpm_get_installed_packages(self):
        p = Popen(['rpm', '-qa', '--qf', '%{n}.%{ARCH}\n'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return []
        return out.split('\n')

    def deb_get_installed_packages(self):
        p = Popen(['dpkg', '-l'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return []
        l = []
        for line in out.split('\n'):
            if not line.startswith('ii'):
                continue
            l.append(line.split()[1])
        return l

    def yum_fix_pkg(self, pkg):
        cmd = ['yum', 'install', '-y', pkg]
        print ' '.join(cmd)
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            if len(err) > 0:
                print err
            return RET_ERR
        return RET_OK

    def apt_fix_pkg(self, pkg):
        r = call(['apt-get', 'install', '-y', pkg])
        if r != 0:
            return RET_ERR
        return RET_OK

    def fixable(self):
        return RET_NA

    def check_pkg(self, pkg, verbose=True):
        if not pkg in self.installed_packages:
            if verbose:
                print >>sys.stderr, 'package', pkg, 'is not installed'
            return RET_ERR
        if verbose:
            print 'package', pkg, 'is installed'
        return RET_OK

    def check(self):
        r = 0
        for pkg in self.packages:
            r |= self.check_pkg(pkg)
        return r

    def fix(self):
        r = 0
        for pkg in self.packages:
            if self.check_pkg(pkg, verbose=False) == RET_OK:
                continue
            r |= self.fix_pkg(pkg)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix [uri]"""%sys.argv[0]
    if len(sys.argv) not in [3, 4]:
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        argv = [sys.argv[1]]
        if len(sys.argv) == 4:
            argv.append(sys.argv[3])
        o = CompPackages(*argv)
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

