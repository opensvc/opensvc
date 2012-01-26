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
import sys
from subprocess import *
from distutils.version import LooseVersion as V

sys.path.append(os.path.dirname(__file__))

from comp import *

class CompVuln(object):
    def __init__(self, prefix='OSVC_COMP_VULN_', uri=None):
        self.uri = uri
        self.prefix = prefix.upper()
        self.highest_avail_version = "0"
        self.fix_list = []
        self.sysname, self.nodename, x, x, self.machine = os.uname()

        if self.sysname not in ['Linux', 'HP-UX', 'AIX', 'SunOS']:
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

        if 'OSVC_COMP_NODES_OS_VENDOR' not in os.environ:
            print >>sys.stderr, "OS_VENDOR is not set. Check your asset"
            raise NotApplicable()

        vendor = os.environ['OSVC_COMP_NODES_OS_VENDOR']
        if vendor in ['Debian', 'Ubuntu']:
            self.get_installed_packages = self.deb_get_installed_packages
            self.fix_pkg = self.apt_fix_pkg
            self.fixable_pkg = self.apt_fixable_pkg
            self.fix_all = None
        elif vendor in ['CentOS', 'Redhat', 'Red Hat']:
            self.get_installed_packages = self.rpm_get_installed_packages
            self.fix_pkg = self.yum_fix_pkg
            self.fixable_pkg = self.yum_fixable_pkg
            self.fix_all = None
        elif vendor in ['HP']:
            if self.uri is None:
                print >>sys.stderr, "URI is not set"
                raise NotApplicable()
            self.get_installed_packages = self.hp_get_installed_packages
            self.fix_pkg = self.hp_fix_pkg
            self.fixable_pkg = self.hp_fixable_pkg
            self.fix_all = self.hp_fix_all
        elif vendor in ['IBM']:
            self.get_installed_packages = self.aix_get_installed_packages
            self.fix_pkg = self.aix_fix_pkg
            self.fixable_pkg = self.aix_fixable_pkg
            self.fix_all = self.aix_fix_all
        elif vendor in ['Oracle']:
            self.get_installed_packages = self.sol_get_installed_packages
            self.fix_pkg = self.sol_fix_pkg
            self.fixable_pkg = self.sol_fixable_pkg
            self.fix_all = self.sol_fix_all
        else:
            print >>sys.stderr, vendor, "not supported"
            raise NotApplicable()

        self.installed_packages = self.get_installed_packages()

    def sol_fix_pkg(self, pkg):
        return RET_NA

    def sol_fixable_pkg(self, pkg):
        return RET_ERR

    def sol_fix_all(self):
        return RET_NA

    def sol_get_installed_packages(self):
        p = Popen(['pkginfo', '-l'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return {}
        return self.sol_parse_pkginfo(out)

    def sol_parse_pkginfo(self, out):
        l = {}
        for line in out.split('\n'):
            v = line.split(':')
            if len(v) != 2:
                continue
            f = v[0].strip()
            if f == "PKGINST":
                pkgname = v[1].strip()
            elif f == "ARCH":
                pkgarch = v[1].strip()
            elif f == "VERSION":
                pkgvers = v[1].strip()
                if pkgname in l:
                    l[pkgname] += [(pkgvers, pkgarch)]
                else:
                    l[pkgname] = [(pkgvers, pkgarch)]
        return l

    def aix_fix_pkg(self, pkg):
        return RET_NA

    def aix_fixable_pkg(self, pkg):
        return RET_ERR

    def aix_fix_all(self):
        return RET_NA

    def aix_get_installed_packages(self):
        p = Popen(['lslpp', '-L', '-c'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return {}
        return self.aix_parse_lslpp(out)

    def aix_parse_lslpp(self, out):
        l = {}
        for line in out.split('\n'):
            if line.startswith('#') or len(line) == 0:
                continue
            v = line.split(':')
            if len(v) < 3:
                continue
            pkgname = v[1].replace('-'+v[2], '')
            if pkgname in l:
                l[pkgname] += [(v[2], "")]
            else:
                l[pkgname] = [(v[2], "")]
        return l

    def hp_fix_pkg(self, pkg):
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        if self.fixable_pkg(pkg) == RET_ERR:
            return RET_ERR
        if self.highest_avail_version == "0":
            return RET_ERR
        self.fix_list.append(pkg["pkgname"]+',r='+self.highest_avail_version)
        return RET_OK

    def hp_fix_all(self):
        r = call(['swinstall', '-x', 'autoreboot=true', '-x', 'mount_all_filesystems=false', '-s', self.uri] + self.fix_list)
        if r != 0:
            return RET_ERR
        return RET_OK

    def hp_fixable_pkg(self, pkg):
        self.highest_avail_version = "0"
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        cmd = ['swlist', '-l', 'product', '-s', self.uri, pkg['pkgname']]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            if "not found on host" in err:
                print >>sys.stderr, '%s > %s not available in repositories'%(pkg['pkgname'], pkg['minver'])
            else:
                print >>sys.stderr, 'can not fetch available packages list'
            return RET_ERR
        l = self.hp_parse_swlist(out)
        if len(l) == 0:
            print >>sys.stderr, '%s > %s not available in repositories'%(pkg['pkgname'], pkg['minver'])
            return RET_ERR
        for v in map(lambda x: x[0], l.values()[0]):
            if V(v) > V(self.highest_avail_version):
                self.highest_avail_version = v
        if V(self.highest_avail_version) < V(pkg['minver']):
            print >>sys.stderr, '%s > %s not available in repositories'%(pkg['pkgname'], pkg['minver'])
            return RET_ERR
        return RET_OK

    def hp_get_installed_packages(self):
        p = Popen(['swlist', '-l', 'product'], stdout=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            print >>sys.stderr, 'can not fetch installed packages list'
            return {}
        return self.hp_parse_swlist(out)

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
            else:
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

    def apt_fixable_pkg(self, pkg, version):
        # TODO
        return RET_NA

    def yum_fixable_pkg(self, pkg):
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        cmd = ['yum', 'list', 'available', pkg['pkgname']]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        (out, err) = p.communicate()
        if p.returncode != 0:
            if "No matching Packages" in err:
                print >>sys.stderr, '%s > %s not available in repositories'%(pkg['pkgname'], pkg['minver'])
            else:
                print >>sys.stderr, 'can not fetch available packages list'
            return RET_ERR
        highest_avail_version = "0"
        for line in out.split('\n'):
            l = line.split()
            if len(l) != 3:
                continue
            if V(l[1]) > V(highest_avail_version):
                highest_avail_version = l[1]
        if V(highest_avail_version) < V(pkg['minver']):
            print >>sys.stderr, '%s > %s not available in repositories'%(pkg['pkgname'], pkg['minver'])
            return RET_ERR
        return RET_OK

    def yum_fix_pkg(self, pkg):
        if self.check_pkg(pkg, verbose=False) == RET_OK:
            return RET_OK
        if self.fixable_pkg(pkg) == RET_ERR:
            return RET_ERR
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

    def get_kver(self):
        s = os.uname()[2]
        s = s.replace('xen', '')
        s = s.replace('PAE', '')
        return s

    def check_pkg(self, pkg, verbose=True):
        if not pkg["pkgname"] in self.installed_packages:
            return RET_OK
        name = pkg["pkgname"]
        r = RET_OK
        max = "0"
        max_v = V(max)
        ok = []
        for vers, arch in self.installed_packages[pkg["pkgname"]]:
            target = V(pkg["minver"])
            actual = V(vers)
            if actual > max_v or max == "0":
                max = vers
                max_v = actual
            if target <= actual:
                ok.append(vers)

        if max == "0":
            # not installed
            return RET_OK

        if len(ok) > 0:
            kver = self.get_kver()
            if name.startswith("kernel") and kver not in ok:
                if verbose:
                    print >>sys.stderr, "kernel", ', '.join(ok), "installed and not vulnerable but vulnerable kernel", kver, "booted"
                return RET_ERR
            return RET_OK

        if arch != "":
            name += "."+arch
        if verbose:
            print >>sys.stderr, 'package', name, vers, 'is vulnerable. upgrade to', pkg["minver"], "(%s)"%pkg["rule"]
        return RET_ERR

    def check(self):
        r = 0
        for pkg in self.packages:
            r |= self.check_pkg(pkg)
        return r

    def fix(self):
        r = 0
        for pkg in self.packages:
            r |= self.fix_pkg(pkg)
        if self.fix_all is not None and len(self.fix_list) > 0:
            self.fix_all()
        return r

    def fixable(self):
        r = 0
        for pkg in self.packages:
            r |= self.fixable_pkg(pkg)
        return r

if __name__ == "__main__":
    syntax = """syntax:
      %s PREFIX check|fixable|fix [uri]"""%sys.argv[0]
    if len(sys.argv) not in (3, 4):
        print >>sys.stderr, "wrong number of arguments"
        print >>sys.stderr, syntax
        sys.exit(RET_ERR)
    try:
        if len(sys.argv) == 4:
            o = CompVuln(sys.argv[1], sys.argv[3])
        else:
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

