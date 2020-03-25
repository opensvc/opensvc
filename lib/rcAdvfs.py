import os
import glob

from utilities.proc import call

class ExInit(Exception):
    pass

class Fset(object):
    """
dor
	Id           : 46a70bfd.000964b0.6.8001
	Files        :      158,  SLim=        0,  HLim=        0
	Blocks  (1k) : 36035624,  SLim=        0,  HLim=        0
	Quota Status : user=on  group=on
	Object Safety: off
	Fragging     : on
	DMAPI        : off
stock_systemes
	Id           : 4ad8612f.000923f8.1.8001
	Clone is     : stock_systemes@osvc_sync
	Files        :   499709,  SLim=        0,  HLim=        0
	Blocks  (1k) : 35305996,  SLim=        0,  HLim=        0
	Quota Status : user=off group=off
	Object Safety: off
	Fragging     : on
	DMAPI        : off
stock_systemes@osvc_sync
	Id           : 4ad8612f.000923f8.2.8001
	Clone of     : stock_systemes
	Revision     : 1
	Object Safety: off
	Fragging     : on
	DMAPI        : off
    """
    def __init__(self, lines):
        self.domain = None
        for line in lines:
            if not line.startswith('\t'):
                self.name = line.strip()
            elif "Id" in line:
                self.fsetid = line.split(':')[-1].strip()
            elif "Clone of" in line:
                self.cloneof = line.split(':')[-1].strip()
            elif "Clone is" in line:
                self.cloneis = line.split(':')[-1].strip()
            elif "Revision" in line:
                self.revision = line.split(':')[-1].strip()
            elif "Files" in line:
                line = line[line.index(':')+1:]
                l = line.split()
                self.files_count = int(l[0].replace(',',''))
                self.files_slim = int(l[2].replace(',',''))
                self.files_hlim = int(l[4].replace(',',''))
            elif "Blocks" in line:
                line = line[line.index(':')+1:]
                l = line.split()
                self.block_count = int(l[0].replace(',',''))
                self.block_slim = int(l[2].replace(',',''))
                self.block_hlim = int(l[4].replace(',',''))

    def fsname(self):
        return "#".join((self.domain.name, self.name))

    def __str__(self):
        s = "fileset:\n"
        s += " fsetid: %s\n" % self.fsetid
        s += " name: %s\n" % self.name
        s += " fsname: %s\n" % self.fsname()
        s += " files_count: %d\n" % self.files_count
        s += " files_slim: %d\n" % self.files_slim
        s += " files_hlim: %d\n" % self.files_hlim
        s += " block_count: %d\n" % self.block_count
        s += " block_slim: %d\n" % self.block_slim
        s += " block_hlim: %d\n" % self.block_hlim
        return s

class Volume(object):
    def __init__(self, s):
        l = s.split()
        if len(l) != 8:
            raise ExInit()
        self.volid = l[0]
        self.size = int(l[1])
        self.free = int(l[2])
        self.used_pct = int(l[3].replace('%',''))
        self.cmode = l[4]
        self.rblks = int(l[5])
        self.wblks = int(l[6])
        self.name = l[7]

    def __str__(self):
        s = "volume:\n"
        s += " volid: %s\n" % self.volid
        s += " name: %s\n" % self.name
        s += " size: %s\n" % self.size
        s += " free: %s\n" % self.free
        s += " used_pct: %s\n" % self.used_pct
        s += " cmode: %s\n" % self.cmode
        s += " rblks: %s\n" % self.rblks
        s += " wblks: %s\n" % self.wblks
        return s

class Fdmn(object):
    def __init__(self, name):
        self.used_pct = 0
        self.size = 0
        self.free = 0

        cmd = ['showfdmn', name]
        ret, out, err = call(cmd)
        if ret != 0:
            raise ExInit()
        d = {}
        """
               Id	       Date Created  LogPgs  Version  Domain Name
46a70bfd.000964b0  Wed Jul 25 10:38:21 2007     512        4  dom1

  Vol    1K-Blks        Free  % Used  Cmode  Rblks  Wblks  Vol Name
   2L   62914560    21056568     67%     on    256    256  /dev/disk/dsk13c
        """
        lines = out.split('\n')
        if len(lines) < 5:
            raise ExInit()
        header = lines[2].split()
        self.domid = header[0]
        self.name = header[-1]
        self.version = header[-2]
        self.logpgs = header[-3]
        self.vols = {}
        self.fsets = {}
        for line in lines[5:]:
            try:
                v = Volume(line)
                self += v
            except ExInit:
                pass

        cmd = ['showfsets', '-k', name]
        ret, out, err = call(cmd)
        if ret != 0:
            raise ExInit()
        lines = out.split('\n')
        n_lines = len(lines)
        if n_lines == 0:
            return
        h = 0
        for i, line in enumerate(lines):
            if i != 0 and not line.startswith('\t') or i == n_lines - 1:
                f = Fset(lines[h:i])
                self += f
            if not line.startswith('\t'):
                h = i

    def __iadd__(self, o):
        if type(o) == Volume:
            self.size += o.size
            self.free += o.free
            self.used_pct = int(100. * (self.size - self.free) / self.size)
            o.domain = self
            self.vols[o.name] = o
        elif type(o) == Fset:
            o.domain = self
            self.fsets[o.name] = o
        return self

    def __str__(self):
        s = "domain:\n"
        s += " domid: %s\n" % self.domid
        s += " name: %s\n" % self.name
        s += " version: %s\n" % self.version
        s += " logpgs: %s\n" % self.logpgs
        for v in self.vols.values():
            s += str(v)
        for v in self.fsets.values():
            s += str(v)
        return s

    def list_volnames(self):
        l = []
        for v in self.vols.values():
            l.append(v.name)
        return l

class Fdmns(object):
    def __init__(self):
        self.load_fdmns()

    def list_fdmns(self):
        return self.fdmns.keys()

    def load_fdmns(self):
        self.fdmns = {}
        for n in glob.glob('/etc/fdmns/*'):
            n = os.path.basename(n)
            if n.startswith('.'):
                continue
            self.fdmns[n] = {}

    def load_fdmn(self, name):
        d = Fdmn(name)
        self.fdmns[name] = d

    def get_fdmn(self, name):
        if name not in self.fdmns:
            return
        if len(self.fdmns[name]) == 0:
            self.load_fdmn(name)
        return self.fdmns[name]

if __name__ == "__main__":
    o = Fdmns()
    print(o.list_fdmns())
    d = o.get_fdmn('dom1')
    print(d)
