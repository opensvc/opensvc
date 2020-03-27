from subprocess import *

"""
lsvg format
===========

VOLUME GROUP:       rootvg                   VG IDENTIFIER:  00082a6a0000d400000001321aa20bf2
VG STATE:           active                   PP SIZE:        64 megabyte(s)
VG PERMISSION:      read/write               TOTAL PPs:      959 (61376 megabytes)
MAX LVs:            256                      FREE PPs:       717 (45888 megabytes)
LVs:                11                       USED PPs:       242 (15488 megabytes)
OPEN LVs:           10                       QUORUM:         2 (Enabled)
TOTAL PVs:          1                        VG DESCRIPTORS: 2
STALE PVs:          0                        STALE PPs:      0
ACTIVE PVs:         1                        AUTO ON:        yes
MAX PPs per VG:     32512
MAX PPs per PV:     1016                     MAX PVs:        32
LTG size (Dynamic): 256 kilobyte(s)          AUTO SYNC:      no
HOT SPARE:          no                       BB POLICY:      relocatable

lsvg -l vgname format
=====================

rootvg:
LV NAME             TYPE       LPs     PPs     PVs  LV STATE      MOUNT POINT
hd5                 boot       1       1       1    closed/syncd  N/A
hd6                 paging     32      32      1    open/syncd    N/A
hd8                 jfs2log    1       1       1    open/syncd    N/A
hd4                 jfs2       16      16      1    open/syncd    /
hd2                 jfs2       40      40      1    open/syncd    /usr
hd9var              jfs2       16      16      1    open/syncd    /var
hd3                 jfs2       16      16      1    open/syncd    /tmp
hd1                 jfs2       16      16      1    open/syncd    /home
hd10opt             jfs2       16      16      1    open/syncd    /opt
lv_logs             jfs2       32      32      1    open/syncd    /logs
lv_moteurs          jfs2       56      56      1    open/syncd    /moteurs

lspv format
===========

hdisk0          00082a6a1aa20b3c                    rootvg          active
hdisk1          00082a6ae73c7bb6                    datavg          active

lspv -l pvname format
=====================

hdisk0:
LV NAME               LPs     PPs     DISTRIBUTION          MOUNT POINT
hd10opt               16      16      00..00..16..00..00    /opt
hd2                   40      40      00..00..40..00..00    /usr
hd9var                16      16      00..00..16..00..00    /var
hd3                   16      16      00..00..16..00..00    /tmp
hd1                   16      16      00..00..16..00..00    /home
hd5                   1       1       01..00..00..00..00    N/A
hd6                   32      32      00..00..32..00..00    N/A
hd8                   1       1       00..00..01..00..00    N/A
hd4                   16      16      00..00..16..00..00    /
lv_logs               32      32      00..32..00..00..00    /logs
lv_moteurs            56      56      00..56..00..00..00    /moteurs

"""

class InitVgError(Exception):
    pass

class Container(dict):
    def __call__(self,key):
        return self.__getitem__(key)

    def __getitem__(self, key):
        dict.__getitem__(self, key)

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __iadd__(self, o):
        dict.__setitem__(self, o.name, o)
        return self

class Vg(object):
    props = [{"prop": "ppsize", "key": "PP SIZE:", "consume": 2}]

    def __init__(self, name):
        self.name = name
        self.lv = Container()
        self.pv = Container()
        self.load_vg(name)
        self.load_lv(name)

    def __str__(self):
        l = []
        l.append("type: vg")
        l.append("name: %s"%self.name)
        l.append("pp size: %d MB"%self.ppsize)
        s = '\n'.join(l)

        for lv in self.lv.values():
            s += str(lv) + '\n'

        return s

    def parse_ppsize(self, l):
        self.ppsize = int(l[0])
        if 'megabyte' in l[1]:
            pass
        elif 'kilobyte' in l[1]:
            self.ppsize /= 1024
        elif 'gigabyte' in l[1]:
            self.ppsize *= 1024

    def load_vg(self, name):
        cmd = ['lsvg', name]
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise InitVgError()
        for line in out.split('\n'):
            for p in self.props:
                if p['key'] not in line:
                    continue
                _line = line[line.index(p['key'])+len(p['key']):]
                l = _line.split()
                getattr(self, 'parse_'+p['prop'])(l[0:p['consume']])

    def load_lv(self, name):
        cmd = ['lsvg', '-l', name]
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise InitVgError()
        for line in out.split('\n'):
            if 'LV NAME' in line:
                continue
            l = line.split()
            if len(l) < 6:
                continue
            _name, _type, _lps, _pps, _pvs, _state = l[0:6]
            _mntpt = ' '.join(l[6:])
            lv = Lv(_name)
            lv.type = _type
            lv.lps = int(_lps)
            lv.pps = int(_pps)
            lv.pvs = int(_pvs)
            lv.state = _state
            lv.mntpt = _mntpt
            lv.size = lv.pps * self.ppsize
            self.lv += lv

class Pv(object):
    def __init__(self, name):
        self.name = name
        self.lv_pps = {}
        self.load_lv_pps(name)

    def load_lv_pps(self, name):
        cmd = ['lspv', '-l', name]
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise InitVgError()
        for line in out.split('\n'):
            if 'LV NAME' in line:
                continue
            l = line.split()
            if len(l) < 4:
                continue
            _name, _lps, _pps, _distrib = l[0:4]
            _mntpt = ' '.join(l[4:])
            if name in self.lv_pps:
                self.lv_pps[_name] += int(_pps)
            else:
                self.lv_pps[_name] = int(_pps)

class Lv(object):
    def __init__(self, name):
        self.name = name
        self.pv_size = {}

    def __str__(self):
        l = []
        l.append("type: lv")
        l.append("name: %s"%self.name)
        l.append("lv size: %d MB"%self.size)
        l.append("pv usage: %s"%self.pv_size.items())
        return '\n'.join(l)

class Lvm(object):
    def __init__(self):
        self.vg = Container()
        self.pv = Container()
        self.load_vg()
        self.load_pv()

    def __str__(self):
        s = ""
        for vgname, vg in self.vg.items():
            s += str(vg) + '\n\n'
        return s

    def load_vg(self):
        cmd = ['lsvg']
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise InitVgError()
        for vg in out.split():
            self.vg += Vg(vg)

    def load_pv(self):
        cmd = ['lspv']
        p = Popen(cmd, stdout=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            raise InitVgError()
        for line in out.split('\n'):
            l = line.split()
            if len(l) != 4:
                continue
            _name, _id, _vgname, _state = l
            pv = Pv(_name)
            pv.id = _id
            pv.vgname = _vgname
            pv.state = _state
            self.pv += pv
            for lvname, lvpps in pv.lv_pps.items():
                vg, lv = self.find_lv(lvname)
                if lv is None:
                    continue
                lv.pv_size[pv.name] = vg.ppsize * lvpps

    def find_lv(self, lvname):
        for vg in self.vg.values():
            for lv in vg.lv.values():
                if lv.name == lvname:
                    return vg, lv
        return None, None

if __name__ == "__main__" :
    lvm = Lvm()
    print(lvm)
