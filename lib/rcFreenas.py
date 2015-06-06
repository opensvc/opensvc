import rcExceptions as ex
import os
import requests
import ConfigParser
import json

pathlib = os.path.dirname(__file__)
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))

requests.packages.urllib3.disable_warnings()
verify = False

class Freenass(object):
    arrays = []

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0
        cf = os.path.join(pathetc, "auth.conf")
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = []
        for s in conf.sections():
            try:
                stype = conf.get(s, 'type')
            except:
                continue
            if stype != "freenas":
                continue
            try:
                name = s
                api = conf.get(s, 'api')
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m += [(name, api, username, password)]
            except:
                print("error parsing section", s)
                pass
        del(conf)
        done = []
        for name, api, username, password in m:
            if self.filtering and name not in self.objects:
                continue
            if name in done:
                continue
            self.arrays.append(Freenas(name, api, username, password))
            done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

    def get_freenas(self, name):
        for array in self.arrays:
            if array.name == name:
                return freenas
        return None

class Freenas(object):
    def __init__(self, name, api, username, password):
        self.name = name
        self.api = api
        self.username = username
        self.password = password
        self.auth = (username, password)
        self.keys = ['version',
                     'volumes',
                     'iscsi_targets',
                     'iscsi_targettoextents',
                     'iscsi_extents']

    def post(self, uri):
        r = requests.post(self.api+uri, auth=self.auth, verify=verify)
        return r.content

    def get(self, uri):
        r = requests.get(self.api+uri+"/?format=json", auth=self.auth, verify=verify)
        return r.content

    def get_version(self):
        buff = self.get("/system/version")
        return buff

    def get_volumes(self):
        buff = self.get("/storage/volume")
        return buff

    def get_iscsi_targets(self):
        buff = self.get("/services/iscsi/target")
        return buff

    def get_iscsi_targettoextents(self):
        buff = self.get("/services/iscsi/targettoextent")
        return buff

    def get_iscsi_extents(self):
        buff = self.get("/services/iscsi/extent")
        return buff

    def add_extent(self, data):
        if 'disk_name' not in data:
            raise ex.excError("'disk_name' key is mandatory")
        if 'size' not in data:
            raise ex.excError("'size' key is mandatory")
        if 'paths' not in data:
            raise ex.excError("'paths' key is mandatory")

        data['disk_name'] = data['disk_name'] + '.1'
        l = data['paths'].split(',')
        paths = []
        for path in l:
            if 'iqn' in path:
                c, s = path.split('-iqn')
                s = 'iqn' + s
                paths.append((c, s))
            elif '-' in path:
                c, s = path.split('-')
                paths.append((c, s))
        if len(paths) == 0:
            raise ex.excError("no initiator to present to")

        pools = data['dg_name'].split(',')
        if len(pools) == 2:
            _pool1 = pools[0].split(':')
            _pool2 = pools[1].split(':')
            if len(_pool1) != 2 or len(_pool2) != 2:
                raise ex.excError("'dg_name' value is misformatted")
            d = {
              'disk_name': data['disk_name'],
              'size': data['size'],
              'sds1': _pool1[0],
              'sds2': _pool2[0],
              'pool1': _pool1[1],
              'pool2': _pool2[1],
              'conn': self.conn,
            }
            cmd = """$v = Add-FreenasVirtualDisk -connection %(conn)s -Name "%(disk_name)s" -Size %(size)dGB  -EnableRedundancy -FirstServer %(sds1)s -FirstPool "%(pool1)s" -SecondServer %(sds2)s -SecondPool "%(pool2)s" ;""" % d
        elif len(pools) == 1:
            _pool1 = pools[0].split(':')
            if len(_pool1) != 2:
                raise ex.excError("'dg_name' value is misformatted")
            d = {
              'disk_name': data['disk_name'],
              'size': data['size'],
              'sds1': _pool1[0],
              'pool1': _pool1[1],
              'conn': self.conn,
            }
            cmd = """$v = Add-FreenasVirtualDisk -connection %(conn)s -Name "%(disk_name)s" -Size %(size)dGB -Server %(sds1)s -Pool "%(pool1)s" ;""" % d
        else:
            raise ex.excError("'dg_name' value is misformatted")
        for machine in self.get_machines(map(lambda x: x[0], paths)):
            cmd += " $v | Serve-FreenasVirtualDisk -connection %s -Machine %s -EnableRedundancy ;"""%(self.conn, machine)
        print(cmd)
        out, err, ret = self.freenascmd(cmd)

    def get_machines(self, ids):
        for i, id in enumerate(ids):
            if 'iqn' in id or ('-' in id and len(id) == 16):
                # iscsi or already in correct format
                continue
            # convert to freenas portname format
            id = list(id.upper())
            for j in (14, 12, 10, 8, 6, 4, 2):
                id.insert(j, '-')
            id = ''.join(id)
            ids[i] = id
        if not hasattr(self, "buff_freenasport"):
            self.buff_freenasport = self.get_freenasport()
        machines = set([])
        for line in self.buff_freenasport.split('\n'):
            if line.startswith('HostId'):
                hostid = line.split(': ')[-1].strip()
            elif line.startswith('Id'):
                id = line.split(': ')[-1].strip()
                if id in ids:
                    machines.add(hostid)
        return machines

if __name__ == "__main__":
    o = Freenass()
    for freenas in o:
        print(freenas.get_version())
