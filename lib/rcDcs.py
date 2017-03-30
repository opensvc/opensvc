from rcUtilities import justcall, which
from xml.etree.ElementTree import XML, fromstring
import rcExceptions as ex
import os
import ConfigParser
import uuid
from rcGlobalEnv import rcEnv

if rcEnv.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.pathbin

def dcscmd(cmd, manager, username, password, dcs=None, conn=None):
    if conn is None:
        conn = uuid.uuid1().hex

    if len(cmd) == 0:
        return

    _cmd = ['ssh', manager]
    if dcs is not None:
        _cmd += ["connect-dcsserver -server %s -username %s -password %s -connection %s ; "%(dcs, username, password, conn)+\
                 cmd+ " ; disconnect-dcsserver -connection %s"%conn]
    else:
        _cmd += [cmd]
    out, err, ret = justcall(_cmd)
    if "ErrorId" in err:
        print(_cmd)
        print(out)
        raise ex.excError("dcs command execution error")
    try:
        out = out.decode("latin1").encode("utf8")
    except:
        pass
    return out, err, ret

class Dcss(object):
    arrays = []

    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        cf = rcEnv.authconf
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
            if stype != "datacore":
                continue
            try:
                manager = s
                dcs = conf.get(s, 'dcs').split()
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m += [(manager, dcs, username, password)]
            except:
                print("error parsing section", s)
                pass
        del(conf)
        done = []
        for manager, dcs, username, password in m:
            for name in dcs:
                if self.filtering and name not in self.objects:
                    continue
                if name in done:
                    continue
                self.arrays.append(Dcs(name, manager, username, password))
                done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

    def get_dcs(self, domain):
        for dcs in self.arrays:
            _domain = dcs.get_domain()
            if _domain == domain:
                return dcs
        return None

class Dcs(object):
    def __init__(self, name, manager, username, password, conn=None):
        self.name = name
        self.manager = manager
        self.username = username
        self.password = password
        self.conn = conn
        if conn is None:
            self.conn = uuid.uuid1().hex
        self.keys = ['dcsservergroup',
                     'dcsserver',
                     'dcspool',
                     'dcspoolperf',
                     'dcslogicaldisk',
                     'dcslogicaldiskperf',
                     'dcsvirtualdisk',
                     'dcsphysicaldisk',
                     'dcsdiskpath',
                     'dcsport',
                     'dcspoolmember']

    def get_domain(self):
        if hasattr(self, 'domain'):
            return self.domain
        buff = self.get_dcsservergroup()
        for line in buff.split('\n'):
            if not line.startswith('Alias'):
                continue
            self.domain = line.split(': ')[-1].strip()
            break
        if hasattr(self, 'domain'):
            return self.domain
        return "unknown"

    def dcscmd(self, cmd):
        return dcscmd(cmd, self.manager, self.username, self.password, dcs=self.name, conn=self.conn)

    def get_dcsservergroup(self):
        cmd = 'get-dcsservergroup -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsserver(self):
        cmd = 'get-dcsserver -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspool(self):
        cmd = 'get-dcspool -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcslogicaldisk(self):
        cmd = 'get-dcslogicaldisk -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsvirtualdisk(self):
        cmd = 'get-dcsvirtualdisk -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsphysicaldisk(self):
        cmd = 'get-dcsphysicaldisk -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsdiskpath(self):
        cmd = 'get-dcsdiskpath -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspoolmember(self):
        cmd = 'get-dcspoolmember -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcspoolperf(self):
        cmd = 'get-dcspool -connection %s | get-dcsperformancecounter -connection %s'%(self.conn, self.conn)
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcslogicaldiskperf(self):
        cmd = 'get-dcslogicaldisk -connection %s | get-dcsperformancecounter -connection %s'%(self.conn, self.conn)
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def get_dcsport(self):
        cmd = 'get-dcsport -connection %s'%self.conn
        print("%s: %s"%(self.name, cmd))
        buff = self.dcscmd(cmd)[0]
        return buff

    def add_vdisk(self, data):
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
            cmd = """$v = Add-DcsVirtualDisk -connection %(conn)s -Name "%(disk_name)s" -Size %(size)dGB  -EnableRedundancy -FirstServer %(sds1)s -FirstPool "%(pool1)s" -SecondServer %(sds2)s -SecondPool "%(pool2)s" ;""" % d
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
            cmd = """$v = Add-DcsVirtualDisk -connection %(conn)s -Name "%(disk_name)s" -Size %(size)dGB -Server %(sds1)s -Pool "%(pool1)s" ;""" % d
        else:
            raise ex.excError("'dg_name' value is misformatted")
        for machine in self.get_machines(map(lambda x: x[0], paths)):
            cmd += " $v | Serve-DcsVirtualDisk -connection %s -Machine %s -EnableRedundancy ;"""%(self.conn, machine)
        print(cmd)
        out, err, ret = self.dcscmd(cmd)

    def get_machines(self, ids):
        for i, id in enumerate(ids):
            if 'iqn' in id or ('-' in id and len(id) == 16):
                # iscsi or already in correct format
                continue
            # convert to dcs portname format
            id = list(id.upper())
            for j in (14, 12, 10, 8, 6, 4, 2):
                id.insert(j, '-')
            id = ''.join(id)
            ids[i] = id
        if not hasattr(self, "buff_dcsport"):
            self.buff_dcsport = self.get_dcsport()
        machines = set([])
        for line in self.buff_dcsport.split('\n'):
            if line.startswith('HostId'):
                hostid = line.split(': ')[-1].strip()
            elif line.startswith('Id'):
                id = line.split(': ')[-1].strip()
                if id in ids:
                    machines.add(hostid)
        return machines

if __name__ == "__main__":
    o = Dcss()
    for dcs in o:
        print(dcs.get_dcsserver())
