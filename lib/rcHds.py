from __future__ import print_function

import os
import ConfigParser
import sys
import json
from xml.etree.ElementTree import XML, fromstring

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import which, justcall, convert_size, lazy
from rcOptParser import OptParser
from optparse import Option

PROG = "nodemgr array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in auth.conf"),
    "pool": Option(
        "--pool", action="store", dest="pool",
        help="The name of the DP pool"),
    "size": Option(
        "--size", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "lun": Option(
        "--lun", action="store", dest="lun",
        help="The LUN ID to assign on LU mapping"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
    "name": Option(
        "--name", action="store", dest="name",
        help="A ldev name"),
    "devnum": Option(
        "--devnum", action="store", dest="devnum",
        help="A XX:CU:LDEV logical unit name"),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_disk": {
            "msg": "Add and present a disk",
            "options": [
                OPT.name,
                OPT.size,
                OPT.pool,
                OPT.mappings,
            ],
        },
    },
    "Delete actions": {
        "del_disk": {
            "msg": "Delete a disk",
            "options": [
                OPT.name,
            ],
        },
    },
    "Modify actions": {
        "resize_disk": {
            "msg": "Resize a disk",
            "options": [
                OPT.name,
                OPT.size,
            ],
        },
    },
    "List actions": {
        "list_arrays": {
            "msg": "List arrays",
        },
        "list_pools": {
            "msg": "List pools",
        },
        "list_arraygroups": {
            "msg": "List array groups",
        },
        "list_domains": {
            "msg": "List host groups",
        },
        "list_ports": {
            "msg": "List ports",
        },
        "list_logicalunits": {
            "msg": "List ports",
        },
    },
}

class Arrays(object):
    arrays = []
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0

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
            if stype != "hds":
                continue
            try:
                bin = conf.get(s, 'bin')
            except:
                bin = None
            try:
                jre_path = conf.get(s, 'jre_path')
                os.environ["HDVM_CLI_JRE_PATH"] = jre_path
            except:
                path
            try:
                url = conf.get(s, 'url')
                arrays = conf.get(s, 'array').split()
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m += [(url, arrays, username, password, bin)]
            except:
                print("error parsing section", s)
                pass

        del(conf)
        done = []
        for url, arrays, username, password, bin in m:
            for name in arrays:
                if self.filtering and name not in self.objects:
                    continue
                if name in done:
                    continue
                self.arrays.append(Array(name, url, username, password, bin))
                done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

    def get_array(self, name):
        for array in self.arrays:
            if array.name == name:
                return array
        return None

class Array(object):
    def __init__(self, name, url, username, password, bin=None):
        self.keys = ['array', 'lu', 'arraygroup', 'port', 'pool']
        self.name = name
        self.model = name.split(".")[0]
        self.serial = name.split(".")[-1]
        self.url = url
        self.username = username
        self.password = password
        if bin is None:
            self.bin = "HiCommandCLI"
        else:
            self.bin = bin

    def cmd(self, cmd, scoped=True):
        if which(self.bin) is None:
            raise ex.excError("Can not find %s"%self.bin)
        l = [
            self.bin, self.url, cmd[0],
            "-u", self.username,
            "-p", self.password,
            "-f", "xml",
        ]
        if scoped:
            l += [
                "serialnum="+self.serial,
                "model="+self.model,
            ]
        if len(cmd) > 1:
            l += cmd[1:]
        #print(" ".join(l))
        out, err, ret = justcall(l)
        if ret != 0:
            raise ex.excError(err)
        return out, err, ret

    def get_array_data(self, scoped=True):
        cmd = ['GetStorageArray']
        out, err, ret = self.cmd(cmd, scoped=scoped)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("StorageArray"):
            data.append(elem.attrib)
        return data

    def get_array(self):
        return json.dumps(self.get_array_data(scoped=True), indent=4)

    def get_lu_data(self):
        cmd = ['GetStorageArray', 'subtarget=Logicalunit', 'lusubinfo=Path,LDEV,VolumeConnection']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("LogicalUnit"):
            data.append(elem.attrib)
        return data

    def get_lu(self):
        return json.dumps(self.get_lu_data(), indent=4)

    def get_pool_data(self):
        cmd = ['GetStorageArray', 'subtarget=Pool']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("Pool"):
            data.append(elem.attrib)
        return data

    @lazy
    def pool_data(self):
        return self.get_pool_data()

    def get_pool(self):
        return json.dumps(self.get_pool_data(), indent=4)

    def get_arraygroup_data(self):
        cmd = ['GetStorageArray', 'subtarget=ArrayGroup']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("ArrayGroup"):
            data.append(elem.attrib)
        return data

    def get_arraygroup(self):
        return json.dumps(self.get_arraygroup_data(), indent=4)

    def get_port_data(self):
        cmd = ['GetStorageArray', 'subtarget=Port']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("Port"):
            port = elem.attrib
            port["worldWidePortName"] = port["worldWidePortName"].replace(".", "")
            data.append(port)
        return data

    @lazy
    def port_data(self):
        return self.get_port_data()

    def get_port(self):
        return json.dumps(self.get_port_data(), indent=4)

    def get_domain_data(self):
        cmd = ['GetStorageArray', 'subtarget=HostStorageDomain', 'hsdsubinfo=WWN']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("HostStorageDomain"):
            d = elem.attrib
            d["WWN"] = []
            for subelem in elem.getiterator("WWN"):
                wwn = subelem.attrib
                wwn["WWN"] = wwn["WWN"].replace(".", "")
                d["WWN"].append(wwn)
            data.append(d)
        return data

    @lazy
    def domain_data(self):
        return self.get_domain_data()

    def get_target_port(self, target):
        for port in self.port_data():
            if target == port["worldWidePortName"]:
                return port

    def get_hba_domain(self, hba_id):
        for hg in self.domain_data():
            for wwn in hg["WWN"]:
                if hba_id == wwn["WWN"]:
                    return hg

    def get_pool_id(self, poolname):
        for pool in self.pool_data:
            if pool["name"] == poolname:
                return pool

    def list_array(self, **kwargs):
        data = self.get_array_data()
        print(json.dumps(data, indent=4))

    def list_arrays(self, **kwargs):
        data = self.get_array_data(scoped=False)
        print(json.dumps(data, indent=4))

    def list_pools(self, **kwargs):
        data = self.get_pool_data()
        print(json.dumps(data, indent=4))

    def list_arraygroups(self, **kwargs):
        data = self.get_arraygroup_data()
        print(json.dumps(data, indent=4))

    def list_ports(self, **kwargs):
        data = self.get_port_data()
        print(json.dumps(data, indent=4))

    def list_logicalunits(self, **kwargs):
        data = self.get_lu_data()
        print(json.dumps(data, indent=4))

    def list_domains(self, **kwargs):
        data = self.get_domain_data()
        print(json.dumps(data, indent=4))

    def translate_mappings(self, mappings):
        internal_mappings = {}
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            domain = self.get_hba_domain(hba_id)["domainID"]
            internal_mappings[domain] = set([self.get_target_port(target)["displayName"] for target in targets])
        return internal_mappings

    def add_map(self, devnum=None, mappings=None, lun=None, name=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        if mappings is None:
            raise ex.excError("--mappings is mandatory")
        results = []
        if mappings is not None:
            internal_mappings = self.translate_mappings(mappings)
            for domain, portnames in internal_mappings.items():
                for portname in portnames:
                    results.append(self._add_map(devnum=devnum, domain=domain, portname=portname, lun=lun, name=name, **kwargs))
        return results

    def _add_map(self, devnum=None, domain=None, portname=None, lun=None, name=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        if domain is None:
            raise ex.excError("--domain is mandatory")
        if portname is None:
            raise ex.excError("--portname is mandatory")
        if lun is None:
            raise ex.excError("--lun is mandatory")
        cmd = [
            "addlun",
            "devnum="+devnum,
            "portname="+portname,
            "domain="+domain,
            "lun="+str(lun),
        ]
        if name:
            cmd.append("name="+name)
        out, err, ret = self.cmd(cmd)
        print(out)
        tree = fromstring(out)
        if ret != 0:
            raise ex.excError(err)

    def add_disk(self, name=None, pool=None, size=None, lun=None, mappings=None, **kwargs):
        if pool is None:
            raise ex.excError("--pool is mandatory")
        if size == 0 or size is None:
            raise ex.excError("--size is mandatory")
        pool_id = self.get_pool_id(pool)["poolID"]
        cmd = [
            "addvirtualvolume",
            "capacity="+str(convert_size(size, _to="KB")),
            "capacitytype=KB",
            "poolid="+str(pool_id),
        ]
        out, err, ret = self.cmd(cmd)
        print(out)
        tree = fromstring(out)
        data = tree.find("LogicalUnit").attrib
        if ret != 0:
            raise ex.excError(err)
        if mappings:
            self.add_map(name=name, devnum=data["displayName"], lun=lun, mappings=mappings)
        #ret = self.get_volumes(volume=name, cluster=cluster)
        return out

def do_action(action, array_name=None, **kwargs):
    o = Arrays()
    array = o.get_array(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
    if not hasattr(array, action):
        raise ex.excError("not implemented")
    ret = getattr(array, action)(**kwargs)
    if ret is not None:
        print(json.dumps(ret, indent=4))

def main():
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    options, action = parser.parse_args()
    kwargs = vars(options)
    do_action(action, **kwargs)

if __name__ == "__main__":
    try:
        ret = main()
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)


