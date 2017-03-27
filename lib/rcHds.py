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
        help="A logical unit label"),
    "devnum": Option(
        "--devnum", action="store", dest="devnum",
        help="A XX:CU:LDEV logical unit name"),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Generic actions": {
        "add_disk": {
            "msg": "Add and present a disk",
            "options": [
                OPT.name,
                OPT.size,
                OPT.pool,
                OPT.mappings,
            ],
        },
        "add_map": {
            "msg": "Present a disk",
            "options": [
                OPT.devnum,
                OPT.mappings,
                OPT.lun,
            ],
        },
        "del_disk": {
            "msg": "Delete a disk",
            "options": [
                OPT.devnum,
            ],
        },
        "del_map": {
            "msg": "Unpresent a disk",
            "options": [
                OPT.devnum,
                OPT.mappings,
            ],
        },
        "rename_disk": {
            "msg": "Rename a disk",
            "options": [
                OPT.devnum,
                OPT.name,
            ],
        },
        "resize_disk": {
            "msg": "Resize a disk",
            "options": [
                OPT.devnum,
                OPT.size,
            ],
        },
    },
    "Low-level actions": {
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
            "msg": "List logical units",
            "options": [
                OPT.devnum,
            ],
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
        self.domain_portname = {}
        self.port_portname = {}

    def cmd(self, cmd, scoped=True, xml=True):
        if which(self.bin) is None:
            raise ex.excError("Can not find %s"%self.bin)
        l = [
            self.bin, self.url, cmd[0],
            "-u", self.username,
            "-p", self.password,
        ]
        if xml:
            l += [
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

    def parse(self, out):
        lines = out.splitlines()

        if lines[0] == "RESPONSE:":
            # discard the "RESPONSE:" first line
            lines = lines[1:]

        def get_key_val(line):
            idx = line.index("=")
            key = line[:idx].strip()
            val = line[idx+1:].strip()
            try:
                val = int(val.replace(" ", "").replace(",", ""))
            except ValueError:
                pass
            return key, val

        def _parse_instance(lines, start, ref_indent):
            #print("parse instance", start, lines[start-1])
            data = {}
            nidx = -1
            for idx, line in enumerate(lines[start:]):
                if nidx > 0 and start+idx < nidx:
                    continue
                indent = len(line) - len(line.lstrip())
                if indent < ref_indent:
                    return data, start+idx
                if line.strip().startswith("List of "):
                    obj_type = line.strip().split()[3]
                    data[obj_type], nidx = _parse_list(lines, start+idx+1)
                try:
                    key, val = get_key_val(line)
                    data[key] = val
                except ValueError:
                    pass
            return data, start+idx

        def _parse_list(lines, start=0):
            #if start > 0:
            #    print("parse list    ", start, lines[start-1])
            data = []
            nidx = -1
            ref_indent = len(lines[start]) - len(lines[start].lstrip())
            marker = lines[start]
            for idx, line in enumerate(lines[start:]):
                if nidx > 0 and start+idx < nidx:
                    continue
                indent = len(line) - len(line.lstrip())
                if indent < ref_indent:
                    return data, start+idx
                if indent > ref_indent:
                    continue
                if line == marker:
                    instance, nidx = _parse_instance(lines, start+idx+1, indent)
                    data.append(instance)
            return data, start+idx

        data, nidx =_parse_list(lines)
        return data

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

    def get_lu_data(self, devnum=None):
        cmd = ['GetStorageArray', 'subtarget=Logicalunit', 'lusubinfo=Path,LDEV,VolumeConnection']
        if devnum:
            cmd += ["displayname="+str(devnum)]
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for e_lu in tree.getiterator("LogicalUnit"):
            lu = e_lu.attrib
            lu["Path"] = []
            for e_path in e_lu.getiterator("Path"):
                lu["Path"].append(e_path.attrib)
            for e_ldev in e_lu.getiterator("LDEV"):
                ldev = e_ldev.attrib
                for e_label in e_ldev.getiterator("ObjectLabel"):
                    lu["label"] = e_label.attrib["label"]
            data.append(lu)
        return data

    def get_lu(self):
        return json.dumps(self.get_lu_data(), indent=4)

    @lazy
    def lu_data(self):
        return self.get_lu_data()

    def to_devnum(self, devnum):
        devnum = str(devnum)
        if ":" in devnum:
            # 00:00:00 or 00:00 format
            devnum = devnum.replace(":", "")
            return str(int(devnum, 16))
        if "." in devnum:
            # <serial>.<culd> format (collector inventory fmt)
            devnum = devnum.split(".")[-1]
            return str(int(devnum, 16))
        if len(devnum) in (32, 33):
            # wwid format
            devnum = devnum[-4:]
            return str(int(devnum, 16))

        return devnum

    def get_logicalunit(self, devnum=None):
        if devnum is None:
            return
        for lu in self.lu_data:
            if lu["devNum"] == devnum:
                return lu

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
            port["worldWidePortName"] = port["worldWidePortName"].replace(".", "").lower()
            data.append(port)
        return data

    @lazy
    def port_data(self):
        return self.get_port_data()

    def get_port(self):
        return json.dumps(self.get_port_data(), indent=4)

    def get_domain_data(self):
        cmd = ['GetStorageArray', 'subtarget=HostStorageDomain', 'hsdsubinfo=WWN,Path']
        out, err, ret = self.cmd(cmd)
        tree = fromstring(out)
        data = []
        for elem in tree.getiterator("HostStorageDomain"):
            d = elem.attrib
            d["WWN"] = []
            d["Path"] = []
            for subelem in elem.getiterator("WWN"):
                wwn = subelem.attrib
                wwn["WWN"] = wwn["WWN"].replace(".", "").lower()
                d["WWN"].append(wwn)
            for subelem in elem.getiterator("Path"):
                path = subelem.attrib
                d["Path"].append(path)
            data.append(d)
        return data

    @lazy
    def domain_data(self):
        return self.get_domain_data()

    def get_target_port(self, target):
        for port in self.port_data:
            if target == port["worldWidePortName"]:
                return port

    def get_hba_domain_used_lun_ids(self, hba_id):
        domain = self.get_hba_domain(hba_id)
        return [int(path["LUN"]) for path in domain["Path"]]

    def get_hba_free_lun_id(self, hba_ids):
        used_lun_ids = set()
        for hba_id in hba_ids:
            used_lun_ids |= set(self.get_hba_domain_used_lun_ids(hba_id))
        for lun_id in range(65536):
            if lun_id not in used_lun_ids:
                return lun_id

    def get_hba_domain(self, hba_id):
        for domain in self.domain_data:
            for wwn in domain["WWN"]:
                if hba_id == wwn["WWN"]:
                    return domain

    def get_pool_by_name(self, poolname):
        for pool in self.pool_data:
            if pool["name"] == poolname:
                return pool

    def get_pool_by_id(self, pool_id):
        for pool in self.pool_data:
            if pool["poolID"] == pool_id:
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

    def list_logicalunits(self, devnum=None, **kwargs):
        data = self.get_lu_data(devnum=devnum)
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
            if domain not in self.domain_portname:
                self.domain_portname[domain] = []
            self.domain_portname[domain].append(hba_id)
            if domain not in internal_mappings:
                internal_mappings[domain] = set()
            for tgt_id in targets:
                port = self.get_target_port(tgt_id)["displayName"]
                if port is None:
                    continue
                if port not in self.port_portname:
                    self.port_portname[port] = []
                self.port_portname[port].append(tgt_id)
                internal_mappings[domain].add(port)
        return internal_mappings

    def del_map(self, devnum=None, mappings=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        results = []
        if mappings is not None:
            internal_mappings = self.translate_mappings(mappings)
        else:
            internal_mappings = {}
            for dom in self.domain_data:
                for path in dom["Path"]:
                    if devnum == path["devNum"]:
                        domain = path["domainID"]
                        portname = path["portName"]
                        if domain not in internal_mappings:
                            internal_mappings[domain] = set()
                        internal_mappings[domain].add(portname)

        for domain, portnames in internal_mappings.items():
            for portname in portnames:
                result = self._del_map(devnum=devnum, domain=domain, portname=portname, **kwargs)
                if result is not None:
                    results.append(result)
        if len(results) > 0:
            return results

    def _del_map(self, devnum=None, domain=None, portname=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        if domain is None:
            raise ex.excError("--domain is mandatory")
        if portname is None:
            raise ex.excError("--portname is mandatory")
        cmd = [
            "deletelun",
            "devnum="+str(devnum),
            "portname="+portname,
            "domain="+domain,
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)

    def add_map(self, devnum=None, mappings=None, lun=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        if mappings is None:
            raise ex.excError("--mappings is mandatory")
        if lun is None:
            hba_ids = [mapping.split(":")[0] for mapping in mappings]
            lun = self.get_hba_free_lun_id(hba_ids)
        if lun is None:
            raise ex.excError("Unable to find a free lun id")
        results = []
        if mappings is not None:
            internal_mappings = self.translate_mappings(mappings)
            for domain, portnames in internal_mappings.items():
                for portname in portnames:
                    result = self._add_map(devnum=devnum, domain=domain, portname=portname, lun=lun, **kwargs)
                    if result is not None:
                        results.append(result)
        if len(results) > 0:
            return results

    def _add_map(self, devnum=None, domain=None, portname=None, lun=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        if domain is None:
            raise ex.excError("--domain is mandatory")
        if portname is None:
            raise ex.excError("--portname is mandatory")
        if lun is None:
            raise ex.excError("--lun is mandatory")
        domain = str(domain)
        devnum = str(devnum)
        for dom in self.domain_data:
            for path in dom["Path"]:
                if portname == path["portName"] and \
                   domain == path["domainID"] and \
                   devnum == path["devNum"]:
                    print("Device %s is already mapped to port %s in domain %s" % (devnum, portname, domain))
                    return
        cmd = [
            "addlun",
            "devnum="+str(devnum),
            "portname="+portname,
            "domain="+domain,
            "lun="+str(lun),
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)
        data = self.parse(out)
        return data[0]["Path"][0]

    def add_disk(self, name=None, pool=None, size=None, lun=None, mappings=None, **kwargs):
        if pool is None:
            raise ex.excError("--pool is mandatory")
        if size == 0 or size is None:
            raise ex.excError("--size is mandatory")
        pool_id = self.get_pool_by_name(pool)["poolID"]
        cmd = [
            "addvirtualvolume",
            "capacity="+str(convert_size(size, _to="KB")),
            "capacitytype=KB",
            "poolid="+str(pool_id),
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)
        data = self.parse(out)
        ret = data[0]["ArrayGroup"][0]["Lu"][0]

        if name:
            self.rename_disk(devnum=ret["devNum"], name=name)
        if mappings:
            self.add_map(name=name, devnum=ret["devNum"], lun=lun, mappings=mappings)
        lun_data = self.get_lu_data(devnum=ret["displayName"])[0]
        self.push_diskinfo(lun_data, name, size)
        mappings = {}
        for path in lun_data["Path"]:
            domain = path["domainID"]
            port = path["portName"]
            if domain not in self.domain_portname:
                continue
            if port not in self.port_portname:
                continue
            for hba_id in self.domain_portname[domain]:
                for tgt_id in self.port_portname[port]:
                    mappings[hba_id+":"+tgt_id] = {
                        "hba_id": hba_id,
                        "tgt_id": tgt_id,
                        "lun": int(path["LUN"]),
                    }
        results = {
            "disk_id": ".".join(lun_data["objectID"].split(".")[-2:]),
            "disk_devid": lun_data["displayName"],
            "mappings": mappings,
            "driver_data": {
                 "lu": lun_data,
            },
        }
        return results

    def resize_disk(self, devnum=None, size=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        if size == 0 or size is None:
            raise ex.excError("--size is mandatory")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="KB")
            data = self.get_logicalunit(devnum=devnum)
            current_size = int(data["capacityInKB"])
            size = str(current_size + incr)
        else:
            size = str(convert_size(size, _to="KB"))
        cmd = [
            "modifyvirtualvolume",
            "capacity="+size,
            "capacitytype=KB",
            "devnums="+str(devnum),
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)

    def del_disk(self, devnum=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        self.del_map(devnum=devnum)
        cmd = [
            "deletevirtualvolume",
            "devnums="+str(devnum),
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)
        self.del_diskinfo(devnum)

    def rename_disk(self, devnum=None, name=None, **kwargs):
        if devnum is None:
            raise ex.excError("--devnum is mandatory")
        if name is None:
            raise ex.excError("--name is mandatory")
        devnum = self.to_devnum(devnum)
        cmd = [
            "modifylabel",
            "devnums="+str(devnum),
            "label="+str(name),
        ]
        out, err, ret = self.cmd(cmd, xml=False)
        if ret != 0:
            raise ex.excError(err)
        data = self.parse(out)
        return data[0]

    def del_diskinfo(self, disk_id):
        if disk_id in (None, ""):
            return
        if self.node is None:
            return
        try:
            ret = self.node.collector_rest_delete("/disks/%s" % disk_id)
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in ret:
            raise ex.excError(ret["error"])
        return ret

    def push_diskinfo(self, data, name, size):
        if self.node is None:
            return
        try:
            ret = self.node.collector_rest_post("/disks", {
                "disk_id": self.serial+"."+str(data["devNum"]),
                "disk_devid": data["devNum"],
                "disk_name": str(name),
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": self.get_pool_by_id(data["dpPoolID"]),
            })
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(ret["error"])
        return ret

def do_action(action, array_name=None, node=None, **kwargs):
    o = Arrays()
    array = o.get_array(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
    array.node = node
    if not hasattr(array, action):
        raise ex.excError("not implemented")
    ret = getattr(array, action)(**kwargs)
    if ret is not None:
        print(json.dumps(ret, indent=4))

def main(argv, node=None):
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    options, action = parser.parse_args(argv)
    kwargs = vars(options)
    do_action(action, node=node, **kwargs)

if __name__ == "__main__":
    try:
        ret = main(sys.argv)
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1
    except IOError as exc:
        if exc.errno == 32:
            # broken pipe
            ret = 1
        else:
            raise
    sys.exit(ret)


