from __future__ import print_function

import logging
import os
import sys
import json
from xml.etree.ElementTree import fromstring

import core.exceptions as ex
from utilities.converters import convert_size
from core.node import Node
from env import Env
from utilities.optparser import OptParser, Option
from utilities.naming import factory, split_path
from utilities.lazy import lazy
from utilities.storage import Storage
from utilities.proc import justcall, which

PROG = "om array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in the node or cluster configuration."),
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
    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = objects
        self.filtering = len(objects) > 0
        if node:
            self.node = node
        else:
            self.node = Node()
        done = []
        for s in self.node.conf_sections(cat="array"):
            try:
                name = self.node.oget(s, "name")
            except Exception:
                name = None
            if not name:
                name = s.split("#", 1)[-1]
            if name in done:
                continue
            if self.filtering and name not in self.objects:
                continue
            try:
                stype = self.node.oget(s, "type")
            except:
                continue
            if stype != "hds":
                continue
            try:
                bin = self.node.oget(s, 'bin')
                jre_path = self.node.oget(s, 'jre_path')
                url = self.node.oget(s, 'url')
                username = self.node.oget(s, 'username')
                password = self.node.oget(s, 'password')
            except Exception as exc:
                print("error parsing section %s: %s" % (s, exc), file=sys.stderr)
                continue

            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue

            self.arrays.append(Array(name, url, username, password, bin=bin, jre_path=jre_path, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

    def get_array(self, name):
        for array in self.arrays:
            if array.name == name:
                return array
        return None

class Array(object):
    def __init__(self, name, url, username, password, bin=None, jre_path=None, node=None):
        self.keys = ['array', 'lu', 'arraygroup', 'port', 'pool']
        self.name = name
        self.node = node
        self.jre_path = jre_path
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
        self.log = logging.getLogger(Env.nodename+".array.sym."+self.name)
        self.journal = []

    def cmd(self, cmd, scoped=True, xml=True, log=False):
        if self.jre_path:
            os.environ["HDVM_CLI_JRE_PATH"] = self.jre_path

        if which(self.bin) is None:
            raise ex.Error("Can not find %s"%self.bin)
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
        if log:
            _l = [] + l
            _l[6] = "xxxx"
            self.log.info(" ".join(_l))
            self.log_cmd(_l)
        out, err, ret = justcall(l)
        if log:
            self.log_result(out, err)
        if ret != 0:
            self.log.error(err)
            raise ex.Error(err)
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

    def get_domain_used_lun_ids(self, domain):
        return [int(path["LUN"]) for path in domain["Path"]]

    def get_free_lun_id(self, used_lun_ids):
        for lun_id in range(65536):
            if lun_id not in used_lun_ids:
                return lun_id

    def get_domains(self, hba_id, tgt_id):
        domains = []
        port = self.get_target_port(tgt_id)
        if port is None:
            return domains
        for domain in self.domain_data:
            for wwn in domain["WWN"]:
                if hba_id == wwn["WWN"] and domain["portName"] == port["displayName"]:
                    domains.append(domain)
        return domains

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
        internal_mappings = []
        used_lun_ids = set()
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            for tgt_id in targets:
                for domain_data in self.get_domains(hba_id, tgt_id):
                    used_lun_ids |= set(self.get_domain_used_lun_ids(domain_data))
                    domain = domain_data["domainID"]
                    portname = domain_data["portName"]
                    _mapping = {
                        "domain": domain_data["domainID"],
                        "portname": domain_data["portName"],
                    }
                    if _mapping in internal_mappings:
                        continue
                    internal_mappings.append(_mapping)
        lun = self.get_free_lun_id(used_lun_ids)
        for idx, mapping in enumerate(internal_mappings):
            internal_mappings[idx]["lun"] = lun
        return internal_mappings

    def del_map(self, devnum=None, mappings=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        results = []
        if mappings is not None:
            internal_mappings = self.translate_mappings(mappings)
        else:
            internal_mappings = []
            for dom in self.domain_data:
                for path in dom["Path"]:
                    if devnum == path["devNum"]:
                        mapping = {
                            "domain": path["domainID"],
                            "portname": path["portName"],
                        }
                        if mapping not in internal_mappings:
                            internal_mappings.append(mapping)

        for mapping in internal_mappings:
            result = self._del_map(devnum=devnum, domain=mapping["domain"],
                                   portname=mapping["portname"], **kwargs)
            if result is not None:
                results.append(result)
        if len(results) > 0:
            return results

    def _del_map(self, devnum=None, domain=None, portname=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        if domain is None:
            raise ex.Error("--domain is mandatory")
        if portname is None:
            raise ex.Error("--portname is mandatory")
        cmd = [
            "deletelun",
            "devnum="+str(devnum),
            "portname="+portname,
            "domain="+domain,
        ]
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)
        return {}

    def add_map(self, devnum=None, mappings=None, lun=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        if mappings is None:
            raise ex.Error("--mappings is mandatory")
        results = []
        if mappings is not None:
            internal_mappings = self.translate_mappings(mappings)
            for mapping in internal_mappings:
                if lun is None:
                    lun = mapping["lun"]
                result = self._add_map(devnum=devnum, domain=mapping["domain"],
                                       portname=mapping["portname"], lun=lun,
                                       **kwargs)
                if result is not None:
                    results.append(result)
        if len(results) > 0:
            return results

    def _add_map(self, devnum=None, domain=None, portname=None, lun=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        if domain is None:
            raise ex.Error("--domain is mandatory")
        if portname is None:
            raise ex.Error("--portname is mandatory")
        if lun is None:
            raise ex.Error("--lun is mandatory")
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
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)
        data = self.parse(out)
        return data[0]["Path"][0]

    def log_cmd(self, cmd):
        self.journal.append([0, " ".join(cmd), {}])

    def log_result(self, out="", err=""):
        for line in out.strip().splitlines():
            self.journal.append([0, line, {}])
        for line in err.strip().splitlines():
            self.journal.append([1, line, {}])

    def add_disk(self, name=None, pool=None, size=None, lun=None, mappings=None, **kwargs):
        if pool is None:
            raise ex.Error("--pool is mandatory")
        if size == 0 or size is None:
            raise ex.Error("--size is mandatory")
        pool_id = self.get_pool_by_name(pool)["poolID"]
        cmd = [
            "addvirtualvolume",
            "capacity="+str(convert_size(size, _to="KB")),
            "capacitytype=KB",
            "poolid="+str(pool_id),
        ]
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)
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
            raise ex.Error("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        if size == 0 or size is None:
            raise ex.Error("--size is mandatory")
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
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)

    def del_disk(self, devnum=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        devnum = self.to_devnum(devnum)
        self.del_map(devnum=devnum)
        cmd = [
            "deletevirtualvolume",
            "devnums="+str(devnum),
        ]
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)
        self.del_diskinfo(devnum)

    def rename_disk(self, devnum=None, name=None, **kwargs):
        if devnum is None:
            raise ex.Error("--devnum is mandatory")
        if name is None:
            raise ex.Error("--name is mandatory")
        devnum = self.to_devnum(devnum)
        cmd = [
            "modifylabel",
            "devnums="+str(devnum),
            "label="+str(name),
        ]
        out, err, ret = self.cmd(cmd, xml=False, log=True)
        if ret != 0:
            raise ex.Error(err)
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
            raise ex.Error(str(exc))
        if "error" in ret:
            self.log.error("failed to delete the disk object in the collector: %s", ret["error"])
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
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(ret["error"])
        return ret

def do_action(action, array_name=None, node=None, **kwargs):
    o = Arrays()
    array = o.get_array(array_name)
    if array is None:
        raise ex.Error("array %s not found" % array_name)
    array.node = node
    node.logger.handlers[1].setLevel(logging.CRITICAL)
    if not hasattr(array, action):
        raise ex.Error("not implemented")
    try:
        ret = getattr(array, action)(**kwargs)
    except ex.Error as exc:
        ret = {
            "log": array.journal,
        }
        print(json.dumps(ret, indent=4))
        raise
    if ret is not None:
        ret["log"] = array.journal
        print(json.dumps(ret, indent=4))
    return ret

def main(argv, node=None):
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    options, action = parser.parse_args(argv)
    kwargs = vars(options)
    try:
        do_action(action, node=node, **kwargs)
        return 0
    except Exception:
        return 1

if __name__ == "__main__":
    try:
        main(sys.argv)
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
    except IOError as exc:
        if exc.errno == 32:
            # broken pipe
            sys.exit(1)
        else:
            raise


