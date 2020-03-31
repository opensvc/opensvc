from __future__ import print_function

import sys
import json
import time

import core.exceptions as ex
from utilities.storage import Storage
from utilities.naming import factory, split_path
from utilities.converters import convert_size
from utilities.optparser import OptParser, Option
from core.node import Node

ERRCODE_MAPPING_HOST_LUN_NOT_EXISTS = 1073804587
ERRCODE_MAPPING_HOST_LUN_EXISTS = 1073804588
ERRCODE_SESSION_TOO_MANY = 1077949067

OBJTYPE_LUN = 11
OBJTYPE_HOSTGROUP = 14
OBJTYPE_HOST = 21
OBJTYPE_SNAPSHOT = 27
OBJTYPE_FC_PORT = 212
OBJTYPE_ETH_PORT = 213
OBJTYPE_REMOTEDISK = 224
OBJTYPE_SMARTQOSPOLICY = 230
OBJTYPE_ISCSILINK = 243
OBJTYPE_MAPPINGVIEW = 245
OBJTYPE_LUNGROUP = 256
OBJTYPE_PORTGROUP = 257
OBJTYPE_MAPPING = 12345
OBJTYPE_SNAPSHOT_CG = 57646
OBJTYPE_CLONE = 57702
OBJTYPE_CLONE_CG = 57703

INITIATOR_TYPE_ISCSI = 222
INITIATOR_TYPE_FC = 223

MAPPING_TYPE_UNMAPPED = 0
MAPPING_TYPE_HOST = 1
MAPPING_TYPE_HOSTGROUP = 2
MAPPING_TYPE_LUN = 3
MAPPING_TYPE_LUNGROUP = 4

OS_LINUX = 0
OS_WINDOWS = 1
OS_SOLARIS = 2
OS_HPUX = 3
OS_AIX = 4
OS_XENSERVER = 5
OS_DARWIN = 6
OS_VMWARE = 7
OS_LINUX_VIS = 8
OS_WINDOWSSERVER = 9
OS_ORACLEVM = 10
OS_VMS = 11
OS_OVMSERVERX86 = 12
OS_OVMSERVERSPARC = 13

HYPERMETRO_PATH_OPTIMIZED_NO = 0
HYPERMETRO_PATH_OPTIMIZED_YES = 1

try:
    import requests
except ImportError:
    raise ex.InitError("the requests module must be installed")

try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass

VERIFY = False

PROG = "om array"
OPT = Storage({
    "help": Option(
        "-h", "--help", default=None, action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", default=None, action="store", dest="array_name",
        help="The name of the array, as defined in node or cluster configuration."),
    "name": Option(
        "--name", default=None, action="store", dest="name",
        help="The object name"),
    "storagepool": Option(
        "--storagepool", default=None, action="store", dest="storagepool",
        help="The storagepool to create the disk into"),
    "size": Option(
        "--size", default="0", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "target": Option(
        "--target", action="append", dest="targets",
        help="A target name to export the disk through. Can be set multiple times."),
    "blocksize": Option(
        "--blocksize", default=512, type=int, action="store", dest="blocksize",
        help="The exported disk blocksize in B"),
    "compression": Option(
        "--compression", default=False, action="store_true", dest="compression",
        help="Toggle compression"),
    "dedup": Option(
        "--dedup", default=False, action="store_true", dest="dedup",
        help="Toggle deduplication"),
    "naa": Option(
        "--naa", default=None, action="store", dest="naa",
        help="The disk naa identifier"),
    "hypermetrodomain": Option(
        "--hypermetrodomain", default=None, action="store", dest="hypermetrodomain",
        help="Create the LUN as HyperMetro pair, and use this domain."),
    "initiator": Option(
        "--initiator", action="append", dest="initiators",
        help="An initiator iqn. Can be specified multiple times."),
    "comment": Option(
        "--comment", action="store", dest="comment",
        help="Description for your reference"),
    "lun": Option(
        "--lun", action="store", type=int, dest="lun",
        help="The logical unit number to assign to the extent on attach to a target. If not specified, a free lun is automatically assigned."),
    "id": Option(
        "--id", action="store", type=int, dest="id",
        help="An object id, as reported by a list action"),
    "alias": Option(
        "--alias", action="store", dest="alias",
        help="An object name alias"),
    "target": Option(
        "--target", action="append", dest="target",
        help="The target object iqn"),
    "target_id": Option(
        "--target-id", action="store", type=int, dest="target_id",
        help="The target object id"),
    "authgroup_id": Option(
        "--auth-group-id", action="store", type=int, dest="authgroup_id",
        help="The auth group object id"),
    "authtype": Option(
        "--auth-type", action="store", default="None", dest="authtype",
        choices=["None", "CHAP", "CHAP Mutual"],
        help="None, CHAP, CHAP Mutual"),
    "portal_id": Option(
        "--portal-id", action="store", type=int, dest="portal_id",
        help="The portal object id"),
    "initiatorgroup": Option(
        "--initiatorgroup", action="append", dest="initiatorgroup",
        help="The initiator group object id"),
    "initiatorgroup_id": Option(
        "--initiatorgroup-id", action="store", type=int, dest="initiatorgroup_id",
        help="The initiator group object id"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_disk": {
            "msg": "Create and map a lun.",
            "options": [
                OPT.name,
                OPT.storagepool,
                OPT.size,
                OPT.target,
                OPT.mappings,
                OPT.lun,
                OPT.compression,
                OPT.dedup,
                OPT.hypermetrodomain,
            ],
        },
        "add_lun": {
            "msg": "Create a lun.",
            "options": [
                OPT.name,
                OPT.storagepool,
                OPT.size,
                OPT.target,
                OPT.lun,
                OPT.compression,
                OPT.dedup,
            ],
        },
        "map": {
            "msg": "Map a lun to the specified initiator:target links",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
                OPT.mappings,
                OPT.lun,
            ],
        },
    },
    "Delete actions": {
        "del_disk": {
            "msg": "Delete and unmap a lun.",
            "options": [
                OPT.id,
                OPT.name,
                OPT.naa,
            ],
        },
        "unmap": {
            "msg": "Unmap the specified initiator:target links",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
                OPT.mappings,
            ],
        },
    },
    "Modify actions": {
        "resize_disk": {
            "msg": "Resize a lun",
            "options": [
                OPT.id,
                OPT.name,
                OPT.naa,
                OPT.size,
            ],
        },
    },
    "List actions": {
        "list_mappings": {
            "msg": "List host ports a disk is mapped to.",
            "options": [
                OPT.id,
                OPT.name,
                OPT.naa,
            ],
        },
        "list_storagepool": {
            "msg": "List configured storage pools.",
        },
        "list_portgroup": {
            "msg": "List configured port groups",
        },
        "list_fc_port": {
            "msg": "List configured fibre channel ports",
        },
        "list_eth_port": {
            "msg": "List configured ethernet ports",
        },
        "list_bond_port": {
            "msg": "List configured ethernet bonded ports",
        },
        "list_sas_port": {
            "msg": "List configured serial attached scsi ports",
        },
        "list_lun": {
            "msg": "List configured extents",
        },
        "list_hostgroup": {
            "msg": "List configured host groups",
        },
        "list_host": {
            "msg": "List configured host",
        },
        "list_host_link": {
            "msg": "List configured host links",
        },
    },
    "Show actions": {
        "show_system": {
            "msg": "Show system information.",
        },
        "show_lun": {
            "msg": "Show configured storage pools.",
            "options": [
                OPT.id,
                OPT.name,
                OPT.naa,
            ],
        },
        "show_storagepool": {
            "msg": "Show configured storage pools.",
            "options": [
                OPT.name,
            ],
        },
    },
}

class Dorados(object):
    arrays = []


    def __init__(self, objects=None, node=None):
        if objects is None:
            objects = []
        self.objects = objects
        self.filtering = len(objects) > 0
        self.timeout = 10
        if node:
            self.node = node
        else:
            self.node = Node()
        done = []
        for s in self.node.conf_sections(cat="array"):
            try:
                name = self.node.oget(s, 'name')
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
            if stype != "dorado":
                continue
            timeout = self.node.oget(s, "timeout")
            try:
                username = self.node.oget(s, "username")
                password = self.node.oget(s, "password")
                api = self.node.oget(s, "api")
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue
            self.arrays.append(Dorado(name, api, username, password, timeout, node=self.node))
            done.append(name)


    def __iter__(self):
        for array in self.arrays:
            yield(array)


    def get_dorado(self, name):
        for array in self.arrays:
            if array.name == name:
                return array
        return None

class Dorado(object):

    def __init__(self, name, api, username, password, timeout, node=None):
        self.node = node
        self.name = name
        self.api = api.rstrip("/")
        self.username = username
        self.password = password
        self.auth = (username, password)
        self.timeout = timeout
        self.session = requests.Session()
        self.session_data = None
        self.keys = ["system",
                     "storagepools",
                     "fc_ports",
                     "luns"]


    def urlpath_device(self, device=None):
        return "/deviceManager/rest/%s" % (device if device else self.name)


    def headers(self, auth=True):
        data = {
            "Content-Type": "application/json",
        }
        if not auth:
            return data
        if not self.session_data:
            self.open_session()
        data["iBaseToken"] = self.session_data["iBaseToken"]
        return data


    def delete(self, uri, data=None):
        api = self.api + self.urlpath_device() + uri
        headers = self.headers()
        if data:
            data = json.dumps(data)
        r = self.session.delete(api, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r.json()


    def put(self, uri, data=None):
        api = self.api + self.urlpath_device() + uri
        headers = self.headers()
        if data:
            data = json.dumps(data)
        r = self.session.put(api, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r.json()


    def post(self, uri, data=None, auth=True):
        api = self.api + self.urlpath_device() + uri
        headers = self.headers(auth=auth)
        if data:
            data = json.dumps(data)
        r = self.session.post(api, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r.json()


    def get(self, uri, params=None):
        api = self.api + self.urlpath_device() + uri
        headers = self.headers()
        r = self.session.get(api, params=params, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r.json()


    def open_session(self, timeout=30):
        d = {
            "username": self.auth[0],
            "password": self.auth[1],
            "scope": 0,
        }
        retries = 0
        while True:
            data = self.post("/sessions", data=d, auth=False)
            code = data.get("error", {}).get("code")
            if code == ERRCODE_SESSION_TOO_MANY and retries < timeout:
                retries += 1
                time.sleep(1)
                continue
            elif code:
                raise ex.Error("open_session error: %s => %s" % ((self.auth[0], "xxx"), data.get("error")))
            self.session_data = data["data"]
            return


    def close_session(self):
        if not self.session_data:
            return
        self.delete("/sessions")
        self.session_data = None


    def get_system(self):
        return self.get("/system/")["data"]


    def get_luns(self):
        return self.get("/lun")["data"]


    def get_hypermetrodomain(self, name):
        d = {
            "filter": "NAME::%s" % name,
        }
        data = self.get("/HyperMetroDomain", params=d)
        try:
            return data["data"][0]
        except (KeyError, IndexError):
            return


    def get_host_fc_links(self, oid):
        d = {
            "PARENTID": oid,
            "INITIATOR_TYPE": INITIATOR_TYPE_FC,
        }
        data = self.get("/host_link", params=d)
        return data["data"]


    def get_host(self):
        data = self.get("/host")
        try:
            return data["data"]
        except KeyError:
            return


    def get_hostgroup(self):
        data = self.get("/hostgroup")
        try:
            return data["data"]
        except KeyError:
            return


    def get_host_link(self):
        data = self.get("/host_link")
        try:
            return data["data"]
        except KeyError:
            return


    def get_fc_initiator(self, hba_id):
        data = self.get("/fc_initiator/%s" % hba_id)
        try:
            return data["data"]
        except KeyError:
            return


    def get_host_fc_initiators(self, host_id):
        d = {
            "filter": "PARENTID::%s" % host_id,
        }
        data = self.get("/fc_initiator", params=d)
        try:
            return data["data"]
        except KeyError:
            return


    def get_host_link_by_parentid(self, parentid):
        d = {
            "PARENTID": parentid,
        }
        data = self.get("/host_link", params=d)
        try:
            return data["data"]
        except KeyError:
            return


    def get_host_link_by_hba_id(self, hba_id, hba_type):
        d = {
            "INITIATOR_TYPE": hba_type,
            "INITIATOR_PORT_WWN": hba_id,
        }
        data = self.get("/host_link", params=d)
        try:
            return data["data"]
        except KeyError:
            return


    def get_storagepool_by_id(self, oid=None):
        data = self.get("/storagepool?filter=ID::%s" % oid)
        try:
            return data["data"][0]
        except KeyError:
            return


    def get_storagepool_by_name(self, name=None):
        data = self.get("/storagepool?filter=NAME::%s" % name)
        try:
            return data["data"][0]
        except KeyError:
            return


    def get_storagepool_id(self, name=None):
        return self.get_storagepool_by_name(name=name)["ID"]


    def get_lun(self, oid=None, name=None, naa=None):
        if oid:
            data = self.get("/lun?filter=ID::%s" % oid)
        elif name:
            data = self.get("/lun?filter=NAME::%s" % name)
        elif naa:
            data = self.get("/lun?filter=WWN::%s" % naa)
        else:
            raise ex.Error("oid, name or naa must be specified to get_lun()")
        try:
            return data["data"][0]
        except KeyError:
            return


    def get_lun_hosts(self, oid):
        d = {
            "ASSOCIATEOBJTYPE": OBJTYPE_LUN,
            "ASSOCIATEOBJID": oid,
        }
        data = self.get("/host/associate", params=d)
        try:
            return data["data"]
        except KeyError:
            return


    def get_lun_mappings(self, oid):
        d = {
            "ASSOCIATEOBJTYPE": OBJTYPE_LUN,
            "ASSOCIATEOBJID": oid,
        }
        data = self.get("/mapping/associate", params=d)
        return data["data"]


    def get_fc_port_associate(self, otype, oid):
        d = {
            "ASSOCIATEOBJTYPE": otype,
            "ASSOCIATEOBJID": oid,
        }
        data = self.get("/fc_port/associate", params=d)
        return data["data"]


    def get_hostgroup_hosts(self, oid):
        d = {
            "ASSOCIATEOBJTYPE": OBJTYPE_HOSTGROUP,
            "ASSOCIATEOBJID": oid,
        }
        data = self.get("/host/associate", params=d)
        return data["data"]


    def get_portgroup_fc_ports(self, oid):
        return self.get_fc_port_associate(OBJTYPE_PORTGROUP, oid)


    def get_storagepools(self):
        return self.get("/storagepool")["data"]


    def get_portgroups(self):
        return self.get("/portgroup")["data"]


    def get_fc_ports(self):
        return self.get("/fc_port")["data"]


    def get_eth_ports(self):
        return self.get("/eth_port")["data"]


    def get_bond_ports(self):
        return self.get("/bond_port")["data"]


    def get_sas_ports(self):
        return self.get("/sas_port")["data"]


    def del_mapping(self, mapping):
        path = '/mapping'
        d = {}
        for key in ("hostId", "hostGroupId", "lunId", "lunGroupId"):
            if mapping[key] != "":
                d[key] = int(mapping[key])
        data = self.delete(path, d)
        code = data.get("error", {}).get("code")
        if code and code != ERRCODE_MAPPING_HOST_LUN_NOT_EXISTS:
            raise ex.Error("delete error: %s %s %s" % (path, d, data.get("error")))
        return data["data"]


    def del_lun(self, oid):
        path = '/lun/%s' % oid
        data = self.delete(path)
        if data.get("error", {}).get("code"):
            raise ex.Error("delete error: %s %s" % (path, data.get("error")))
        return data["data"]


    def add_lun(self, name=None, size=None, storagepool=None,
                compression=True, dedup=True,
                **kwargs):
        for key in ["name", "size", "storagepool"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        size = convert_size(size, _to="B") // 512
        path = "/lun"
        d = {
            "NAME": name,
            "PARENTID": self.get_storagepool_id(storagepool),
            "CAPACITY": size,
            "MSGRETURNTYPE": 1, # sync
            "ALLOCTYPE": 1, # thin
            "ENABLECOMPRESSION": compression,
            "ENABLESMARTDEDUP": dedup,
        }
        data = self.post(path, d)
        if data.get("error", {}).get("code"):
            raise ex.Error("add lun error: %s => %s" % (d, data.get("error")))
        return data["data"]


    def list_mappings(self, id=None, name=None, naa=None, **kwargs):
        lun_data = self.get_lun(oid=id, name=name, naa=naa)
        if lun_data is None:
            raise ex.Error("lun not found")
        return self._list_mappings(lun_data["ID"], lun_data["WWN"])
        

    def get_mappings(self, lun_id):
        lun_mappings = self.get_lun_mappings(lun_id)
        mappings = {}
        for d in lun_mappings:
            if d["portGroupId"] == "":
                targets = self.list_fc_port()
            else:
                targets = self.get_portgroup_fc_ports(d["portGroupId"])
            targets = [t["WWN"] for t in targets]
            if d["hostGroupId"] == "":
                hbas = self.get_host_fc_initiators(d["hostId"])
            else:
                hbas = []
                for host in self.get_hostgroup_hosts(d["hostGroupId"]):
                    hbas += self.get_host_fc_initiators(host["ID"])
            hbas = [h["ID"] for h in hbas]
            for hba_id in hbas:
                for tgt_id in targets:
                    mappings[hba_id+":"+tgt_id+":"+lun_id] = {
                       "mapping": d,
                       "tgt_id": tgt_id,
                       "hba_id": hba_id,
                    }
        return mappings


    def _list_mappings(self, lun_id, disk_id):
        lun_mappings = self.get_lun_mappings(lun_id)
        mappings = {}
        for d in lun_mappings:
            if d["portGroupId"] == "":
                targets = self.list_fc_port()
            else:
                targets = self.get_portgroup_fc_ports(d["portGroupId"])
            targets = [t["WWN"] for t in targets]
            if d["hostGroupId"] == "":
                hbas = self.get_host_fc_initiators(d["hostId"])
            else:
                hbas = []
                for host in self.get_hostgroup_hosts(d["hostGroupId"]):
                    hbas += self.get_host_fc_initiators(host["ID"])
            hbas = [h["ID"] for h in hbas]
            for hba_id in hbas:
                for tgt_id in targets:
                    mappings[hba_id+":"+tgt_id+":"+disk_id] = {
                       "mapping": d,
                       "disk_id": disk_id,
                       "tgt_id": tgt_id,
                       "hba_id": hba_id,
                    }
        return mappings


    def resize_disk(self, id=None, name=None, naa=None, size=None, **kwargs):
        if size is None:
            raise ex.Error("'size' key is mandatory")
        if name is None and naa is None:
            raise ex.Error("'name' or 'naa' must be specified")
        lun_data = self.get_lun(oid=id, name=name, naa=naa)
        if lun_data is None:
            raise ex.Error("extent not found")
        storagepool = self.get_storagepool_by_id(lun_data["PARENTID"])
        if storagepool is None:
            raise ex.Error("storagepool not found")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="B") // 512
            current_size = int(lun_data["ALLOCCAPACITY"]) * 512
            size = current_size + incr
        else:
            size = convert_size(size, _to="B") // 512

        d = {
            "CAPACITY": size,
            "ID": lun_data["ID"],
        }
        data = self.put("/lun/expand", d)
        if data.get("error", {}).get("code"):
            raise ex.Error("expand_lun error: %s => %s" % ((lun_data["ID"], size), data.get("error")))
        return data["data"]


    def del_hostgroup(self, oid=None, **kwargs):
        if oid is None:
            raise ex.Error("'id' is mandatory")
        response = self.delete('/hostgroup/%s' % oid)
        if response.status_code != 200:
            raise ex.Error(str(response))


    def add_hostgroup(self, name=None, desc=None, hosts=None, **kwargs):
        if name is None:
            raise ex.Error("name is mandatory")
        d = {
            "NAME": name,
        }
        if desc:
            d["DESCRIPTION"] = desc

        data = self.post('/hostgroup', d)
        hostgroup = data["data"]
        for host in hosts:
            self.associate_hostgroup_host(hostgroup["ID"], host)
        return hostgroup


    def map_lun_to_host(self, lun_id, host_id, hostlun_id=None):
        d = {
            "lunId": lun_id,
            "hostId": host_id,
        }
        if hostlun_id:
            d["hostLunIdStart"] = hostlun_id
        data = self.post('/mapping', d)
        code = data.get("error", {}).get("code")
        if code and code != ERRCODE_MAPPING_HOST_LUN_EXISTS:
            raise ex.Error("map_lun_to_host error: %s => %s" % ((lun_id, host_id), data.get("error")))
        return data["data"]


    def pair_luns(self, local_id, remote_id, domain):
        domain_id = self.get_hypermetrodomain(domain)["ID"]
        return self.add_hypermetropair(local_id, remote_id, domain_id)


    def pause_lun_hypermetropair(self, oid):
        hypermetropair = self.lun_hypermetropair(oid)
        if not hypermetropair:
            return
        self.pause_hypermetropair(hypermetropair["ID"])
        return hypermetropair


    def pause_hypermetropair(self, oid):
        d = {
            "ID": oid,
        }
        data = self.put("/HyperMetroPair/disable_hcpair", d)
        if data.get("error", {}).get("code"):
            raise ex.Error("pause_hypermetropair error: %s => %s" % (oid, data.get("error")))
        return data["data"]


    def synchronize_hypermetropair(self, oid):
        d = {
            "ID": oid,
        }
        data = self.put("/HyperMetroPair/synchronize_hcpair", d)
        if data.get("error", {}).get("code"):
            raise ex.Error("synchronize_hypermetropair error: %s => %s" % (oid, data.get("error")))
        return data["data"]


    def synchronize_lun_hypermetropair(self, oid):
        hypermetropair = self.lun_hypermetropair(oid)
        if not hypermetropair:
            return
        self.synchronize_hypermetropair(hypermetropair["ID"])
        return hypermetropair


    def del_lun_hypermetropair(self, oid):
        hypermetropair = self.lun_hypermetropair(oid)
        if not hypermetropair:
            return
        self.del_hypermetropair(hypermetropair["ID"])
        return hypermetropair


    def lun_hypermetropair(self, oid):
        hypermetropair = self.get_hypermetropair("LOCALOBJID", oid)
        if not hypermetropair:
            hypermetropair = self.get_hypermetropair("REMOTEOBJID", oid)
        return hypermetropair


    def del_hypermetropair(self, oid):
        data = self.delete('/HyperMetroPair/%s' % oid)
        if data.get("error", {}).get("code"):
            raise ex.Error("del_hypermetropair error: %s => %s" % (oid, data.get("error")))
        return data["data"]


    def add_hypermetropair(self, local_id, remote_id, domain_id):
        d = {
            "DOMAINID": domain_id,
            "HCRESOURCETYPE": 1,
            "LOCALOBJID": local_id,
            "REMOTEOBJID": remote_id,
            "ISFIRSTSYNC": True,
        }
        data = self.post('/HyperMetroPair', d)
        if data.get("error", {}).get("code"):
            raise ex.Error("add_hypermetropair error: %s => %s" % ((local_id, remote_id, domain_id), data.get("error")))
        return data["data"]


    def get_hypermetropair(self, key, oid):
        d = {
            "filter": "%s::%s" % (key, oid),
        }
        data = self.get('/HyperMetroPair', params=d)
        if data.get("error", {}).get("code"):
            raise ex.Error("get_hypermetropair error: %s => %s" % ((key, oid), data.get("error")))
        try:
            return data["data"][0]
        except (KeyError, IndexError):
            return


    def associate_hostgroup_host(self, hostgroup_id, host_id):
        d = {
            "ID": hostgroup_id,
            "ASSOCIATEOBJTYPE": OBJTYPE_HOST,
            "ASSOCIATEOBJID": str(host_id),
        }
        data = self.post('/hostgroup/associate', d)
        if data.get("error", {}).get("code"):
            raise ex.Error("associate_hostgroup_host error: %s => %s" % ((hostgroup_id, host_id), data.get("error")))
        return data["data"]


    def del_disk(self, id=None, name=None, naa=None, **kwargs):
        if id is None and name is None and naa is None:
            raise ex.Error("'id', 'name' or 'naa' must be specified")
        data = self.get_lun(oid=id, name=name, naa=naa)
        if data is None:
            return
        results = {}
        response = self.pause_lun_hypermetropair(data["ID"])
        results["pause_hypermetropair"] = response
        response = self._unmap_lun(data["ID"])
        results["unmap"] = response
        response = self.del_lun_hypermetropair(data["ID"])
        results["del_hypermetropair"] = response
        response = self.del_lun(data["ID"])
        results["del_lun"] = data
        return results


    def hbagroup_by_targetgroup(self, mappings):
        data = {}
        for mapping in mappings:
            hba, targets = mapping.split(":", 1)
            targets = ",".join(sorted(targets.split(",")))
            if targets not in data:
                data[targets] = set([hba])
            else:
                data[targets].add(hba)
        l = []
        for targets in data:
            t = targets.split(",")
            h = sorted(list(data[targets]))
            l.append((h, t))
        return l


    def translate_mappings(self, mappings):
        targets = set()
        for mapping in mappings:
            elements = mapping.split(":", 1)
            targets |= set((elements[-1]).split(","))
        targets = list(targets)
        return targets


    def split_mappings(self, mappings):
        data = []
        for mapping in mappings:
            elements = mapping.split(":", 1)
            for target in set((elements[-1]).split(",")):
                data.append((elements[0], target))
        return data


    def unmap_lun(self, id=None, name=None, naa=None, mappings=None, **kwargs):
        if not name and not naa and not id:
            raise ex.Error("'id', 'name' or 'naa' is mandatory")
        lun_data = self.get_lun(oid=id, name=name, naa=naa)
        if not lun_data:
            raise ex.Error("no lun found")
        return self._unmap_lun(lun_data["ID"], mappings)


    def _unmap_lun(self, lun_id, mappings=None):
        current_mappings = self.get_mappings(lun_id)
        results = []
        mappings_done = set()
        if mappings:
            toremove = self.split_mappings(mappings)
        else:
            toremove = None
        for d in current_mappings.values():
            key = (d["hba_id"], d["tgt_id"])
            if d["mapping"]["ID"] in mappings_done:
                continue
            if toremove is None or key in toremove:
                self.del_mapping(d["mapping"])
                mappings_done.add(d["mapping"]["ID"])
                results.append(d)
        return results


    def find_hosts(self, hbagroup):
        hosts = {}
        hba_ids = set()
        for hba_id in hbagroup:
            hba = self.get_fc_initiator(hba_id)
            if not hba:
                raise ex.Error("fc initiator %s not found" % hba_id)
            host_id = hba["PARENTID"]
            host_name = hba["PARENTNAME"]
            host_hbas = self.get_host_fc_initiators(host_id)
            valid_host = True
            for host_hba in host_hbas:
                valid_host &= host_hba["ID"] in hbagroup
            if not valid_host:
                print("invalid host:", host_name)
                continue
            hosts[host_id] = host_name
            hba_ids |= set([h["ID"] for h in host_hbas])
        if hba_ids != set(hbagroup):
            return
        return hosts


    def map_lun(self, id=None, name=None, naa=None, targets=None, mappings=None, lun=None, lun_data=None, **kwargs):
        if not name and not naa and not id:
            raise ex.Error("'id', 'name' or 'naa' is mandatory")
        if targets is None and mappings is None:
            raise ex.Error("'targets' or 'mappings' must be specified")

        if lun_data is None:
            lun_data = self.get_lun(oid=id, name=name, naa=naa)
        if lun_data is None:
            raise ex.Error("lun not found")

        results = []
        for hbagroup, targetgroup in self.hbagroup_by_targetgroup(mappings):
            hosts = self.find_hosts(hbagroup)
            if hosts is None:
                raise ex.Error("could not find a set of array hosts equivalent to: %s" % hbagroup)
            for host_id in hosts:
                result = self.map_lun_to_host(lun_id=lun_data["ID"], host_id=host_id, hostlun_id=lun)
                results.append(result)
        return results


    def add_disk(self, name=None, size=None, storagepool=None, targets=None,
                 mappings=None, compression=True, dedup=True, lun=None,
                 hypermetrodomain=None, **kwargs):
        for key in ["name", "size", "storagepool"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)

        # lun
        data = self.add_lun(name=name, size=size, storagepool=storagepool, compression=compression, dedup=dedup)

        if "WWN" not in data:
            raise ex.Error("no WWN in data")

        # mappings
        if mappings:
            mappings = self.map_lun(name=name, mappings=mappings, targets=targets, lun=lun, lun_data=data)

        # collector update
        warnings = []
        try:
            self.add_diskinfo(data, size, storagepool)
        except Exception as exc:
            warnings.append(str(exc))
        disk_id = data["WWN"]
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["ID"],
            "mappings": sorted(self.list_mappings(naa=disk_id).values(), key=lambda x: (x["hba_id"], x["tgt_id"], x["disk_id"])),
        }
        if warnings:
            results["warnings"] = warnings

        return results


    def show_system(self, **kwargs):
        return self.get_system()


    def show_storagepool(self, **kwargs):
        return self.get_storagepool_by_name(kwargs["name"])


    def list_storagepool(self, **kwargs):
        return self.get_storagepools()


    def list_host(self, **kwargs):
        return self.get_host()


    def list_hostgroup(self, **kwargs):
        return self.get_hostgroup()


    def list_host_link(self, **kwargs):
        return self.get_host_link()


    def show_lun(self, **kwargs):
        return self.get_lun(oid=kwargs.get("id"), name=kwargs.get("name"), naa=kwargs.get("naa"))


    def list_lun(self, **kwargs):
        return self.get_luns()


    def list_portgroup(self, **kwargs):
        return self.get_portgroups()


    def list_fc_port(self, **kwargs):
        return self.get_fc_ports()


    def list_eth_port(self, **kwargs):
        return self.get_eth_ports()


    def list_bond_port(self, **kwargs):
        return self.get_bond_ports()


    def list_sas_port(self, **kwargs):
        return self.get_sas_ports()


    def del_diskinfo(self, disk_id):
        if disk_id in (None, ""):
            return
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_delete("/disks/%s" % disk_id)
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in result:
            raise ex.Error(result["error"])
        return result


    def add_diskinfo(self, data, size=None, storagepool=None):
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_post("/disks", {
                "disk_id": data["WWN"],
                "disk_devid": data["ID"],
                "disk_name": data["NAME"],
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": storagepool,
            })
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(result["error"])
        return result


    # method aliases
    create_disk = add_disk
    unmap = unmap_lun
    map = map_lun


def do_action(action, array_name=None, node=None, **kwargs):
    o = Dorados()
    array = o.get_dorado(array_name)
    if array is None:
        raise ex.Error("array %s not found" % array_name)
    array.node = node
    if not hasattr(array, action):
        raise ex.Error("not implemented")
    result = getattr(array, action)(**kwargs)
    array.close_session()
    if result is not None:
        try:
            print(json.dumps(result, indent=4))
        except TypeError:
            print(json.dumps({"error": "unserializable result: %s" % result}, indent=4))


def main(argv, node=None):
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    options, action = parser.parse_args(argv)
    kwargs = vars(options)
    do_action(action, node=node, **kwargs)


def debug_on():
    try:
        import httplib
    except ImportError:
        import http.client as httplib
    import logging
    httplib.HTTPConnection.debuglevel = 1
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

#debug_on()

if __name__ == "__main__":
    try:
        main(sys.argv)
        ret = 0
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)


