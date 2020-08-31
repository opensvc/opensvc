from __future__ import print_function

import sys
import json

import core.exceptions as ex
from foreign.six.moves.urllib.parse import quote_plus # pylint: disable=import-error
from utilities.storage import Storage
from utilities.naming import factory, split_path
from utilities.converters import convert_size
from utilities.optparser import OptParser, Option
from utilities.string import bdecode
from core.node import Node

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
    "volume": Option(
        "--volume", default=None, action="store", dest="volume",
        help="The volume to create the disk into"),
    "size": Option(
        "--size", default="0", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "target": Option(
        "--target", action="append", dest="targets",
        help="A target name to export the disk through. Can be set multiple times."),
    "blocksize": Option(
        "--blocksize", default=512, type=int, action="store", dest="blocksize",
        help="The exported disk blocksize in B"),
    "secure_tpc": Option(
        "--secure-tpc", default=True, action="store_false", dest="insecure_tpc",
        help="Set the insecure_tpc flag to False"),
    "compression": Option(
        "--compression", default="inherit", action="store", dest="compression",
        choices=["off", "inherit", "lzjb", "lz4", "gzip", "gzip-9", "zle"],
        help="Toggle compression"),
    "sparse": Option(
        "--sparse", default=False, action="store_true", dest="sparse",
        help="Toggle zvol sparse mode."),
    "dedup": Option(
        "--dedup", default="off", action="store", dest="dedup", choices=["on", "off"],
        help="Toggle dedup"),
    "naa": Option(
        "--naa", default=None, action="store", dest="naa",
        help="The disk naa identifier"),
    "initiator": Option(
        "--initiator", action="append", dest="initiators",
        help="An initiator iqn. Can be specified multiple times."),
    "auth_network": Option(
        "--auth-network", default="", action="store", dest="auth_network",
        help="Network authorized to access to the iSCSI target. ip or cidr addresses or 'ALL' for any ips"),
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
    "authmethod": Option(
        "--auth-method", action="store", default="NONE", dest="authmethod",
        choices=["NONE", "CHAP", "CHAP Mutual"],
        help="NONE, CHAP, CHAP Mutual"),
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
        "add_iscsi_file": {
            "msg": "Add and present a file-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.volume,
                OPT.size,
                OPT.target,
                OPT.blocksize,
                OPT.secure_tpc,
                OPT.mappings,
                OPT.lun,
            ],
        },
        "add_iscsi_zvol": {
            "msg": "Add and present a zvol-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.volume,
                OPT.size,
                OPT.target,
                OPT.blocksize,
                OPT.secure_tpc,
                OPT.compression,
                OPT.sparse,
                OPT.dedup,
                OPT.mappings,
                OPT.lun,
            ],
        },
        "map_iscsi_zvol": {
            "msg": "Map an extent to the specified initiator:target links",
            "options": [
                OPT.name,
                OPT.target,
                OPT.mappings,
                OPT.lun,
            ],
        },
        "add_iscsi_initiatorgroup": {
            "msg": "Declare a group of iscsi initiator iqn, for use in targetgroups which are portal-target-initiator relations",
            "options": [
                OPT.initiator,
                OPT.comment,
                OPT.auth_network,
            ],
        },
        "add_iscsi_target": {
            "msg": "Declare a iscsi target, for use in targetgroups which are portal-target-initiator relations",
            "options": [
                OPT.name,
                OPT.alias,
            ],
        },
        "add_iscsi_targetgroup": {
            "msg": "Declare a iscsi target group, which is a portal-target-initiator relation",
            "options": [
                OPT.name,
                OPT.portal_id,
                OPT.target_id,
                OPT.target,
                OPT.initiatorgroup,
                OPT.initiatorgroup_id,
                OPT.authgroup_id,
                OPT.authmethod,
            ],
        },
    },
    "Delete actions": {
        "del_iscsi_file": {
            "msg": "Delete and unpresent a file-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.naa,
                OPT.volume,
            ],
        },
        "del_iscsi_zvol": {
            "msg": "Delete and unpresent a zvol-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.naa,
                OPT.volume,
            ],
        },
        "unmap": {
            "msg": "Unmap the specified initiator:target links",
            "options": [
                OPT.mappings,
            ],
        },
        "unmap_iscsi_zvol": {
            "msg": "Unmap an extent from the specified initiator:target links",
            "options": [
                OPT.name,
                OPT.mappings,
            ],
        },
        "del_iscsi_initiatorgroup": {
            "msg": "Delete a group of iscsi initiator iqn, used in targets which are portal-target-initiator relations",
            "options": [
                OPT.id,
            ],
        },
        "del_iscsi_target": {
            "msg": "Delete a iscsi target, used in targets which are portal-target-initiator relations",
            "options": [
                OPT.id,
                OPT.name,
            ],
        },
        "del_iscsi_targetgroup": {
            "msg": "Delete a iscsi target group, which is a portal-target-initiator relation",
            "options": [
                OPT.id,
            ],
        },
    },
    "Modify actions": {
        "resize_zvol": {
            "msg": "Resize a zvol",
            "options": [
                OPT.name,
                OPT.naa,
                OPT.size,
            ],
        },
    },
    "List actions": {
        "list_mappings": {
            "msg": "List configured volumes",
            "options": [
                OPT.name,
                OPT.naa,
            ],
        },
        "list_volume": {
            "msg": "List configured volumes",
        },
        "list_iscsi_portal": {
            "msg": "List configured portals",
        },
        "list_iscsi_target": {
            "msg": "List configured targets",
        },
        "list_iscsi_targettoextent": {
            "msg": "List configured target-to-extent relations",
        },
        "list_iscsi_extent": {
            "msg": "List configured extents",
        },
        "list_iscsi_initiatorgroup": {
            "msg": "List configured initiator groups",
        },
    },
}

class Freenass(object):
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
            name = s.split("#", 1)[-1]
            if name in done:
                continue
            if self.filtering and name not in self.objects:
                continue
            try:
                stype = self.node.oget(s, "type")
            except:
                continue
            if stype != "freenas":
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
            self.arrays.append(Freenas(name, api, username, password, timeout, node=self.node))
            done.append(name)

    def __iter__(self):
        for array in self.arrays:
            yield(array)

    def get_freenas(self, name):
        for array in self.arrays:
            if array.name == name:
                return array
        return None

class Freenas(object):
    def __init__(self, name, api, username, password, timeout, node=None):
        self.node = node
        self.name = name
        self.api = api.split("/api")[0]
        self.username = username
        self.password = password
        self.auth = (username, password)
        self.timeout = timeout
        self.keys = ['version',
                     'volumes',
                     'iscsi_targets',
                     'iscsi_targettoextents',
                     'iscsi_extents']

    def delete(self, uri, data=None, timeout=None, api="v2.0"):
        timeout = timeout or self.timeout
        ep = self.api + "/api/" + api + uri + "/"
        headers = {'Content-Type': 'application/json'}
        if data:
            data = json.dumps(data)
        try:
            r = requests.delete(ep, data=data, auth=self.auth, timeout=timeout, verify=VERIFY, headers=headers)
        except Exception as exc:
            raise ex.Error("DELETE %s %s => %s" % (ep, data, exc))
        content = bdecode(r.content)
        if r.status_code != 200:
            raise ex.Error("DELETE %s %s => %d: %s" % (ep, data, r.status_code, content))
        return content

    def put(self, uri, data=None, timeout=None, api="v2.0"):
        timeout = timeout or self.timeout
        ep = self.api + "/api/" + api + uri + "/"
        headers = {'Content-Type': 'application/json'}
        if data:
            data = json.dumps(data)
        try:
            r = requests.put(ep, data=data, auth=self.auth, timeout=timeout, verify=VERIFY, headers=headers)
        except Exception as exc:
            raise ex.Error("PUT %s %s => %s" % (ep, data, exc))
        content = bdecode(r.content)
        if r.status_code != 200:
            raise ex.Error("PUT %s %s => %d: %s" % (ep, data, r.status_code, content))
        return content

    def post(self, uri, data=None, timeout=None, api="v2.0"):
        timeout = timeout or self.timeout
        ep = self.api + "/api/" + api + uri + "/"
        headers = {'Content-Type': 'application/json'}
        if data:
            data = json.dumps(data)
        try:
            r = requests.post(ep, data=data, auth=self.auth, timeout=timeout, verify=VERIFY, headers=headers)
        except Exception as exc:
            raise ex.Error("POST %s %s => %s" % (ep, data, exc))
        content = bdecode(r.content)
        if r.status_code != 200:
            raise ex.Error("POST %s %s => %d: %s" % (ep, data, r.status_code, content))
        return content

    def get(self, uri, params=None, timeout=None, api="v2.0"):
        timeout = timeout or self.timeout
        ep = self.api + "/api/" + api + uri + "/"
        try:
            r = requests.get(ep, params=params, auth=self.auth, timeout=timeout, verify=VERIFY)
        except Exception as exc:
            raise ex.Error("GET %s %s => %s" % (ep, params, exc))
        content = bdecode(r.content)
        if r.status_code != 200:
            raise ex.Error("GET %s %s => %d: %s" % (ep, params, r.status_code, content))
        return content

    # OK
    def get_version(self):
        buff = self.get("/system/version")
        return buff

    # OK
    def get_volumes(self):
        buff = self.get("/pool/dataset", {"limit": 0})
        return buff

    def get_pools(self):
        buff = self.get("/storage/volume", {"limit": 0}, api="v1.0")
        return buff

    def get_iscsi_target_id(self, tgt_id):
        buff = self.get("/iscsi/target/id/%d" % tgt_id)
        return buff

    # OK
    def get_iscsi_targets(self):
        buff = self.get("/iscsi/target", {"limit": 0})
        return buff

    # OK
    def get_iscsi_targettoextents(self):
        buff = self.get("/iscsi/targetextent", {"limit": 0})
        return buff

    # OK
    def get_iscsi_extents(self):
        buff = self.get("/iscsi/extent", {"limit": 0})
        return buff

    # OK
    def get_iscsi_portal(self):
        buff = self.get("/iscsi/portal", {"limit": 0})
        return buff

    # OK
    def get_iscsi_targetgroup(self):
        buff = self.get("/iscsi/target", {"limit": 0})
        return buff

    def get_iscsi_targetgroup_id(self, tg_id):
        buff = self.get("/iscsi/target/id/%d" % tg_id)
        return buff

    def get_iscsi_authorizedinitiator(self):
        buff = self.get("/iscsi/initiator", {"limit": 0})
        return buff

    def get_iscsi_authorizedinitiator_id(self, initiator_id):
        buff = self.get("/iscsi/initiator/id/%d" % initiator_id)
        return buff

    def get_iscsi_target_ids(self, target_names):
        buff = self.get_iscsi_targets()
        data = json.loads(buff)
        l = []
        for target in data:
            if target["name"] in target_names:
                l.append(target["id"])
        return l

    def get_iscsi_initiatorgroup_ids(self, initiator_names):
        buff = self.get_iscsi_authorizedinitiator()
        data = json.loads(buff)
        l = []
        for initiator in initiator_names:
            for item in data:
                if initiator in item["initiators"]:
                    l.append(item["id"])
        return l

    def get_iscsi_extents_data(self):
        buff = self.get_iscsi_extents()
        data = json.loads(buff)
        return data

    def get_iscsi_extent(self, naa=None, name=None):
        data = self.get_iscsi_extents_data()
        if naa and not naa.startswith("0x"):
            naa = "0x" + naa
        for extent in data:
            if name and name == extent["name"]:
                return extent
            if naa and naa == extent["naa"]:
                return extent

    def del_iscsi_extent(self, extent_id):
        path = "/iscsi/extent/id/%d" % extent_id
        self.delete(path)

    def add_iscsi_zvol_extent(self, name=None, size=None, volume=None,
                              insecure_tpc=True, blocksize=512, sparse=False, compression="inherit", **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        data = self.add_zvol(name=name, size=size, volume=volume, sparse=sparse, compression=compression, **kwargs)
        d = {
            "type": "DISK",
            "name": name,
            "insecure_tpc": insecure_tpc,
            "blocksize": blocksize,
            "disk": "zvol/%s/%s" % (volume, name),
        }
        buff = self.post("/iscsi/extent", d)
        data = json.loads(buff)
        return data


    def add_iscsi_file_extent(self, name=None, size=None, volume=None,
                              insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        size = convert_size(size, _to="B")
        d = {
            "type": "FILE",
            "name": name,
            "insecure_tpc": insecure_tpc,
            "blocksize": blocksize,
            "filesize": size,
            "path": "/mnt/%s/%s" % (volume, name),
        }
        buff = self.post("/iscsi/extent", d)
        data = json.loads(buff)
        return data

    def add_iscsi_targets_to_extent(self, extent_id=None, targets=None,
                                    lun=None, **kwargs):
        for key in ["extent_id", "targets"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        target_ids = self.get_iscsi_target_ids(targets)
        if lun is None:
            lun = self.get_aligned_lun(target_ids)
        data = []
        for target_id in target_ids:
            data.append(self.add_iscsi_target_to_extent(target_id, extent_id, lun=lun))
        return data

    def del_iscsi_targetextent(self, id):
        buff = self.delete("/iscsi/targetextent/id/%d" % id, data=True)
        data = json.loads(buff)
        return data

    def add_iscsi_target_to_extent(self, target_id, extent_id, lun=None):
        d = {
            "target": target_id,
            "extent": extent_id,
            "lunid": lun,
        }
        buff = self.post("/iscsi/targetextent", d)
        data = json.loads(buff)
        return data

    def del_iscsi_targetextent_of_extent(self, extent_id):
        d = {
            "extent": extent_id,
        }
        buff = self.get("/iscsi/targetextent", d)
        data = json.loads(buff)
        for d in data:
            self.del_iscsi_targetextent(d["id"])
        return data

    def del_zvol(self, name=None, volume=None, **kwargs):
        for key in ["name", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        data = self.get_zvol(volume, name)
        path = '/pool/dataset/id/%s' % quote_plus(data["id"])
        self.delete(path)

    def add_zvol(self, name=None, size=None, volume=None,
                 compression="inherit", dedup="off", sparse=False,
                 **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        size = convert_size(size, _to="B")
        d = {
            "name": "%s/%s" % (volume, name),
            "type": "VOLUME",
            "volsize": size,
            "sparse": sparse,
            "deduplication": dedup.upper(),
        }
        if compression != "inherit":
            d["compression"] = compression.upper()
        buff = self.post('/pool/dataset', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.Error(buff)

    def get_zvol(self, volume=None, name=None):
        params = {
            "name": "%s/%s" % (volume, name),
        }
        buff = self.get('/pool/dataset', params)
        try:
            return json.loads(buff)[0]
        except (IndexError, ValueError):
            raise ex.Error(buff)

    def get_aligned_lun(self, target_ids):
        tte_data = json.loads(self.get_iscsi_targettoextents())
        luns = [d["lunid"] for d in tte_data if d["target"] in target_ids]
        for lun in range(2^16):
            if luns.count(lun) == 0:
                return lun
        return

    # OK
    def list_mappings(self, name=None, naa=None, **kwargs):
        tte_data = json.loads(self.get_iscsi_targettoextents())
        #print("tte_data <%s>"%tte_data)
        if name is not None or naa is not None:
            data = self.get_iscsi_extent(name=name, naa=naa)
            if data is None:
                raise ex.Error("extent not found")
            extent_id = data["id"]
            tte_data = [d for d in tte_data if d["extent"] == extent_id]
        extent_data = {}
        for d in json.loads(self.get_iscsi_extents()):
            extent_data[d["id"]] = d
        #print("\nextent_data <%s>"%extent_data)
        target_data = {}
        for d in json.loads(self.get_iscsi_targets()):
            target_data[d["id"]] = d
        #print("\ntarget_data <%s>"%target_data)
        tg_by_target = {}
        for d in json.loads(self.get_iscsi_targetgroup()):
            tg_by_target[d["id"]] = d["groups"]
        #print("\ntg_by_target <%s>"%tg_by_target)
        ig_data = {}
        for d in json.loads(self.get_iscsi_authorizedinitiator()):
            ig_data[d["id"]] = d
        #print("\nig_data <%s>"%ig_data)
        mappings = {}
        #print("\nSTART LOOP")
        for d in tte_data:
            #print("d <%s>"%d)
            disk_id = extent_data[d["extent"]]["naa"].replace("0x", "")
            #print("disk_id <%s>"%disk_id)
            #for tg in tg_by_target.get(d["id"], []):
            for tg in tg_by_target.get(d["target"], []):
                #print("current_tg <%s>"%tg)
                ig_id = tg["initiator"]
                ig = ig_data[ig_id]
                #print("ig_id <%s>   ig <%s>"%(ig_id, ig))
                #for hba_id in ig["initiators"].split("\n"):
                for hba_id in ig["initiators"]:
                    tgt_id = target_data[d["target"]]["name"]
                    #tgt_id = "tgtid"
                    mappings[hba_id+":"+tgt_id+":"+disk_id] = {
                       "targetgroup": tg,
                       "extent": d,
                       "disk_id": disk_id,
                       "tgt_id": tgt_id,
                       "hba_id": hba_id,
                    }
        return mappings

    def resize_zvol(self, name=None, naa=None, size=None, **kwargs):
        if size is None:
            raise ex.Error("'size' key is mandatory")
        if name is None and naa is None:
            raise ex.Error("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            raise ex.Error("extent not found")
        volume = self.extent_volume(data)
        if volume is None:
            raise ex.Error("volume not found")
        zvol_data = self.get_zvol(volume=volume, name=data["name"])
        if zvol_data is None:
            raise ex.Error("zvol not found")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="B")
            current_size = int(zvol_data["volsize"]["parsed"])
            size = current_size + incr
        else:
            size = convert_size(size, _to="B")

        d = {
            "volsize": size,
        }
        buff = self.put('/pool/dataset/id/%s' % quote_plus(zvol_data["id"]), d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.Error(buff)

    def del_iscsi_initiatorgroup(self, id=None, **kwargs):
        content = self.get_iscsi_authorizedinitiator_id(id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.Error("initiator group not found")
        self._del_iscsi_initiatorgroup(ig_id=id, **kwargs)
        return data

    def _del_iscsi_initiatorgroup(self, ig_id=None, **kwargs):
        if ig_id is None:
            raise ex.Error("'id' in mandatory")
        self.delete('/iscsi/initiator/%d' % ig_id)

    def get_iscsi_targettoextent(self, id=None, **kwargs):
        if id is None:
            raise ex.Error("'id' in mandatory")
        content = self.get('/iscsi/targetextent/%d' % id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.Error("targettoextent not found")
        return data

    def add_iscsi_initiatorgroup(self, **kwargs):
        data = self._add_iscsi_initiatorgroup(**kwargs)
        return data

    def _add_iscsi_initiatorgroup(self, initiators=None, auth_network="", comment=None,
                                  **kwargs):
        for key in ["initiators"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        anet = list(auth_network.split(","))
        d = {
            "initiators": initiators,
            "auth_network": anet,
        }
        if comment:
            d["comment"] = comment

        buff = self.post('/iscsi/initiator/', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.Error(buff)

    # targetgroup
    def del_iscsi_targetgroup(self, id=None, **kwargs):
        content = self.get_iscsi_targetgroup_id(id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.Error("target group not found")
        self._del_iscsi_targetgroup(tg_id=id, **kwargs)
        return data

    def _del_iscsi_targetgroup(self, tg_id=None, **kwargs):
        if tg_id is None:
            raise ex.Error("'tg_id' is mandatory")
        self.delete('/iscsi/target/%d' % tg_id)

    def add_iscsi_targetgroup(self, **kwargs):
        if kwargs.get("portal_id") is None:
            kwargs["portal_id"] = 1
        for key in ["initiatorgroup", "target"]:
            idkey = key + "_id"
            val = kwargs.get(idkey)
            if val is None:
                ids = []
            else:
                ids = [val]

            names = kwargs.get(key)
            if names is not None:
                fn = "get_iscsi_%s_ids" % key
                ids += getattr(self, fn)(names)
                if not ids:
                    raise ex.Error("no '%s' ids found" % key)
                del kwargs[key]
            kwargs[idkey] = ids

        data = []
        for tid in kwargs["target_id"]:
            for igid in kwargs["initiatorgroup_id"]:
                _data = self._add_iscsi_targetgroup(
                    portal_id=kwargs.get("portal_id"),
                    initiatorgroup_id=igid,
                    target_id=tid,
                    authmethod=kwargs.get("authmethod"),
                    authgroup_id=kwargs.get("authgroup_id"),
                )
                data.append(_data)
        return data

    def _add_iscsi_targetgroup(self, portal_id=None, initiatorgroup_id=None,
                               target_id=None, authmethod="NONE",
                               authgroup_id=None, **kwargs):
        buff = self.get_iscsi_target_id(target_id)
        data = json.loads(buff)
        for g in data["groups"]:
            if portal_id == g["portal"] and initiatorgroup_id == g["initiator"]:
                return data
        d = {
            "initiator": initiatorgroup_id,
            "portal": portal_id,
            "authmethod": authmethod,
            "auth": authgroup_id,
        }
        data["groups"].append(d)
        del data["id"]
        buff = self.put('/iscsi/target/id/%d' % target_id, data)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.Error(buff)

    # target
    # OK
    def del_iscsi_target(self, id=None, name=None, **kwargs):
        if id is None and name:
            try:
                id = self.get_iscsi_target_ids([name])[0]
            except IndexError:
                return
        if id is None:
            raise ex.Error("'id' is mandatory")
        content = self.get_iscsi_target_id(id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.Error("target not found")
        self._del_iscsi_target(id=id, **kwargs)
        return data

    # OK
    def _del_iscsi_target(self, id=None, **kwargs):
        if id is None:
            raise ex.Error("'id' is mandatory")
        self.delete('/iscsi/target/id/%d' % id)

    # OK
    def add_iscsi_target(self, **kwargs):
        data = self._add_iscsi_target(**kwargs)
        return data

    # OK
    def _add_iscsi_target(self, name=None, alias=None, **kwargs):
        for key in ["name"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        d = {
            "name": name,
        }
        if alias:
            d["alias"] = alias

        buff = self.post('/iscsi/target/', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.Error(buff)

    def add_iscsi_file(self, name=None, size=None, volume=None, targets=None,
                       mappings=None, insecure_tpc=True, blocksize=512, lun=None, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)

        if targets is None and mappings is None:
            raise ex.Error("'targets' or 'mappings' must be specified")

        if mappings is not None and targets is None:
            targets = self.translate_mappings(mappings)

        data = self.add_iscsi_file_extent(name=name, size=size, volume=volume, **kwargs)

        if "id" not in data:
            if "name" in data:
                if isinstance(data["name"], list):
                    raise ex.Error("\n".join(data["name"]))
                raise ex.Error(data["name"])
            raise ex.Error(str(data))

        self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, lun=lun, **kwargs)
        disk_id = data["naa"].replace("0x", "")
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["id"],
            "mappings": sorted(self.list_mappings(naa=disk_id).values(), key=lambda x: (x["hba_id"], x["tgt_id"], x["disk_id"])),
        }
        return results

    def del_iscsi_file(self, name=None, naa=None, **kwargs):
        if name is None and naa is None:
            raise ex.Error("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            return
        self.del_iscsi_targetextent_of_extent(data["id"])
        self.del_iscsi_extent(data["id"])
        return data

    def translate_mappings(self, mappings):
        targets = set()
        for mapping in mappings:
            elements = mapping.split(":iqn.")
            targets |= set(("iqn."+elements[-1]).split(","))
        targets = list(targets)
        return targets

    def split_mappings(self, mappings):
        data = []
        for mapping in mappings:
            elements = mapping.split(":iqn.")
            for target in set(("iqn."+elements[-1]).split(",")):
                data.append((elements[0], target))
        return data

    def unmap(self, mappings=None, **kwargs):
        for key in ["mappings"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        targets = self.split_mappings(mappings)
        current_mappings = self.list_mappings()
        results = []
        for mapping in self.split_mappings(mappings):
            for tg in current_mappings.values():
                if tg["tgt_id"] != mapping[1] or \
                   tg["hba_id"] != mapping[0]:
                    continue
                result = self.del_iscsi_targetgroup(id=tg["targetgroup"]["id"])
                results.append(result)
        return results

    def unmap_iscsi_zvol(self, name=None, mappings=None, **kwargs):
        for key in ["name", "mappings"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        targets = self.split_mappings(mappings)
        current_mappings = self.list_mappings()
        extent = self.get_iscsi_extent(name=name)
        if not extent:
            raise ex.Error("no extent found for disk : %s" % (name))
        results = []
        for mapping in self.split_mappings(mappings):
            mapping = ":".join(mapping)+":"+extent["naa"].replace("0x", "")
            tg = current_mappings.get(mapping)
            if not tg:
                continue
            result = self.del_iscsi_targetextent(tg["extent"]["id"])
            results.append(result)
        return results

    def map_iscsi_zvol(self, name=None, targets=None, mappings=None, lun=None, **kwargs):
        for key in ["name"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        if targets is None and mappings is None:
            raise ex.Error("'targets' or 'mappings' must be specified")

        if mappings is not None and targets is None:
            targets = self.translate_mappings(mappings)

        data = self.get_iscsi_extent(name=name)
        if data is None:
            raise ex.Error("zvol not found")
        results = self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, lun=lun, **kwargs)
        return results

    def add_iscsi_zvol(self, name=None, size=None, volume=None, targets=None,
                       mappings=None, insecure_tpc=True, blocksize=512, sparse=False, lun=None, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)

        # extent
        data = self.add_iscsi_zvol_extent(name=name, size=size, volume=volume, sparse=sparse, **kwargs)

        if "id" not in data:
            if "name" in data:
                if isinstance(data["name"], list):
                    raise ex.Error("\n".join(data["name"]))
                raise ex.Error(data["name"])
            raise ex.Error(str(data))

        # mappings
        if targets is not None or mappings is not None:
            if mappings is not None and targets is None:
                targets = self.translate_mappings(mappings)
            self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, lun=lun, **kwargs)

        # collector update
        warnings = []
        try:
            self.add_diskinfo(data, size, volume)
        except Exception as exc:
            warnings.append(str(exc))
        disk_id = data["naa"].replace("0x", "")
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["id"],
            "mappings": sorted(self.list_mappings(naa=disk_id).values(), key=lambda x: (x["hba_id"], x["tgt_id"], x["disk_id"])),
        }
        if warnings:
            results["warnings"] = warnings

        return results

    def del_iscsi_zvol(self, name=None, naa=None, volume=None, **kwargs):
        if name is None and naa is None:
            raise ex.Error("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None and volume is None:
            raise ex.Error("extent not found: need to specify the volume")
        if volume is None:
            try:
                volume = self.extent_volume(data)
            except ValueError:
                raise ex.Error("failed to identify zvol. may be a file ?")
        if data:
            self.del_iscsi_targetextent_of_extent(data["id"])
            self.del_iscsi_extent(data["id"])
        else:
            data = {}
        self.del_zvol(name=name, volume=volume)
        warnings = []
        try:
            self.del_diskinfo(data["naa"].replace("0x", ""))
        except Exception as exc:
            warnings.append(str(exc))
        if warnings:
            data["warnings"] = warnings
        return data

    def extent_volume(self, data):
        path = data["path"].split("/")
        volume = path[path.index("zvol")+1]
        return volume

    def list_pools(self, **kwargs):
        return json.loads(self.get_pools())

    def list_volume(self, **kwargs):
        return json.loads(self.get_volumes())

    def list_iscsi_target(self, **kwargs):
        return json.loads(self.get_iscsi_targets())

    def list_iscsi_targettoextent(self, **kwargs):
        return json.loads(self.get_iscsi_targettoextents())

    def list_iscsi_portal(self, **kwargs):
        return json.loads(self.get_iscsi_portal())

    def list_iscsi_extent(self, **kwargs):
        return json.loads(self.get_iscsi_extents())

    def list_iscsi_initiatorgroup(self, **kwargs):
        return json.loads(self.get_iscsi_authorizedinitiator())

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

    def add_diskinfo(self, data, size=None, volume=None):
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_post("/disks", {
                "disk_id": data["naa"].replace("0x", ""),
                "disk_devid": data["id"],
                "disk_name": data["name"],
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": volume,
            })
        except Exception as exc:
            raise ex.Error(str(exc))
        if "error" in data:
            raise ex.Error(result["error"])
        return result

def do_action(action, array_name=None, node=None, **kwargs):
    o = Freenass()
    array = o.get_freenas(array_name)
    if array is None:
        raise ex.Error("array %s not found" % array_name)
    array.node = node
    if not hasattr(array, action):
        raise ex.Error("not implemented")
    result = getattr(array, action)(**kwargs)
    if result is not None:
        print(json.dumps(result, indent=4))

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


