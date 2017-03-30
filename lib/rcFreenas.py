from __future__ import print_function

import sys
import os
import json
from optparse import Option
import requests

from rcConfigParser import RawConfigParser
import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import convert_size, bdecode
from rcOptParser import OptParser


try:
    requests.packages.urllib3.disable_warnings()
except:
    pass

VERIFY = False

PROG = "nodemgr array"
OPT = Storage({
    "help": Option(
        "-h", "--help", default=None, action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", default=None, action="store", dest="array_name",
        help="The name of the array, as defined in auth.conf"),
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
        "--compression", default="on", action="store", dest="compression",
        choices=["on", "off", "inherit", "lzjb", "lz4", "gzip", "gzip-9", "zle"],
        help="Toggle compression"),
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
        "--auth-network", default="ALL", action="store", dest="auth_network",
        help="Network authorized to access to the iSCSI target. ip or cidr addresses or 'ALL' for any ips"),
    "comment": Option(
        "--comment", action="store", dest="comment",
        help="Description for your reference"),
    "id": Option(
        "--id", action="store", type=int, dest="id",
        help="An object id, as reported by a list action"),
    "alias": Option(
        "--alias", action="store", dest="alias",
        help="An object name alias"),
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
                OPT.dedup,
                OPT.mappings,
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
                OPT.portal_id,
                OPT.target_id,
                OPT.initiatorgroup_id,
                OPT.authgroup_id,
                OPT.authtype,
            ],
        },
    },
    "Delete actions": {
        "del_iscsi_file": {
            "msg": "Delete and unpresent a file-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.naa,
            ],
        },
        "del_iscsi_zvol": {
            "msg": "Delete and unpresent a zvol-backed iscsi disk",
            "options": [
                OPT.name,
                OPT.naa,
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
        "list_iscsi_targetgroup": {
            "msg": "List configured target groups",
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

    def __init__(self, objects=[]):
        self.objects = objects
        self.filtering = len(objects) > 0
        self.index = 0
        cf = rcEnv.authconf
        if not os.path.exists(cf):
            return
        conf = RawConfigParser()
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
        del conf
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
                return array
        return None

class Freenas(object):
    def __init__(self, name, api, username, password):
        self.node = None
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

    def delete(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.delete(api, data=json.dumps(data), auth=self.auth, verify=VERIFY, headers=headers)
        return r

    def put(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.put(api, data=json.dumps(data), auth=self.auth, verify=VERIFY, headers=headers)
        return bdecode(r.content)

    def post(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.post(api, data=json.dumps(data), auth=self.auth, verify=VERIFY, headers=headers)
        return bdecode(r.content)

    def post2(self, uri, data=None):
        api = self.api.replace("api/v1.0", "")+uri
        s = requests.Session()
        r = s.get(api)
        csrf_token = r.cookies['csrftoken']
        data["csrfmiddlewaretoken"] = csrf_token
        r = requests.post(api, data=data, auth=self.auth, verify=VERIFY)
        return bdecode(r.content)

    def get(self, uri, params=None):
        r = requests.get(self.api+uri+"/?format=json", params=params, auth=self.auth, verify=VERIFY)
        return bdecode(r.content)

    def get_version(self):
        buff = self.get("/system/version")
        return buff

    def get_volume(self, name):
        buff = self.get("/storage/volume/%s" % name, {"limit": 0})
        return buff

    def get_volume_datasets(self, name):
        buff = self.get("/storage/volume/%s/datasets" % name, {"limit": 0})
        return buff

    def get_volumes(self):
        buff = self.get("/storage/volume", {"limit": 0})
        return buff

    def get_iscsi_target_id(self, tgt_id):
        buff = self.get("/services/iscsi/target/%d" % tgt_id)
        return buff

    def get_iscsi_targets(self):
        buff = self.get("/services/iscsi/target", {"limit": 0})
        return buff

    def get_iscsi_targettoextents(self):
        buff = self.get("/services/iscsi/targettoextent", {"limit": 0})
        return buff

    def get_iscsi_extents(self):
        buff = self.get("/services/iscsi/extent", {"limit": 0})
        return buff

    def get_iscsi_portal(self):
        buff = self.get("/services/iscsi/portal", {"limit": 0})
        return buff

    def get_iscsi_targetgroup(self):
        buff = self.get("/services/iscsi/targetgroup", {"limit": 0})
        return buff

    def get_iscsi_targetgroup_id(self, tg_id):
        buff = self.get("/services/iscsi/targetgroup/%d" % tg_id)
        return buff

    def get_iscsi_authorizedinitiator(self):
        buff = self.get("/services/iscsi/authorizedinitiator", {"limit": 0})
        return buff

    def get_iscsi_authorizedinitiator_id(self, initiator_id):
        buff = self.get("/services/iscsi/authorizedinitiator/%d" % initiator_id)
        return buff

    def get_iscsi_target_ids(self, target_names):
        buff = self.get_iscsi_targets()
        data = json.loads(buff)
        l = []
        for target in data:
            if target["iscsi_target_name"] in target_names:
                l.append(target["id"])
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
            if name and name == extent["iscsi_target_extent_name"]:
                return extent
            if naa and naa == extent["iscsi_target_extent_naa"]:
                return extent

    def del_iscsi_extent(self, extent_id):
        path = "/services/iscsi/extent/%d" % extent_id
        response = self.delete(path)
        if response.status_code != 204:
            raise ex.excError("delete error: %s (%d)" % (path, response.status_code))

    def add_iscsi_zvol_extent(self, name=None, size=None, volume=None,
                              insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        data = self.add_zvol(name=name, size=size, volume=volume, **kwargs)
        d = {
            "iscsi_target_extent_type": "Disk",
            "iscsi_target_extent_name": name,
            "iscsi_target_extent_insecure_tpc": insecure_tpc,
            "iscsi_target_extent_blocksize": blocksize,
            "iscsi_target_extent_disk": "zvol/%s/%s" % (volume, name),
        }
        buff = self.post("/services/iscsi/extent", d)
        data = json.loads(buff)
        return data


    def add_iscsi_file_extent(self, name=None, size=None, volume=None,
                              insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        size = convert_size(size, _to="MB")
        d = {
            "iscsi_target_extent_type": "File",
            "iscsi_target_extent_name": name,
            "iscsi_target_extent_insecure_tpc": insecure_tpc,
            "iscsi_target_extent_blocksize": blocksize,
            "iscsi_target_extent_filesize": str(size)+"MB",
            "iscsi_target_extent_path": "/mnt/%s/%s" % (volume, name),
        }
        buff = self.post("/services/iscsi/extent", d)
        data = json.loads(buff)
        return data

    def add_iscsi_targets_to_extent(self, extent_id=None, targets=None,
                                    **kwargs):
        for key in ["extent_id", "targets"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        target_ids = self.get_iscsi_target_ids(targets)
        data = []
        for target_id in target_ids:
            data.append(self.add_iscsi_target_to_extent(target_id, extent_id))
        return data

    def add_iscsi_target_to_extent(self, target_id, extent_id):
        d = {
            "iscsi_target": target_id,
            "iscsi_extent": extent_id,
        }
        buff = self.post("/services/iscsi/targettoextent", d)
        data = json.loads(buff)
        return data

    def del_zvol(self, name=None, volume=None, **kwargs):
        for key in ["name", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        path = '/storage/volume/%s/zvols/%s' % (volume, name)
        response = self.delete(path)
        if response.status_code != 204:
            raise ex.excError("delete error: %s (%d)" % (path, response.status_code))

    def add_zvol(self, name=None, size=None, volume=None,
                 compression="inherit", dedup="off",
                 **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        size = convert_size(size, _to="MB")
        d = {
            "name": name,
            "volsize": str(size)+"MB",
            "compression": compression,
            "dedup": dedup,
        }
        buff = self.post('/storage/volume/%s/zvols/' % volume, d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    def get_zvol(self, volume=None, name=None):
        buff = self.get('/storage/volume/%s/zvols/%s' % (volume, name))
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    def list_mappings(self, name=None, naa=None, **kwargs):
        tte_data = json.loads(self.get_iscsi_targettoextents())
        if name is not None or naa is not None:
            data = self.get_iscsi_extent(name=name, naa=naa)
            if data is None:
                raise ex.excError("extent not found")
            extent_id = data["id"]
            tte_data = [d for d in tte_data if d["iscsi_extent"] == extent_id]
        extent_data = {}
        for d in json.loads(self.get_iscsi_extents()):
            extent_data[d["id"]] = d
        target_data = {}
        for d in json.loads(self.get_iscsi_targets()):
            target_data[d["id"]] = d
        tg_by_target = {}
        for d in json.loads(self.get_iscsi_targetgroup()):
            if d["iscsi_target"] in tg_by_target:
                tg_by_target[d["iscsi_target"]].append(d)
            else:
                tg_by_target[d["iscsi_target"]] = [d]
        ig_data = {}
        for d in json.loads(self.get_iscsi_authorizedinitiator()):
            ig_data[d["id"]] = d
        mappings = []
        for d in tte_data:
            for tg in tg_by_target[d["iscsi_target"]]:
                ig_id = tg["iscsi_target_initiatorgroup"]
                ig = ig_data[ig_id]
                for hba_id in ig["iscsi_target_initiator_initiators"].split("\n"):
                    mappings.append({
                       "disk_id": extent_data[d["iscsi_extent"]]["iscsi_target_extent_naa"].replace("0x", ""),
                       "tgt_id": target_data[tg["iscsi_target"]]["iscsi_target_name"],
                       "hba_id": hba_id,
                    })
        return mappings

    def resize_zvol(self, name=None, naa=None, size=None, **kwargs):
        if size is None:
            raise ex.excError("'size' key is mandatory")
        if name is None and naa is None:
            raise ex.excError("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            raise ex.excError("extent not found")
        volume = self.extent_volume(data)
        if volume is None:
            raise ex.excError("volume not found")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="MB")
            zvol_data = self.get_zvol(volume=volume, name=data["iscsi_target_extent_name"])
            current_size = convert_size(int(zvol_data["volsize"]), _to="MB")
            size = str(current_size + incr) + "MB"
        else:
            size = str(convert_size(size, _to="MB")) + "MB"

        d = {
            "volsize": size,
        }
        buff = self.put('/storage/volume/%s/zvols/%s' % (volume, data["iscsi_target_extent_name"]), d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    def del_iscsi_initiatorgroup(self, ig_id=None, **kwargs):
        content = self.get_iscsi_authorizedinitiator_id(ig_id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.excError("initiator group not found")
        self._del_iscsi_initiatorgroup(ig_id=ig_id, **kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _del_iscsi_initiatorgroup(self, ig_id=None, **kwargs):
        if id is None:
            raise ex.excError("'id' in mandatory")
        response = self.delete('/services/iscsi/authorizedinitiator/%d' % ig_id)
        if response.status_code != 204:
            raise ex.excError(str(response))

    def add_iscsi_initiatorgroup(self, **kwargs):
        data = self._add_iscsi_initiatorgroup(**kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _add_iscsi_initiatorgroup(self, initiators=None, auth_network="ALL", comment=None,
                                  **kwargs):
        for key in ["initiators"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        d = {
            "iscsi_target_initiator_initiators": ",".join(initiators),
            "iscsi_target_initiator_auth_network": auth_network,
        }
        if comment:
            d["iscsi_target_initiator_comment"] = comment

        buff = self.post('/services/iscsi/authorizedinitiator/', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    # targetgroup
    def del_iscsi_targetgroup(self, tg_id=None, **kwargs):
        content = self.get_iscsi_targetgroup_id(tg_id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.excError("target group not found")
        self._del_iscsi_targetgroup(tg_id=tg_id, **kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _del_iscsi_targetgroup(self, tg_id=None, **kwargs):
        if tg_id is None:
            raise ex.excError("'tg_id' is mandatory")
        response = self.delete('/services/iscsi/targetgroup/%d' % tg_id)
        if response.status_code != 204:
            raise ex.excError(str(response))

    def add_iscsi_targetgroup(self, **kwargs):
        data = self._add_iscsi_targetgroup(**kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _add_iscsi_targetgroup(self, portal_id=None, initiatorgroup_id=None,
                               target_id=None, authtype="None",
                               authgroup_id=None, **kwargs):
        for key in ["portal_id", "initiatorgroup_id", "target_id"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        d = {
            "iscsi_target": target_id,
            "iscsi_target_initiatorgroup": initiatorgroup_id,
            "iscsi_target_portalgroup": portal_id,
            "iscsi_target_authtype": authtype,
            "iscsi_target_authgroup": -1,
            "iscsi_target_initialdigest": "Auto",
        }
        if authgroup_id:
            d["iscsi_target_authgroup"] = authgroup_id

        print(d)
        buff = self.post('/services/iscsi/targetgroup/', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    # target
    def del_iscsi_target(self, target_id=None, **kwargs):
        content = self.get_iscsi_target_id(target_id)
        try:
            data = json.loads(content)
        except ValueError:
            raise ex.excError("target not found")
        self._del_iscsi_target(target_id=target_id, **kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _del_iscsi_target(self, target_id=None, **kwargs):
        if target_id is None:
            raise ex.excError("'target_id' is mandatory")
        response = self.delete('/services/iscsi/target/%d' % target_id)
        if response.status_code != 204:
            raise ex.excError(str(response))

    def add_iscsi_target(self, **kwargs):
        data = self._add_iscsi_target(**kwargs)
        print(json.dumps(data, indent=8))
        return data

    def _add_iscsi_target(self, name=None, alias=None, **kwargs):
        for key in ["name"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        d = {
            "iscsi_target_name": name,
        }
        if alias:
            d["iscsi_target_alias"] = alias

        buff = self.post('/services/iscsi/target/', d)
        try:
            return json.loads(buff)
        except ValueError:
            raise ex.excError(buff)

    def add_iscsi_file(self, name=None, size=None, volume=None, targets=None,
                       mappings=None, insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)

        if targets is None and mappings is None:
            raise ex.excError("'targets' or 'mappings' must be specified")

        if mappings is not None and targets is None:
            targets = self.translate_mappings(mappings)

        data = self.add_iscsi_file_extent(name=name, size=size, volume=volume, **kwargs)

        if "id" not in data:
            if "iscsi_target_extent_name" in data:
                if isinstance(data["iscsi_target_extent_name"], list):
                    raise ex.excError("\n".join(data["iscsi_target_extent_name"]))
                raise ex.excError(data["iscsi_target_extent_name"])
            raise ex.excError(str(data))
        self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, **kwargs)
        disk_id = data["iscsi_target_extent_naa"].replace("0x", "")
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["id"],
            "mappings": self.list_mappings(naa=disk_id),
        }
        return results

    def del_iscsi_file(self, name=None, naa=None, **kwargs):
        if name is None and naa is None:
            raise ex.excError("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            return
        self.del_iscsi_extent(data["id"])
        print(json.dumps(data, indent=8))

    def translate_mappings(self, mappings):
        targets = set()
        for mapping in mappings:
            elements = mapping.split(":")
            targets |= set(elements[-1].split(","))
        targets = list(targets)
        return targets

    def add_iscsi_zvol(self, name=None, size=None, volume=None, targets=None,
                       mappings=None, insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume"]:
            if locals()[key] is None:
                raise ex.excError("'%s' key is mandatory" % key)
        if targets is None and mappings is None:
            raise ex.excError("'targets' or 'mappings' must be specified")

        if mappings is not None and targets is None:
            targets = self.translate_mappings(mappings)

        data = self.add_iscsi_zvol_extent(name=name, size=size, volume=volume, **kwargs)

        if "id" not in data:
            if "iscsi_target_extent_name" in data:
                if isinstance(data["iscsi_target_extent_name"], list):
                    raise ex.excError("\n".join(data["iscsi_target_extent_name"]))
                raise ex.excError(data["iscsi_target_extent_name"])
            raise ex.excError(str(data))
        self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, **kwargs)
        self.add_diskinfo(data, size, volume)
        disk_id = data["iscsi_target_extent_naa"].replace("0x", "")
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["id"],
            "mappings": self.list_mappings(naa=disk_id),
        }
        return results

    def del_iscsi_zvol(self, name=None, naa=None, **kwargs):
        if name is None and naa is None:
            raise ex.excError("'name' or 'naa' must be specified")
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            return
        volume = self.extent_volume(data)
        self.del_iscsi_extent(data["id"])
        self.del_zvol(name=name, volume=volume)
        self.del_diskinfo(data["iscsi_target_extent_naa"].replace("0x", ""))
        print(json.dumps(data, indent=8))

    def extent_volume(self, data):
        path = data["iscsi_target_extent_path"].split("/")
        volume = path[path.index("zvol")+1]
        return volume

    def list_volume(self, **kwargs):
        data = json.loads(self.get_volumes())
        print(json.dumps(data, indent=8))

    def list_iscsi_target(self, **kwargs):
        data = json.loads(self.get_iscsi_targets())
        print(json.dumps(data, indent=8))

    def list_iscsi_targettoextent(self, **kwargs):
        data = json.loads(self.get_iscsi_targettoextents())
        print(json.dumps(data, indent=8))

    def list_iscsi_portal(self, **kwargs):
        data = json.loads(self.get_iscsi_portal())
        print(json.dumps(data, indent=8))

    def list_iscsi_targetgroup(self, **kwargs):
        data = json.loads(self.get_iscsi_targetgroup())
        print(json.dumps(data, indent=8))

    def list_iscsi_extent(self, **kwargs):
        data = json.loads(self.get_iscsi_extents())
        print(json.dumps(data, indent=8))

    def list_iscsi_initiatorgroup(self, **kwargs):
        data = json.loads(self.get_iscsi_authorizedinitiator())
        print(json.dumps(data, indent=8))

    def del_diskinfo(self, disk_id):
        if disk_id in (None, ""):
            return
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_delete("/disks/%s" % disk_id)
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in result:
            raise ex.excError(result["error"])
        return result

    def add_diskinfo(self, data, size=None, volume=None):
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_post("/disks", {
                "disk_id": data["iscsi_target_extent_naa"].replace("0x", ""),
                "disk_devid": data["id"],
                "disk_name": data["iscsi_target_extent_name"],
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": volume,
            })
        except Exception as exc:
            raise ex.excError(str(exc))
        if "error" in data:
            raise ex.excError(result["error"])
        return result

def do_action(action, array_name=None, node=None, **kwargs):
    o = Freenass()
    array = o.get_freenas(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
    array.node = node
    if not hasattr(array, action):
        raise ex.excError("not implemented")
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

if __name__ == "__main__":
    try:
        main(sys.argv)
        ret = 0
    except ex.excError as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)


