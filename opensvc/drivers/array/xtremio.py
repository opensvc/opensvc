from __future__ import print_function

import sys
import json
import logging

import core.exceptions as ex
from env import Env
from utilities.storage import Storage
from utilities.naming import factory, split_path
from utilities.converters import convert_size
from utilities.optparser import OptParser, Option
from core.node import Node

try:
    import requests
except ImportError:
    raise ex.InitError("the requests module must be installed")

try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass
verify = False

PROG = "om array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in the node or cluster configuration."),
    "name": Option(
        "--name", action="store", dest="name",
        help="The object name"),
    "size": Option(
        "--size", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "tags": Option(
        "--tag", action="append", dest="tags",
        help="An object tag. Can be set multiple times."),
    "blocksize": Option(
        "--blocksize", type=int, action="store", dest="blocksize",
        help="The exported disk blocksize in B"),
    "alignment_offset": Option(
        "--alignment-offset", type=int, action="store", dest="alignment_offset",
        help="The alignment offset for Volumes of 512 LB size is between 0 and "
             "7. If omitted, the offset value is 0.  Volumes of logical block "
             "size 4096 must not be defined with an offset."),
    "small_io_alerts": Option(
        "--small-io-alerts", action="store", dest="small_io_alerts",
        choices=["enabled", "disabled"],
        help="Enable or disable small input/output Alerts"),
    "unaligned_io_alerts": Option(
        "--unaligned-io-alerts", action="store", dest="unaligned_io_alerts",
        choices=["enabled", "disabled"],
        help="Enable or disable unaligned input/output Alerts"),
    "vaai_tp_alerts": Option(
        "--vaai-tp-alerts", action="store", dest="vaai_tp_alerts",
        choices=["enabled", "disabled"],
        help="Enable or disable VAAI TP Alerts"),
    "access": Option(
        "--access", action="store", dest="access",
        choices=["no_access", "read_access", "write_access"],
        help="A Volume is created with write access rights."
             "Volumes can be modified after being created and"
             "have their access levels' changed."),
    "naa": Option(
        "--naa", action="store", dest="naa",
        help="The volume naa identifier"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --targetgroup and --initiatorgroup. Can be specified multiple times."),
    "initiators": Option(
        "--initiators", action="append", dest="initiators",
        help="An initiator id. Can be specified multiple times."),
    "initiator": Option(
            "--initiator", action="store", dest="initiator",
            help="An initiator id"),
    "targets": Option(
        "--targets", action="append", dest="targets",
        help="A target name to export the disk through. Can be set multiple times."),
    "target": Option(
        "--target", action="store", dest="target",
        help="A target name or id"),
    "targetgroup": Option(
        "--targetgroup", action="store", dest="targetgroup",
        help="A target group name or id"),
    "initiatorgroup": Option(
        "--initiatorgroup", action="store", dest="initiatorgroup",
        help="The initiator group id or name"),
    "volume": Option(
        "--volume", action="store", dest="volume",
        help="A volume name or id"),
    "lun": Option(
        "--lun", action="store", type=int, dest="lun",
        help="Unique LUN identification, exposing the Volume to"
             "the host"),
    "mapping": Option(
        "--mapping", action="store", type=int, dest="mapping",
        help="A lun mapping index"),
})

GLOBAL_OPTS = [
    OPT.array,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_disk": {
            "msg": "Add a volume",
            "options": [
                OPT.name,
                OPT.size,
                OPT.blocksize,
                OPT.tags,
                OPT.alignment_offset,
                OPT.small_io_alerts,
                OPT.unaligned_io_alerts,
                OPT.vaai_tp_alerts,
                OPT.access,
                OPT.mappings,
            ],
        },
        "add_map": {
            "msg": "Map a volume to an initiator group and target group",
            "options": [
                OPT.volume,
                OPT.mappings,
                OPT.initiatorgroup,
                OPT.targetgroup,
            ],
        },
        "del_disk": {
            "msg": "Delete a volume",
            "options": [
                OPT.volume,
            ],
        },
        "del_map": {
            "msg": "Unmap a volume from an initiator group and target group",
            "options": [
                OPT.mapping,
                OPT.volume,
                OPT.initiatorgroup,
                OPT.targetgroup,
            ],
        },
        "resize_disk": {
            "msg": "Resize a volume",
            "options": [
                OPT.volume,
                OPT.size,
            ],
        },
    },
    "Low-level actions": {
        "list_initiators": {
            "msg": "List configured initiators",
            "options": [
                OPT.initiator,
            ],
        },
        "list_initiator_groups": {
            "msg": "List configured initiator groups",
            "options": [
                OPT.initiatorgroup,
            ],
        },
        "list_initiators_connectivity": {
            "msg": "List configured initiator groups",
        },
        "list_mappings": {
            "msg": "List configured mappings",
            "options": [
                OPT.mapping,
                OPT.volume,
            ],
        },
        "list_targets": {
            "msg": "List configured targets",
            "options": [
                OPT.target,
            ],
        },
        "list_target_groups": {
            "msg": "List configured target groups",
            "options": [
                OPT.targetgroup,
            ],
        },
        "list_volumes": {
            "msg": "List configured volumes",
            "options": [
                OPT.volume,
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
            if stype != "xtremio":
                continue
            try:
                api = self.node.oget(s, 'api')
                username = self.node.oget(s, 'username')
                password = self.node.oget(s, 'password')
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue
            self.arrays.append(Array(name, api, username, password, node=self.node))
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
    def __init__(self, name, api, username, password, node=None):
        self.node = node
        self.name = name
        self.api = api
        self.username = username
        self.password = password
        self.auth = (username, password)
        self.keys = [
            'clusters_details',
            'volumes_details',
            'targets_details',
        ]

        self.tg_portname = {}
        self.ig_portname = {}
        self.log = logging.getLogger(Env.nodename+".array.xtremio."+self.name)

    def convert_ids(self, data):
        if data is None:
            return data
        for key in data:
            if not isinstance(key, str):
                continue
            if not key.endswith("-id"):
                continue
            try:
                data[key] = int(data[key])
            except ValueError:
                pass
        return data

    def delete(self, uri, params=None, data=None):
        if params is None:
            params = {}
        params["cluster-name"] = self.name
        headers = {"Cache-Control": "no-cache"}
        data = self.convert_ids(data)
        if not uri.startswith("http"):
            uri = self.api + uri
        response = requests.delete(uri, params=params, data=json.dumps(data),
                                   auth=self.auth, verify=verify,
                                   headers=headers)
        if response.status_code != 200:
            raise ex.Error(response.content)
        return 0

    def put(self, uri, params=None, data=None):
        if data is None:
            data = {}
        data["cluster-id"] = self.name
        headers = {"Cache-Control": "no-cache"}
        data = self.convert_ids(data)
        if not uri.startswith("http"):
            uri = self.api + uri
        response = requests.put(uri, params=params, data=json.dumps(data), auth=self.auth,
                                verify=verify, headers=headers)
        if response.status_code != 200:
            raise ex.Error(response.content)
        return

    def post(self, uri, params=None, data=None):
        if data is None:
            data = {}
        data["cluster-id"] = self.name
        headers = {"Cache-Control": "no-cache"}
        data = self.convert_ids(data)
        if not uri.startswith("http"):
            uri = self.api + uri
        response = requests.post(uri, params=params, data=json.dumps(data), auth=self.auth,
                                 verify=verify, headers=headers)
        ret = json.loads(response.content)
        if response.status_code == 201:
            return self.get(ret["links"][0]["href"])
        raise ex.Error(response.content)

    def get(self, uri, params=None):
        if params is None:
            params = {}
        params["cluster-name"] = self.name
        headers = {"Cache-Control": "no-cache"}
        if not uri.startswith("http"):
            uri = self.api + uri
        r = requests.get(uri, params=params, auth=self.auth, verify=verify)
        return json.loads(r.content)

    def get_clusters_details(self):
        data = self.get("/clusters", params={"full": 1})
        return json.dumps(data["clusters"], indent=8)

    def get_targets_details(self):
        data = self.get("/targets", params={"full": 1})
        return json.dumps(data["targets"], indent=8)

    def get_volumes_details(self):
        data = self.get("/volumes", params={"full": 1})
        return json.dumps(data["volumes"], indent=8)

    def add_disk(self, name=None, size=None, blocksize=None, tags=None,
                 access=None, vaai_tp_alerts=None,
                 small_io_alerts=None, unaligned_io_alerts=None,
                 alignment_offset=None, mappings=None, **kwargs):
        if name is None:
            raise ex.Error("--name is mandatory")
        if size == 0 or size is None:
            raise ex.Error("--size is mandatory")
        d = {
            "vol-name": name,
            "vol-size": str(convert_size(size, _to="MB"))+"M",
        }
        if blocksize is not None:
            d["lb-size"] = blocksize
        if small_io_alerts is not None:
            d["small-io-alerts"] = small_io_alerts
        if unaligned_io_alerts is not None:
            d["unaligned-io-alerts"] = unaligned_io_alerts
        if access is not None:
            d["vol-access"] = access
        if vaai_tp_alerts is not None:
            d["vaai-tp-alerts"] = vaai_tp_alerts
        if alignment_offset is not None:
            d["alignment-offset"] = alignment_offset
        self.post("/volumes", data=d)
        driver_data = {}
        if mappings:
            mappings_data = self.add_map(volume=name, mappings=mappings)
        driver_data["volume"] = self.get_volumes(volume=name)["content"]
        driver_data["mappings"] = [val for val in mappings_data.values()]
        results = {
            "driver_data": driver_data,
            "disk_id": driver_data["volume"]["naa-name"],
            "disk_devid": driver_data["volume"]["index"],
            "mappings": {},
        }
        for ig, tg in list(mappings_data.keys()):
            if ig not in self.ig_portname:
                continue
            for hba_id in self.ig_portname[ig]:
                if tg not in self.tg_portname:
                    continue
                for tgt_id in self.tg_portname[tg]:
                    results["mappings"][hba_id+":"+tgt_id] = {
                        "hba_id": hba_id,
                        "tgt_id": tgt_id,
                        "lun": mappings_data[(ig, tg)]["lun"],
                    }
        self.push_diskinfo(results, name, size)
        return results

    def resize_disk(self, volume=None, size=None, **kwargs):
        if volume is None:
            raise ex.Error("--volume is mandatory")
        if volume == "":
            raise ex.Error("--volume can not be empty")
        if size == 0 or size is None:
            raise ex.Error("--size is mandatory")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="KB")
            data = self.get_volumes(volume=volume)
            current_size = int(data["content"]["vol-size"])
            size = str(current_size + incr)+"K"
        d = {
            "vol-size": str(convert_size(size, _to="MB"))+"M",
        }
        uri = "/volumes"
        params = {}
        if volume is not None:
            try:
                int(volume)
                uri += "/"+str(volume)
            except ValueError:
                params["name"] = volume
        self.put(uri, params=params, data=d)
        ret = self.get_volumes(volume=volume)
        return ret

    def get_volume_mappings(self, volume=None, **kwargs):
        params = {"full": 1}
        uri = "/lun-maps"
        if volume is None:
            raise ex.Error("--volume is mandatory")
        data = self.get_volumes(volume=volume)
        vol_name = data["content"]["name"]
        params["filter"] = "vol-name:eq:"+vol_name
        data = self.get(uri, params=params)
        return data

    def del_volume_mappings(self, volume=None, **kwargs):
        data = self.get_volume_mappings(volume=volume)
        for mapping in data["lun-maps"]:
            self.del_map(mapping=mapping["index"])

    def del_disk(self, volume=None, **kwargs):
        if volume is None:
            raise ex.Error("--volume is mandatory")
        if volume == "":
            raise ex.Error("volume can not be empty")
        data = self.get_volumes(volume=volume)
        if "content" not in data:
            raise ex.Error("volume %s does not exist" % volume)
        disk_id = data["content"]["naa-name"]
        self.del_volume_mappings(volume=volume)
        params = {}
        uri = "/volumes"
        try:
            int(volume)
            uri += "/"+str(volume)
        except ValueError:
            params["name"] = volume
        ret = self.delete(uri, params=params)
        self.del_diskinfo(disk_id)
        return ret

    def convert_hba_id(self, hba_id):
        hba_id = hba_id[0:2] + ":" + \
                 hba_id[2:4] + ":" + \
                 hba_id[4:6] + ":" + \
                 hba_id[6:8] + ":" + \
                 hba_id[8:10] + ":" + \
                 hba_id[10:12] + ":" + \
                 hba_id[12:14] + ":" + \
                 hba_id[14:16]
        return hba_id

    def get_hba_initiatorgroup(self, hba_id):
        params = {"full": 1}
        uri = "/initiators"
        hba_id = self.convert_hba_id(hba_id)
        params["filter"] = "port-address:eq:"+hba_id
        data = self.get(uri, params=params)
        if len(data["initiators"]) == 0:
            raise ex.Error("no initiator found with port-address=%s" % hba_id)
        if len(data["initiators"][0]["ig-id"]) == 0:
            raise ex.Error("initiator %s found in no initiatorgroup" % hba_id)
        return data["initiators"][0]["ig-id"][-1]

    def get_target_targetgroup(self, hba_id):
        params = {"full": 1}
        uri = "/targets"
        hba_id = self.convert_hba_id(hba_id)
        params["filter"] = "port-address:eq:"+hba_id
        data = self.get(uri, params=params)
        if len(data["targets"]) == 0:
            raise ex.Error("no target found with port-address=%s" % hba_id)
        if len(data["targets"][0]["tg-id"]) == 0:
            raise ex.Error("target %s found in no targetgroup" % hba_id)
        return data["targets"][0]["tg-id"][-1]

    def translate_mappings(self, mappings):
        internal_mappings = {}
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            ig = self.get_hba_initiatorgroup(hba_id)
            if ig not in self.ig_portname:
                self.ig_portname[ig] = []
            self.ig_portname[ig].append(hba_id)
            internal_mappings[ig] = set()
            for target in targets:
                tg = self.get_target_targetgroup(target)
                if tg not in self.tg_portname:
                    self.tg_portname[tg] = []
                self.tg_portname[tg].append(target)
                internal_mappings[ig].add(tg)
        return internal_mappings

    def add_map(self, volume=None, mappings=None, initiatorgroup=None, targetgroup=None,
                lun=None, **kwargs):
        if volume is None:
            raise ex.Error("--volume is mandatory")
        results = {}
        if mappings is not None and initiatorgroup is None:
            internal_mappings = self.translate_mappings(mappings)
            for ig, tgs in internal_mappings.items():
                for tg in tgs:
                    map_data = self._add_map(volume=volume, initiatorgroup=ig, targetgroup=tg, lun=lun, **kwargs)
                    results[(ig, tg)] = map_data
        else:
            map_data = self._add_map(volume=volume, initiatorgroup=initiatorgroup, targetgroup=targetgroup, lun=lun, **kwargs)
            results[(initiatorgroup, targetgroup)] = map_data
        return results

    def _add_map(self, volume=None, initiatorgroup=None, targetgroup=None,
                 lun=None, **kwargs):
        if initiatorgroup is None:
            raise ex.Error("--initiatorgroup is mandatory")
        d = {
            "vol-id": volume,
            "ig-id": initiatorgroup,
        }
        if targetgroup is not None:
            d["tg-id"] = targetgroup
        if lun is not None:
            d["lun"] = lun
        ret = self.post("/lun-maps", data=d)
        return ret["content"]

    def del_map(self, mapping=None, **kwargs):
        if mapping is None:
            raise ex.Error("--mapping is mandatory")
        if mapping == "":
            raise ex.Error("mapping can not be empty")
        params = {}
        uri = "/lun-maps"
        if mapping is not None:
            try:
                int(mapping)
                uri += "/"+str(mapping)
            except ValueError:
                params["name"] = mapping
        return self.delete(uri, params=params)

    def list_target_groups(self, targetgroup=None, **kwargs):
        params = {"full": 1}
        uri = "/target-groups"
        if targetgroup is not None:
            try:
                int(targetgroup)
                uri += "/"+str(targetgroup)
            except ValueError:
                params["name"] = targetgroup
        data = self.get(uri, params=params)
        if "target-groups" in data:
            print(json.dumps(data["target-groups"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def get_initiators(self, initiator=None, **kwargs):
        params = {"full": 1}
        uri = "/initiators"
        if initiator is not None:
            try:
                int(initiator)
                uri += "/"+str(initiator)
            except ValueError:
                params["name"] = initiator
        data = self.get(uri, params=params)
        return data

    def list_initiators(self, initiator=None, **kwargs):
        data = self.get_initiators(initiator=initiator, **kwargs)
        if "initiators" in data:
            print(json.dumps(data["initiators"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_initiator_groups(self, initiatorgroup=None, **kwargs):
        params = {"full": 1}
        uri = "/initiator-groups"
        if initiatorgroup is not None:
            try:
                int(initiatorgroup)
                uri += "/"+str(initiatorgroup)
            except ValueError:
                params["name"] = initiatorgroup
        data = self.get(uri, params=params)
        if "initiator-groups" in data:
            print(json.dumps(data["initiator-groups"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_initiators_connectivity(self, **kwargs):
        params = {}
        uri = "/initiators-connectivity"
        data = self.get(uri, params=params)
        if "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_targets(self, target=None, **kwargs):
        params = {"full": 1}
        uri = "/targets"
        if target is not None:
            try:
                int(target)
                uri += "/"+str(target)
            except ValueError:
                params["name"] = target
        data = self.get(uri, params=params)
        if "targets" in data:
            print(json.dumps(data["targets"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_mappings(self, mapping=None, volume=None, **kwargs):
        params = {"full": 1}
        uri = "/lun-maps"
        if mapping is not None:
            try:
                int(mapping)
                uri += "/"+str(mapping)
            except ValueError:
                params["name"] = mapping
        if volume is not None:
            try:
                int(volume)
                params["filter"] = "vol-index:eq:"+str(volume)
                print(params)
            except ValueError:
                params["filter"] = "vol-name:eq:"+volume
        data = self.get(uri, params=params)
        if "targets" in data:
            print(json.dumps(data["lun-maps"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def get_volumes(self, volume=None, **kwargs):
        params = {"full": 1}
        uri = "/volumes"
        if volume is not None:
            try:
                int(volume)
                uri += "/"+str(volume)
            except ValueError:
                params["name"] = volume
        data = self.get(uri, params=params)
        return data

    def list_volumes(self, volume=None, **kwargs):
        data = self.get_volumes(volume=volume, **kwargs)
        if "volumes" in data:
            print(json.dumps(data["volumes"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

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
        if data["disk_id"] in (None, ""):
            data["disk_id"] = self.name+"."+str(data["driver_info"]["volume"]["index"])
        try:
            ret = self.node.collector_rest_post("/disks", {
                "disk_id": data["disk_id"],
                "disk_devid": data["disk_devid"],
                "disk_name": name,
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": "default",
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
    if not hasattr(array, action):
        raise ex.Error("not implemented")
    array.node = node
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
        main(sys.argv)
        ret = 0
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)


