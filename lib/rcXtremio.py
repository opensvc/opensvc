from __future__ import print_function

import sys
import os
import requests
import ConfigParser
import json

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import justcall,convert_size
from rcOptParser import OptParser
from optparse import Option


requests.packages.urllib3.disable_warnings()
verify = False

PROG = "nodemgr array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in auth.conf"),
    "cluster": Option(
        "--cluster", action="store", dest="cluster",
        help="The name or id of the arry cluster. Optional for single-cluster setups, mandatory otherwise"),
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
    "initiator": Option(
        "--initiator", action="append", dest="initiators",
        help="An initiator iqn. Can be specified multiple times."),
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
    OPT.cluster,
]

DEPRECATED_ACTIONS = []

ACTIONS = {
    "Add actions": {
        "add_volume": {
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
            ],
        },
        "add_map": {
            "msg": "Map a volume to an initiator group and target group",
            "options": [
                OPT.volume,
                OPT.initiatorgroup,
                OPT.targetgroup,
            ],
        },
    },
    "Delete actions": {
        "del_volume": {
            "msg": "Delete a volume",
            "options": [
                OPT.volume,
            ],
        },
        "del_map": {
            "msg": "Unmap a volume from an initiator group and target group",
            "options": [
                OPT.cluster,
                OPT.mapping,
                OPT.volume,
                OPT.initiatorgroup,
                OPT.targetgroup,
            ],
        },
    },
    "List actions": {
        "list_initiatorgroups": {
            "msg": "List configured initiator groups",
            "options": [
                OPT.initiatorgroup,
            ],
        },
        "list_targets": {
            "msg": "List configured targets",
            "options": [
                OPT.target,
            ],
        },
        "list_targetgroups": {
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
            if stype != "xtremio":
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
            self.arrays.append(Array(name, api, username, password))
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
    def __init__(self, name, api, username, password):
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

    def delete(self, uri, data=None):
        if not uri.startswith("http"):
            uri = self.api + uri
        api = uri+"/"
        headers = {'Content-Type': 'application/json'}
        print(api, data)
        return
        response = requests.delete(api, data=json.dumps(data), auth=self.auth, verify=verify, headers=headers)
        if response.status_code == 200:
            return
        data = json.loads(response.content)
        return data

    def post(self, uri, data=None):
        if not uri.startswith("http"):
            uri = self.api + uri
        api = uri+"/"
        headers = {'Content-Type': 'application/json'}
        print(api, data)
        return
        r = requests.post(api, data=json.dumps(data), auth=self.auth, verify=verify, headers=headers)
        data = json.loads(r.content)
        return self.get(data["links"]["href"])

    def get(self, uri):
        if not uri.startswith("http"):
            uri = self.api + uri
        r = requests.get(uri, auth=self.auth, verify=verify)
        return json.loads(r.content)

    def get_dereference(self, uri, key="children"):
        data = self.get(uri)
        if key not in data:
            return data
        for idx, child in enumerate(data[key]):
            if "href" in child:
                data[key][idx] = self.get(child["href"])
        return data[key]

    def get_clusters_details(self):
        data = self.get_dereference("/clusters", "clusters")
        return json.dumps(data, indent=8)

    def get_targets_details(self):
        data = self.get_dereference("/targets", "targets")
        return json.dumps(data, indent=8)

    def get_volumes_details(self):
        data = self.get_dereference("/volumes", "volumes")
        return json.dumps(data, indent=8)

    def add_volume(self, name=None, size=None, blocksize=None, tags=None,
                   cluster=None, access=None, vaai_tp_alerts=None,
                   small_io_alerts=None, unaligned_io_alerts=None,
                   alignment_offset=None, **kwargs):
        if name is None:
            raise ex.excError("--name is mandatory")
        if size == 0 or size is None:
            raise ex.excError("--size is mandatory")
        d = {
            "vol-name": name,
            "vol-size": str(convert_size(size, _to="MB"))+"M",
        }
        if cluster is not None:
            d["cluster-id"] = cluster
        if tags is not None:
            d["tags"] = tags
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
        return self.post("/volumes", d)

    def del_volumes(self, cluster=None, volume=None, **kwargs):
        if volume is None:
            raise ex.excError("--volume is mandatory")
        if volume == "":
            raise ex.excError("mapping can not be empty")
        args = []
        uri = "/volumes"
        if volume is not None:
            try:
                int(volume)
                uri += "/"+str(volume)
            except ValueError:
                args.append("name=" + volume)
        if cluster is not None:
            args.append("cluster-id=" + cluster)
        if len(args) > 0:
            uri += "?" + "&".join(args)
        return self.delete(uri)

    def add_map(self, volume=None, initiatorgroup=None, targetgroup=None,
                cluster=None, lun=None, **kwargs):
        if volume is None:
            raise ex.excError("--volume is mandatory")
        if initiatorgroup is None:
            raise ex.excError("--initiatorgroup is mandatory")
        d = {
            "vol-id": volume,
            "ig-id": initiatorgroup,
        }
        if targetgroup is not None:
            d["tg-id"] = targetgroup
        if cluster is not None:
            d["cluster-id"] = cluster
        if lun is not None:
            d["lun"] = lun
        return self.post("/lun-maps", d)

    def del_map(self, mapping=None, cluster=None, **kwargs):
        if mapping is None:
            raise ex.excError("--mapping is mandatory")
        if mapping == "":
            raise ex.excError("mapping can not be empty")
        args = []
        uri = "/lun-maps"
        if mapping is not None:
            try:
                int(mapping)
                uri += "/"+str(mapping)
            except ValueError:
                args.append("name=" + mapping)
        if cluster is not None:
            args.append("cluster-id=" + cluster)
        if len(args) > 0:
            uri += "?" + "&".join(args)
        return self.delete(uri)

    def list_targetgroups(self, cluster=None, targetgroup=None, **kwargs):
        args = []
        uri = "/target-groups"
        if targetgroup is not None:
            try:
                int(targetgroup)
                uri += "/"+str(targetgroup)
            except ValueError:
                args.append("name=" + targetgroup)
        if cluster is not None:
            args.append("cluster-id=" + cluster)
        if len(args) > 0:
            uri += "?" + "&".join(args)
        data = self.get(uri)
        if "target-groups" in data:
            print(json.dumps(data["target-groups"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_targets(self, cluster=None, target=None, **kwargs):
        args = []
        uri = "/targets"
        if target is not None:
            try:
                int(target)
                uri += "/"+str(target)
            except ValueError:
                args.append("name=" + target)
        if cluster is not None:
            args.append("cluster-id=" + cluster)
        if len(args) > 0:
            uri += "?" + "&".join(args)
        data = self.get(uri)
        if "targets" in data:
            print(json.dumps(data["targets"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

    def list_volumes(self, cluster=None, volume=None, **kwargs):
        args = []
        uri = "/volumes"
        if volume is not None:
            try:
                int(volume)
                uri += "/"+str(volume)
            except ValueError:
                args.append("name=" + volume)
        if cluster is not None:
            args.append("cluster-id=" + cluster)
        if len(args) > 0:
            uri += "?" + "&".join(args)
        data = self.get(uri)
        if "volumes" in data:
            print(json.dumps(data["volumes"], indent=8))
        elif "content" in data:
            print(json.dumps(data["content"], indent=8))
        else:
            print(json.dumps(data, indent=8))

def do_action(action, array_name=None, **kwargs):
    o = Arrays()
    array = o.get_array(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
    if not hasattr(array, action):
        raise ex.excError("not implemented")
    getattr(array, action)(**kwargs)

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


