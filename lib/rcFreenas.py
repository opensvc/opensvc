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
        "-h", "--help", default=None, action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "array": Option(
        "-a", "--array", default=None, action="store", dest="array_name",
        help="The name of the array, as defined in auth.conf"),
    "name": Option(
        "--name", default=None, action="store", dest="name",
        help="The disk symbolic name"),
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
    },
    "List actions": {
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
        "list_iscsi_authorizedinitiator": {
            "msg": "List configured authorized initiator",
        },
    },
}

class Freenass(object):
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
                return array
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

    def delete(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.delete(api, data=json.dumps(data), auth=self.auth, verify=verify, headers=headers)
        return r

    def post(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.post(api, data=json.dumps(data), auth=self.auth, verify=verify, headers=headers)
        return r.content

    def post2(self, uri, data=None):
        api = self.api.replace("api/v1.0", "")+uri
        s = requests.Session()
        r = s.get(api)
        csrf_token = r.cookies['csrftoken']
        data["csrfmiddlewaretoken"] = csrf_token
        r = requests.post(api, data=data, auth=self.auth, verify=verify)
        return r.content

    def get(self, uri):
        r = requests.get(self.api+uri+"/?format=json", auth=self.auth, verify=verify)
        return r.content

    def get_version(self):
        buff = self.get("/system/version")
        return buff

    def get_volume(self, name):
        buff = self.get("/storage/volume/%s" % name)
        return buff

    def get_volume_datasets(self, name):
        buff = self.get("/storage/volume/%s/datasets" % name)
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

    def get_iscsi_portal(self):
        buff = self.get("/services/iscsi/portal")
        return buff

    def get_iscsi_targetgroup(self):
        buff = self.get("/services/iscsi/targetgroup")
        return buff

    def get_iscsi_authorizedinitiator(self):
        buff = self.get("/services/iscsi/authorizedinitiator")
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

    def add_iscsi_file(self, name=None, size=None, volume=None, targets=None,
                       insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume", "targets"]:
            if locals()[key] is None:
                 raise ex.excError("'%s' key is mandatory" % key)

        data = self.add_iscsi_file_extent(name=name, size=size, volume=volume, **kwargs)

        if "id" not in data:
            if "iscsi_target_extent_name" in data:
                if isinstance(data["iscsi_target_extent_name"], list):
                    raise ex.excError("\n".join(data["iscsi_target_extent_name"]))
                raise ex.excError(data["iscsi_target_extent_name"])
            raise ex.excError(str(data))
        self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, **kwargs)
        print(json.dumps(data, indent=8))

    def del_iscsi_file(self, name=None, naa=None, **kwargs):
        if name is None and naa is None:
            raise ex.excError("'name' or 'naa' must be specified" % key)
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            return
        self.del_iscsi_extent(data["id"])
        print(json.dumps(data, indent=8))

    def add_iscsi_zvol(self, name=None, size=None, volume=None, targets=None,
                       insecure_tpc=True, blocksize=512, **kwargs):
        for key in ["name", "size", "volume", "targets"]:
            if locals()[key] is None:
                 raise ex.excError("'%s' key is mandatory" % key)

        data = self.add_iscsi_zvol_extent(name=name, size=size, volume=volume, **kwargs)

        if "id" not in data:
            if "iscsi_target_extent_name" in data:
                if isinstance(data["iscsi_target_extent_name"], list):
                    raise ex.excError("\n".join(data["iscsi_target_extent_name"]))
                raise ex.excError(data["iscsi_target_extent_name"])
            raise ex.excError(str(data))
        self.add_iscsi_targets_to_extent(extent_id=data["id"], targets=targets, **kwargs)
        print(json.dumps(data, indent=8))

    def del_iscsi_zvol(self, name=None, naa=None, **kwargs):
        if name is None and naa is None:
            raise ex.excError("'name' or 'naa' must be specified" % key)
        data = self.get_iscsi_extent(name=name, naa=naa)
        if data is None:
            return
        path = data["iscsi_target_extent_path"].split("/")
        volume = path[path.index("zvol")+1]
        self.del_iscsi_extent(data["id"])
        self.del_zvol(name=name, volume=volume)
        print(json.dumps(data, indent=8))

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

    def list_iscsi_authorizedinitiator(self, **kwargs):
        data = json.loads(self.get_iscsi_authorizedinitiator())
        print(json.dumps(data, indent=8))

def do_action(action, array_name=None, **kwargs):
    o = Freenass()
    array = o.get_freenas(array_name)
    if array is None:
        raise ex.excError("array %s not found" % array_name)
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


