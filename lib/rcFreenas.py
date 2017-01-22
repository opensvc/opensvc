import rcExceptions as ex
import os
import requests
import ConfigParser
import json
from rcGlobalEnv import rcEnv
from rcUtilities import justcall

requests.packages.urllib3.disable_warnings()
verify = False

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

    def post(self, uri, data=None):
        api = self.api+uri+"/"
        headers = {'Content-Type': 'application/json'}
        r = requests.post(api, data=json.dumps(data), auth=self.auth, verify=verify, headers=headers)
        print(r.text)
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

    def get_iscsi_target_ids(self, target_names):
        buff = self.get_iscsi_targets()
        data = json.loads(buff)
        l = []
        for target in data:
            if target["iscsi_target_name"] in target_names:
                l.append(target["id"])
        return l

    def add_disk(self, data):
        extent_id = self.add_iscsi_extent(data)
        self.add_iscsi_targets_to_extent(extent_id, data)

    def add_zvol(self, data):
        if 'dataset' not in data:
            raise ex.excError("'dataset' key is mandatory")
        if 'size' not in data:
            raise ex.excError("'size' key is mandatory")
        buff = self.get_volume(data["volume"])
        v = json.loads(buff)
        d = {
          "zvol_name": data["name"],
          "zvol_size": str(data["size"])+"MiB",
          "zvol_compression": "inherit",
          "zvol_force": "on",
          "zvol_sparse": "on",
          "zvol_blocksize": "16K",
        }
        buff = self.post('/storage/zvol/create/%s/'%data["volume"], d)

    def add_iscsi_extent(self, data):
        if 'name' not in data:
            raise ex.excError("'disk_name' key is mandatory")
        if 'size' not in data:
            raise ex.excError("'size' key is mandatory")
        if 'volume' not in data:
            raise ex.excError("'volume' key is mandatory")
        d = {
          "iscsi_target_extent_type": "File",
          "iscsi_target_extent_name": data["name"],
          "iscsi_target_extent_insecure_tpc": data.get("insecure_tpc", True),
          "iscsi_target_extent_blocksize": data.get("blocksize", 512),
          "iscsi_target_extent_filesize": str(data["size"])+"MB",
          "iscsi_target_extent_path": "/mnt/%s/%s" % (data["volume"], data["name"]),
        }
        buff = self.post("/services/iscsi/extent", d)
        try:
            extent_id = json.loads(buff)["id"]
        except KeyError:
            raise ex.excError
        return extent_id

    def add_iscsi_targets_to_extent(self, extent_id, data):
        if 'targets' not in data:
            raise ex.excError("'targets' key is mandatory")
        target_ids = self.get_iscsi_target_ids(data["targets"])
        for target_id in target_ids:
            self.add_iscsi_target_to_extent(target_id, extent_id)

    def add_iscsi_target_to_extent(self, target_id, extent_id):
        d = {
          "iscsi_target": target_id,
          "iscsi_extent": extent_id,
        }
        buff = self.post("/services/iscsi/targettoextent", d)


if __name__ == "__main__":
    o = Freenass()
    for freenas in o:
        print(freenas.get_version())
