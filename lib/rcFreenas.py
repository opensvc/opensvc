import rcExceptions as ex
import os
import requests
import ConfigParser
import json

pathlib = os.path.dirname(__file__)
pathetc = os.path.realpath(os.path.join(pathlib, '..', 'etc'))
pathtmp = os.path.realpath(os.path.join(pathlib, '..', 'tmp'))

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
        cf = os.path.join(pathetc, "auth.conf")
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

    def add_disk(self, data):
        self.add_zvol(data)

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
        print(d)
        buff = self.post('/storage/zvol/create/%s/'%data["volume"], d)
        print(buff)

    def add_extent(self, data):
        if 'name' not in data:
            raise ex.excError("'disk_name' key is mandatory")
        if 'size' not in data:
            raise ex.excError("'size' key is mandatory")
        if 'dataset' not in data:
            raise ex.excError("'dataset' key is mandatory")
        if 'volume' not in data:
            raise ex.excError("'volume' key is mandatory")
        d =   {
          "iscsi_target_extent_type": "ZVOL",
          "iscsi_target_extent_name": data["name"],
#          "iscsi_target_extent_filesize": str(data["size"])+"MB",
          "iscsi_target_extent_path": "%s/%s/%s" % (data["volume"], data["dataset"], data["name"])
        }
        print(d)
        buff = self.post("/services/iscsi/extent", d)
        print(buff)


if __name__ == "__main__":
    o = Freenass()
    for freenas in o:
        print(freenas.get_version())
