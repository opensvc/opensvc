from __future__ import print_function

import os
import sys
import json
import logging
import time

import foreign.jwt as jwt

import core.exceptions as ex
from env import Env
from utilities.files import makedirs
from utilities.storage import Storage
from utilities.naming import factory, split_path
from utilities.converters import convert_size
from utilities.optparser import OptParser, Option
from core.node import Node

WWID_PREFIX = "624a9370"
RENEW_STATUS = 403
ITEMS_PER_PAGE = 100
MAX_PAGES = 1000
DAY_MS = 24*60*60*1000
REQUEST_TIMEOUT = 10

try:
    import requests
except ImportError:
    raise ex.InitError("the requests module must be installed")

try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass

PROG = "om array"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "now": Option(
        "--now", action="store_true", dest="now",
        help="Delete disk after flagging it destroyed (DANGER)"),
    "truncate": Option(
        "--truncate", action="store_true", dest="truncate",
        help="Allow truncating a resized volume (DANGER)"),
    "array": Option(
        "-a", "--array", action="store", dest="array_name",
        help="The name of the array, as defined in the node or cluster configuration."),
    "name": Option(
        "--name", action="store", dest="name",
        help="The object name"),
    "pod": Option(
        "--pod", action="store", dest="pod",
        help="The pod name"),
    "filter": Option(
        "--filter", action="store", dest="qfilter",
        help="The items filtering expression. ex: id='1' and serial='abc' and pod.name='pod1' and destroyed='false'."),
    "id": Option(
        "--id", action="store", dest="id",
        help="The item id"),
    "serial": Option(
        "--serial", action="store", dest="serial",
        help="The serial number"),
    "size": Option(
        "--size", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "blocksize": Option(
        "--blocksize", type=int, action="store", dest="blocksize",
        help="The exported disk blocksize in B"),
    "wwn": Option(
        "--wwn", action="store", dest="wwn",
        help="The world wide port number identifier"),
    "naa": Option(
        "--naa", action="store", dest="naa",
        help="The volume naa identifier"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map in replacement of --host and --hostgroup. Can be specified multiple times."),
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
    "host": Option(
        "--host", action="store", dest="host",
        help="The host name"),
    "hostgroup": Option(
        "--hostgroup", action="store", dest="hostgroup",
        help="The host group name"),
    "volumegroup": Option(
        "--volumegroup", action="store", dest="volumegroup",
        help="A volume group name"),
    "volume-name": Option(
        "--volume-name", action="store", dest="volume_name",
        help="A volume name"),
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
                OPT.mappings,
            ],
        },
        "add_map": {
            "msg": "Map a volume to an initiator group and target group",
            "options": [
                OPT.id,
                OPT.name,
                OPT.serial,
                OPT.mappings,
                OPT.host,
                OPT.hostgroup,
            ],
        },
        "del_disk": {
            "msg": "Delete a volume",
            "options": [
                OPT.now,
                OPT.id,
                OPT.name,
                OPT.serial,
            ],
        },
        "del_map": {
            "msg": "Unmap a volume from an initiator group and target group",
            "options": [
                OPT.id,
                OPT.name,
                OPT.serial,
                OPT.mappings,
                OPT.hostgroup,
            ],
        },
        "resize_disk": {
            "msg": "Resize a volume",
            "options": [
                OPT.id,
                OPT.name,
                OPT.serial,
                OPT.size,
                OPT.truncate,
            ],
        },
    },
    "Low-level actions": {
        "list_hosts": {
            "msg": "List hosts",
            "options": [
                OPT.filter,
            ],
        },
        "list_connections": {
            "msg": "List configured connections",
            "options": [
                OPT.filter,
            ],
        },
        "list_targets": {
            "msg": "List configured targets",
            "options": [
                OPT.target,
            ],
        },
        "list_hardware": {
            "msg": "List hardware",
            "options": [
                OPT.filter,
            ],
        },
        "list_arrays": {
            "msg": "List arrays",
            "options": [
                OPT.filter,
            ],
        },
        "list_hostgroups": {
            "msg": "List host-groups",
            "options": [
                OPT.filter,
            ],
        },
        "list_volumegroups": {
            "msg": "List configured volume-groups",
            "options": [
                OPT.filter,
                OPT.name,
                OPT.id,
            ],
        },
        "list_network_interfaces": {
            "msg": "List array network interfaces",
            "options": [
                OPT.filter,
                OPT.name,
            ],
        },
        "list_ports": {
            "msg": "List array target and replication ports",
            "options": [
                OPT.filter,
                OPT.name,
                OPT.wwn,
            ],
        },
        "list_pods": {
            "msg": "List configured pods",
            "options": [
                OPT.filter,
                OPT.name,
                OPT.id,
            ],
        },
        "list_volumes": {
            "msg": "List configured volumes",
            "options": [
                OPT.filter,
                OPT.pod,
                OPT.name,
                OPT.id,
                OPT.serial,
            ],
        },
    },
}

ts = int(time.time()*1000)
DAY_MS = 24*60*60*1000

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
            if stype != "pure":
                continue
            try:
                api = self.node.oget(s, 'api')
                client_id = self.node.oget(s, 'client_id')
                key_id = self.node.oget(s, 'key_id')
                username = self.node.oget(s, 'username')
                issuer = self.node.oget(s, 'issuer')
                insecure = self.node.oget(s, 'insecure')
                secret = self.node.oget(s, 'secret')
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            try:
                secname, namespace, _ = split_path(secret)
                sec = factory("sec")(secname, namespace=namespace, volatile=True)
                private_key = sec.decode_key("private_key")
            except Exception as exc:
                print("error reading private_key from %s: %s", secname, exc, file=sys.stderr)
                continue
            self.arrays.append(Array(name, api=api, username=username, client_id=client_id, key_id=key_id, issuer=issuer, insecure=insecure, private_key=private_key, node=self.node))
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
    def __init__(self, name, api, username=None, client_id=None, key_id=None, issuer=None, insecure=True, private_key=None, node=None):
        self.node = node
        self.name = name
        self.api = api
        self.vapi = api + "/api/2.8"
        self.username = username
        self.client_id = client_id
        self.key_id = key_id
        self.issuer = issuer
        self.verify = not insecure
        self.private_key = private_key
        self.keys = [
            "arrays",
            "hardware",
            "pods",
            "volumes",
            "volumegroups",
            "ports",
        ]

        self.tg_portname = {}
        self.ig_portname = {}
        self.log = logging.getLogger(Env.nodename+".array.pure."+self.name)
        self.token = None

    def get_token(self):
        token = self.get_cached_token()
        if token:
            return token
        token = self.new_token()
        self.cache_token(token)
        return token

    def get_cached_token(self):
        if self.token:
            return self.token
        p = self.token_cache_file()
        try:
            with open(p, "r") as f:
                return f.read()
        except Exception:
            return None

    def expire_token(self):
        self.token = None
        p = self.token_cache_file()
        try:
            os.remove(p)
        except Exception:
            pass
       
    def cache_token(self, token):
        self.token = token
        p = self.token_cache_file()
        d = os.path.dirname(p)
        makedirs(d, 0o700)
        fd = os.open(p, os.O_CREAT|os.O_WRONLY, mode=0o600)
        try:
            os.write(fd, token.encode())
        finally:
            os.close(fd)

    def token_cache_file(self):
        return os.path.join(os.sep, "run", "opensvc", "array", self.name, "token")

    def new_token(self):
        key = self.private_key.encode()
        try:
            from cryptography.hazmat.primitives import serialization  # pylint: disable=import-error
            from cryptography.hazmat.backends import default_backend  # pylint: disable=import-error
            private_key = serialization.load_pem_private_key(key, password=None, backend=default_backend())
        except Exception as exc:
            raise ex.Error(exc)
        ts = int(time.time()*1000)
        jwt_headers = {
            "kid": self.key_id,
        }
        jwt_payload = {
            "aud": self.client_id,
            "sub": self.username,
            "iss": self.issuer or self.username,
            "iat": ts,
            "exp": ts+DAY_MS,
        }
        encoded_jwt = jwt.encode(jwt_payload, private_key, headers=jwt_headers, algorithm="RS256")
        encoded_jwt = encoded_jwt.decode()
        data = {
            "content-type": "application/json",
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": encoded_jwt,
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        }
        try:
            resp = requests.post(self.api+"/oauth2/1.0/token", data=data, verify=self.verify, timeout=REQUEST_TIMEOUT)
        except Exception as exc:
            raise ex.Error(exc)
        if resp.status_code != 200:
            raise ex.Error(resp.text)

        # {
        #   "access_token":"eyJ....eyJ....ln9...",
        #   "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
        #   "token_type":"Bearer",
        #   "expires_in":86399
        # }
        return resp.json()["access_token"]

    def delete(self, uri, params=None, data=None, renewed=False):
        if params is None:
            params = {}
        headers = self.headers()
        r = requests.delete(self.vapi+uri, headers=headers, params=params, json=data, verify=self.verify, timeout=REQUEST_TIMEOUT)
        if not renewed and r.status_code == RENEW_STATUS:
            self.expire_token()
            return self.delete(uri, params=params, data=data, renewed=True)
        if r.status_code != 200:
            raise ex.Error(r.text)

    def patch(self, uri, params=None, data=None, renewed=False):
        if params is None:
            params = {}
        headers = self.headers()
        r = requests.patch(self.vapi+uri, headers=headers, params=params, json=data, verify=self.verify, timeout=REQUEST_TIMEOUT)
        if not renewed and r.status_code == RENEW_STATUS:
            self.expire_token()
            return self.patch(uri, params=params, data=data, renewed=True)
        if r.status_code != 200:
            raise ex.Error(r.text)
        return r.json()

    def put(self, uri, params=None, data=None, renewed=False):
        if params is None:
            params = {}
        headers = self.headers()
        r = requests.put(self.vapi+uri, headers=headers, params=params, json=data, verify=self.verify, timeout=REQUEST_TIMEOUT)
        if not renewed and r.status_code == RENEW_STATUS:
            self.expire_token()
            return self.put(uri, params=params, data=data, renewed=True)
        if r.status_code != 200:
            raise ex.Error(r.text)
        return r.json()

    def post(self, uri, params=None, data=None, renewed=False):
        if params is None:
            params = {}
        headers = self.headers()
        r = requests.post(self.vapi+uri, headers=headers, params=params, json=data, verify=self.verify, timeout=REQUEST_TIMEOUT)
        if not renewed and r.status_code == RENEW_STATUS:
            self.expire_token()
            return self.post(uri, params=params, data=data, renewed=True)
        if r.status_code != 200:
            raise ex.Error(r.text)
        return r.json()


    def get(self, uri, params=None, renewed=False):
        params = params or {}
        params["limit"] = ITEMS_PER_PAGE
        headers = self.headers()
        r = requests.get(self.vapi+uri, headers=headers, params=params, verify=self.verify, timeout=REQUEST_TIMEOUT)
        if not renewed and r.status_code == RENEW_STATUS:
            self.expire_token()
            return self.get(uri, params=params, renewed=True)
        if r.status_code != 200:
            raise ex.Error(r.text)
        return r.json()

    def get_items(self, uri, params=None):
        params = params or {}
        items = []
        i = 1
        while i < MAX_PAGES:
            i += 1
            data = self.get(uri, params=params)
            items += data.get("items", [])
            token = data.get("continuation_token")
            more = data.get("more_items_remaining")
            if more:
                if "offset" in params:
                     params["offset"] += ITEMS_PER_PAGE
                else:
                     params["offset"] = ITEMS_PER_PAGE
            elif token:
                params["continuation_token"] = token
            else:
                break
        return items

    def headers(self):
        return {
            "Cache-Control": "no-cache",
            "Authorization": "Bearer " + self.get_token(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_clusters_details(self):
        data = self.get("/clusters", params={"full": 1})
        return json.dumps(data["clusters"], indent=8)

    def get_targets_details(self):
        data = self.get("/targets", params={"full": 1})
        return json.dumps(data["targets"], indent=8)

    def get_hba_host(self, hba_id):
        #hba_id = self.convert_hba_id(hba_id)
        hosts = self.get_hosts(qfilter="wwns='%s'" % hba_id)
        if len(hosts) == 0:
            raise ex.Error("no host found for hba id %s" % hba_id)
        return hosts[0]

    def get_hosts(self, qfilter=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        params["filter"] = " and ".join(filters)
        return self.get_items("/hosts", params=params)

    def list_hosts(self, qfilter=None, **kwargs):
        data = self.get_hosts(qfilter=qfilter)
        print(json.dumps(data, indent=8))

    def get_hostgroups(self, qfilter=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        params["filter"] = " and ".join(filters)
        return self.get_items("/host-groups", params)

    def list_hostgroups(self, qfilter=None, **kwargs):
        data = self.get_hostgroups(qfilter=qfilter)
        print(json.dumps(data, indent=8))

    def get_connections(self, qfilter=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        params["filter"] = " and ".join(filters)
        return self.get_items("/connections", params)

    def list_connections(self, qfilter=None, **kwargs):
        data = self.get_connections(qfilter=qfilter)
        print(json.dumps(data, indent=8))

    def get_hardware(self, qfilter=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        params["filter"] = " and ".join(filters)
        return self.get_items("/hardware", params)

    def list_hardware(self, qfilter=None, **kwargs):
        data = self.get_hardware(qfilter=qfilter)
        print(json.dumps(data, indent=8))

    def get_arrays(self, qfilter=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        params["filter"] = " and ".join(filters)
        return self.get_items("/arrays", params)

    def list_arrays(self, qfilter=None, **kwargs):
        data = self.get_arrays(qfilter=qfilter)
        print(json.dumps(data, indent=8))

    def get_volumegroups(self, qfilter=None, id=None, name=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        if id is not None:
            filters += ["id='%s'" % id]
        if name is not None:
            filters += ["name='%s'" % name]
        params["filter"] = " and ".join(filters)
        return self.get_items("/volume-groups", params)

    def list_volumegroups(self, qfilter=None, id=None, name=None, **kwargs):
        data = self.get_volumegroups(qfilter=qfilter, id=id, name=name)
        print(json.dumps(data, indent=8))

    def get_network_interfaces(self, qfilter=None, name=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        if name is not None:
            filters += ["name='%s'" % name]
        params["filter"] = " and ".join(filters)
        return self.get_items("/network-interfaces", params)

    def list_network_interfaces(self, qfilter=None, name=None, **kwargs):
        data = self.get_network_interfaces(qfilter=qfilter, name=name)
        print(json.dumps(data, indent=8))

    def get_ports(self, qfilter=None, wwn=None, name=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        if wwn is not None:
            filters += ["wwn='%s'" % wwn]
        if name is not None:
            filters += ["name='%s'" % name]
        params["filter"] = " and ".join(filters)
        return self.get_items("/ports", params)

    def list_ports(self, qfilter=None, wwn=None, name=None, **kwargs):
        data = self.get_ports(qfilter=qfilter, wwn=wwn, name=name)
        print(json.dumps(data, indent=8))

    def get_pods(self, qfilter=None, id=None, name=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        if id is not None:
            filters += ["id='%s'" % id]
        if name is not None:
            filters += ["name='%s'" % name]
        params["filter"] = " and ".join(filters)
        return self.get_items("/pods", params)

    def list_pods(self, qfilter=None, id=None, name=None, **kwargs):
        data = self.get_pods(qfilter=qfilter, id=id, name=name)
        print(json.dumps(data, indent=8))

    def get_volumes(self, qfilter=None, id=None, name=None, serial=None, pod=None):
        params = {}
        filters = []
        if qfilter is not None:
            filters += [qfilter]
        if id is not None:
            filters += ["id='%s'" % id]
        if name is not None:
            filters += ["name='%s'" % name]
        if serial is not None:
            filters += ["serial='%s'" % serial]
        if pod is not None:
            filters += ["pod.name='%s'" % pod]
        params["filter"] = " and ".join(filters)
        return self.get_items("/volumes", params)

    def list_volumes(self, qfilter=None, id=None, name=None, serial=None, pod=None, **kwargs):
        data = self.get_volumes(qfilter=qfilter, id=id, name=name, serial=serial, pod=pod)
        print(json.dumps(data, indent=8))

    def add_disk(self, name=None, size=None, mappings=None, **kwargs):
        if name is None:
            raise ex.Error("--name is mandatory")
        if size == 0 or size is None:
            raise ex.Error("--size is mandatory")
        params = {
            "names": [name],
        }
        d = {
            "subtype": "regular",
            "provisioned": convert_size(size, _to="B"),
        }
        self.post("/volumes", data=d, params=params)
        if mappings:
            mappings_data = self.add_map(name=name, mappings=mappings)
        else:
            mappings_data = []
        driver_data = {}
        driver_data["volume"] = self.get_volumes(name=name)[0]
        driver_data["mappings"] = self.get_connections(qfilter="volume.id='%s'" % driver_data["volume"]["id"])
        results = {
            "driver_data": driver_data,
            "disk_id": WWID_PREFIX + driver_data["volume"]["serial"].lower(),
            "disk_devid": driver_data["volume"]["id"],
            "mappings": {},
        }
        targets = self.get_targets()
        for m in mappings_data:
            for hba_id in m["hba_ids"]:
                for tgt_id in targets:
                    results["mappings"][hba_id+":"+tgt_id] = {
                        "hba_id": hba_id,
                        "tgt_id": tgt_id,
                        "lun": m["data"][0]["lun"],
                    }
        self.push_diskinfo(results, name, size)
        return results

    def resize_disk(self, id=None, name=None, serial=None, size=None, truncate=False, **kwargs):
        if name and id:
            raise ex.Error("--name and --id are exclusive")
        if not name and not id and not serial:
            raise ex.Error("--name or --id is required")
        if size == 0 or size is None:
            raise ex.Error("--size is required")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="B")
            data = self.get_volumes(id=id, name=name, serial=serial)
            current_size = int(data[0]["provisioned"])
            size = current_size + incr
        else:
            size = convert_size(size, _to="B")
        d = {
            "provisioned": size,
        }
        params = {}
        if truncate:
            params["truncate"] = 'true'
        if id:
            params["ids"] = ",".join([id])
        if name:
            params["names"] = ",".join([name])
        self.patch("/volumes", params=params, data=d)
        ret = self.get_volumes(id=id, name=name)
        return ret

    def get_volume_connections(self, id=None, name=None, serial=None, **kwargs):
        vols = self.get_volumes(id=id, name=name, serial=serial)
        if len(vols) == 0:
            return []
        params = {}
        params["filter"] = "volume.name='%s'" % vols[0]["name"]
        return self.get_items("/connections", params=params)

    def del_disk(self, id=None, name=None, serial=None, now=False, **kwargs):
        if name and id:
            raise ex.Error("--name and --id are exclusive")
        if not name and not id and not serial:
            raise ex.Error("--name or --id is required")
        data = self.get_volumes(name=name, id=id, serial=serial)
        if len(data) == 0:
            raise ex.Error("volume does not exist")

        disk_id = WWID_PREFIX + data[0]["serial"].lower()
        self.del_map(id=data[0]["id"])
        params = {}
        params["names"] = data[0]["name"]
        if not data[0]["destroyed"]:
            self.patch("/volumes", params=params, data={"destroyed": True})
        if now:
            self.delete("/volumes", params=params)
        self.del_diskinfo(disk_id)
        return data

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

    def get_hba_hostgroup(self, hba_id):
        params = {}
        hba_id = self.convert_hba_id(hba_id)
        params["filter"] = "wwns='%s'" % hba_id
        data = self.get_items("hosts", params=params)
        if len(data) == 0:
            raise ex.Error("no initiator found with wwn=%s" % hba_id)
        for d in data:
            if not d["is_local"]:
                continue
            if not d["host_group"]["name"]:
                continue
            return d["host_group"]["name"]
        raise ex.Error("initiator %s found in no hostgroup" % hba_id)

    def get_targets(self):
        l = []
        qfilter = "services='scsi-fc' and enabled='true'"
        for d in self.get_network_interfaces(qfilter=qfilter):
            l.append(d["fc"]["wwn"].replace(":", "").lower())
        return l

    def mappings_to_hostgroups(self, mappings):
        m = {}
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            hostgroup = self.get_hba_hostgroup(hba_id)
            for target in targets:
                qfilter = "fc.wwn='%s' and services='scsi-fc' and enabled='true'" % self.convert_hba_id(target)
                tg = self.get_network_interfaces(qfilter=qfilter)
                if not tg:
                    continue
                name = hostgroup["name"]
                if name in m:
                    m[name] += [hba_id]
                else:
                    m[name] = [hba_id]
        return m

    def mappings_to_hosts(self, mappings):
        m = {}
        for mapping in mappings:
            elements = mapping.split(":")
            hba_id = elements[0]
            targets = elements[-1].split(",")
            host = self.get_hba_host(hba_id)
            for target in targets:
                qfilter = "fc.wwn='%s' and services='scsi-fc' and enabled='true'" % self.convert_hba_id(target)
                tg = self.get_network_interfaces(qfilter=qfilter)
                if not tg:
                    continue
                name = host["name"]
                if name in m:
                    m[name] += [hba_id]
                else:
                    m[name] = [hba_id]
        return m

    def add_map(self, id=None, name=None, serial=None, mappings=None, host=None, hostgroup=None, lun=None, **kwargs):
        results = []
        if mappings is not None and hostgroup is None:
            for host, hba_ids in self.mappings_to_hosts(mappings).items():
                map_data = self._add_map(id=id, name=name, serial=serial, host=host, lun=lun, **kwargs)
                results += [{"hba_ids": hba_ids, "data": map_data}]
        elif host:
            map_data = self._add_map(id=id, name=name, serial=serial, host=host, lun=lun, **kwargs)
            results += [map_data]
        elif hostgroup:
            map_data = self._add_map(id=id, name=name, serial=serial, hostgroup=hostgroup, lun=lun, **kwargs)
            results += [map_data]
        else:
            raise ex.Error("one of --mappings --host --hostgroup is required")
        return results

    def _add_map(self, id=None, name=None, serial=None, host=None, hostgroup=None, lun=None, **kwargs):
        if not hostgroup and not host:
            raise ex.Error("--hostgroup or --host is required")
        vols = self.get_volumes(id=id, name=name, serial=serial)
        if len(vols) == 0:
            raise ex.Error("volume not found")
        vol = vols[0]
        data = {}
        params = {}
        params["volume_names"] = vol["name"]
        if hostgroup:
            params["host_group_names"] = hostgroup
        if host:
            params["host_names"] = host
        if lun is not None:
            data["lun"] = lun
        return self.post("/connections", data=data, params=params)["items"]

    def del_map(self, id=None, name=None, serial=None, mappings=None, host=None, hostgroup=None, **kwargs):
        results = []
        if mappings is not None and hostgroup is None:
            for host, hba_ids in self.mappings_to_hosts(mappings).items():
                self._del_map(id=id, name=name, serial=serial, host=host, **kwargs)
                results += [host]
        else:
            hostgroup_deleted = set()
            for c in self.get_volume_connections(id=id, name=name, serial=serial):
                if host:
                    if c["host"]["name"] != host:
                        continue
                    self._del_map(id=id, name=name, serial=serial, host=c["host"]["name"], **kwargs)
                    results += [c]
                elif hostgroup:
                    if c["host_group"]["name"] != hostgroup:
                        continue
                    if c["host_group"]["name"] in hostgroup_deleted:
                        results += [c]
                        continue
                    self._del_map(id=id, name=name, serial=serial, hostgroup=c["host_group"]["name"], **kwargs)
                    results += [c]
                    hostgroup_deleted.add(c["host_group"]["name"])
                elif c["host_group"]["name"]:
                    if c["host_group"]["name"] in hostgroup_deleted:
                        results += [c]
                        continue
                    self._del_map(id=id, name=name, serial=serial, hostgroup=c["host_group"]["name"], **kwargs)
                    results += [c]
                    hostgroup_deleted.add(c["host_group"]["name"])
                else:
                    self._del_map(id=id, name=name, serial=serial, host=c["host"]["name"], **kwargs)
                    results += [c]
        return results

    def _del_map(self, id=None, name=None, serial=None, host=None, hostgroup=None, **kwargs):
        vols = self.get_volumes(id=id, name=name, serial=serial)
        if len(vols) == 0:
            raise ex.Error("volume not found")
        vol = vols[0]
        params = {}
        params["volume_names"] = vol["name"]
        if hostgroup:
            params["host_group_names"] = hostgroup
        if host:
            params["host_names"] = host
        self.delete("/connections", params=params)

    def del_diskinfo(self, disk_id):
        if disk_id in (None, ""):
            return
        if self.node is None:
            return
        try:
            ret = self.node.collector_rest_delete("/disks/%s" % disk_id)
        except Exception as exc:
            self.log.error("failed to delete the disk object in the collector: %s", exc)
            return
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
                "disk_group": "",
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


