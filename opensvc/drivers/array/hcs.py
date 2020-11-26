from __future__ import print_function

import sys
import json
import time

import core.exceptions as ex
from utilities.storage import Storage
from utilities.lazy import lazy
from utilities.naming import factory, split_path
from utilities.converters import convert_size
from utilities.optparser import OptParser, Option
from core.node import Node

ERRCODE_MAPPING_HOST_LUN_NOT_EXISTS = 1073804587
ERRCODE_MAPPING_HOST_LUN_EXISTS = 1073804588
ERRCODE_SESSION_TOO_MANY = 1077949067

MAX_COUNT = 16384
VIRTUAL_LDEV_ID_NONE = 65534
VIRTUAL_LDEV_ID_GAD = 65535
MODEL_ID = {
    "VSP G370": "886000",
    "VSP G700": "886000",
    "VSP G900": "886000",
    "VSP F370": "886000",
    "VSP F700": "886000",
    "VSP F900": "886000",
    "VSP G350": "882000",
    "VSP F350": "882000",
    "VSP G800": "836000",
    "VSP F800": "836000",
    "VSP G400": "834000",
    "VSP G600": "834000",
    "VSP F400": "834000",
    "VSP F600": "834000",
    "VSP G200": "832000",
    "VSP G1000": "800000",
    "VSP G1500": "800000",
    "VSP F1500": "800000",
    "Virtual Storage Platform": "700000",
    "HUS VM": "730000",
}
RETRYABLE_ERROR_MSG_IDS = [
    "KART00003-E",
    "KART00006-E",
    "KART30003-E",
    "KART30090-E",
    "KART30095-E",
    "KART30096-E",
    "KART30097-E",
    "KART40042-E",
    "KART40049-E",
    "KART40051-E",
    "KART40052-E",
    "KART30000-E",
    "KART30008-E",
    "KART30072-E",
    "KART30074-E",
    "KART30085-E", # fake: The user ID or password is incorrect.
]
RETRYABLE_LOCK_ERROR_MSG_IDS = [
    "KART30000-E",
    "KART40050-E",
    "KART40052-E",
    "KART40052-E",
]
RETRYABLE_STATUS = {
    401: "unauthorized",
    403: "forbidden",
    500: "server error",
    503: "service unavailable",
    504: "gateway timeout",
}
NON_RETRYABLE_STATUS = {
    400: "bad request",
    404: "not found",
    405: "method not allowed",
    406: "not acceptable",
    409: "conflict",
    411: "length required",
    412: "precondition failed",
    415: "unsupported media type",
    417: "expectation failed",
    502: "proxy error",
}

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
    "pool": Option(
        "--pool", default=None, action="store", dest="pool",
        help="The pool to create the disk into"),
    "size": Option(
        "--size", default="0", action="store", dest="size",
        help="The disk size, expressed as a size expression like 1g, 100mib, ..."),
    "target": Option(
        "--target", action="append", dest="targets",
        help="A target name to export the disk through. Can be set multiple times."),
    "compression": Option(
        "--compression", default=False, action="store_true", dest="compression",
        help="Toggle compression"),
    "dedup": Option(
        "--dedup", default=False, action="store_true", dest="dedup",
        help="Toggle deduplication"),
    "naa": Option(
        "--naa", default=None, action="store", dest="naa",
        help="The disk naa identifier"),
    "resource_group": Option(
        "--resource-group", default=None, type="int", dest="resource_group",
        help="Assign the ldevs to the specified resource group."),
    "start_ldev_id": Option(
        "--start-ldev-id", default=None, type="int", dest="start_ldev_id",
        help="Assign the ldevs an id in a range starting with --start-ldev-id"),
    "end_ldev_id": Option(
        "--end-ldev-id", default=None, type="int", dest="end_ldev_id",
        help="Assign the ldevs an id in a range ending with --end-ldev-id"),
    "lun": Option(
        "--lun", action="store", type=int, dest="lun",
        help="The logical unit number to assign to the extent on attach to a target. If not specified, a free lun is automatically assigned."),
    "id": Option(
        "--id", action="store", dest="id",
        help="An object id, as reported by a list action"),
    "target": Option(
        "--target", action="append", dest="target",
        help="The target object iqn"),
    "hba_id": Option(
        "--hba-id", action="store", dest="hba_id",
        help="The initiator port name"),
    "mappings": Option(
        "--mappings", action="append", dest="mappings",
        help="A <hba_id>:<tgt_id>,<tgt_id>,... mapping used in add map. Can be specified multiple times."),
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
                OPT.pool,
                OPT.size,
                OPT.target,
                OPT.mappings,
                OPT.lun,
                OPT.compression,
                OPT.dedup,
                OPT.start_ldev_id,
                OPT.end_ldev_id,
                OPT.resource_group,
            ],
        },
        "add_ldev": {
            "msg": "Create a ldev.",
            "options": [
                OPT.name,
                OPT.pool,
                OPT.size,
                OPT.compression,
                OPT.dedup,
                OPT.start_ldev_id,
                OPT.end_ldev_id,
                OPT.resource_group,
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
            "msg": "Delete and unmap a ldev.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "del_ldev": {
            "msg": "Delete a ldev.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
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
        "rename_disk": {
            "msg": "Rename a ldev",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "resize_disk": {
            "msg": "Resize a ldev",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
                OPT.size,
            ],
        },
        "clear_reservation": {
            "msg": "Clear ldev reservation from the storage array.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "add_ldev_range_to_resource_group": {
            "msg": "Preassign a ldev range to the specified resource group.",
            "options": [
                OPT.start_ldev_id,
                OPT.end_ldev_id,
                OPT.resource_group,
            ],
        },
        "discard_zero_page": {
            "msg": "Reclaim freeable space of a DP volume.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "unassign_virtual_ldevid": {
            "msg": "Unassign the virtual ldev id.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
    },
    "List actions": {
        "list_mappings": {
            "msg": "List host ports a disk is mapped to.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "list_pools": {
            "msg": "List configured storage pools.",
        },
        "list_fc_port": {
            "msg": "List configured fibre channel ports",
        },
        "list_ldevs": {
            "msg": "List configured extents",
        },
        "list_host_groups": {
            "msg": "List configured host groups",
        },
        "list_resource_groups": {
            "msg": "List configured resource groups",
        },
        "list_supported_host_modes": {
            "msg": "List the hostmodes support by the storage array",
        },
        "list_virtual_storages": {
            "msg": "List the configured virtual storage machines",
        },
    },
    "Show actions": {
        "show_system": {
            "msg": "Show system information.",
        },
        "show_ldev": {
            "msg": "Show configured ldev.",
            "options": [
                OPT.id,
                OPT.naa,
                OPT.name,
            ],
        },
        "show_hostgroup": {
            "msg": "Show configured hostgroup.",
            "options": [
                OPT.id,
                OPT.hba_id,
            ],
        },
        "show_pool": {
            "msg": "Show configured storage pools.",
            "options": [
                OPT.name,
            ],
        },
        "show_virtual_storage": {
            "msg": "Show a virtual storage machine.",
            "options": [
                OPT.id,
            ],
        },
    },
    "Sessions": {
        "list_sessions": {
            "msg": "List the currently active sessions on the storage array",
        },
        "add_session": {
            "msg": "Open a new session on the storage array",
        },
        "del_session": {
            "msg": "Delete a session on the storage array",
            "options": [
                OPT.id,
            ],
        },
        "unlock": {
            "msg": "Unlock a resource group",
        },
    },
}

def apilock(func):
    def _func(self, *args, **kwargs):
        self.lock()
        try:
            ret = func(self, *args, **kwargs)
        finally:
            self._unlock()
        return ret
    return _func

def apiretry(func):
    def is_job_response(data):
        if not data:
            return False
        if "jobId" not in data:
            return False
        if "state" not in data:
            return False
        if data.get("status") == "Completed":
            return False
        return True

    def is_retryable_error(data):
        #return data.get("error", {}).get("messageId") in RETRYABLE_ERROR_MSG_IDS or \
        return data.get("messageId") in RETRYABLE_ERROR_MSG_IDS

    def check_condition(self, data, condition, msg):
        common_condition = lambda x: is_retryable_error(x) and not is_job_response(x)
        try:
            cc = common_condition(data)
        except Exception:
            cc = False
        try:
            sc = condition(data)
            if sc and msg:
                self.log.info(msg)
        except Exception:
            sc = False
        return sc or cc

    def _func(self, uri, retry=None, base="device", **kwargs):
        if base == "device":
            base_path = self.urlpath_device()
        else:
            base_path = self.urlpath_base()
        uri = self.api + base_path + uri
        try:
            if self.log and func.__name__ != "get" and "/sessions" not in uri:
                self.log.info("%s %s %s", func.__name__, uri, kwargs)
        except (KeyError, IndexError, AttributeError, TypeError):
            pass
        retry = retry or {}
        count = retry.get("count", self.retry)
        delay = retry.get("delay", self.delay)
        msg = retry.get("message")
        condition = retry.get("condition", lambda: False)
        for _ in range(count):
            try:
                r = func(self, uri, **kwargs)
            except ex.Error as exc:
                raise ex.Error(str(exc))
            except Exception as exc:
                raise ex.Error("hcs api request error: %s" % str(exc))
            if r.status_code == 401:
                self.session_data = None
                time.sleep(0.5)
                continue
            if r.status_code in RETRYABLE_STATUS:
                desc = RETRYABLE_STATUS[r.status_code]
                if msg:
                    try:
                        data = r.json()
                    except Exception:
                        data = {}
                    self.dump(func.__name__, uri, status=r.status_code, _result=data, **kwargs)
                self.log.info("  response status %d: %s (%s)" % (r.status_code, desc, uri), {"f_stream": False})
                time.sleep(delay)
                continue
            if r.status_code in NON_RETRYABLE_STATUS:
                desc = NON_RETRYABLE_STATUS[r.status_code]
                try:
                    data = r.json()
                except Exception:
                    data = {}
                self.dump(func.__name__, uri, status=r.status_code, _result=data, **kwargs)
                raise ex.Error("response status %d: %s (%s)" % (r.status_code, desc, uri))
            if not r.text:
                return
            data = r.json()
            if check_condition(self, data, condition, msg):
                time.sleep(delay)
                continue
            if func.__name__ != "get":
                data = self.check_result(func.__name__, uri, result=data, **kwargs)
                if check_condition(self, data, condition, msg):
                    time.sleep(delay)
                    continue
                if data.get("state") == "Failed":
                    self.dump(func.__name__, uri, _result=data, **kwargs)
            try:
                if func.__name__ != "get" and "/sessions" not in uri and "jobId" not in data:
                    self.log.info("%s", data)
            except (KeyError, IndexError, AttributeError, TypeError):
                pass
            return data
        if r.status_code in RETRYABLE_STATUS:
            desc = RETRYABLE_STATUS[r.status_code]
        elif r.status_code in NON_RETRYABLE_STATUS:
            desc = NON_RETRYABLE_STATUS[r.status_code]
        else:
            desc = "unknown"
        raise ex.Error("response status %d: %s, api request retries exhausted (%s)" % (r.status_code, desc, uri))

    return _func

class Hcss(object):
    arrays = []


    def __init__(self, objects=None, node=None, log=None):
        if objects is None:
            objects = []
        self.objects = objects
        self.filtering = len(objects) > 0
        self.timeout = 10
        if node:
            self.node = node
        else:
            self.node = Node()
        if log:
            self.log = log
        else:
            self.log = self.node.log
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
            if stype != "hcs":
                continue
            timeout = self.node.oget(s, "timeout")
            try:
                username = self.node.oget(s, "username")
                password = self.node.oget(s, "password")
                http_proxy = self.node.oget(s, "http_proxy")
                https_proxy = self.node.oget(s, "https_proxy")
                api = self.node.oget(s, "api")
                model = self.node.oget(s, "model")
                retry = self.node.oget(s, "retry")
                delay = self.node.oget(s, "delay")
            except:
                print("error parsing section", s, file=sys.stderr)
                continue
            try:
                secname, namespace, _ = split_path(password)
                password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
            except Exception as exc:
                print("error decoding password: %s", exc, file=sys.stderr)
                continue
            o = Hcs(
                name=name,
                model=model,
                api=api,
                username=username,
                password=password,
                timeout=timeout,
                http_proxy=http_proxy,
                https_proxy=https_proxy,
                retry=retry,
                delay=delay,
                node=self.node,
                log=self.log,
            )
            self.arrays.append(o)
            done.append(name)


    def __iter__(self):
        for array in self.arrays:
            yield(array)


    def get_hcs(self, name):
        for array in self.arrays:
            if array.name == name:
                return array
        return None

class Hcs(object):

    def __init__(self, name=None, model=None, api=None,
                 username=None, password=None, timeout=None,
                 http_proxy=None, https_proxy=None,
                 retry=30, delay=10,
                 node=None, log=None):
        self.node = node
        self.log = log
        self.name = name
        self.model = model
        self.api = api.rstrip("/")
        self.username = username
        self.password = password
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy
        self.auth = (username, password)
        self.timeout = timeout
        self.retry = retry
        self.delay = delay
        self.session_data = None
        self.locked = False
        self.keys = ["system",
                     "pools",
                     "fc_ports",
                     "ldevs"]
        self.session = requests.Session()
        if self.proxies:
            self.session.proxies = self.proxies
        self.naa_templates = {}


    @property
    def storage_id(self):
        pad = 6 - len(self.name)
        if pad <= 0:
            pad = ""
        else:
            pad = "0" * pad
        return MODEL_ID[self.model] + pad + self.name

    def urlpath_base(self):
        return "/ConfigurationManager/v1"

    def urlpath_device(self, device=None):
        return "/ConfigurationManager/v1/objects/storages/%s" % (device if device else self.storage_id)

    def headers(self, auth=True):
        data = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if not auth:
            return data
        if not self.session_data:
            self.open_session()
        data["Authorization"] = "Session %s" % self.session_data["token"]
        return data

    @property
    def proxies(self):
        if not self.http_proxy and not self.https_proxy:
            return
        data = {}
        if self.http_proxy:
            data["http"] = self.http_proxy
        if self.https_proxy:
            data["https"] = self.https_proxy
        return data

    @apiretry
    def delete(self, uri, data=None, base="device", retry=None):
        headers = self.headers()
        if data:
            data = json.dumps(data)
        r = self.session.delete(uri, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r


    @apiretry
    def put(self, uri, data=None, base="device", retry=None):
        headers = self.headers()
        if data:
            data = json.dumps(data, indent=4)
        r = self.session.put(uri, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r

    @apiretry
    def post(self, uri, data=None, auth=True, base="device", retry=None):
        headers = self.headers(auth=auth)
        if data:
            data = json.dumps(data, indent=4)
        if auth:
            r = self.session.post(uri, data=data, timeout=self.timeout, verify=VERIFY, headers=headers)
        else:
            r = requests.post(uri, data=data, timeout=self.timeout, verify=VERIFY, headers=headers, auth=self.auth, proxies=self.proxies)
        return r

    @apiretry
    def get(self, uri, params=None, base="device", retry=None):
        headers = self.headers()
        r = self.session.get(uri, params=params, timeout=self.timeout, verify=VERIFY, headers=headers)
        return r


    def add_session(self, **kwargs):
        data = self._open_session()
        return data

    def open_session(self):
        self.session_data = self._open_session()
        self.log.info("session id %d" % self.session_data["sessionId"], {"f_stream": False})

    def _open_session(self):
        retries = 0
        while True:
            data = self.post("/sessions", auth=False)
            code = data.get("error", {}).get("code")
            if code == ERRCODE_SESSION_TOO_MANY and retries < self.timeout:
                retries += 1
                time.sleep(1)
                continue
            elif code:
                raise ex.Error("open_session error: %s => %s" % ((self.auth[0], "xxx"), data.get("error")))
            return data

    def close_session(self):
        if not self.session_data:
            return
        if self.locked:
            self._unlock()
        self.delete("/sessions/%d" % self.session_data["sessionId"])
        self.session_data = None


    def get_system(self):
        """
        {
            "storageDeviceId" : "88xxxxxxxx91",
            "model" : "VSP G350",
            "serialNumber" : xxxx91,
            "ctl1Ip" : "192.168.xx.xx",
            "ctl2Ip" : "192.168.xx.xx",
            "dkcMicroVersion" : "88-06-02/10",
            "communicationModes" : [ {
                "communicationMode" : "lanConnectionMode"
            } ],
            "isSecure" : true,
            "targetCtl" : "CTL1",
            "usesSvp" : false
        }
        """
        return self.get("/")


    def fmt_naa(self, ldev_id, rg_id):
        template = self.get_naa_template(rg_id)
        if template is None:
            return
        ldev_id = hex(int(ldev_id))[2:]
        ldev_id = (4 - len(ldev_id)) * "0" + ldev_id
        return template[:28] + ldev_id


    def get_ldevs(self):
        data = self.get("/ldevs", params={"ldevOption": "dpVolume", "count": MAX_COUNT})["data"]
        for i, d in enumerate(data):
            data[i]["naaId"] = self.fmt_naa(d["ldevId"], d["resourceGroupId"]) or ",".join([self.name, str(d["resourceGroupId"]), str(d["ldevId"])])
        return data


    def get_naa_template(self, rg_id):
        try:
            data = self.naa_templates[rg_id]
        except KeyError:
            try:
                data = self.get_any_mapped_ldev(rg_id)
            except IndexError:
                data = {}
            try:
                data = self.get_ldev(oid=data["ldevId"])["naaId"]
            except KeyError:
                data = None
            self.naa_templates[rg_id] = data
        return data


    def get_any_mapped_ldev(self, rg_id):
        params={
            "ldevOption": "luMapped",
            "resourceGroupId": rg_id,
            "count": 1,
        }
        return self.get("/ldevs", params=params)["data"][0]


    def get_resource_groups(self):
        data = self.get("/resource-groups")
        try:
            return data["data"]
        except KeyError:
            return


    def get_host_groups(self):
        data = self.get("/host-groups")
        try:
            return data["data"]
        except KeyError:
            return


    def get_virtual_storage(self, oid):
        data = self.get("/virtual-storages/%s" % oid)
        return data


    def get_virtual_storages(self):
        data = self.get("/virtual-storages")
        try:
            return data["data"]
        except KeyError:
            return


    def get_pool_by_id(self, oid=None):
        data = self.get("/pools", params={"poolId": oid})
        try:
            return data["data"][0]
        except KeyError:
            return


    def get_pools(self):
        return self.get("/pools")["data"]

    def get_pool_by_name(self, name=None):
        params = {
            "$query": "pool.storageDeviceId eq '%s' and pool.poolName eq '%s'" % (self.storage_id, name),
        }
        data = self.get("/views/pools", params=params, base="")
        try:
            return data["data"][0]["pool"]
        except (IndexError, KeyError):
            return

    def get_pool_id(self, name=None):
        return self.get_pool_by_name(name=name)["poolId"]

    def ldev_id_from_naa(self, naa):
        return int(naa[-8:], 16)

    def get_ldev(self, oid=None, name=None, naa=None):
        if oid is not None:
            try:
                oid = int(oid)
            except ValueError:
                oid = int(oid, 16)
            data = self.get("/ldevs/%d" % oid)
        elif naa:
            ldev_id = self.ldev_id_from_naa(naa)
            data = self.get_ldev(oid=ldev_id)
            if data and "naaId" in data and naa != data["naaId"]:
                raise ex.Error("expected naa %s for ldev id %s, found %s. use --id" % (naa, ldev_id, data["naaId"]))
            return data
        elif name:
            data = self.get("/views/ldevs", params={"$query": "ldev.storageDeviceId eq '%s' and ldev.label eq '%s'" % (self.storage_id, name)}, base="")
            if len(data["data"]) > 1:
                raise ex.Error("multiple ldev found for label %s: %s" % (name, ",".join([str(d["ldev"]["ldevId"]) for d in data["data"]])))
            try:
                return self.get_ldev(data["data"][0]["ldev"]["ldevId"])
            except (KeyError, IndexError):
                return
        else:
            raise ex.Error("oid, name or naa must be specified to get_ldev()")
        return data

    def get_fc_ports(self):
        data = self.get("/ports")["data"]
        return [d for d in data if d["portType"] == "FIBRE"]

    def del_mapping(self, mapping):
        data = self.unmap_ldev_from_host(
            host_group_id=mapping["hostGroupNumber"],
            port_id=mapping["portId"],
            lun_id=mapping["lun"]
        )
        return data


    def _del_ldev(self, ldev=None, **kwargs):
        try:
            ldev_id = (ldev["ldevId"])
        except KeyError:
            raise ex.Error("_del_ldev: invalid input ldev data")

        if ldev.get("poolId") is None:
            # already deleted
            return

        # dataReductionMode = disabled
        # dataReductionStatus = DISABLED
        force = ldev.get("dataReductionMode") != "disabled"

        d = {
            "isDataReductionDeleteForceExecute": force,
        }
        path = '/ldevs/%d' % ldev_id
        data = self.delete(path, data=d)
        return data


    def add_ldev(self, name=None, size=None, pool=None,
                 compression=True, dedup=True,
                 start_ldev_id=None, end_ldev_id=None,
                 resource_group=0, **kwargs):
        for key in ["name", "size", "pool"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)
        size = convert_size(size, _to="B") // 512
        if compression and dedup:
            data_reduction_mode = "compression_deduplication"
        elif compression and not dedup:
            data_reduction_mode = "compression"
        else:
            data_reduction_mode = "disabled"
        d = {
            "poolId": self.get_pool_id(pool),
            "blockCapacity": size,
            "dataReductionMode": data_reduction_mode,
            "isParallelExecutionEnabled": True,
        }
        if start_ldev_id is not None:
            d["startLdevId"] = start_ldev_id
        if end_ldev_id is not None:
            d["endLdevId"] = end_ldev_id
        path = "/ldevs"
        data = self.post(path, data=d)
        ldev_id = int(data["affectedResources"][0].split("/")[-1])
        ldev = self.get_ldev(oid=ldev_id)
        self.set_label(ldev_id, name)

        virtual_ldev_id = ldev.get("virtualLdevId")
        _resource_group = ldev.get("resourceGroupId")
        if resource_group != _resource_group:
            self.unset_virtual_ldev_id(ldev)
            #self._unlock()
            self.set_ldev_resource_group(ldev_id, resource_group)
            #self.lock()
            self.set_virtual_ldev_id(ldev_id, ldev_id)
        elif virtual_ldev_id != ldev_id:
            self.unset_virtual_ldev_id(ldev)
            self.set_virtual_ldev_id(ldev_id, ldev_id)
        return ldev

    def lock(self):
        if self.locked:
            return
        data = {
            "parameters": {
                "waitTime": 30,
            }
        }
        retry = {
            "condition": lambda x: x.get("error", {}).get("messageId") in RETRYABLE_LOCK_ERROR_MSG_IDS
        }
        self.put("/%s/services/resource-group-service/actions/lock/invoke" % self.storage_id, data=data, base="", retry=retry)
        self.locked = True

    def unlock(self, **kwargs):
        return self._unlock(force=True)

    def _unlock(self, force=False):
        if not force and not self.locked:
            return
        retry = {
            "condition": lambda x: x.get("error", {}).get("messageId") in RETRYABLE_LOCK_ERROR_MSG_IDS
        }
        data = self.put("/%s/services/resource-group-service/actions/unlock/invoke" % self.storage_id, base="", retry=retry)
        self.locked = False
        return data

    def set_virtual_ldev_id(self, ldev_id, virtual_ldev_id):
        data = self.get("/ldevs/%d/actions/assign-virtual-ldevid" % ldev_id)
        data["parameters"]["virtualLdevId"] = virtual_ldev_id
        self.put("/ldevs/%d/actions/assign-virtual-ldevid/invoke" % ldev_id, data=data)

    def unassign_virtual_ldevid(self, id=None, name=None, naa=None, **kwargs):
        ldev = self.get_ldev(oid=id, name=name, naa=naa)
        self.unset_virtual_ldev_id(ldev)
        return self.get_ldev(oid=id, name=name, naa=naa)

    @apilock
    def unset_virtual_ldev_id(self, ldev):
        ldev_id = ldev["ldevId"]
        if ldev.get("virtualLdevId") == VIRTUAL_LDEV_ID_NONE:
            return
        data = self.get("/ldevs/%d/actions/unassign-virtual-ldevid" % ldev_id)
        if "errorSource" in data:
            # already unassigned
            return
        self.put("/ldevs/%d/actions/unassign-virtual-ldevid/invoke" % ldev_id, data=data)

    def add_ldev_range_to_resource_group(self, start_ldev_id=None, end_ldev_id=None, resource_group=0, **kwargs):
        if start_ldev_id is None or end_ldev_id is None:
            raise ex.Error("'start_ldev_id' and 'end_ldev_id' must be defined")
        data = {
            "parameters": {
                "startLdevId": start_ldev_id,
                "endLdevId": end_ldev_id,
            }
        }
        self.put("/resource-groups/%d/actions/add-resource/invoke" % resource_group, data=data)

    def set_ldev_resource_group(self, ldev_id, resource_group):
        data = {
            "parameters": {
                "ldevIds": [ldev_id],
            }
        }
        self.put("/resource-groups/%d/actions/add-resource/invoke" % resource_group, data=data)

    def rename_disk(self, id=None, name=None, naa=None, **kwargs):
        if id is None:
            ldev = self.get_ldev(naa=naa)
            id = ldev["ldevId"]
        self.set_label(id, name)

    def check_result(self, method, uri=None, headers=None, data=None, result=None, **kwargs):
        """
        {
            "jobId": 892,
            "self": "/ConfigurationManager/v1/objects/storages/882000452491/jobs/892",
            "userId": "restapi",
            "status": "Initializing",
            "state": "Queued",
            "createdTime": "2020-11-03T08:50:46Z",
            "updatedTime": "2020-11-03T08:50:46Z",
            "request": {
                "requestUrl": "/ConfigurationManager/v1/objects/storages/882000452491/ldevs",
                "requestMethod": "POST",
                "requestBody": "{\n    \"poolId\": 0,\n    \"blockCapacity\": 2097152,\n    \"dataReductionMode\": \"compression_deduplication\"\n}"
            }
        }

        {
            "jobId" : 892,
            "self" : "/ConfigurationManager/v1/objects/storages/882000452491/jobs/892",
            "userId" : "restapi",
            "status" : "Completed",
            "state" : "Succeeded",
            "createdTime" : "2020-11-03T08:50:46Z",
            "updatedTime" : "2020-11-03T08:50:49Z",
            "completedTime" : "2020-11-03T08:50:49Z",
            "request" : {
                "requestUrl" : "/ConfigurationManager/v1/objects/storages/882000452491/ldevs",
                "requestMethod" : "POST",
                "requestBody" : "{\n    \"poolId\": 0,\n    \"blockCapacity\": 2097152,\n    \"dataReductionMode\": \"compression_deduplication\"\n}"
            },
            "affectedResources" : [ "/ConfigurationManager/v1/objects/storages/882000452491/ldevs/3" ]
        }
        """
        #if "errorSource" in result:
        #    self.dump(method, uri, headers=headers, data=data, _result=result)
        if "jobId" in result:
            while True:
                r = self.get("/jobs/%d" % result["jobId"])
                if r is None:
                    return
                if r["status"] == "Completed":
                    return r
                time.sleep(2)
        return result

    def dump(self, method, uri=None, headers=None, data=None, status=None, _result=None, **kwargs):
        import traceback
        traceback.print_stack()
        buff = "%s %s\n" % (method, uri)
        if headers:
            buff += "headers:\n"
            buff += json.dumps(headers, indent=4)
            buff += "\n"
        if data:
            buff += "body:\n"
            try:
                buff += json.dumps(data, indent=4)
            except ValueError:
                buff += str(data)
            buff += "\n"
        if status:
            buff += "status: %s\n" % status
        if _result:
            buff += "result:\n"
            buff += json.dumps(_result, indent=4)
            buff += "\n"
        raise ex.Error(buff)

    @apilock
    def set_label(self, oid, label):
        d = {
            "label": label,
        }
        data = self.put("/ldevs/%d" % oid, data=d)
        return data

    def del_session(self, id=None, **kwargs):
        d = {
            "force": True
        }
        return self.delete("/sessions/%d" % int(id), data=d)

    def list_sessions(self, **kwargs):
        return self.get("/sessions")

    def list_supported_host_modes(self, **kwargs):
        return self.get("/supported-host-modes/instance")

    def list_mappings(self, id=None, naa=None, name=None, **kwargs):
        ldev_data = self.get_ldev(oid=id, naa=naa, name=name)
        if ldev_data is None:
            raise ex.Error("ldev not found")
        return self.get_mappings(ldev_data)

    def get_mappings(self, ldev_data):
        mappings = {}
        naa = ldev_data.get("naaId")
        for d in ldev_data.get("ports", []):
            tgt_id = self.port_wwn_by_id.get(d["portId"])
            hba_id = self.host_wwn_by_id(d["hostGroupNumber"], d["portId"])
            lun_id = d["lun"]
            mappings[hba_id+":"+tgt_id+":"+str(lun_id)] = {
                "mapping": d,
                "tgt_id": tgt_id,
                "hba_id": hba_id,
                "disk_id": naa,
            }
        return mappings

    def resize_disk(self, id=None, naa=None, name=None, size=None, **kwargs):
        if size is None:
            raise ex.Error("'size' key is mandatory")
        if name is None and id is None and naa is None:
            raise ex.Error("'name', 'naa' or 'id' must be specified")
        ldev = self.get_ldev(oid=id, naa=naa, name=name)
        if ldev is None:
            raise ex.Error("ldev not found")
        pool = self.get_pool_by_id(ldev["poolId"])
        if pool is None:
            raise ex.Error("pool not found")
        if size.startswith("+"):
            incr = convert_size(size.lstrip("+"), _to="B") // 512
        else:
            current_size = int(ldev["blockCapacity"])
            size = convert_size(size, _to="B") // 512
            incr = size - current_size
        if incr <= 0:
            raise ex.Error("negative ldev expansion is not allowed")

        d = {
            "parameters": {
                "additionalBlockCapacity": incr,
            }
        }
        data = self.put("/ldevs/%d/actions/expand/invoke" % ldev["ldevId"], data=d)
        data = self.get_ldev(ldev["ldevId"])
        return data


    def unmap_ldev_from_host(self, host_group_id=None, port_id=None, lun_id=None):
        oid = ",".join([
            str(port_id),
            str(host_group_id),
            str(lun_id),
        ])
        retry = {
            "condition": lambda x: x.get("error", {}).get("detailCode") == "30000E-2-B958-0233",
            "message": "retry unmaping %s: LU is executing host I/O" % oid,
        }
        data = self.delete('/luns/%s' % oid, retry=retry)
        return data


    def map_ldev_to_host(self, ldev_id=None, host_group_id=None, port_id=None, lun_id=None):
        d = {
            "ldevId": ldev_id,
            "hostGroupNumber": host_group_id,
            "portId": port_id,
            "lun": lun_id,
        }
        data = self.post('/luns', data=d)
        return data


    def del_ldev(self, id=None, naa=None, name=None, **kwargs):
        if id is None and name is None and naa is None:
            raise ex.Error("'id', 'naa' or 'name' must be specified")
        data = self.get_ldev(oid=id, name=name)
        if data is None:
            return
        return self._del_ldev(data)

    #@apilock
    def del_disk(self, id=None, name=None, naa=None, **kwargs):
        if id is None and name is None and naa is None:
            raise ex.Error("'id', 'naa' or 'name' must be specified")
        data = self.get_ldev(oid=id, naa=naa, name=name)
        if data is None:
            return
        results = {}
        response = self._unmap_lun(data)
        results["unmap"] = response
        response = self._del_ldev(data)
        results["del_ldev"] = data
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
        if not name and id is None and naa is None:
            raise ex.Error("'id' 'naa' or 'name' is mandatory")
        ldev_data = self.get_ldev(oid=id, naa=naa, name=name)
        if not ldev_data:
            raise ex.Error("no ldev found")
        return self._unmap_lun(ldev_data, mappings)


    def _unmap_lun(self, ldev_data, mappings=None):
        current_mappings = self.get_mappings(ldev_data)
        results = []
        mappings_done = set()
        if mappings:
            toremove = self.split_mappings(mappings)
        else:
            toremove = None
        for d in current_mappings.values():
            key = (d["hba_id"], d["tgt_id"])
            # {'portId': 'CL6-B', 'hostGroupNumber': 101, 'hostGroupName': 'ROS093SRDL3861_2', 'lun': 0}
            if key in mappings_done:
                continue
            if toremove is None or key in toremove:
                self.del_mapping(d["mapping"])
                mappings_done.add(key)
                results.append(d)
        return results

    def host_wwn_by_id(self, hostgroup_id, port_id):
        params = {
            "$query": "hostGroup.storageDeviceId eq %s and " \
                      "hostGroup.hostGroupNumber eq '%s' and " \
                      "hostGroup.portId eq '%s'" % (
                          self.storage_id,
                          hostgroup_id,
                          port_id
                      )
        }
        path = "/views/host-groups-host-wwns-wwns"
        data = self.get(path, params=params, base="")
        return data["data"][0]["wwn"]["wwn"]

    def show_hostgroup(self, id=None, hba_id=None, **kwargs):
        if id is not None:
            return [self.get_hostgroup_by_id(id)]
        elif hba_id:
            return self.get_hostgroups_by_hba(hba_id)
        else:
            raise ex.Error("'id' or 'hba_id' must be specified")

    def get_hostgroup_by_id(self, oid):
        path = "/host-groups/%s" % oid
        data = self.get(path)
        return data

    def get_hostgroups_by_hba(self, hba_id):
        params = {
            "$query": "hostGroup.storageDeviceId eq %s and wwn.wwn eq '%s'" % (self.storage_id, hba_id)
        }
        path = "/views/host-groups-host-wwns-wwns"
        data = self.get(path, params=params, base="")
        try:
            return [d.get("hostGroup") for d in data["data"]]
        except IndexError:
            return

    def find_hostgroups(self, hbagroup, targetgroup):
        hg_ids = set()
        data = []
        port_ids = [self.port_id_by_wwn.get(w) for w in targetgroup]
        for hba_id in hbagroup:
            _data = []
            for hg in self.get_hostgroups_by_hba(hba_id):
                if not hg:
                    continue
                if hg["hostGroupId"] in hg_ids:
                    continue
                if hg["portId"] not in port_ids:
                    continue
                hg_ids.add(hg["hostGroupId"])
                _data.append(hg)
            if not _data:
                raise ex.Error("hostgroup containing hba id %s not found" % hba_id)
            data += _data
        return data


    @apilock
    def map_lun(self, id=None, naa=None, name=None, targets=None, mappings=None, lun=None, ldev_data=None, **kwargs):
        if not name and id is None and naa is None:
            raise ex.Error("'id' 'naa' or 'name' is mandatory")
        if targets is None and mappings is None:
            raise ex.Error("'targets' or 'mappings' must be specified")

        if ldev_data is None:
            ldev_data = self.get_ldev(oid=id, naa=naa, name=name)
        if ldev_data is None:
            raise ex.Error("ldev not found")

        results = []
        for hbagroup, targetgroup in self.hbagroup_by_targetgroup(mappings):
            hostgroups = self.find_hostgroups(hbagroup, targetgroup)
            if not hostgroups:
                raise ex.Error("could not find a set of array hosts equivalent to: %s" % hbagroup)
            for hostgroup in hostgroups:
                result = self.map_ldev_to_host(ldev_id=ldev_data["ldevId"], host_group_id=hostgroup["hostGroupNumber"], port_id=hostgroup["portId"], lun_id=lun)
                results.append(result)
        return results


    #@apilock
    def add_disk(self, name=None, size=None, pool=None, targets=None,
                 mappings=None, compression=True, dedup=True, lun=None,
                 start_ldev_id=None, end_ldev_id=None, resource_group=0, **kwargs):
        for key in ["name", "size", "pool"]:
            if locals()[key] is None:
                raise ex.Error("'%s' key is mandatory" % key)

        # lun
        data = self.add_ldev(
            name=name, size=size, pool=pool,
            compression=compression, dedup=dedup,
            start_ldev_id=start_ldev_id, end_ldev_id=end_ldev_id,
            resource_group=resource_group,
        )

        # mappings
        if mappings:
            mappings = self.map_lun(id=data["ldevId"], mappings=mappings, targets=targets, lun=lun, ldev_data=data)

        data = self.get_ldev(data["ldevId"])

        if "naaId" not in data:
            print(json.dumps(data, indent=4))
            raise ex.Error("no WWN in data")

        # collector update
        warnings = []
        try:
            self.add_diskinfo(data, size, pool)
        except Exception as exc:
            warnings.append(str(exc))
        disk_id = data["naaId"]
        results = {
            "driver_data": data,
            "disk_id": disk_id,
            "disk_devid": data["ldevId"],
            "mappings": sorted(self.list_mappings(naa=disk_id).values(), key=lambda x: (x["hba_id"], x["tgt_id"], x["disk_id"])),
        }
        if warnings:
            results["warnings"] = warnings

        return results


    def show_system(self, **kwargs):
        return self.get_system()


    def show_pool(self, **kwargs):
        return self.get_pool_by_name(kwargs["name"])


    def list_pools(self, **kwargs):
        return self.get_pools()


    def list_virtual_storages(self, **kwargs):
        return self.get_virtual_storages()


    def list_host_groups(self, **kwargs):
        return self.get_host_groups()


    def list_resource_groups(self, **kwargs):
        return self.get_resource_groups()


    def discard_zero_page(self, id=None, name=None, naa=None, **kwargs):
        data = self.get_ldev(oid=id, name=name, naa=naa)
        return self.put("/ldevs/%s/actions/discard-zero-page/invoke" % data["ldevId"])

    @staticmethod
    def fmt_lun_path(port_id, hostgroup_id, lun_id):
        return ",".join([str(port_id), str(hostgroup_id), str(lun_id)])

    def clear_reservation(self, id=None, name=None, naa=None, **kwargs):
        data = self.get_ldev(oid=id, name=name, naa=naa)
        result = []
        for i, port in enumerate(data.get("ports", [])):
            result.append(self._clear_reservation(port["portId"], port["hostGroupNumber"], port["lun"]))
        return result

    def _clear_reservation(self, port_id, hostgroup_id, lun_id):
        return self.post("/luns/%s/actions/release-lu-host-reserve/invoke" % self.fmt_lun_path(port_id, hostgroup_id, lun_id))


    def show_virtual_storage(self, id=None, **kwargs):
        return self.get_virtual_storage(id)


    def show_ldev(self, id=None, name=None, naa=None, **kwargs):
        data = self.get_ldev(oid=id, name=name, naa=naa)
        for i, port in enumerate(data.get("ports", [])):
            data["ports"][i]["lun_data"] = self.get_lun(port["portId"], port["hostGroupNumber"], port["lun"])
        return data

    def get_lun(self, port_id, hostgroup_id, lun_id):
        return self.get("/luns/%s" % self.fmt_lun_path(port_id, hostgroup_id, lun_id))

    def list_ldevs(self, **kwargs):
        return self.get_ldevs()


    def list_fc_port(self, **kwargs):
        return self.get_fc_ports()

    @lazy
    def port_id_by_wwn(self):
        data = {}
        for d in self.get_fc_ports():
            data[d["wwn"]] = d["portId"]
        return data

    @lazy
    def port_wwn_by_id(self):
        data = {}
        for d in self.get_fc_ports():
            data[d["portId"]] = d["wwn"]
        return data

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


    def add_diskinfo(self, data, size=None, pool=None):
        if self.node is None:
            return
        try:
            result = self.node.collector_rest_post("/disks", {
                "disk_id": data["naaId"],
                "disk_devid": data["ldevId"],
                "disk_name": data["label"],
                "disk_size": convert_size(size, _to="MB"),
                "disk_alloc": 0,
                "disk_arrayid": self.name,
                "disk_group": pool,
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
    o = Hcss()
    array = o.get_hcs(array_name)
    if array is None:
        raise ex.Error("array %s not found" % array_name)
    array.node = node
    if not hasattr(array, action):
        raise ex.Error("not implemented")
    try:
        result = getattr(array, action)(**kwargs)
    finally:
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


