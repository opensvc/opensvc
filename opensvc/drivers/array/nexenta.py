import json
import base64

import core.exceptions as ex
from foreign.six.moves.urllib.request import Request, urlopen # pylint: disable=import-error
from foreign.six.moves.urllib.error import URLError # pylint: disable=import-error
from utilities.naming import factory, split_path
from core.node import Node

class logger(object):
    def __init__(self):
        pass

    def info(self, msg):
        print(msg)

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)

class Nexenta(object):
    def __init__(self, head, log=None, node=None):
        self.object_type_cache = {}
        self.head = head
        self.auto_prefix = "svc:/system/filesystem/zfs/auto-sync:"
        self.username = None
        self.password = None
        self.port = 2000
        if node:
            self.node = node
        else:
            self.node = Node()
        if log is not None:
            self.log = log
        else:
            self.log = self.node.log

    def init(self):
        if self.username is not None and self.password is not None:
            return
        s = "array#" + self.head
        try:
            stype = self.node.oget(s, "type")
        except Exception:
            raise ex.Error("no array configuration for head %s"%self.head)
        if stype != "nexenta":
            raise ex.Error("array %s type is not nexanta" % self.head)
        try:
            self.username = self.node.oget(s, "username")
        except Exception:
            raise ex.Error("no username information for head %s"%self.head)
        try:
            self.password = self.node.oget(s, "password")
        except Exception:
            raise ex.Error("no password information for head %s"%self.head)
        self.port = self.node.oget(s, "port")
        try:
            secname, namespace, _ = split_path(self.password)
            self.password = factory("sec")(secname, namespace=namespace, volatile=True).decode_key("password")
        except Exception as exc:
            raise ex.Error("error decoding password: %s" % exc)
        self.url = 'https://%(head)s:%(port)d/rest/nms/ <https://%(head)s:%(port)d/rest/nms/>'%dict(head=self.head, port=self.port)

    def rest(self, obj, method, params):
        self.init()
        data = {"method": method, "params": params, "object": obj}
        data = json.dumps(data)
        request = Request(self.url, data)
        base64string = base64.encodestring('%s:%s' % (self.username, self.password))[:-1]
        request.add_header('Authorization', 'Basic %s' % base64string)
        request.add_header('Content-Type' , 'application/json')
        try:
            response = urlopen(request)
        except URLError:
            raise ex.Error("unreachable head %s"%self.head)
        response = json.loads(response.read())
        return response

    def dbus_auth_keys_list(self):
        data = self.rest("appliance", "dbus_auth_keys_list", [])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def ssh_list_bindings(self):
        data = self.rest("appliance", "ssh_list_bindings", [])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def ssh_unbind(self, user, hostport, force="0"):
        data = self.rest("appliance", "ssh_unbind", [user, hostport, force])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def ssh_bind(self, user, hostport, password):
        data = self.rest("appliance", "ssh_bind", [user, hostport, password])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_get_names(self):
        data = self.rest("autosync", "get_names", [''])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_disable(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "disable", [name])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_enable(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "enable", [name])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_execute(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "execute", [name])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_get_state(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "get_state", [name])
        if data['error'] is not None:
            raise ex.Error(data['error'])
        return data['result']

    def autosync_set_prop(self, name, prop, value):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "set_child_prop", [name, prop, value])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def autosync_get_props(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("autosync", "get_child_props", [name, ''])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def autosync_register(self, name):
        if not name.startswith(self.auto_prefix):
            name = self.auto_prefix+name
        data = self.rest("runner", "register", [name, {}, {}])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def zvol_clone(self, src, dst):
        data = self.rest("zvol", "clone", [src, dst])
        if data['error'] is not None:
            raise ex.Error(data["error"])

    def folder_clone(self, src, dst):
        data = self.rest("folder", "clone", [src, dst])
        if data['error'] is not None:
            raise ex.Error(data["error"])

    def clone(self, src, dst):
        snap = "@".join([src, dst.replace('/','_')])
        object_type = self.object_type(src)
        if object_type == "folder":
            self.folder_clone(snap, dst)
        elif object_type == "zvol":
            self.zvol_clone(snap, dst)
        else:
            raise ex.Error("object type %s is not cloneable"%str(object_type))

    def snapshot_create(self, src, dst, recursive=0):
        dst = dst.replace('/','_')
        object_type = self.object_type(src)
        if object_type == "folder":
            self.folder_snapshot(src, dst, recursive)
        elif object_type == "zvol":
            self.zvol_snapshot(src, dst, recursive)
        else:
            raise ex.Error("object type %s is not snapable"%str(object_type))

    def zvol_snapshot(self, src, dst, recursive=0):
        data = self.rest("zvol", "create_snapshot", [src, dst, recursive])
        if data['error'] is not None:
            raise ex.Error(data["error"])

    def folder_snapshot(self, src, dst, recursive=0):
        snap = "@".join([src, dst])
        data = self.rest("snapshot", "create", [snap, recursive])
        if data['error'] is not None:
            raise ex.Error(data["error"])

    def snapshot_destroy(self, src, dst, recursive=''):
        snap = "@".join([src, dst])
        data = self.rest("snapshot", "destroy", [snap, recursive])
        if data['error'] is not None:
            raise ex.Error(data["error"])

    def snapshot_get_names(self):
        data = self.rest("snapshot", "get_names", [''])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def folder_get_names(self):
        data = self.rest("folder", "get_names", [''])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        for folder in data['result']:
            self.object_type_cache[folder] = "folder"
        return data['result']

    def zvol_get_names(self):
        data = self.rest("zvol", "get_names", [''])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        for zvol in data['result']:
            self.object_type_cache[zvol] = "zvol"
        return data['result']

    def object_type(self, o):
        if o in self.object_type_cache:
            return self.object_type_cache[o]
        if o in self.folder_get_names():
            self.object_type_cache[o] = "folder"
            return "folder"
        elif o in self.zvol_get_names():
            self.object_type_cache[o] = "zvol"
            return "zvol"
        else:
            raise ex.Error("can not determine type of object %s"%o)

    def set_prop(self, name, prop, val):
        otype = self.object_type(name)
        return self._set_prop(otype, name, prop, val)

    def _set_prop(self, otype, name, prop, val):
        data = self.rest(otype, "set_child_prop", [name, prop, val])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def get_props(self, name):
        otype = self.object_type(name)
        return self._get_props(otype, name)

    def _get_props(self, otype, name):
        data = self.rest(otype, "get_child_props", [name, ''])
        if data['error'] is not None:
            raise ex.Error(data["error"])
        return data['result']

    def set_can_mount(self, name):
        p = self.get_props(name)
        if not 'canmount' in p:
            return
        self.set_prop(name, "canmount", "on")

    def autosync_set_can_mount(self, name):
        folders = self.folder_get_names()
        props = self.autosync_get_props(name)

        if props['zfs/from-host'] == 'localhost':
            synchead = props['zfs/from-fs']
        else:
            synchead = props['zfs/to-fs']

        synchead = synchead.lstrip('/')
        for folder in folders:
            if not folder.startswith(synchead):
                continue
            self.set_can_mount(folder)
            self.log.info("set 'canmount = on' on folder %s"%folder)

    def snapclone(self, src, dst):
        self.snapshot_create(src, dst)
        self.clone(src, dst)

if __name__ == "__main__":
    o = Nexenta("nexenta1")
    #names = o.autosync_register("test")
    #print(o.set_prop("vol1/folder1", "canmount", "on"))
    #print(o.get_props("vol1/folder1"))
    print(Nexenta("nexenta1").dbus_auth_keys_list())
    print(Nexenta("nexenta2").dbus_auth_keys_list())
    #print(o.autosync_set_can_mount("vol1-folder1-000"))
    #names = o.autosync_get_names()
    #print(o.autosync_set_prop(names[0], "zfs/reverse_capable", "1"))
    #print(o.autosync_get_state(names[0]))
    #print(o.autosync_get_props(names[0]))
    #print(o.snapshot_create("vol1/zvol1", "test"))
    #print(o.snapshot_get_names())
    #print(o.snapshot_destroy("vol1/zvol1", "test"))
    #print(o.snapclone("vol1/folder1", "vol1/folder2"))

