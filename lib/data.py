import os
import sys
import base64
import re
import fnmatch
import shutil
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_path, fmt_path, factory, want_context
from svc import BaseSvc
from converters import print_size
import rcExceptions as ex
import rcStatus

class DataMixin(object):
    def add(self):
        self._add(self.options.key, self.options.value_from)

    def _add_key(self, key, data):
        pass

    def add_key(self, key, data):
        if want_context():
            self.remote_add_key(key, data)
        else:
            self._add_key(key, data)

    def remote_add_key(self, key, data):
        req = {
            "action": "set_key",
            "node": "ANY",
            "options": {
                "path": self.path,
                "key": key,
                "data": data,
            }
        }
        result = self.daemon_get(req, timeout=5)
        status, error, info = self.parse_result(result)
        if info:
            print(info)
        if status:
            raise ex.excError(error)

    def _add(self, key=None, value_from=None):
        if key and sys.stdin and value_from in ("-", "/dev/stdin"):
            self.add_stdin(key)
        elif key and self.options.value:
            self.add_key(key, self.options.value)
        elif value_from and os.path.isdir(value_from):
            self.add_directory(key, value_from)
        elif value_from and os.path.isfile(value_from):
            self.add_file(key, value_from)
        elif value_from:
            self.add_glob(key, value_from)
        else:
            raise ex.excError("missing arguments")

    def add_stdin(self, key):
        data = ""
        for line in sys.stdin.readlines():
            data += line
        self.add_key(key, data)

    def add_file(self, key, path):
        if key is None:
            key = os.path.basename(path)
        #key = key.replace(".", "_")
        with open(path, "rb") as ofile:
            data = ofile.read()
        self.add_key(key, data)

    def add_glob(self, key, path):
        if key is None:
            key = ""
        fpaths = glob.glob(path)
        for path in fpaths:
            _key = os.path.join(key, os.path.basename(path))
            self.add_file(_key, path)

    def add_directory(self, key, path):
        if key is None:
            key = ""
        plen = len(os.path.dirname(path)) + 1
        def recurse(key, path):
            for root, dirs, files in os.walk(path):
                for fname in files:
                    fpath = os.path.join(path, fname)
                    _key = os.path.join(key, fpath[plen:])
                    self.add_file(_key, fpath)
                for fname in dirs:
                    fpath = os.path.join(path, fname)
                    recurse(key, fpath)
        recurse(key, path)


    def decode(self):
        buff = self.decode_key(self.options.key)
        if buff is None:
            raise ex.excError("could not decode the secret key '%s'" % self.options.key)
        try:
            sys.stdout.write(buff)
        except Exception:
            sys.stdout.buffer.write(buff)

    def keys(self):
        data = sorted(self.data_keys())
        if self.options.format in ("json", "flat_json"):
            return data
        for key in data:
            print(key)

    def data_keys(self):
        """
        Return the list of keys in the data section.
        """
        config = self.print_config_data()
        return [key for key in config.get("data", {}).keys()]

    def resolve_key(self, key):
        if key is None:
            return []
        prefix = key + "/"
        return [_key for _key in self.data_keys() if \
                key == _key or \
                _key.startswith(prefix) or \
                fnmatch.fnmatch(_key, key)]

    def install_key(self, key, path):
        """
        Install a key decoded data in the host's volatile storage.
        """
        vpath = self.key_path(key, path)
        # paranoid checks before rmtree()/unlink()
        if ".." in vpath:
            return
        data = self.decode_key(key)
        if data is None:
            raise ex.excError
        if os.path.isdir(vpath):
            self.log.info("remove %s key %s directory at location %s", self.desc, key, vpath)
            shutil.rmtree(vpath)
        vdir = os.path.dirname(vpath)
        if os.path.isfile(vdir) or os.path.islink(vdir):
            self.log.info("remove %s key %s file at parent location %s", self.desc, key, vdir)
            os.unlink(vdir)
        makedirs(vdir)
        self.write_key(vpath, data)

    def write_key(self, vpath, data):
        mtime = os.path.getmtime(self.paths.cf)
        if os.path.exists(vpath):
            if mtime == os.path.getmtime(vpath):
                return
            with open(vpath, "r") as ofile:
                current = ofile.read()
            if current == data:
                os.utime(vpath, (mtime, mtime))
                return
        self.log.info("install %s %s", self.desc, vpath)
        with open(vpath, "w") as ofile:
            os.chmod(vpath, self.default_mode)
            ofile.write(data)
            os.utime(vpath, (mtime, mtime))

    def key_path(self, key, path):
        """
        The full path to host's volatile storage file containing the key decoded data.
        """
        if path.endswith("/"):
            return os.path.join(path.rstrip(os.sep), key.strip(os.sep))
        else:
            return path

    def _install(self, key, path):
        """
        Install the <key> decoded data in the host's volatile storage.
        """
        keys = self.resolve_key(key)
        if not keys:
            raise ex.excError("%s key %s not found" % (self.desc, key))
        for _key in keys:
            self.install_key(_key, path)

    def install(self):
        self.postinstall(self.options.key)

    def postinstall(self, key=None):
        """
        Refresh installed keys
        """
        for path in self.node.svcs_selector("*/svc/*", namespace=self.namespace):
            name, _, _ = split_path(path)
            svc = factory("svc")(name, namespace=self.namespace, volatile=True, node=self.node, log=self.log)
            for vol in svc.get_resources("volume"):
                if vol.has_data(self.kind, self.path, key) and vol._status() == rcStatus.UP:
                    vol._install_data(self.kind)

