import os
import sys
import base64
import re
import fnmatch
import shutil
import glob

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs
from svc import BaseSvc
from converters import print_size
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Sec(BaseSvc):
    kind = "sec"

    @lazy
    def kwdict(self):
        return __import__("secdict")

    def add(self):
        self._add(self.options.key, self.options.value_from)

    def _add(self, key=None, value_from=None):
        if key and sys.stdin and value_from in ("-", "/dev/stdin"):
            self.add_stdin(key)
        elif key and self.options.value:
            self.add_secret(key, self.options.value)
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
        self.add_secret(key, data)

    def add_file(self, key, path):
        if key is None:
            key = os.path.basename(path)
        #key = key.replace(".", "_")
        with open(path, "r") as ofile:
            data = ofile.read()
        self.add_secret(key, data)

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

    def add_secret(self, key, data):
        if not key:
            raise ex.excError("secret key name can not be empty")
        if not data:
            raise ex.excError("secret value can not be empty")
        data = "crypt:"+base64.urlsafe_b64encode(self.encrypt(data, cluster_name="join", encode=True)).decode()
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("secret key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        if os.path.exists(self.key_path(key)):
            self.install_key(key)

    def decode(self):
        print(self.decode_secret(self.options.key))

    def decode_secret(self, key):
        if not key:
            raise ex.excError("secret key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.excError("secret key %s does not exist or has no value" % key)
        if data.startswith("crypt:"):
            data = data[6:]
            return self.decrypt(base64.urlsafe_b64decode(data))[1]

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

    def install_key(self, key):
        """
        Install a key decoded data in the host's volatile storage.
        """
        vpath = self.key_path(key)
        # paranoid checks before rmtree()/unlink()
        if ".." in vpath:
            return
        if os.path.isdir(vpath):
            self.log.info("remove secret key %s directory at location %s", key, vpath)
            shutil.rmtree(vpath)
        vdir = os.path.dirname(vpath)
        if os.path.isfile(vdir) or os.path.islink(vdir):
            self.log.info("remove secret key %s file at parent location %s", key, vdir)
            os.unlink(vdir)
        makedirs(vdir)
        data = self.decode_secret(key)
        self.write_key(vpath, data)

    def write_key(self, vpath, data):
        secmtime = os.path.getmtime(self.paths.cf)
        if os.path.exists(vpath):
            if secmtime == os.path.getmtime(vpath):
                return
            with open(vpath, "r") as ofile:
                current = ofile.read()
            if current == data:
                os.utime(vpath, (secmtime, secmtime))
                return
        self.log.info("install secret %s", vpath)
        with open(vpath, "w") as ofile:
            os.chmod(vpath, 0o0600)
            ofile.write(data)
            os.utime(vpath, (secmtime, secmtime))

    @lazy
    def secret_dir(self):
        """
        The secret private directory in host's volatile storage.
        """
        if self.namespace:
            return os.path.join(rcEnv.paths.secrets, "namespaces", self.namespace, "sec", self.svcname)
        else:
            return os.path.join(rcEnv.paths.secrets, "sec", self.svcname)

    def key_path(self, key):
        """
        The full path to host's volatile storage file containing the key decoded data.
        """
        return os.path.join(self.secret_dir, key)

    def _install(self, key):
        """
        Install the <key> decoded data in the host's volatile storage.
        """
        keys = self.resolve_key(key)
        if not keys:
            raise ex.excError("secret key %s not found" % key)
        for _key in keys:
            self.install_key(_key)

    def install(self):
        """
        The "install" action entrypoint.
        """
        self._install(self.options.key)

    def installed_keys(self):
        return [key for key in self.data_keys() if os.path.exists(self.key_path(key))]

    def postinstall(self):
        """
        Refresh installed keys
        """
        for key in self.installed_keys():
            self.install_key(key)

