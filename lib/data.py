import os
import sys
import base64
import re
import fnmatch
import shutil
import glob
import tempfile

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_path, fmt_path, factory, want_context, bencode, which
from svc import BaseSvc
from converters import print_size
import rcExceptions as ex
import rcStatus

class DataMixin(object):


    def add(self):
        self._add(self.options.key, self.options.value_from)


    def remove(self):
        return self._remove(self.options.key)


    def append(self):
        self._add(self.options.key, self.options.value_from, append=True)


    def _remove(self, key):
        if key not in self.data_keys():
            return
        return self.unset_multi(["data."+key])


    def _add_key(self, key, data):
        pass


    def add_key(self, key, data, append=False):
        if append:
            data = self.decode_key(key) + data
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
        result = self.daemon_post(req, timeout=5)
        status, error, info = self.parse_result(result)
        if info:
            print(info)
        if status:
            raise ex.excError(error)


    def _add(self, key=None, value_from=None, append=False):
        if key and sys.stdin and value_from in ("-", "/dev/stdin"):
            self.add_stdin(key, append=append)
        elif key and self.options.value is not None:
            self.add_key(key, self.options.value, append=append)
        elif value_from and os.path.isdir(value_from):
            self.add_directory(key, value_from, append=append)
        elif value_from and os.path.isfile(value_from):
            self.add_file(key, value_from, append=append)
        elif value_from:
            self.add_glob(key, value_from, append=append)
        else:
            raise ex.excError("missing arguments")


    def add_stdin(self, key, append=False):
        if append:
            data = self.decode_key(key)
        else:
            data = ""
        for line in sys.stdin.readlines():
            data += line
        self.add_key(key, data)


    def add_file(self, key, path, append=None):
        if key is None:
            key = os.path.basename(path)
        if append:
            data = bencode(self.decode_key(key))
        else:
            data = b""
        with open(path, "rb") as ofile:
            data += ofile.read()
        self.add_key(key, data)


    def add_glob(self, key, path, append=False):
        if key is None:
            key = ""
        fpaths = glob.glob(path)
        for path in fpaths:
            _key = os.path.join(key, os.path.basename(path))
            self.add_file(_key, path, append=append)


    def add_directory(self, key, path, append=False):
        if key is None:
            key = ""
        plen = len(os.path.dirname(path)) + 1
        for root, dirs, files in os.walk(path):
            for fname in files:
                fpath = os.path.join(root, fname)
                _key = os.path.join(key, fpath[plen:])
                self.add_file(_key, fpath, append=append)


    @staticmethod
    def tempfilename():
        tmpf = tempfile.NamedTemporaryFile()
        try:
            return tmpf.name
        finally:
            tmpf.close()


    def edit(self):
        if self.options.key is None:
            self.edit_config()
            return
        buff = self.decode_key(self.options.key)
        no_newline = buff.count(os.linesep) == 0
        if buff is None:
            raise ex.excError("could not decode the secret key '%s'" % self.options.key)
        if "EDITOR" in os.environ:
            editor = os.environ["EDITOR"]
        elif os.name == "nt":
            editor = "notepad"
        else:
            editor = "vi"
        if not which(editor):
            raise ex.excError("%s not found" % editor)
        fpath = self.tempfilename()
        try:
            with open(fpath, "wb") as f:
                f.write(buff)
        except TypeError as exc:
            with open(fpath, "w") as f:
                f.write(buff)
        try:
            os.system(' '.join((editor, fpath)))
            with open(fpath, "r") as f:
                edited = f.read()
            if no_newline and edited.count(os.linesep) == 1 and edited.endswith(os.linesep):
                self.log.debug("striping trailing newline from edited key value")
                edited = edited.rstrip(os.linesep)
            if buff == edited:
                return
            self.add_key(self.options.key, edited)
        finally:
            os.unlink(fpath)


    def decode(self):
        buff = self.decode_key(self.options.key)
        if buff is None:
            raise ex.excError("could not decode the secret key '%s'" % self.options.key)
        try:
            sys.stdout.buffer.write(buff)
        except (TypeError, AttributeError):
            # buff is not binary, .buffer is not supported
            sys.stdout.write(buff)


    def keys(self):
        data = sorted(self.data_keys())
        if self.options.format in ("json", "flat_json"):
            return data
        for key in data:
            print(key)


    def has_key(self, key):
        return key in self.data_keys()


    def data_keys(self):
        """
        Return the list of keys in the data section.
        """
        config = self.print_config_data()
        return [key for key in config.get("data", {}).keys()]


    def data_dirs(self):
        dirs = set()
        keys = self.data_keys()
        for key in keys:
            path = key
            while True:
                path = os.path.dirname(path)
                if not path or path == '/':
                    break
                if path in keys:
                    continue
                dirs.add(path)
        return sorted(list(dirs))


    def resolve_key(self, key):
        if key is None:
            return []
        keys = self.data_keys()
        dirs = self.data_dirs()
        done = set()

        def recurse(key, done):
            data = []
            for path in dirs:
                if path != key and not fnmatch.fnmatch(path, key):
                    continue
                rkeys, rdone = recurse(path+"/*", done)
                done |= rdone
                data.append({
                    "type": "dir",
                    "path": path,
                    "keys": rkeys,
                })
            for path in keys:
                if path != key and not fnmatch.fnmatch(path, key):
                    continue
                if path in done:
                    continue
                done.add(path)
                data.append({
                    "type": "file",
                    "path": path,
                })
            return data, done

        return recurse(key, done)[0]


    def install_key(self, key, path):
        if key["type"] == "file":
            vpath = self.key_path(key, path)
            self.install_file_key(key["path"], vpath)
        elif key["type"] == "dir":
            self.install_dir_key(key, path)


    def install_dir_key(self, data, path):
        """
        Install a key decoded data in the host's volatile storage.
        """
        if path.endswith("/"):
            dirname = os.path.basename(data["path"])
            dirpath = os.path.join(path.rstrip("/"), dirname, "")
        else:
            dirname = os.path.basename(path)
            dirpath = path + "/"
        makedirs(dirpath)
        for key in data["keys"]:
            self.install_key(key, dirpath)


    def install_file_key(self, key, vpath):
        """
        Install a key decoded data in the host's volatile storage.
        """
        # paranoid checks before rmtree()/unlink()
        if ".." in vpath:
            return
        data = self.decode_key(key)
        if data is None:
            raise ex.excError("no data in key %s" % key)
        if os.path.isdir(vpath):
            self.log.info("remove %s key %s directory at location %s", self.desc, key, vpath)
            shutil.rmtree(vpath)
        vdir = os.path.dirname(vpath)
        if os.path.isfile(vdir) or os.path.islink(vdir):
            self.log.info("remove %s key %s file at parent location %s", self.desc, key, vdir)
            os.unlink(vdir)
        makedirs(vdir)
        self.write_key(vpath, data, key=key)


    def key_path(self, key, path):
        """
        The full path to host's volatile storage file containing the key decoded data.
        """
        if path.endswith("/"):
            name = os.path.basename(key["path"].rstrip("/"))
            npath = os.path.join(path.rstrip("/"), name)
        else:
            npath = path
        return npath


    def write_key(self, vpath, data, key=None):
        mtime = os.path.getmtime(self.paths.cf)
        try:
            data = data.encode()
        except (AttributeError, UnicodeDecodeError, UnicodeEncodeError):
            # already bytes
            pass
        if os.path.exists(vpath):
            if mtime == os.path.getmtime(vpath):
                return
            with open(vpath, "rb") as ofile:
                current = ofile.read()
            if current == data:
                os.utime(vpath, (mtime, mtime))
                return
        self.log.info("install %s/%s in %s", self.name, key, vpath)
        with open(vpath, "wb") as ofile:
            os.chmod(vpath, self.default_mode)
            ofile.write(data)
            os.utime(vpath, (mtime, mtime))


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
        for path in self.node.svcs_selector("*/svc/*", namespace=self.namespace, local=True):
            name, _, _ = split_path(path)
            svc = factory("svc")(name, namespace=self.namespace, volatile=True, node=self.node, log=self.log)
            for vol in svc.get_resources("volume"):
                if vol.has_data(self.kind, self.path, key) and vol._status() == rcStatus.UP:
                    vol._install_data(self.kind)

