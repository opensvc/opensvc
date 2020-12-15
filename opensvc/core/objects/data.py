import os
import sys
import fnmatch
import shutil
import glob
import tempfile
import re

import core.exceptions as ex
import core.status
import foreign.six
from core.comm import DEFAULT_DAEMON_TIMEOUT
from core.contexts import want_context
from utilities.edit_file import edit_file
from utilities.files import create_protected_file, makedirs, read_unicode_file
from utilities.string import try_decode
from utilities.naming import factory, split_path, list_services
from utilities.proc import find_editor
from utilities.string import bencode


class DataMixin(object):
    def add(self):
        if self.options.key and self.has_key(self.options.key):
            raise ex.Error("key '%s' already exists. use the 'change' action to change the current value." % self.options.key)
        self._add(self.options.key, self.options.value_from)

    def change(self):
        self._add(self.options.key, self.options.value_from)

    def remove(self):
        return self.remove_key(self.options.key)

    def append(self):
        self._add(self.options.key, self.options.value_from, append=True)

    def remove_key(self, key):
        if key not in self.data_keys():
            return
        return self.unset_multi(["data." + key])

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
        result = self.daemon_post(req, timeout=DEFAULT_DAEMON_TIMEOUT)
        status, error, info = self.parse_result(result)
        if info:
            print(info)
        if status:
            raise ex.Error(error)

    def _add(self, key=None, value_from=None, append=False):
        if key and sys.stdin and value_from in ("-", "/dev/stdin"):
            self.add_stdin(key, append=append)
        elif key and self.options.value is not None:
            self.add_key(key, self.options.value, append=append)
        elif value_from and re.match("^(http://|https://|ftp://|ftps://)", value_from):
            self.add_uri(key, value_from, append=append)
        elif value_from and os.path.isdir(value_from):
            self.add_directory(key, value_from, append=append)
        elif value_from and os.path.isfile(value_from):
            self.add_file(key, value_from, append=append)
        elif value_from:
            self.add_glob(key, value_from, append=append)
        else:
            self.add_key(key, "", append=append)

    def add_stdin(self, key, append=False):
        if append:
            data = self.decode_key(key)
        else:
            data = b""
        if foreign.six.PY2:
            data += sys.stdin.read()
        else:
            data += sys.stdin.buffer.read()
        self.add_key(key, data)

    def add_uri(self, key, path, append=None):
        from utilities.uri import Uri
        secure = self.node.oget("node", "secure_fetch")
        try:
            with Uri(path, secure=secure).fetch() as fpath:
                return self.add_file(key, fpath, append=append)
        except IOError as exc:
            raise ex.Error("download %s failed: %s" % (path, exc))

    def add_file(self, key, path, append=None):
        if key is None:
            key = os.path.basename(path)
        if append:
            data = bencode(self.decode_key(key))
        else:
            data = b""
        if os.path.islink(path) and not os.path.exists(path):
            raise ex.Error("broken symlink %s => %s" % (path, os.readlink(path)))
        with open(path, "rb") as ofile:
            data += ofile.read()
        self.add_key(key, data)

    def add_glob(self, key, path, append=False):
        if key is None:
            key = ""
        fpaths = glob.glob(path)
        if not fpaths:
            raise ex.Error("not data could be found at %s" % path)
        for path in fpaths:
            if os.path.isfile(path):
                _key = os.path.join(key, os.path.basename(path))
                try:
                    self.add_file(_key, path, append=append)
                except ex.Error as exc:
                    self.log.warning("skip: %s", exc)
            elif os.path.isdir(path):
                dir_key = os.path.join(key, os.path.basename(path))
                self.add_directory(dir_key, path, append=append)

    def add_directory(self, key, path, append=False):
        if key:
            sub_key_position = len(path)
            key_prefix = key
        else:
            key_prefix = ''
            sub_key_position = len(os.path.dirname(path))
        for root, dirs, files in os.walk(path):
            for fname in files:
                fpath = os.path.join(root, fname)
                file_key = os.path.join(key_prefix, fpath[sub_key_position:].lstrip(os.sep))
                try:
                    self.add_file(file_key, fpath, append=append)
                except ex.Error as exc:
                    self.log.warning("skip: %s", exc)

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
        if buff is None:
            raise ex.Error("could not decode the secret key '%s'" % self.options.key)
        if isinstance(buff, bytes):
            try:
                self.log.debug("try decode binary key")
                buff = buff.decode()
            except:
                raise ex.Error("binary keys are not editable")
        no_newline = os.sep not in try_decode(buff)
        editor = find_editor()
        fpath = self.tempfilename()
        create_protected_file(fpath, buff)
        try:
            edit_file(editor, fpath)
            edited = read_unicode_file(fpath)
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
            raise ex.Error("could not decode the secret key '%s'" % self.options.key)
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

    def resolve_key(self, key_to_resolve):
        if key_to_resolve is None:
            return []
        keys = self.data_keys()
        dirs = self.data_dirs()

        def recurse(key, done):
            data = []
            for path in dirs:
                if path != key and not fnmatch.fnmatch(path, key):
                    continue
                rkeys, rdone = recurse(path + "/*", done)
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

        return recurse(key_to_resolve, set())[0]

    def install_key(self, key, path, uid=None, gid=None, mode=None, dirmode=None):
        if key["type"] == "file":
            vpath = self.key_path(key, path)
            return self.install_file_key(key["path"], vpath, uid=uid, gid=gid, mode=mode, dirmode=dirmode)
        elif key["type"] == "dir":
            return self.install_dir_key(key, path, uid=uid, gid=gid, mode=mode, dirmode=dirmode)

    def install_dir_key(self, data, path, uid=None, gid=None, mode=None, dirmode=None):
        """
        Install a key decoded data in the host's volatile storage.
        """
        if path.endswith("/"):
            dirname = os.path.basename(data["path"])
            dirpath = os.path.join(path.rstrip("/"), dirname, "")
        else:
            dirpath = path + "/"
        makedirs(dirpath, uid=uid, gid=gid, mode=dirmode)
        changed = False
        for key in data["keys"]:
            changed |= self.install_key(key, dirpath, uid=uid, gid=uid, mode=mode, dirmode=dirmode)
        return changed

    def install_file_key(self, key, vpath, uid=None, gid=None, mode=None, dirmode=None):
        """
        Install a key decoded data in the host's volatile storage.
        """
        # paranoid checks before rmtree()/unlink()
        if ".." in vpath:
            return
        data = self.decode_key(key)
        if data is None:
            raise ex.Error("no data in key %s" % key)
        if os.path.isdir(vpath):
            self.log.info("remove %s key %s directory at location %s", self.desc, key, vpath)
            shutil.rmtree(vpath)
        vdir = os.path.dirname(vpath)
        if os.path.isfile(vdir) or os.path.islink(vdir):
            self.log.info("remove %s key %s file at parent location %s", self.desc, key, vdir)
            os.unlink(vdir)
        makedirs(vdir, uid=uid, gid=gid, mode=dirmode)
        return self.write_key(vpath, data, key=key, uid=uid, gid=gid, mode=mode)

    @staticmethod
    def key_path(key, path):
        """
        The full path to host's volatile storage file containing the key decoded data.
        """
        if path.endswith("/"):
            name = os.path.basename(key["path"].rstrip("/"))
            npath = os.path.join(path.rstrip("/"), name)
        else:
            npath = path
        return npath

    def write_key(self, vpath, data, key=None, uid=None, gid=None, mode=None):
        """
        Return False if the file did not change.
        Return True if the file changed.
        """
        mtime = os.path.getmtime(self.paths.cf)
        try:
            data = data.encode()
        except (AttributeError, UnicodeDecodeError, UnicodeEncodeError):
            # already bytes
            pass
        if os.path.exists(vpath):
            self.set_perm(vpath, uid, gid, mode)
            if mtime == os.path.getmtime(vpath):
                return False
            with open(vpath, "rb") as ofile:
                current = ofile.read()
            if current == data:
                os.utime(vpath, (mtime, mtime))
                return False
        self.log.info("install %s/%s in %s", self.name, key, vpath)
        with open(vpath, "wb") as ofile:
            self.set_perm(vpath, uid, gid, mode)
            ofile.write(data)
            os.utime(vpath, (mtime, mtime))
        return True

    def set_perm(self, path, uid=None, gid=None, mode=None):
        os.chmod(path, mode or self.default_mode)
        uid = uid if uid is not None else -1
        gid = gid if gid is not None else -1
        os.chown(path, uid, gid)

    def _install(self, key, path):
        """
        Install the <key> decoded data in the host's volatile storage.
        """
        keys = self.resolve_key(key)
        if not keys:
            raise ex.Error("%s key %s not found" % (self.desc, key))
        for _key in keys:
            self.install_key(_key, path)

    def post_commit(self):
        self.postinstall()

    def install(self):
        self.postinstall(self.options.key)

    def postinstall(self, key=None):
        """
        Refresh installed keys
        """
        changed_volumes = set()
        for path in list_services(namespace=self.namespace, kinds=["svc"]):
            name, _, _ = split_path(path)
            svc = factory("svc")(name, namespace=self.namespace, volatile=True, node=self.node, log=self.log)
            for res in svc.get_resources("volume"):
                if res.has_data(self.kind, self.path, key) and res._status() == core.status.UP:
                    installed = res._install_data(self.kind)
                    if installed:
                        changed_volumes.add(res.volsvc.path)
                    if res.volsvc.path in changed_volumes:
                        res.send_signals()
