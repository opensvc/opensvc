from __future__ import print_function

import os
import sys
import base64
import re
import fnmatch
import shutil
import glob
import tempfile
import six

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_path, fmt_path, factory
from core.objects.svc import BaseSvc
from utilities.converters import print_size
from core.objects.data import DataMixin
from utilities.ssl import gen_cert, get_expire
from utilities.string import bencode, bdecode
import core.exceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Sec(DataMixin, BaseSvc):
    kind = "sec"
    desc = "secret"
    default_mode = 0o0600

    @lazy
    def kwstore(self):
        from .secdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .secdict import full_kwstore
        return full_kwstore()

    def on_create(self):
        if self.oget("DEFAULT", "cn") and "certificate" not in self.data_keys():
            self.gen_cert()

    def _add_key(self, key, data):
        if not key:
            raise ex.Error("secret key name can not be empty")
        if data is None:
            raise ex.Error("secret value can not be empty")
        data = "crypt:"+base64.urlsafe_b64encode(self.encrypt(data, cluster_name="join", encode=True)).decode()
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("secret key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        self.postinstall(key)

    def decode_key(self, key):
        if not key:
            raise ex.Error("secret key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.Error("secret %s key %s does not exist or has no value" % (self.path, key))
        if data.startswith("crypt:"):
            data = data[6:]
            return self.decrypt(base64.urlsafe_b64decode(data.encode("ascii")))[2]

    def gen_cert(self):
        data = {}
        for key in ("cn", "c", "st", "l", "o", "ou", "email", "alt_names", "bits", "validity", "ca"):
            val = self.oget("DEFAULT", key)
            if val is not None:
                data[key] = val

        ca = data.get("ca")
        casec = None
        if ca is not None:
            casecname, canamespace, _ = split_path(ca)
            casec = factory("sec")(casecname, namespace=canamespace, log=self.log, volatile=True)
            if not casec.exists():
                raise ex.Error("ca secret %s does not exist" % ca)

        for key in ("crt", "key", "csr"):
            data[key] = self.tempfilename()

        if "alt_names" in data:
            data["cnf"] = self.tempfilename()

        try:
            if casec:
                for key, kw in (("cacrt", "certificate"), ("cakey", "private_key")):
                    if kw not in casec.data_keys():
                        continue
                    data[key] = self.tempfilename()
                    buff = bdecode(casec.decode_key(kw))
                    with open(data[key], "w") as ofile:
                        ofile.write(buff)
            gen_cert(log=self.log, **data)
            self._add("private_key", value_from=data["key"])
            if data.get("crt") is not None:
                self._add("certificate", value_from=data["crt"])
            if data.get("csr") is not None:
                self._add("certificate_signing_request", value_from=data["csr"])
            if data.get("cakey") is None:
                self._add("certificate_chain", value_from=data["crt"])
            else:
                # merge cacrt and crt
                chain = self.tempfilename()
                try:
                    with open(data["crt"], "r") as ofile:
                        buff = ofile.read()
                    with open(data["cacrt"], "r") as ofile:
                        buff += ofile.read()
                    with open(chain, "w") as ofile:
                        ofile.write(buff)
                    self._add("certificate_chain", value_from=chain)
                finally:
                    try:
                        os.unlink(chain)
                    except Exception:
                        pass
            self.add_key("fullpem", self._fullpem())
        finally:
            for key in ("crt", "key", "cacrt", "cakey", "csr", "cnf"):
                if key not in data:
                    continue
                try:
                    os.unlink(data[key])
                except Exception:
                    pass

    def get_cert_expire(self):
        buff = bdecode(self.decode_key("certificate"))
        return get_expire(buff)

    def pkcs12(self):
        if six.PY3:
            sys.stdout.buffer.write(self._pkcs12(self.options.password))  # pylint: disable=no-member
        else:
            print(self._pkcs12(self.options.password))

    def _pkcs12(self, password):
        required = set(["private_key", "certificate_chain"])
        if required & set(self.data_keys()) != required:
            self.gen_cert()
        from subprocess import Popen, PIPE
        import tempfile
        _tmpcert = tempfile.NamedTemporaryFile()
        _tmpkey = tempfile.NamedTemporaryFile()
        tmpcert = _tmpcert.name
        tmpkey = _tmpkey.name
        _tmpcert.close()
        _tmpkey.close()
        if password is None:
            from getpass import getpass
            pwd = getpass("Password: ", stream=sys.stderr)
            if not pwd:
                pwd = "\n"
        elif password in ["/dev/stdin", "-"]:
            pwd = sys.stdin.readline()
        else:
            pwd = password+"\n"
        if six.PY3:
            pwd = bencode(pwd)
        try:
            with open(tmpkey, "w") as _tmpkey:
                os.chmod(tmpkey, 0o600)
                _tmpkey.write(bdecode(self.decode_key("private_key")))
            with open(tmpcert, "w") as _tmpcert:
                os.chmod(tmpcert, 0o600)
                _tmpcert.write(bdecode(self.decode_key("certificate_chain")))
            cmd = ["openssl", "pkcs12", "-export", "-in", tmpcert, "-inkey", tmpkey, "-passout", "stdin"]
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
            out, err = proc.communicate(input=pwd)
            if err:
                print(err, file=sys.stderr)
            return out
        finally:
            if os.path.exists(tmpcert):
                os.unlink(tmpcert)
            if os.path.exists(tmpkey):
                os.unlink(tmpkey)

    def fullpem(self):
        print(self._fullpem())

    def _fullpem(self):
        required = set(["private_key", "certificate_chain"])
        if required & set(self.data_keys()) != required:
            self.gen_cert()
        buff = bdecode(self.decode_key("private_key"))
        buff += bdecode(self.decode_key("certificate_chain"))
        return buff
