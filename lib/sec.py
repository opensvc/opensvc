import os
import sys
import base64
import re
import fnmatch
import shutil
import glob
import tempfile

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_svcpath, fmt_svcpath, factory
from svc import BaseSvc
from converters import print_size
from data import DataMixin
from rcSsl import gen_cert
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

class Sec(DataMixin, BaseSvc):
    kind = "sec"
    desc = "secret"
    default_mode = 0o0600

    @lazy
    def kwdict(self):
        return __import__("secdict")

    def add_key(self, key, data):
        if not key:
            raise ex.excError("secret key name can not be empty")
        if not data:
            raise ex.excError("secret value can not be empty")
        data = "crypt:"+base64.urlsafe_b64encode(self.encrypt(data, cluster_name="join", encode=True)).decode()
        self.set_multi(["data.%s=%s" % (key, data)])
        self.log.info("secret key '%s' added (%s)", key, print_size(len(data), compact=True, unit="b"))
        # refresh if in use
        self.postinstall(key)

    def decode_key(self, key):
        if not key:
            raise ex.excError("secret key name can not be empty")
        data = self.oget("data", key)
        if not data:
            raise ex.excError("secret key %s does not exist or has no value" % key)
        if data.startswith("crypt:"):
            data = data[6:]
            return self.decrypt(base64.urlsafe_b64decode(data.encode("ascii")))[1]

    @staticmethod
    def tempfilename():
        tmpf = tempfile.NamedTemporaryFile()
        try:
            return tmpf.name
        finally:
            tmpf.close()

    def gen_cert(self):
        data = {}
        for key in ("cn", "c", "st", "l", "o", "ou", "email", "alt_names", "bits", "validity", "ca"):
            val = self.oget("DEFAULT", key)
            if val is not None:
                data[key] = val

        ca = data.get("ca")
        casec = None
        if ca is not None:
            casecname, _, _ = split_svcpath(ca)
            casecpath = fmt_svcpath(casecname, self.namespace, "sec")
            casec = factory("sec")(casecname, namespace=self.namespace, log=self.log, volatile=True)
            if not casec.exists():
                raise ex.excError("ca secret %s does not exist" % casecpath)

        for key in ("crt", "key", "csr"):
            data[key] = self.tempfilename()

        try:
            if casec:
                for key, kw in (("cacrt", "certificate"), ("cakey", "private_key")):
                    data[key] = self.tempfilename()
                    buff = casec.decode_key(kw)
                    with open(data[key], "w") as ofile:
                        ofile.write(buff)
            gen_cert(log=self.log, **data)
            self._add("private_key", value_from=data["key"])
            self._add("certificate", value_from=data["crt"])
            if data.get("csr") is not None:
                self._add("certificate_signing_request", value_from=data["csr"])
            if data.get("ca") is None:
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
        finally:
            for key in ("crt", "key", "cacrt", "cakey", "csr"):
                if key not in data:
                    continue
                try:
                    os.unlink(data[key])
                except Exception:
                    pass

