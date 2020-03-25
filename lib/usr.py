import os
import sys
import base64
import re
import fnmatch
import shutil
import glob
import tempfile

from rcGlobalEnv import rcEnv
from rcUtilities import lazy, makedirs, split_path, factory
from svc import BaseSvc
from rcSsl import gen_cert
from sec import Sec
from utilities.string import bdecode
import rcExceptions as ex

DEFAULT_STATUS_GROUPS = [
]

OPENSSL_CRL_CONF = """
[ ca ]
default_ca = %(clustername)s

[ %(clustername)s ]
database = %(p_crlindex)s
crlnumber = %(p_crlnumber)s
default_days = 365
default_crl_days = 30
default_md = default
preserve = no

[ crl_ext ]
authorityKeyIdentifier = keyid:always,issuer:always
"""

DEFAULT_SACC_CERT_VALIDITY = "1d"

class Usr(Sec, BaseSvc):
    kind = "usr"
    desc = "user"

    @lazy
    def kwstore(self):
        return __import__("usrdict").KEYS

    @lazy
    def full_kwstore(self):
        return __import__("usrdict").full_kwstore()

    def on_create(self):
        changes = []
        has_ca = False
        if not self.oget("DEFAULT", "cn"):
            if self.namespace == "system":
                changes.append("cn=%s" % self.name)
            else:
                changes.append("cn=%s" % self.fullname)
        if self.namespace != "system":
            try:
                self.conf_get("DEFAULT", "validity")
            except ex.OptNotFound:
                changes.append("validity=%s" % DEFAULT_SACC_CERT_VALIDITY)
            grant = "guest:" + self.namespace
            changes.append("grant=%s" % grant)
        if not self.oget("DEFAULT", "ca"):
            capath = self.node.oget("cluster", "ca")
            if capath is None:
                capath = "system/sec/ca-" + self.node.cluster_name
            name, namespace, kind = split_path(capath)
            casec = factory("sec")(name, namespace="system", volatile=True, log=self.log)
            if casec.exists():
                has_ca = True
                changes.append("ca=%s" % capath)
            else:
                print("no cluster CA defined. skip certificate generation.")
        if changes:
            self.set_multi(changes)
        if has_ca and "certificate" not in self.data_keys() and "private_key" in casec.data_keys():
            self.gen_cert()

    @lazy
    def ca(self):
        capath = self.oget("DEFAULT", "ca")
        name, namespace, kind = split_path(capath)
        return factory("sec")(name, namespace=namespace, volatile=True, node=self.node)

    def revoke(self):
        if "certificate" not in self.data_keys():
            raise ex.excError("can not revoke: this certificate is signed by an external CA, and should be revoked there.")
        p_ca_key = os.path.join(rcEnv.paths.certs, "ca_private_key")
        p_ca_crt = os.path.join(rcEnv.paths.certs, "ca_certiticate")
        p_crl = os.path.join(rcEnv.paths.certs, "ca_crl")
        p_crlconf = os.path.join(rcEnv.paths.certs, "openssl-crl.conf")
        p_crlnumber = os.path.join(rcEnv.paths.certs, "crlnumber")
        p_crlindex = os.path.join(rcEnv.paths.certs, "crlindex")
        p_usr_crt = os.path.join(rcEnv.paths.certs, "%s_certificate" % self.name)
        if "crlnumber" not in self.data_keys():
            self.ca.add_key("crlnumber", "00")
        if "crlconf" not in self.data_keys():
            self.ca.add_key("crlconf", OPENSSL_CRL_CONF % dict(p_crlindex=p_crlindex, p_crlnumber=p_crlnumber, clustername=self.node.cluster_name))
        if "crlindex" in self.data_keys():
            self.ca.install_key("crlindex", p_crlindex)
        else:
            with open(p_crlindex, "w") as f:
                pass
        self.ca.install_key("crlnumber", p_crlnumber)
        self.ca.install_key("crlconf", p_crlconf)
        self.ca.install_key("private_key", p_ca_key)
        self.ca.install_key("certificate", p_ca_crt)
        self.install_key("certificate", p_usr_crt)
        cmd = ["openssl", "ca",
               "-keyfile", p_ca_key,
               "-cert", p_ca_crt,
               "-revoke", p_usr_crt,
               "-config", p_crlconf]
        self.vcall(cmd)
        cmd = ["openssl", "ca",
               "-keyfile", p_ca_key,
               "-cert", p_ca_crt,
               "-gencrl",
               "-out", p_crl,
               "-config", p_crlconf]
        self.vcall(cmd)
        with open(p_crlindex) as f:
            buff = f.read()
        self.ca.add_key("crlindex", buff)
        with open(p_crl) as f:
            buff = f.read()
        self.ca.add_key("crl", buff)

