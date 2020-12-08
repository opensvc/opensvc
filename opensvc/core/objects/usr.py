import os

from env import Env
from utilities.naming import split_path, factory
from utilities.lazy import lazy
from core.objects.svc import BaseSvc
from .sec import Sec
import core.exceptions as ex

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
        from .usrdict import KEYS
        return KEYS

    @lazy
    def full_kwstore(self):
        from .usrdict import KEYS
        return KEYS

    def on_create(self):
        changes = []
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
        if self.ca:
            changes.append("ca=%s" % self.ca.path)
        else:
            print("no signing-capable CA in %s. skip certificate generation." % ",".join(self.capaths))
        if changes:
            self.set_multi(changes)
        if self.ca and "certificate" not in self.data_keys() and "private_key" in self.ca.data_keys():
            self.gen_cert()

    @property
    def capaths(self):
        capath = self.oget("DEFAULT", "ca")
        if capath:
            capaths = [capath]
        else:
            capaths = self.node.oget("cluster", "ca")
            if not capaths:
                capaths = ["system/sec/ca-" + self.node.cluster_name]
        return capaths
        
    @lazy
    def ca(self):
        for capath in self.capaths:
            name, namespace, kind = split_path(capath)
            casec = factory("sec")(name, namespace="system", volatile=True, log=self.log)
            if casec.exists() and "private_key" in casec.data_keys():
                return casec

    def revoke(self):
        if "certificate" not in self.data_keys():
            raise ex.Error("can not revoke: this certificate is signed by an external CA, and should be revoked there.")
        ca = factory("sec")(self.ca.name, namespace=self.ca.namespace, node=self.node)
        p_ca_key = os.path.join(Env.paths.certs, "ca_private_key." + ca.fullname)
        p_ca_crt = os.path.join(Env.paths.certs, "ca_certiticate." + ca.fullname)
        p_crl = os.path.join(Env.paths.certs, "ca_crl." + ca.fullname)
        p_crlconf = os.path.join(Env.paths.certs, "openssl-crl.conf." + ca.fullname)
        p_crlnumber = os.path.join(Env.paths.certs, "crlnumber." + ca.fullname)
        p_crlindex = os.path.join(Env.paths.certs, "crlindex." + ca.fullname)
        p_usr_crt = os.path.join(Env.paths.certs, "certificate." + self.fullname)
        if "crlnumber" not in ca.data_keys():
            ca.add_key("crlnumber", "00")
        if "crlconf" not in ca.data_keys():
            ca.add_key("crlconf", OPENSSL_CRL_CONF % dict(p_crlindex=p_crlindex, p_crlnumber=p_crlnumber, clustername=self.node.cluster_name))
        if "crlindex" in ca.data_keys():
            ca.install_file_key("crlindex", p_crlindex)
        else:
            with open(p_crlindex, "w") as f:
                pass
        ca.install_file_key("crlnumber", p_crlnumber)
        ca.install_file_key("crlconf", p_crlconf)
        ca.install_file_key("private_key", p_ca_key)
        ca.install_file_key("certificate", p_ca_crt)
        self.install_file_key("certificate", p_usr_crt)
        cmd = ["openssl", "ca",
               "-keyfile", p_ca_key,
               "-cert", p_ca_crt,
               "-revoke", p_usr_crt,
               "-config", p_crlconf]
        ret, out, err = self.vcall(cmd, err_to_info=True)
        if "Already revoked" in err:
            return
        cmd = ["openssl", "ca",
               "-keyfile", p_ca_key,
               "-cert", p_ca_crt,
               "-gencrl",
               "-out", p_crl,
               "-config", p_crlconf]
        self.vcall(cmd, err_to_info=True)
        if not os.path.exists(p_crl):
            raise ex.Error("%s does not exist. rollback transaction." % p_crl)
        with open(p_crl) as f:
            buff = f.read()
        ca.add_key("crl", buff)
        with open(p_crlnumber) as f:
            buff = f.read()
        ca.add_key("crlnumber", buff)
        with open(p_crlindex) as f:
            buff = f.read()
        ca.add_key("crlindex", buff)

