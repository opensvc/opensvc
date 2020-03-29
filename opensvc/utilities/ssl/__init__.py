import os
import datetime
import shutil
import time
from subprocess import Popen, PIPE

import core.exceptions as ex
from utilities.files import makedirs
from utilities.proc import justcall
from utilities.string import is_string
from utilities.net.ipaddress import ip_address

keymap = [
    ("C", "c"),
    ("ST", "st"),
    ("L", "l"),
    ("O", "o"),
    ("OU", "ou"),
    ("CN", "cn"),
    ("emailAddress", "email"),
]

def gen_cert(log=None, **data):
    for k in ("key", "crt"):
        d = os.path.dirname(data[k])
        if not os.path.isdir(d):
            makedirs(d)
    data["subject"] = format_subject(**data)
    data["alt_names"] = format_alt_names(**data)
    gen_csr(log=log, **data)
    if data.get("cakey") is None:
        gen_self_signed_cert(log=log, **data)
    else:
        gen_ca_signed_cert(log=log, **data)

def format_subject(**data):
    l = [""]
    for k, sk in keymap:
        if sk not in data:
            continue
        l.append(k+"="+data[sk])
    l.append("")
    subject = "/".join(l)
    return subject

def format_alt_names(**data):
    if "alt_names" not in data:
        return
    if len(data["alt_names"]) == 0:
        return
    l = []
    for i, d in enumerate(data["alt_names"]):
        try:
            ip = ip_address(d)
        except ValueError:
            ip = None
        if d.startswith("IP:") or d.startswith("DNS:"):
            l.append(d)
        elif ip:
            l.append("IP:%s" % d)
        else:
            l.append("DNS:%s" % d)
    return "subjectAltName = %s" % ",".join(l)

def gen_self_signed_cert(log=None, **data):
    days = data["validity"]
    if days < 1:
        days = 1
    cmd = ["openssl", "req", "-x509", "-nodes",
           "-key", data["key"],
           "-out", data["crt"],
           "-days", str(days),
           "-subj", "%s" % data["subject"]]
    if data.get("alt_names"):
        cmd += ["-addext", data.get("alt_names")]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(out+err)

def gen_ca_signed_cert(log=None, **data):
    sign_csr(log=log, **data)

def gen_csr(log=None, **data):
    cmd = ["openssl", "req", "-new", "-nodes",
           "-newkey", "rsa:%d" % data.get("bits", 4096),
           "-keyout", data["key"],
           "-out", data["csr"],
           "-subj", "%s" % data["subject"]]
    if data.get("alt_names"):
        cmd += ["-addext", data.get("alt_names")]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(out+err)

def sign_csr(log=None, **data):
    days = data["validity"]
    if days < 1:
        days = 1
    cmd = ["openssl", "x509", "-req",
           "-in", data["csr"],
           "-CA", data["cacrt"],
           "-CAkey", data["cakey"],
           "-CAcreateserial",
           "-out", data["crt"],
           "-days", str(days),
           "-sha256"]
    if data.get("alt_names"):
        write_openssl_cnf(data)
        cmd += ["-extfile", data["cnf"], "-extensions", "SAN"]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.Error(out+err)

def write_openssl_cnf(data):
    openssl_cnf_location = [
        '/etc/ssl/openssl.cnf',
        '/etc/pki/tls/openssl.cnf',
    ]
    openssl_cnf = None
    for loc in openssl_cnf_location:
       if os.path.exists(loc):
           openssl_cnf = loc
    if not openssl_cnf:
        raise ex.Error("could not determine openssl.cnf location")
    shutil.copy(openssl_cnf, data["cnf"])
    with open(data["cnf"], "a") as f:
        f.write("\n[SAN]\n%s\n" % data["alt_names"])

def get_expire(data):
    if not data:
        return
    cmd = ["openssl", "x509", "-noout", "-enddate"]
    out, err, ret = justcall(cmd, input=data)
    out = out.split("=", 1)[-1].strip()
    if ret != 0:
        return
    try:
        return time.mktime(datetime.datetime.strptime(out, "%b %d %H:%M:%S %Y %Z").timetuple())
    except ValueError:
        return None

