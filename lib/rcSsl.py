import os
import datetime
import time
from subprocess import Popen, PIPE

import rcExceptions as ex
from rcUtilities import makedirs, is_string, justcall

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
    if data.get("ca") is None:
        gen_self_signed_cert(log=log, **data)
    else:
        gen_ca_signed_cert(log=log, **data)

def format_subject(**data):
    l = [""]
    for k, sk in keymap:
        if sk not in data:
            continue
        l.append(k+"="+data[sk])
    if "alt_names" in data and len(data["alt_names"]) > 0:
        dns = []
        for i, d in enumerate(data["alt_names"]):
            if is_string(d):
                dns.append("DNS.%d=%s" % (i+1, d))
            elif "DNS" in d:
                dns.append("DNS.%d=%s" % (i+1, d["DNS"]))
        l.append("subjectAltName="+",".join(dns))
    l.append("")
    subject = "/".join(l)
    return subject

def gen_self_signed_cert(log=None, **data):
    days = data["validity"]
    if days < 1:
        days = 1
    cmd = ["openssl", "req", "-x509", "-nodes",
           "-newkey", "rsa:%d" % data.get("bits", 4096),
           "-keyout", data["key"],
           "-out", data["crt"],
           "-days", str(days),
           "-subj", "%s" % data["subject"]]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.excError(out+err)

def gen_ca_signed_cert(log=None, **data):
    gen_csr(log=log, **data)
    sign_csr(log=log, **data)

def gen_csr(log=None, **data):
    cmd = ["openssl", "req", "-new", "-nodes",
           "-newkey", "rsa:%d" % data.get("bits", 4096),
           "-keyout", data["key"],
           "-out", data["csr"],
           "-subj", "%s" % data["subject"]]
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.excError(out+err)

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
    if log:
        log.info(" ".join(cmd))
    out, err, ret = justcall(cmd)
    if ret != 0:
        raise ex.excError(out+err)

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

