from __future__ import print_function

import os
import json

import rcExceptions as ex
from rcGlobalEnv import rcEnv

def split_context(buff):
    user, buff = buff.rsplit("@", 1)
    cluster, namespace = buff.split("/", 1)
    return user, cluster, namespace

def get_context(context=None):
    """
    Get the listener address and port from ~/.opensvc/config
    """
    if context is None:
        try:
            context = os.environ["OSVC_CONTEXT"]
        except KeyError:
            return

    info = {}
    fpath = os.path.join(os.path.expanduser("~"), ".opensvc", "config")
    try:
        with open(fpath, "r") as ofile:
            data = json.load(ofile)
    except ValueError as exc:
        raise ex.excError("invalid context: %s: %s" % (fpath, str(exc)))
    except (IOError, OSError):
        data = {}

    # context => user, cluster, namespace
    context_data = data.get("contexts", {}).get(context)
    if context_data:
        try:
            user = context_data["user"]
            cluster = context_data["cluster"]
        except KeyError as exc:
            raise ex.excError("invalid context: %s: key %s not found" % (context, str(exc)))
        namespace = context_data.get("namespace")
    else:
        try:
            user, cluster, namespace = split_context(context)
        except Exception:
            raise ex.excError("invalid context '%s'. should be <user>/<cluster>[/<namespace>] or the name of a context defined in %s" % (context, fpath))

    # cluster data
    cdata = data.get("clusters", {}).get(cluster)
    if cdata is None:
        raise ex.excError("invalid context '%s'. cluster not found in %s" % (context, fpath))
    info["cluster"] = cdata
    
    certificate_authority = cdata.get("certificate_authority")
    if certificate_authority is None:
        raise ex.excError("invalid context '%s'. cluster.%s.certificate_authority not found in %s" % (context, cluster, fpath))

    server = cdata.get("server")
    if server is None:
        raise ex.excError("invalid context '%s'. cluster.%s.server not found in %s" % (context, cluster, fpath))

    server = server.replace("tls://", "").strip("/")
    if ":" in server:
        addr, port = server.split(":", 1)
    else:
        addr = server
        port = rcEnv.listener_tls_port
    info["cluster"]["addr"] = addr
    try:
        info["cluster"]["port"] = int(port)
    except Exception:
        raise ex.excError("invalid context '%s'. port %s number is not integer" % (context, port))

    # user data
    udata = data.get("users", {}).get(user)
    if udata is None:
        raise ex.excError("invalid context '%s'. user not found in %s" % (context, fpath))
    info["user"] = udata
    info["namespace"] = namespace
    
    key = info.get("user", {}).get("client_key")
    if key is None:
        raise ex.excError("invalid context '%s'. user.%s.client_key not found in %s" % (context, user, fpath))
    if not os.path.exists(key):
        raise ex.excError("invalid context '%s'. user.%s.client_key %s not found" % (context, user, key))
    cert = info.get("user", {}).get("client_certificate")
    if cert is None:
        raise ex.excError("invalid context '%s'. user.%s.client_certificate not found in %s" % (context, user, fpath))
    if not os.path.exists(cert):
        raise ex.excError("invalid context '%s'. user.%s.client_certificate %s not found" % (context, user, cert))

    #print(json.dumps(info, indent=4))
    return info

def want_context():
    return "OSVC_CONTEXT" in os.environ

