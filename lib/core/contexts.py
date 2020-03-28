from __future__ import print_function

import os
import json
import sys

import core.exceptions as ex
from env import Env
from utilities.optparser import OptParser, Option
from utilities.storage import Storage

def split_context(buff):
    user, buff = buff.rsplit("@", 1)
    cluster, namespace = buff.split("/", 1)
    return user, cluster, namespace

def contexts_config_path():
    return os.path.join(os.path.expanduser("~"), ".opensvc", "config")

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
    fpath = contexts_config_path()
    try:
        with open(fpath, "r") as ofile:
            data = json.load(ofile)
    except ValueError as exc:
        raise ex.Error("invalid context: %s: %s" % (fpath, str(exc)))
    except (IOError, OSError):
        data = {}

    # context => user, cluster, namespace
    context_data = data.get("contexts", {}).get(context)
    if context_data:
        try:
            user = context_data["user"]
            cluster = context_data["cluster"]
        except KeyError as exc:
            raise ex.Error("invalid context: %s: key %s not found" % (context, str(exc)))
        namespace = context_data.get("namespace")
    else:
        try:
            user, cluster, namespace = split_context(context)
        except Exception:
            raise ex.Error("invalid context '%s'. should be <user>/<cluster>[/<namespace>] or the name of a context defined in %s" % (context, fpath))

    # cluster data
    cdata = data.get("clusters", {}).get(cluster)
    if cdata is None:
        raise ex.Error("invalid context '%s'. cluster not found in %s" % (context, fpath))
    info["cluster"] = cdata
    
    certificate_authority = cdata.get("certificate_authority")

    server = cdata.get("server")
    if server is None:
        raise ex.Error("invalid context '%s'. cluster.%s.server not found in %s" % (context, cluster, fpath))

    server = server.replace("tls://", "").strip("/")
    server = server.replace("https://", "").strip("/")
    if ":" in server:
        addr, port = server.split(":", 1)
    else:
        addr = server
        port = Env.listener_tls_port
    info["cluster"]["addr"] = addr
    try:
        info["cluster"]["port"] = int(port)
    except Exception:
        raise ex.Error("invalid context '%s'. port %s number is not integer" % (context, port))

    # user data
    udata = data.get("users", {}).get(user)
    if udata is None:
        raise ex.Error("invalid context '%s'. user not found in %s" % (context, fpath))
    info["user"] = udata
    info["namespace"] = namespace
    
    cert = info.get("user", {}).get("client_certificate")
    if cert is None:
        raise ex.Error("invalid context '%s'. user.%s.client_certificate not found in %s" % (context, user, fpath))
    if not os.path.exists(cert):
        raise ex.Error("invalid context '%s'. user.%s.client_certificate %s not found" % (context, user, cert))

    key = info.get("user", {}).get("client_key")
    if key is None:
        # consider 'client_certificate' points to a full pem
        info["user"]["client_key"] = cert
    elif not os.path.exists(key):
        raise ex.Error("invalid context '%s'. user.%s.client_key %s not found" % (context, user, key))
    #print(json.dumps(info, indent=4))
    return info

def want_context():
    return "OSVC_CONTEXT" in os.environ

def write_context(data):
    fpath = contexts_config_path()
    dpath = os.path.dirname(fpath)
    try:
        os.makedirs(dpath, 0o0700)
    except OSError as exc:
        if exc.errno == 17:
            pass
        else:
            raise
    with open(fpath, "w") as ofile:
        json.dump(data, ofile, indent=4)

def load_context():
    fpath = contexts_config_path()
    try:
        with open(fpath, "r") as ofile:
            data = json.load(ofile)
    except ValueError as exc:
        raise ex.Error("invalid context: %s: %s" % (fpath, str(exc)))
    except (IOError, OSError):
        data = {}
    return data

def user_create(name=None, client_certificate=None, client_key=None, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if name in cdata.get("users", {}):
        # preload current values for incremental changes
        data = cdata["users"][name]
    else:
        data = {}
    if client_certificate:
        data["client_certificate"] = client_certificate
    if client_key:
        data["client_key"] = client_key
    if "users" not in cdata:
        cdata["users"] = {}
    cdata["users"][name] = data
    write_context(cdata)

def user_delete(name, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if name not in cdata.get("users", {}):
        return
    del cdata["users"][name]
    write_context(cdata)

def user_list(**kwargs):
    cdata = load_context()
    for name in cdata.get("users", {}):
        print(name)

def user_show(name=None, **kwargs):
    cdata = load_context()
    if name is None:
        print(json.dumps(cdata.get("users", {}), indent=4))
    else:
        print(json.dumps(cdata.get("users", {}).get(name, {}), indent=4))

def cluster_create(name=None, server=None, certificate_authority=None, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if name in cdata.get("clusters", {}):
        # preload current values for incremental changes
        data = cdata["clusters"][name]
    else:
        data = {}
    if server:
        data["server"] = server
    if certificate_authority:
        data["certificate_authority"] = certificate_authority
    if "clusters" not in cdata:
        cdata["clusters"] = {}
    cdata["clusters"][name] = data
    write_context(cdata)

def cluster_delete(name=None, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if name not in cdata.get("clusters", {}):
        return
    del cdata["clusters"][name]
    write_context(cdata)

def cluster_list(**kwargs):
    cdata = load_context()
    for name in cdata.get("clusters", {}):
        print(name)

def cluster_show(name=None, **kwargs):
    cdata = load_context()
    if name is None:
        print(json.dumps(cdata.get("clusters", {}), indent=4))
    else:
        print(json.dumps(cdata.get("clusters", {}).get(name, {}), indent=4))

def get(**kwargs):
    raise ex.Error("The 'om' alias must be sourced to handle ctx get")

def set(**kwargs):
    raise ex.Error("The 'om' alias must be sourced to handle ctx set")

def unset(**kwargs):
    raise ex.Error("The 'om' alias must be sourced to handle ctx unset")

def create(name=None, cluster=None, user=None, namespace=None, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if not cluster:
        raise ex.Error("cluster is mandatory")
    if cluster not in cdata.get("clusters", {}):
        raise ex.Error("unknown cluster %s" % cluster)
    if not user:
        raise ex.Error("user is mandatory")
    if user not in cdata.get("users", {}):
        raise ex.Error("unknown user %s" % user)
    if name in cdata.get("contexts", {}):
        # preload current values for incremental changes
        data = cdata["contexts"][name]
    else:
        data = {}
    if cluster:
        data["cluster"] = cluster
    if user:
        data["user"] = user
    if namespace:
        data["namespace"] = namespace
    if "contexts" not in cdata:
        cdata["contexts"] = {}
    cdata["contexts"][name] = data
    write_context(cdata)

def delete(name=None, **kwargs):
    if name is None:
        raise ex.Error("name is mandatory")
    cdata = load_context()
    if name not in cdata.get("contexts", {}):
        return
    del cdata["contexts"][name]
    write_context(cdata)

def list(**kwargs):
    cdata = load_context()
    for name in cdata.get("contexts", {}):
        print(name)

def show(name=None, **kwargs):
    cdata = load_context()
    if name is None:
        print(json.dumps(cdata.get("contexts", {}), indent=4))
    else:
        print(json.dumps(cdata.get("contexts", {}).get(name, {}), indent=4))

PROG = "om ctx"
OPT = Storage({
    "help": Option(
        "-h", "--help", action="store_true", dest="parm_help",
        help="show this help message and exit"),
    "user": Option(
        "--user", action="store", dest="user",
        help="User name."),
    "cluster": Option(
        "--cluster", action="store", dest="cluster",
        help="Cluster name."),
    "namespace": Option(
        "--namespace", action="store", dest="namespace",
        help="Namespace name or glob pattern."),
    "name": Option(
        "--name", action="store", dest="name",
        help="The name of the object to create or delete."),
    "certificate_authority": Option(
        "--certificate-authority", action="store", dest="certificate_authority",
        help="The certificate authority pem file path."),
    "server": Option(
        "--server", action="store", dest="server",
        help="The uri where to contact the cluster. ex: tls://1.2.3.4:1215."),
    "client_certificate": Option(
        "--client-certificate", action="store", dest="client_certificate",
        help="The client certificate pem file path."),
    "client_key": Option(
        "--client-key", action="store", dest="client_key",
        help="The client key pem file path."),
})

ACTIONS = {
    "Users": {
        "user_create": {
            "msg": "Create or update a user.",
            "options": [
                OPT.name,
                OPT.client_certificate,
                OPT.client_key,
            ],
        },
        "user_show": {
            "msg": "Show a user configuration.",
            "options": [
                OPT.name,
            ],
        },
        "user_delete": {
            "msg": "Delete a user.",
            "options": [
                OPT.name,
            ],
        },
        "user_list": {
            "msg": "List defined users.",
        },
    },
    "Clusters": {
        "cluster_create": {
            "msg": "Create or update a cluster.",
            "options": [
                OPT.name,
                OPT.server,
                OPT.certificate_authority,
            ],
        },
        "cluster_delete": {
            "msg": "Delete a cluster.",
            "options": [
                OPT.name,
            ],
        },
        "cluster_show": {
            "msg": "Show a cluster configuration.",
            "options": [
                OPT.name,
            ],
        },
        "cluster_list": {
            "msg": "List defined clusters.",
        },
    },
    "Contexts": {
        "get": {
            "msg": "Show the current context. The context is valid for this shell session only.",
        },
        "set": {
            "msg": "Switch the current context. The context is valid for this shell session only.",
        },
        "unset": {
            "msg": "Unset the current context.",
        },
        "create": {
            "msg": "Create or update a context.",
            "options": [
                OPT.name,
                OPT.namespace,
                OPT.cluster,
                OPT.user,
            ],
        },
        "delete": {
            "msg": "Delete a context.",
            "options": [
                OPT.name,
            ],
        },
        "show": {
            "msg": "Show a context configuration.",
            "options": [
                OPT.name,
            ],
        },
        "list": {
            "msg": "List defined contexts.",
        },
    },
}

DEPRECATED_ACTIONS = {}
GLOBAL_OPTS = {}

def main(argv):
    parser = OptParser(prog=PROG, options=OPT, actions=ACTIONS,
                       deprecated_actions=DEPRECATED_ACTIONS,
                       global_options=GLOBAL_OPTS)
    try:
        options, action = parser.parse_args(argv[1:])
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        return 1
    kwargs = vars(options)
    globals()[action](**kwargs)

if __name__ == "__main__":
    try:
        ret = main(sys.argv)
    except ex.Error as exc:
        print(exc, file=sys.stderr)
        ret = 1
    sys.exit(ret)

