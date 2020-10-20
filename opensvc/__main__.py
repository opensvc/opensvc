from __future__ import print_function

import os
import sys

HELP = """Usage:

  om ns set <namespace>       Set the current namespace
  om ns unset                 Unset the current namespace
  om ns get                   Get the current namespace

  om mon                      Monitor the cluster
  om ctx                      Manage Remote Connections contexts
  om node                     Manage Cluster Nodes
  om cluster                  Manage Cluster Configuration
  om nscfg                    Manage Namespace Configuration
  om svc                      Manage Services
  om vol                      Manage Persistent Data Volumes
  om cfg                      Manage Configurations
  om sec                      Manage Secrets
  om usr                      Manage Users
  om net                      Manage Networks
  om pool                     Manage Storage Pools
  om daemon                   Manage Agent Daemon
  om array                    Manage Storage Arrays

  om <selector> <options>     Manage the selected objects


Selector:
  <selector>[,<selector>,...] Unioned selectors
  <selector>[+<selector>+...] Intersected selectors

Path Selectors:
  <namespace>/<kind>/<name>   Fully qualified path
  <kind>/<name>               Path relative to the current namespace
  <name>                      Service <name> in the current namespace
  **                          All objects
  **/<name>                   All objects named <name>
  <namespace>/**              All objects in the <namespace> namespace
  <namespace>/<kind>/*        All <kind> objects in the <namespace> namespace

Status Selectors:
  <jsonpath>:                 All objects with the <jsonpath> referenced key existing in the instance status data
  <jsonpath>=<val>            All objects with the <jsonpath> referenced key value equals to <val> in the instance status data

Config Selectors:
  <keyword>:                  All objects with the <keyword> existing in the instance config data
  <keyword>=<value>           All objects with the <keyword> value equals to <val> in the instance config data
"""

def main(argv=None):
    try:
        arg1 = argv[1]
    except IndexError:
        print(HELP, file=sys.stderr)
        return 1
    if arg1 == "ns":
        print("The 'om' alias must be sourced to handle ns actions", file=sys.stderr)
        return 1
    elif arg1 == "ctx":
        from core.contexts import main
        ret = main(argv[1:])
        return ret
    elif arg1 == "svc":
        from commands.svc import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "vol":
        from commands.vol import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "nscfg":
        from commands.nscfg import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "cfg":
        from commands.cfg import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "sec":
        from commands.sec import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "usr":
        from commands.usr import Mgr
        os.environ["OSVC_KIND"] = arg1
        ret = Mgr()(argv=argv[2:])
        return ret
    elif arg1 == "node":
        from commands.node import main
        ret = main(argv=argv[2:])
        return ret
    elif arg1 == "array":
        from commands.node import main
        ret = main(argv=argv[1:])
        return ret
    elif arg1 in ("net", "network"):
        from commands.network import main
        ret = main(argv=argv[2:])
        return ret
    elif arg1 == "pool":
        from commands.pool import main
        ret = main(argv=argv[2:])
        return ret
    elif arg1 == "daemon":
        from commands.daemon import main
        ret = main(argv=argv[2:])
        return ret
    elif arg1 == "mon":
        from commands.svcmon import main
        ret = main(argv=argv[1:])
        return ret
    else:
        from commands.mgr import Mgr
        ret = Mgr(selector=arg1)(argv=argv[2:])
        return ret

if __name__ == "__main__":
    ret = main(sys.argv)
    sys.exit(ret)

