from __future__ import print_function
import os
import sys
import logging
import re
import socket
import glob
import ast
import operator as op
import platform

from rcGlobalEnv import *
from rcNode import discover_node
from rcUtilities import *
import rcLogger
import resSyncRsync
import rcExceptions as ex
import rcUtilities
import rcConfigParser

# supported operators in arithmetic expressions
operators = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul,
             ast.Div: op.truediv, ast.Pow: op.pow, ast.BitXor: op.xor,
             ast.USub: op.neg, ast.FloorDiv: op.floordiv, ast.Mod: op.mod}

if 'PATH' not in os.environ:
    os.environ['PATH'] = ""
os.environ['LANG'] = 'C'
os.environ['PATH'] += ':/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin'

def eval_expr(expr):
    """ arithmetic expressions evaluator
    """
    def eval_(node):
        if isinstance(node, ast.Num): # <number>
            return node.n
        elif isinstance(node, ast.BinOp): # <left> <operator> <right>
            return operators[type(node.op)](eval_(node.left), eval_(node.right))
        elif isinstance(node, ast.UnaryOp): # <operator> <operand> e.g., -1
            return operators[type(node.op)](eval_(node.operand))
        else:
            raise TypeError(node)
    return eval_(ast.parse(expr, mode='eval').body)

def handle_reference(svc, conf, ref, scope=False, impersonate=None):
        # hardcoded references
        if ref == "nodename":
            return rcEnv.nodename
        if ref == "svcname":
            return svc.svcname
        if ref == "svcmgr":
            return rcEnv.svcmgr
        if ref == "nodemgr":
            return rcEnv.nodemgr

        if "[" in ref and ref.endswith("]"):
            i = ref.index("[")
            index = ref[i+1:-1]
            ref = ref[:i]
            index = int(handle_references(svc, conf, index, scope=scope, impersonate=impersonate))
        else:
            index = None

        # use DEFAULT as the implicit section
        n_dots = ref.count(".")
        if n_dots == 0:
            _section = "DEFAULT"
            _v = ref
        elif n_dots == 1:
            _section, _v = ref.split(".")
        else:
            raise ex.excError("%s: reference can have only one dot" % ref)

        if len(_section) == 0:
            raise ex.excError("%s: reference section can not be empty" % ref)
        if len(_v) == 0:
            raise ex.excError("%s: reference option can not be empty" % ref)

        if _v[0] == "#":
            return_length = True
            _v = _v[1:]
        else:
            return_length = False

        val = _handle_reference(svc, conf, ref, _section, _v, scope=scope, impersonate=impersonate)

        if return_length:
            return str(len(val.split()))

        if not index is None:
            return val.split()[index]

        return val

def _handle_reference(svc, conf, ref, _section, _v, scope=False, impersonate=None):
        # give os env precedence over the env cf section
        if _section == "env" and _v.upper() in os.environ:
            return os.environ[_v.upper()]

        if _section != "DEFAULT" and not conf.has_section(_section):
            raise ex.excError("%s: section %s does not exist" % (ref, _section))

        try:
            return conf_get(svc, conf, _section, _v, "string", scope=scope, impersonate=impersonate)
        except ex.OptNotFound as e:
            raise ex.excError("%s: unresolved reference (%s)" % (ref, str(e)))

        raise ex.excError("%s: unknown reference" % ref)

def _handle_references(svc, conf, s, scope=False, impersonate=None):
    while True:
        m = re.search(r'{\w[\w#\.\[\]]*}', s)
        if m is None:
            return s
        ref = m.group(0).strip("{}")
        val = handle_reference(svc, conf, ref, scope=scope, impersonate=impersonate)
        s = s[:m.start()] + val + s[m.end():]

def _handle_expressions(s):
    while True:
        m = re.search(r'\$\((.+)\)', s)
        if m is None:
            return s
        expr = m.group(1)
        val = eval_expr(expr)
        s = s[:m.start()] + str(val) + s[m.end():]

def handle_references(svc, conf, s, scope=False, impersonate=None):
    key = (s, scope, impersonate)
    if hasattr(svc, "ref_cache") and svc.ref_cache is not None and key in svc.ref_cache:
        return svc.ref_cache[key]
    try:
        val = _handle_references(svc, conf, s, scope=scope, impersonate=impersonate)
        val = _handle_expressions(val)
        val = _handle_references(svc, conf, val, scope=scope, impersonate=impersonate)
    except Exception as e:
        raise ex.excError("%s: reference evaluation failed: %s" %(s, str(e)))
    if hasattr(svc, "ref_cache") and svc.ref_cache is not None:
        svc.ref_cache[key] = val
    return val

def conf_get(svc, conf, s, o, t, scope=False, impersonate=None):
    if not scope:
        val = conf_get_val_unscoped(svc, conf, s, o)
    else:
        val = conf_get_val_scoped(svc, conf, s, o, impersonate=impersonate)

    try:
        val = handle_references(svc, conf, val, scope=scope, impersonate=impersonate)
    except ex.excError:
        if o.startswith("pre_") or o.startswith("post_") or o.startswith("blocking_"):
            pass
        else:
            raise

    if t == 'string':
        pass
    elif t == 'boolean':
        val = rcUtilities.convert_bool(val)
    elif t == 'integer':
        try:
            val = int(val)
        except:
            val = rcUtilities.convert_size(val)
    else:
        raise Exception("unknown keyword type: %s" % t)

    return val

def conf_get_val_unscoped(svc, conf, s, o):
    if conf.has_option(s, o):
        return conf.get(s, o)
    raise ex.OptNotFound("unscoped keyword %s.%s not found" % (s, o))

def conf_get_val_scoped(svc, conf, s, o, impersonate=None):
    if impersonate is None:
        nodename = rcEnv.nodename
    else:
        nodename = impersonate

    if conf.has_option(s, o+"@"+nodename):
        val = conf.get(s, o+"@"+nodename)
    elif conf.has_option(s, o+"@nodes") and \
         nodename in svc.nodes:
        val = conf.get(s, o+"@nodes")
    elif conf.has_option(s, o+"@drpnodes") and \
         nodename in svc.drpnodes:
        val = conf.get(s, o+"@drpnodes")
    elif conf.has_option(s, o+"@encapnodes") and \
         nodename in svc.encapnodes:
        val = conf.get(s, o+"@encapnodes")
    elif conf.has_option(s, o+"@flex_primary") and \
         nodename == svc.flex_primary:
        val = conf.get(s, o+"@flex_primary")
    elif conf.has_option(s, o+"@drp_flex_primary") and \
         nodename == svc.drp_flex_primary:
        val = conf.get(s, o+"@drp_flex_primary")
    elif conf.has_option(s, o):
        try:
            val = conf.get(s, o)
        except Exception as e:
            raise ex.excError("param %s.%s: %s"%(s, o, str(e)))
    else:
        raise ex.OptNotFound("scoped keyword %s.%s not found" % (s, o))

    return val

def conf_get_string(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'string', scope=False)

def conf_get_string_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'string', scope=True, impersonate=impersonate)

def conf_get_boolean(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'boolean', scope=False)

def conf_get_boolean_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'boolean', scope=True, impersonate=impersonate)

def conf_get_int(svc, conf, s, o):
    return conf_get(svc, conf, s, o, 'integer', scope=False)

def conf_get_int_scope(svc, conf, s, o, impersonate=None):
    return conf_get(svc, conf, s, o, 'integer', scope=True, impersonate=impersonate)

def svcmode_mod_name(svcmode=''):
    """Returns (moduleName, serviceClassName) implementing the class for
    a given service mode. For example:
    hosted => ('svcHosted', 'SvcHosted')
    """
    if svcmode == 'hosted':
        return ('svcHosted', 'SvcHosted')
    elif svcmode == 'sg':
        return ('svcSg', 'SvcSg')
    elif svcmode == 'rhcs':
        return ('svcRhcs', 'SvcRhcs')
    elif svcmode == 'vcs':
        return ('svcVcs', 'SvcVcs')
    raise ex.excError("unknown service mode: %s"%svcmode)

def get_tags(conf, section, svc):
    try:
        s = conf_get_string_scope(svc, conf, section, 'tags')
    except ex.OptNotFound:
        s = ""
    return set(s.split())

def get_optional(conf, section, svc):
    if not conf.has_section(section):
        try:
            return conf_get_boolean_scope(svc, conf, "DEFAULT", "optional")
        except:
            return False

    # deprecated
    if conf.has_option(section, 'optional_on'):
        nodes = set([])
        l = conf.get(section, "optional_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
        if rcEnv.nodename in nodes:
            return True
        return False

    try:
        return conf_get_boolean_scope(svc, conf, section, "optional")
    except:
        return False

def get_monitor(conf, section, svc):
    if not conf.has_section(section):
        try:
            return conf_get_boolean_scope(svc, conf, "DEFAULT", "monitor")
        except:
            return False

    # deprecated
    if conf.has_option(section, 'monitor_on'):
        nodes = set([])
        l = conf.get(section, "monitor_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
        if rcEnv.nodename in nodes:
            return True
        return False

    try:
        return conf_get_boolean_scope(svc, conf, section, "monitor")
    except:
        return False

def get_rcmd(conf, section, svc):
    if not conf.has_section(section):
        return
    try:
        return conf_get_string_scope(svc, conf, section, 'rcmd').split()
    except ex.OptNotFound:
        return

def get_subset(conf, section, svc):
    if not conf.has_section(section):
        return
    try:
        return conf_get_string_scope(svc, conf, section, 'subset')
    except ex.OptNotFound:
        return
    return

def get_osvc_root_path(conf, section, svc):
    if not conf.has_section(section):
        return
    try:
        return conf_get_string_scope(svc, conf, section, 'osvc_root_path')
    except ex.OptNotFound:
        return
    return

def get_restart(conf, section, svc):
    if not conf.has_section(section):
        if conf.has_option('DEFAULT', 'restart'):
            try:
                return conf_get_int_scope(svc, conf, section, 'restart')
            except ex.OptNotFound:
                return 0
        else:
            return 0
    try:
        return conf_get_int_scope(svc, conf, section, 'restart')
    except ex.OptNotFound:
        return 0
    return 0

def get_disabled(conf, section, svc):
    # service-level disable takes precedence over all resource-level disable method
    if conf.has_option('DEFAULT', 'disable'):
        svc_disable = conf.getboolean("DEFAULT", "disable")
    else:
        svc_disable = False

    if svc_disable is True:
        return True

    if section == "":
        return svc_disable

    # unscopable enable_on option (takes precedence over disable and disable_on)
    nodes = set([])
    if conf.has_option(section, 'enable_on'):
        l = conf_get_string_scope(svc, conf, section, "enable_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
        if rcEnv.nodename in nodes:
            return False

    # scoped disable option
    try:
        r = conf_get_boolean_scope(svc, conf, section, 'disable')
    except ex.OptNotFound:
        r = False
    except Exception as e:
        print(e, "... consider section as disabled")
        r = True
    if r:
        return r

    # unscopable disable_on option
    nodes = set([])
    if conf.has_option(section, 'disable_on'):
        l = conf.get(section, "disable_on").split()
        for i in l:
            if i == 'nodes': nodes |= svc.nodes
            elif i == 'drpnodes': nodes |= svc.drpnodes
            else: nodes |= set([i])
    if rcEnv.nodename in nodes:
        return True

    return False

def need_scsireserv(svc, conf, section):
    """scsireserv = true can be set globally or in a specific
    resource section
    """
    r = False
    try:
        r = conf_get_boolean_scope(svc, conf, section, 'scsireserv')
    except ex.OptNotFound:
        defaults = conf.defaults()
        if 'scsireserv' in defaults:
            r = bool(defaults['scsireserv'])
    return r

def add_scsireserv(svc, resource, conf, section):
    if not need_scsireserv(svc, conf, section):
        return
    try:
        sr = __import__('resScsiReserv'+rcEnv.sysname)
    except ImportError:
        sr = __import__('resScsiReserv')

    kwargs = {}
    pr_rid = resource.rid+"pr"

    try:
        kwargs["prkey"] = conf_get_string_scope(svc, conf, resource.rid, 'prkey')
    except ex.OptNotFound:
        pass

    try:
        pa = conf_get_boolean_scope(svc, conf, resource.rid, 'no_preempt_abort')
    except ex.OptNotFound:
        pa = False

    try:
        kwargs['optional'] = get_optional(conf, pr_rid, svc)
    except ex.OptNotFound:
        kwargs['optional'] = resource.is_optional()

    try:
        kwargs['disabled'] = get_disabled(conf, pr_rid, svc)
    except ex.OptNotFound:
        kwargs['disabled'] = resource.is_disabled()

    try:
        kwargs['restart'] = get_restart(conf, pr_rid, svc)
    except ex.OptNotFound:
        kwargs['restart'] = resource.restart

    try:
        kwargs['monitor'] = get_monitor(conf, pr_rid, svc)
    except ex.OptNotFound:
        kwargs['monitor'] = resource.monitor

    try:
        kwargs['tags'] = get_tags(conf, pr_rid, svc)
    except:
        kwargs['tags'] = set([])

    kwargs['rid'] = resource.rid
    kwargs['tags'] |= resource.tags
    kwargs['peer_resource'] = resource
    kwargs['no_preempt_abort'] = pa

    r = sr.ScsiReserv(**kwargs)
    svc += r

def add_triggers(svc, resource, conf, section):
    triggers = [
      'pre_unprovision', 'post_unprovision',
      'pre_provision', 'post_provision',
      'pre_stop', 'pre_start',
      'post_stop', 'post_start',
      'pre_sync_nodes', 'pre_sync_drp',
      'post_sync_nodes', 'post_sync_drp',
      'post_sync_resync', 'pre_sync_resync',
      'post_sync_update', 'pre_sync_update',
      'post_run', 'pre_run',
    ]
    compat_triggers = [
      'pre_syncnodes', 'pre_syncdrp',
      'post_syncnodes', 'post_syncdrp',
      'post_syncresync', 'pre_syncresync',
      'post_syncupdate', 'pre_syncupdate',
    ]
    for trigger in triggers + compat_triggers:
        for prefix in ("", "blocking_"):
            try:
                s = conf_get_string_scope(svc, conf, resource.rid, prefix+trigger)
            except ex.OptNotFound:
                continue
            if trigger in compat_triggers:
                trigger = trigger.replace("sync", "sync_")
            setattr(resource, prefix+trigger, s)

def add_requires(svc, resource, conf, section):
    actions = [
      'unprovision', 'provision'
      'stop', 'start',
      'sync_nodes', 'sync_drp', 'sync_resync', 'sync_break', 'sync_update',
      'run',
    ]
    for action in actions:
        try:
            s = conf_get_string_scope(svc, conf, section, action+'_requires')
        except ex.OptNotFound:
            continue
        s = s.replace("stdby ", "stdby_")
        l = s.split(" ")
        l = list(map(lambda x: x.replace("stdby_", "stdby "), l))
        setattr(resource, action+'_requires', l)

def add_triggers_and_requires(svc, resource, conf, section):
    add_triggers(svc, resource, conf, section)
    add_requires(svc, resource, conf, section)

def always_on_nodes_set(svc, conf, section):
    try:
        always_on_opt = conf.get(section, "always_on").split()
    except:
        always_on_opt = []
    always_on = set([])
    if 'nodes' in always_on_opt:
        always_on |= svc.nodes
    if 'drpnodes' in always_on_opt:
        always_on |= svc.drpnodes
    always_on |= set(always_on_opt) - set(['nodes', 'drpnodes'])
    return always_on

def get_sync_args(conf, s, svc):
    kwargs = {}
    defaults = conf.defaults()

    if conf.has_option(s, 'sync_max_delay'):
        kwargs['sync_max_delay'] = conf_get_int_scope(svc, conf, s, 'sync_max_delay')
    elif 'sync_max_delay' in defaults:
        kwargs['sync_max_delay'] = conf_get_int_scope(svc, conf, 'DEFAULT', 'sync_max_delay')

    if conf.has_option(s, 'schedule'):
        kwargs['schedule'] = conf_get_string_scope(svc, conf, s, 'schedule')
    elif conf.has_option(s, 'period') or conf.has_option(s, 'sync_period'):
        # old schedule syntax compatibility
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(conf, s, prefix='sync_')
    elif 'sync_schedule' in defaults:
        kwargs['schedule'] = conf_get_string_scope(svc, conf, 'DEFAULT', 'sync_schedule')
    elif 'sync_period' in defaults:
        # old schedule syntax compatibility for internal sync
        from rcScheduler import Scheduler
        kwargs['schedule'] = Scheduler().sched_convert_to_schedule(conf, s, prefix='sync_')

    return kwargs

def add_resources(restype, svc, conf):
    for s in conf.sections():
        if restype == "pool":
            restype = "zpool"
        if restype in ("disk", "vg", "zpool") and re.match(restype+'#.+pr', s, re.I) is not None:
            # persistent reserv resource are declared by their peer resource:
            # don't add them from here
            continue
        if s != 'app' and s != restype and re.match(restype+'#', s, re.I) is None:
            continue
        if svc.encap and 'encap' not in get_tags(conf, s, svc):
            continue
        if not svc.encap and 'encap' in get_tags(conf, s, svc):
            svc.has_encap_resources = True
            continue
        if s in svc.resources_by_id:
            continue
        globals()['add_'+restype](svc, conf, s)

def add_ip_gce(svc, conf, s):
    kwargs = {}

    try:
        rtype = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype != "gce":
        return

    try:
        kwargs['ipName'] = conf_get_string_scope(svc, conf, s, 'ipname')
    except ex.OptNotFound:
        svc.log.error("ipname must be defined in config file section %s" % s)
        return

    try:
        kwargs['ipDev'] = conf_get_string_scope(svc, conf, s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error("ipdev must be defined in config file section %s" % s)
        return

    try:
        kwargs['eip'] = conf_get_string_scope(svc, conf, s, 'eip')
    except ex.OptNotFound:
        pass

    try:
        kwargs['routename'] = conf_get_string_scope(svc, conf, s, 'routename')
    except ex.OptNotFound:
        pass

    try:
        kwargs['gce_zone'] = conf_get_string_scope(svc, conf, s, 'gce_zone')
    except ex.OptNotFound:
        pass

    ip = __import__('resIpGce')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    r = ip.Ip(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_ip_amazon(svc, conf, s):
    kwargs = {}

    try:
        rtype = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype != "amazon":
        return

    try:
        kwargs['ipName'] = conf_get_string_scope(svc, conf, s, 'ipname')
    except ex.OptNotFound:
        svc.log.error("nor ipname and ipname@%s defined in config file section %s"%(rcEnv.nodename, s))
        return

    try:
        kwargs['ipDev'] = conf_get_string_scope(svc, conf, s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error("ipdev must be defined in config file section %s" % s)
        return

    try:
        kwargs['eip'] = conf_get_string_scope(svc, conf, s, 'eip')
    except ex.OptNotFound:
        pass

    ip = __import__('resIpAmazon')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    r = ip.Ip(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_ip(svc, conf, s):
    """Parse the configuration file and add an ip object for each [ip#n]
    section. Ip objects are stored in a list in the service object.
    """
    try:
        rtype = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        rtype = None

    if rtype == "amazon":
        return add_ip_amazon(svc, conf, s)
    elif rtype == "gce":
        return add_ip_gce(svc, conf, s)

    kwargs = {}

    try:
        kwargs['ipName'] = conf_get_string_scope(svc, conf, s, 'ipname')
    except ex.OptNotFound:
        pass

    try:
        kwargs['ipDev'] = conf_get_string_scope(svc, conf, s, 'ipdev')
    except ex.OptNotFound:
        svc.log.error('ipdev not found in ip section %s'%s)
        return

    try:
        kwargs['ipDevExt'] = conf_get_string_scope(svc, conf, s, 'ipdevext')
    except ex.OptNotFound:
        pass

    try:
        kwargs['mask'] = conf_get_string_scope(svc, conf, s, 'netmask')
    except ex.OptNotFound:
        pass

    try:
        kwargs['gateway'] = conf_get_string_scope(svc, conf, s, 'gateway')
    except ex.OptNotFound:
        pass

    try:
        kwargs['zone'] = conf_get_string_scope(svc, conf, s, 'zone')
    except ex.OptNotFound:
        pass

    try:
        kwargs['container_rid'] = conf_get_string_scope(svc, conf, s, 'container_rid')
    except ex.OptNotFound:
        pass

    if rtype == "docker":
        try:
            kwargs['network'] = conf_get_string_scope(svc, conf, s, 'network')
        except ex.OptNotFound:
            pass
        try:
            kwargs['del_net_route'] = conf_get_boolean_scope(svc, conf, s, 'del_net_route')
        except ex.OptNotFound:
            pass

    if rtype == "crossbow":
        if 'zone' in kwargs:
            svc.log.error("'zone' and 'type=crossbow' are incompatible in section %s"%s)
            return
        ip = __import__('resIpCrossbow')
    elif 'zone' in kwargs:
        ip = __import__('resIpZone')
    elif rtype == "docker" or "container_rid" in kwargs:
        ip = __import__('resIpDocker'+rcEnv.sysname)
    else:
        ip = __import__('resIp'+rcEnv.sysname)

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    r = ip.Ip(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_md(svc, conf, s):
    kwargs = {}

    try:
        kwargs['uuid'] = conf_get_string_scope(svc, conf, s, 'uuid')
    except ex.OptNotFound:
        svc.log.error("uuid must be set in section %s"%s)
        return

    try:
        kwargs['shared'] = conf_get_string_scope(svc, conf, s, 'shared')
    except ex.OptNotFound:
        if len(svc.nodes|svc.drpnodes) < 2:
            kwargs['shared'] = False
            svc.log.debug("md %s shared param defaults to %s due to single node configuration"%(s, kwargs['shared']))
        else:
            l = [ p for p in conf.options(s) if "@" in p ]
            if len(l) > 0:
                kwargs['shared'] = False
                svc.log.debug("md %s shared param defaults to %s due to scoped configuration"%(s, kwargs['shared']))
            else:
                kwargs['shared'] = True
                svc.log.debug("md %s shared param defaults to %s due to unscoped configuration"%(s, kwargs['shared']))

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    m = __import__('resDiskMdLinux')
    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_drbd(svc, conf, s):
    """Parse the configuration file and add a drbd object for each [drbd#n]
    section. Drbd objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['res'] = conf_get_string(svc, conf, s, 'res')
    except ex.OptNotFound:
        svc.log.error("res must be set in section %s"%s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    mod = __import__('resDiskDrbd')
    r = mod.Drbd(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_vdisk(svc, conf, s):
    kwargs = {}
    devpath = {}

    for attr, val in conf.items(s):
        if 'path@' in attr:
            devpath[attr.replace('path@','')] = val

    if len(devpath) == 0:
        svc.log.error("path@node must be set in section %s"%s)
        return

    kwargs['devpath'] = devpath
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    m = __import__('resDiskVdisk')
    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_stonith(svc, conf, s):
    if rcEnv.nodename in svc.drpnodes:
        # no stonith on DRP nodes
        return

    kwargs = {}

    try:
        _type = conf_get_string(svc, conf, s, 'type')
        if len(_type) > 1:
            _type = _type[0].upper()+_type[1:].lower()
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    if _type in ('Ilo'):
        try:
            kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
        except ex.OptNotFound:
            pass
        try:
            kwargs['name'] = conf_get_string_scope(svc, conf, s, 'target')
        except ex.OptNotFound:
            pass

        if 'name' not in kwargs:
            svc.log.error("target must be set in section %s"%s)
            return
    elif _type in ('Callout'):
        try:
            kwargs['cmd'] = conf_get_string_scope(svc, conf, s, 'cmd')
        except ex.OptNotFound:
            pass

        if 'cmd' not in kwargs:
            svc.log.error("cmd must be set in section %s"%s)
            return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)

    st = __import__('resStonith'+_type)
    try:
        st = __import__('resStonith'+_type)
    except ImportError:
        svc.log.error("resStonith%s is not implemented"%_type)
        return

    r = st.Stonith(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_hb(svc, conf, s):
    if rcEnv.nodename in svc.drpnodes:
        # no heartbeat on DRP nodes
        return

    kwargs = {}

    try:
        hbtype = conf_get_string(svc, conf, s, 'type').lower()
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string(svc, conf, s, 'name')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)

    if hbtype == 'openha':
        hbtype = 'OpenHA'
    elif hbtype == 'linuxha':
        hbtype = 'LinuxHA'

    try:
        hb = __import__('resHb'+hbtype)
    except ImportError:
        svc.log.error("resHb%s is not implemented"%hbtype)
        return

    r = hb.Hb(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_loop(svc, conf, s):
    """Parse the configuration file and add a loop object for each [loop#n]
    section. Loop objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['loopFile'] = conf_get_string_scope(svc, conf, s, 'file')
    except ex.OptNotFound:
        svc.log.error("file must be set in section %s"%s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDiskLoop'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resDiskLoop%s is not implemented"%rcEnv.sysname)
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r


def add_disk_gce(svc, conf, s):
    kwargs = {}
    try:
        kwargs['names'] = conf_get_string_scope(svc, conf, s, 'names').split()
    except ex.OptNotFound:
        svc.log.error("names must be set in section %s"%s)
        return

    try:
        kwargs['gce_zone'] = conf_get_string_scope(svc, conf, s, 'gce_zone')
    except ex.OptNotFound:
        svc.log.error("gce_zone must be set in section %s"%s)
        return

    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    m = __import__('resDiskGce')

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_disk_amazon(svc, conf, s):
    kwargs = {}
    try:
        kwargs['volumes'] = conf_get_string_scope(svc, conf, s, 'volumes').split()
    except ex.OptNotFound:
        svc.log.error("volumes must be set in section %s"%s)
        return

    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    m = __import__('resDiskAmazon')

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_rados(svc, conf, s):
    kwargs = {}
    try:
        kwargs['images'] = conf_get_string_scope(svc, conf, s, 'images').split()
    except ex.OptNotFound:
        pass
    try:
        kwargs['keyring'] = conf_get_string_scope(svc, conf, s, 'keyring')
    except ex.OptNotFound:
        pass
    try:
        kwargs['client_id'] = conf_get_string_scope(svc, conf, s, 'client_id')
    except ex.OptNotFound:
        pass
    try:
        lock_shared_tag = conf_get_string_scope(svc, conf, s, 'lock_shared_tag')
    except ex.OptNotFound:
        lock_shared_tag = None
    try:
        lock = conf_get_string_scope(svc, conf, s, 'lock')
    except ex.OptNotFound:
        lock = None

    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDiskRados'+rcEnv.sysname)
    except ImportError:
        svc.log.error("disk type rados is not implemented")
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

    if not lock:
        return

    # rados locking resource
    kwargs["rid"] = kwargs["rid"]+"lock"
    kwargs["lock"] = lock
    kwargs["lock_shared_tag"] = lock_shared_tag
    r = m.DiskLock(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r


def add_raw(svc, conf, s):
    kwargs = {}
    disk_type = "Raw"+rcEnv.sysname
    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except:
        zone = None
    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['group'] = conf_get_string_scope(svc, conf, s, 'group')
    except ex.OptNotFound:
        pass
    try:
        kwargs['perm'] = conf_get_string_scope(svc, conf, s, 'perm')
    except ex.OptNotFound:
        pass
    try:
        kwargs['create_char_devices'] = conf_get_boolean_scope(svc, conf, s, 'create_char_devices')
    except ex.OptNotFound:
        pass
    try:
        devs = conf_get_string_scope(svc, conf, s, 'devs')
        if zone is not None:
            devs = devs.replace(":", ":<%s>" % zone)
        kwargs['devs'] = set(devs.split())
    except ex.OptNotFound:
        svc.log.error("devs must be set in section %s"%s)
        return

    # backward compat : the dummy keyword is deprecated in favor of
    # the standard "noaction" tag.
    try:
        dummy = conf_get_boolean_scope(svc, conf, s, 'dummy')
    except ex.OptNotFound:
        dummy = False

    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDisk'+disk_type)
    except ImportError:
        svc.log.error("disk type %s driver is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    if dummy:
        r.tags.add("noaction")
    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_gandi(svc, conf, s):
    disk_type = "Gandi"
    kwargs = {}
    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return
    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return
    try:
        kwargs['node'] = conf_get_string_scope(svc, conf, s, 'node')
    except ex.OptNotFound:
        pass
    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['group'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        pass
    try:
        kwargs['perm'] = conf_get_string_scope(svc, conf, s, 'perm')
    except ex.OptNotFound:
        pass

    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDisk'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_disk_compat(svc, conf, s):
    try:
        disk_type = conf_get_string_scope(svc, conf, s, 'type')
        if len(disk_type) >= 2:
            disk_type = disk_type[0].upper() + disk_type[1:].lower()
    except ex.OptNotFound:
        disk_type = rcEnv.sysname

    if disk_type == 'Drbd':
        add_drbd(svc, conf, s)
        return
    if disk_type == 'Vdisk':
        add_vdisk(svc, conf, s)
        return
    if disk_type == 'Vmdg':
        add_vmdg(svc, conf, s)
        return
    if disk_type == 'Pool':
        add_zpool(svc, conf, s)
        return
    if disk_type == 'Zpool':
        add_zpool(svc, conf, s)
        return
    if disk_type == 'Loop':
        add_loop(svc, conf, s)
        return
    if disk_type == 'Md':
        add_md(svc, conf, s)
        return
    if disk_type == 'Gce':
        add_disk_gce(svc, conf, s)
        return
    if disk_type == 'Amazon':
        add_disk_amazon(svc, conf, s)
        return
    if disk_type == 'Rados':
        add_rados(svc, conf, s)
        return
    if disk_type == 'Raw':
        add_raw(svc, conf, s)
        return
    if disk_type == 'Gandi':
        add_gandi(svc, conf, s)
        return
    if disk_type == 'Veritas':
        add_veritas(svc, conf, s)
        return

    raise ex.OptNotFound

def add_veritas(svc, conf, s):
    kwargs = {}
    try:
        # deprecated keyword 'vgname'
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'vgname')
    except ex.OptNotFound:
        pass
    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        if "name" not in kwargs:
            svc.log.error("name must be set in section %s"%s)
            return
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDiskVgVeritas')
    except ImportError:
        svc.log.error("disk type veritas is not implemented")
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_vg(svc, conf, s):
    try:
        add_disk_compat(svc, conf, s)
        return
    except ex.OptNotFound:
        pass

    disk_type = rcEnv.sysname
    kwargs = {}
    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'vgname')
    except ex.OptNotFound:
        pass
    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        if "name" not in kwargs:
            svc.log.error("name must be set in section %s"%s)
            return
    try:
        kwargs['dsf'] = conf_get_boolean_scope(svc, conf, s, 'dsf')
    except ex.OptNotFound:
        pass
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    try:
        m = __import__('resDiskVg'+disk_type)
    except ImportError:
        svc.log.error("disk type %s is not implemented"%disk_type)
        return

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_disk(svc, conf, s):
    """Parse the configuration file and add a disk object for each [disk#n]
    section. Disk objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        disk_type = conf_get_string_scope(svc, conf, s, 'type')
        if len(disk_type) >= 2:
            disk_type = disk_type[0].upper() + disk_type[1:].lower()
    except ex.OptNotFound:
        disk_type = rcEnv.sysname

    if disk_type == 'Drbd':
        add_drbd(svc, conf, s)
        return
    if disk_type == 'Vdisk':
        add_vdisk(svc, conf, s)
        return
    if disk_type == 'Vmdg':
        add_vmdg(svc, conf, s)
        return
    if disk_type == 'Pool':
        add_zpool(svc, conf, s)
        return
    if disk_type == 'Zpool':
        add_zpool(svc, conf, s)
        return
    if disk_type == 'Loop':
        add_loop(svc, conf, s)
        return
    if disk_type == 'Md':
        add_md(svc, conf, s)
        return
    if disk_type == 'Gce':
        add_disk_gce(svc, conf, s)
        return
    if disk_type == 'Amazon':
        add_disk_amazon(svc, conf, s)
        return
    if disk_type == 'Rados':
        add_rados(svc, conf, s)
        return
    if disk_type == 'Raw':
        add_raw(svc, conf, s)
        return
    if disk_type == 'Gandi':
        add_gandi(svc, conf, s)
        return
    if disk_type == 'Veritas':
        add_veritas(svc, conf, s)
        return
    if disk_type == 'Lvm' or disk_type == 'Vg' or disk_type == rcEnv.sysname:
        add_vg(svc, conf, s)
        return

def add_vmdg(svc, conf, s):
    kwargs = {}

    try:
        kwargs['container_id'] = conf_get_string_scope(svc, conf, s, 'container_id')
    except ex.OptNotFound:
        svc.log.error("container_id must be set in section %s"%s)
        return

    if not conf.has_section(kwargs['container_id']):
        svc.log.error("%s.container_id points to an invalid section"%kwargs['container_id'])
        return

    try:
        container_type = conf_get_string_scope(svc, conf, kwargs['container_id'], 'type')
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%kwargs['container_id'])
        return

    if container_type == 'ldom':
        m = __import__('resDiskLdom')
    else:
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['name'] = s
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Disk(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_zpool(svc, conf, s):
    """Parse the configuration file and add a zpool object for each disk.zpool
    section. Pools objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'poolname')
    except ex.OptNotFound:
        pass

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        pass

    if "name" not in kwargs:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except ex.OptNotFound:
        zone = None

    m = __import__('resDiskZfs')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Disk(**kwargs)

    if zone is not None:
        r.tags.add('zone')
        r.tags.add(zone)

    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_share(svc, conf, s):
    try:
        _type = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        svc.log.error("type must be set in section %s"%s)
        return

    fname = 'add_share_'+_type
    if fname not in globals():
        svc.log.error("type '%s' not supported in section %s"%(_type, s))
    globals()[fname](svc, conf, s)

def add_share_nfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("path must be set in section %s"%s)
        return

    try:
        kwargs['opts'] = conf_get_string_scope(svc, conf, s, 'opts')
    except ex.OptNotFound:
        svc.log.error("opts must be set in section %s"%s)
        return

    try:
        m = __import__('resShareNfs'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resShareNfs%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = m.Share(**kwargs)

    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_fs_directory(svc, conf, s):
    kwargs = {}

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("path must be set in section %s"%s)
        return

    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        pass

    try:
        kwargs['group'] = conf_get_string_scope(svc, conf, s, 'group')
    except ex.OptNotFound:
        pass

    try:
        kwargs['perm'] = conf_get_string_scope(svc, conf, s, 'perm')
    except ex.OptNotFound:
        pass

    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except:
        zone = None

    if zone is not None:
        zp = None
        for r in svc.get_resources("container.zone", discard_disabled=False):
            if r.name == zone:
                try:
                    zp = r.get_zonepath()
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs['path'] = zp+'/root'+kwargs['path']
        if "<%s>" % zone != zp:
            kwargs['path'] = os.path.realpath(kwargs['path'])

    mod = __import__('resFsDir')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = mod.FsDir(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add('zone')

    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_fs(svc, conf, s):
    """Parse the configuration file and add a fs object for each [fs#n]
    section. Fs objects are stored in a list in the service object.
    """
    kwargs = {}

    try:
        kwargs['fsType'] = conf_get_string_scope(svc, conf, s, 'type')
    except ex.OptNotFound:
        kwargs['fsType'] = ""

    if kwargs['fsType'] == "directory":
        add_fs_directory(svc, conf, s)
        return

    try:
        kwargs['device'] = conf_get_string_scope(svc, conf, s, 'dev')
    except ex.OptNotFound:
        svc.log.error("dev must be set in section %s"%s)
        return

    try:
        kwargs['mountPoint'] = conf_get_string_scope(svc, conf, s, 'mnt')
    except ex.OptNotFound:
        svc.log.error("mnt must be set in section %s"%s)
        return

    if kwargs['mountPoint'][-1] != "/" and kwargs['mountPoint'][-1] == '/':
        """ Remove trailing / to not risk losing rsync src trailing /
            upon snap mountpoint substitution.
        """
        kwargs['mountPoint'] = kwargs['mountPoint'][0:-1]

    try:
        kwargs['mntOpt'] = conf_get_string_scope(svc, conf, s, 'mnt_opt')
    except ex.OptNotFound:
        kwargs['mntOpt'] = ""

    try:
        kwargs['snap_size'] = conf_get_int_scope(svc, conf, s, 'snap_size')
    except ex.OptNotFound:
        pass

    try:
        zone = conf_get_string_scope(svc, conf, s, 'zone')
    except:
        zone = None

    if zone is not None:
        zp = None
        for r in svc.get_resources("container.zone", discard_disabled=False):
            if r.name == zone:
                try:
                    zp = r.get_zonepath()
                except:
                    zp = "<%s>" % zone
                break
        if zp is None:
            svc.log.error("zone %s, referenced in %s, not found"%(zone, s))
            raise ex.excError()
        kwargs['mountPoint'] = zp+'/root'+kwargs['mountPoint']
        if "<%s>" % zone != zp:
            kwargs['mountPoint'] = os.path.realpath(kwargs['mountPoint'])

    try:
        mount = __import__('resFs'+rcEnv.sysname)
    except ImportError:
        svc.log.error("resFs%s is not implemented"%rcEnv.sysname)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = mount.Mount(**kwargs)

    if zone is not None:
        r.tags.add(zone)
        r.tags.add('zone')

    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_esx(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerEsx')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Esx(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_hpvm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerHpVm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.HpVm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_ldom(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerLdom')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Ldom(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_vbox(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerVbox')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Vbox(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_xen(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerXen')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Xen(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_zone(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['delete_on_stop'] = conf_get_boolean_scope(svc, conf, s, 'delete_on_stop')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerZone')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Zone(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)



def add_containers_vcloud(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['vapp'] = conf_get_string_scope(svc, conf, s, 'vapp')
    except ex.OptNotFound:
        svc.log.error("vapp must be set in section %s"%s)
        return

    m = __import__('resContainerVcloud')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.CloudVm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_amazon(svc, conf, s):
    kwargs = {}

    # mandatory keywords
    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    # optional keywords
    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    # provisioning keywords
    try:
        kwargs['image_id'] = conf_get_string_scope(svc, conf, s, 'image_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['size'] = conf_get_string_scope(svc, conf, s, 'size')
    except ex.OptNotFound:
        pass

    try:
        kwargs['key_name'] = conf_get_string_scope(svc, conf, s, 'key_name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['subnet'] = conf_get_string_scope(svc, conf, s, 'subnet')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerAmazon')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.CloudVm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_openstack(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cloud_id'] = conf_get_string_scope(svc, conf, s, 'cloud_id')
    except ex.OptNotFound:
        svc.log.error("cloud_id must be set in section %s"%s)
        return

    try:
        kwargs['size'] = conf_get_string_scope(svc, conf, s, 'size')
    except ex.OptNotFound:
        svc.log.error("size must be set in section %s"%s)
        return

    try:
        kwargs['key_name'] = conf_get_string_scope(svc, conf, s, 'key_name')
    except ex.OptNotFound:
        svc.log.error("key_name must be set in section %s"%s)
        return

    try:
        kwargs['shared_ip_group'] = conf_get_string_scope(svc, conf, s, 'shared_ip_group')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerOpenstack')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.CloudVm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_vz(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerVz')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Vz(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_kvm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerKvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Kvm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_srp(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerSrp')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Srp(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_lxc(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        kwargs['name'] = svc.svcname

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    try:
        kwargs['cf'] = conf_get_string_scope(svc, conf, s, 'cf')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerLxc')

    kwargs['rid'] = s
    kwargs['rcmd'] = get_rcmd(conf, s, svc)
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Lxc(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_docker(svc, conf, s):
    kwargs = {}

    try:
        kwargs['run_image'] = conf_get_string_scope(svc, conf, s, 'run_image')
    except ex.OptNotFound:
        svc.log.error("'run_image' parameter is mandatory in section %s"%s)
        return

    try:
        kwargs['run_command'] = conf_get_string_scope(svc, conf, s, 'run_command')
    except ex.OptNotFound:
        pass

    try:
        kwargs['run_args'] = conf_get_string_scope(svc, conf, s, 'run_args')
    except ex.OptNotFound:
        pass

    try:
        kwargs['run_swarm'] = conf_get_string_scope(svc, conf, s, 'run_swarm')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerDocker')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Docker(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_ovm(svc, conf, s):
    kwargs = {}

    try:
        kwargs['uuid'] = conf_get_string_scope(svc, conf, s, 'uuid')
    except ex.OptNotFound:
        svc.log.error("uuid must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['guestos'] = conf_get_string_scope(svc, conf, s, 'guestos')
    except ex.OptNotFound:
        pass

    m = __import__('resContainerOvm')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Ovm(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers_jail(svc, conf, s):
    kwargs = {}

    try:
        kwargs['jailroot'] = conf_get_string_scope(svc, conf, s, 'jailroot')
    except ex.OptNotFound:
        svc.log.error("jailroot must be set in section %s"%s)
        return

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("name must be set in section %s"%s)
        return

    try:
        kwargs['ips'] = conf_get_string_scope(svc, conf, s, 'ips').split()
    except ex.OptNotFound:
        pass

    try:
        kwargs['ip6s'] = conf_get_string_scope(svc, conf, s, 'ip6s').split()
    except ex.OptNotFound:
        pass

    m = __import__('resContainerJail')

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)
    kwargs['osvc_root_path'] = get_osvc_root_path(conf, s, svc)

    r = m.Jail(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r
    add_scsireserv(svc, r, conf, s)

def add_containers(svc, conf):
    for t in rcEnv.vt_supported:
        add_containers_resources(t, svc, conf)

def add_containers_resources(subtype, svc, conf):
    add_sub_resources('container', subtype, svc, conf)

def add_mandatory_syncs(svc, conf):
    """Mandatory files to sync:
    1/ to all nodes: service definition
    2/ to drpnodes: system files to replace on the drpnode in case of startdrp
    """

    """1
    """
    if len(svc.nodes|svc.drpnodes) > 1:
        kwargs = {}
        src = []
        src.append(os.path.join(rcEnv.pathetc, svc.svcname))
        src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.conf'))
        src.append(os.path.join(rcEnv.pathetc, svc.svcname+'.d'))
        localrc = os.path.join(rcEnv.pathetc, svc.svcname+'.dir')
        cluster = os.path.join(rcEnv.pathetc, svc.svcname+'.cluster')
        if os.path.exists(cluster):
            src.append(cluster)
        if os.path.exists(localrc):
            src.append(localrc)
        for rs in svc.resSets:
            for r in rs.resources:
                src += r.files_to_sync()
        dst = os.path.join("/")
        exclude = ['--exclude=*.core']
        targethash = {'nodes': svc.nodes, 'drpnodes': svc.drpnodes}
        kwargs['rid'] = "sync#i0"
        kwargs['src'] = src
        kwargs['dst'] = dst
        kwargs['options'] = ['-R']+exclude
        if conf.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += rcUtilities.cmdline2list(conf.get(kwargs['rid'], 'options'))
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
        kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
        kwargs.update(get_sync_args(conf, kwargs['rid'], svc))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

    """2
    """
    if len(svc.drpnodes) == 0:
        return

    targethash = {'drpnodes': svc.drpnodes}
    """ Reparent all PRD backed-up file in drp_path/node on the drpnode
    """
    dst = os.path.join(rcEnv.drp_path, rcEnv.nodename)
    i = 0
    for src, exclude in rcEnv.drp_sync_files:
        """'-R' triggers rsync relative mode
        """
        kwargs = {}
        src = [ s for s in src if os.path.exists(s) ]
        if len(src) == 0:
            continue
        i += 1
        kwargs['rid'] = "sync#i"+str(i)
        kwargs['src'] = src
        kwargs['dst'] = dst
        kwargs['options'] = ['-R']+exclude
        if conf.has_option(kwargs['rid'], 'options'):
            kwargs['options'] += rcUtilities.cmdline2list(conf.get(kwargs['rid'], 'options'))
        kwargs['target'] = targethash
        kwargs['internal'] = True
        kwargs['disabled'] = get_disabled(conf, kwargs['rid'], svc)
        kwargs['optional'] = get_optional(conf, kwargs['rid'], svc)
        kwargs.update(get_sync_args(conf, kwargs['rid'], svc))
        r = resSyncRsync.Rsync(**kwargs)
        svc += r

def add_syncs_resources(subtype, svc, conf):
    add_sub_resources('sync', subtype, svc, conf, default_subtype="rsync")

def add_sub_resources(restype, subtype, svc, conf, default_subtype=None):
    for s in conf.sections():
        if re.match(restype+'#', s, re.I) is None:
            continue
        if svc.encap and 'encap' not in get_tags(conf, s, svc):
            continue
        if not svc.encap and 'encap' in get_tags(conf, s, svc):
            svc.has_encap_resources = True
            continue
        try:
            res_subtype = conf_get_string_scope(svc, conf, s, "type")
        except ex.OptNotFound:
            res_subtype = default_subtype

        if subtype != res_subtype:
            continue

        globals()['add_'+restype+'s_'+subtype](svc, conf, s)

def add_syncs(svc, conf):
    add_syncs_resources('rsync', svc, conf)
    add_syncs_resources('netapp', svc, conf)
    add_syncs_resources('nexenta', svc, conf)
    add_syncs_resources('radossnap', svc, conf)
    add_syncs_resources('radosclone', svc, conf)
    add_syncs_resources('symclone', svc, conf)
    add_syncs_resources('symsnap', svc, conf)
    add_syncs_resources('symsrdfs', svc, conf)
    add_syncs_resources('hp3par', svc, conf)
    add_syncs_resources('hp3parsnap', svc, conf)
    add_syncs_resources('ibmdssnap', svc, conf)
    add_syncs_resources('evasnap', svc, conf)
    add_syncs_resources('necismsnap', svc, conf)
    add_syncs_resources('btrfssnap', svc, conf)
    add_syncs_resources('zfssnap', svc, conf)
    add_syncs_resources('s3', svc, conf)
    add_syncs_resources('dcssnap', svc, conf)
    add_syncs_resources('dcsckpt', svc, conf)
    add_syncs_resources('dds', svc, conf)
    add_syncs_resources('zfs', svc, conf)
    add_syncs_resources('btrfs', svc, conf)
    add_syncs_resources('docker', svc, conf)
    add_mandatory_syncs(svc, conf)

def add_syncs_docker(svc, conf, s):
    kwargs = {}

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split(' ')
    except ex.OptNotFound:
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    m = __import__('resSyncDocker')
    r = m.SyncDocker(**kwargs)
    svc += r

def add_syncs_btrfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = conf_get_boolean(svc, conf, s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    btrfs = __import__('resSyncBtrfs')
    r = btrfs.SyncBtrfs(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_zfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['recursive'] = conf_get_boolean(svc, conf, s, 'recursive')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    zfs = __import__('resSyncZfs')
    r = zfs.SyncZfs(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_dds(svc, conf, s):
    kwargs = {}

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    dsts = {}
    for node in svc.nodes | svc.drpnodes:
        dst = conf_get_string_scope(svc, conf, s, 'dst', impersonate=node)
        dsts[node] = dst

    if len(dsts) == 0:
        for node in svc.nodes | svc.drpnodes:
            dsts[node] = kwargs['src']

    kwargs['dsts'] = dsts

    try:
        kwargs['target'] = conf_get_string_scope(svc, conf, s, 'target').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have target set" % s)
        return

    try:
        kwargs['sender'] = conf_get_string(svc, conf, s, 'sender')
    except ex.OptNotFound:
        pass

    try:
        kwargs['snap_size'] = conf_get_int_scope(svc, conf, s, 'snap_size')
    except ex.OptNotFound:
        pass

    try:
        kwargs['delta_store'] = conf_get_string_scope(svc, conf, s, 'delta_store')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    dds = __import__('resSyncDds')
    r = dds.syncDds(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_dcsckpt(svc, conf, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(conf_get_string_scope(svc, conf, s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(conf_get_string_scope(svc, conf, s, 'manager').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    import json
    pairs = []
    if 'pairs' in conf.options(s):
        try:
            pairs = json.loads(conf.get(s, 'pairs'))
            if len(pairs) == 0:
                svc.log.error("config file section %s must have 'pairs' set" % s)
                return
        except:
            svc.log.error("json error parsing 'pairs' in section %s" % s)
    kwargs['pairs'] = pairs

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncDcsCkpt'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsCkpt')
    r = sc.syncDcsCkpt(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_dcssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['dcs'] = set(conf_get_string(svc, conf, s, 'dcs').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'dcs' set" % s)
        return

    try:
        kwargs['manager'] = set(conf_get_string(svc, conf, s, 'manager').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'manager' set" % s)
        return

    try:
        kwargs['snapname'] = set(conf_get_string(svc, conf, s, 'snapname').split())
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'snapname' set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncDcsSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncDcsSnap')
    r = sc.syncDcsSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_s3(svc, conf, s):
    kwargs = {}

    try:
        kwargs['full_schedule'] = conf_get_string_scope(svc, conf, s, 'full_schedule')
    except ex.OptNotFound:
        pass

    try:
        kwargs['options'] = conf_get_string_scope(svc, conf, s, 'options').split()
    except ex.OptNotFound:
        pass

    try:
        kwargs['snar'] = conf_get_string_scope(svc, conf, s, 'snar')
    except ex.OptNotFound:
        pass

    try:
        kwargs['bucket'] = conf_get_string_scope(svc, conf, s, 'bucket')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have bucket set" % s)
        return

    try:
        kwargs['src'] = conf_get_string_scope(svc, conf, s, 'src').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    sc = __import__('resSyncS3')
    r = sc.syncS3(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_zfssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keep'] = conf_get_int_scope(svc, conf, s, 'keep')
    except ex.OptNotFound:
        pass

    try:
        kwargs['recursive'] = conf_get_boolean_scope(svc, conf, s, 'recursive')
    except ex.OptNotFound:
        pass

    try:
        kwargs['dataset'] = conf_get_string_scope(svc, conf, s, 'dataset').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dataset set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    sc = __import__('resSyncZfsSnap')
    r = sc.syncZfsSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_btrfssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string_scope(svc, conf, s, 'name')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keep'] = conf_get_int_scope(svc, conf, s, 'keep')
    except ex.OptNotFound:
        pass

    try:
        kwargs['subvol'] = conf_get_string_scope(svc, conf, s, 'subvol').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have subvol set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    sc = __import__('resSyncBtrfsSnap')
    r = sc.syncBtrfsSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_necismsnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['array'] = conf_get_string_scope(svc, conf, s, 'array')
    except ex.OptNotFound:
        pass

    try:
        kwargs['devs'] = conf_get_string_scope(svc, conf, s, 'devs')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have devs set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncNecIsmSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncNecIsmSnap')
    r = sc.syncNecIsmSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_evasnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['eva_name'] = conf_get_string(svc, conf, s, 'eva_name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have eva_name set" % s)
        return

    try:
        kwargs['snap_name'] = conf_get_string(svc, conf, s, 'snap_name')
    except ex.OptNotFound:
        kwargs['snap_name'] = svc.svcname

    import json
    pairs = []
    if 'pairs' in conf.options(s):
        pairs = json.loads(conf.get(s, 'pairs'))
    if len(pairs) == 0:
        svc.log.error("config file section %s must have pairs set" % s)
        return
    else:
        kwargs['pairs'] = pairs

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncEvasnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncEvasnap')
    r = sc.syncEvasnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_hp3parsnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['array'] = conf_get_string_scope(svc, conf, s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    try:
        vv_names = conf_get_string_scope(svc, conf, s, 'vv_names').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have vv_names set" % s)
        return

    if len(vv_names) == 0:
        svc.log.error("config file section %s must have at least one vv_name set" % s)
        return

    kwargs['vv_names'] = vv_names

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncHp3parSnap'+rcEnv.sysname)
    except:
        sc = __import__('resSyncHp3parSnap')
    r = sc.syncHp3parSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_hp3par(svc, conf, s):
    kwargs = {}

    try:
        kwargs['mode'] = conf_get_string_scope(svc, conf, s, 'mode')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have mode set" % s)
        return

    try:
        kwargs['array'] = conf_get_string_scope(svc, conf, s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    rcg_names = {}
    for node in svc.nodes | svc.drpnodes:
        array = conf_get_string_scope(svc, conf, s, 'array', impersonate=node)
        rcg = conf_get_string_scope(svc, conf, s, 'rcg', impersonate=node)
        rcg_names[array] = rcg

    if len(rcg_names) == 0:
        svc.log.error("config file section %s must have rcg set" % s)
        return

    kwargs['rcg_names'] = rcg_names

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncHp3par'+rcEnv.sysname)
    except:
        sc = __import__('resSyncHp3par')
    r = sc.syncHp3par(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_symsrdfs(svc, conf, s):
    kwargs = {}

    try:
        kwargs['symdg'] = conf_get_string(svc, conf, s, 'symdg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have symdg set" % s)
        return

    try:
        kwargs['rdfg'] = conf_get_int(svc, conf, s, 'rdfg')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have rdfg number set" % s)
        return


    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncSymSrdfS'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymSrdfS')
    r = sc.syncSymSrdfS(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r


def add_syncs_radosclone(svc, conf, s):
    kwargs = {}

    try:
        kwargs['client_id'] = conf_get_string_scope(svc, conf, s, 'client_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keyring'] = conf_get_string_scope(svc, conf, s, 'keyring')
    except ex.OptNotFound:
        pass

    try:
        kwargs['pairs'] = conf_get_string_scope(svc, conf, s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncRados'+rcEnv.sysname)
    except:
        sc = __import__('resSyncRados')
    r = sc.syncRadosClone(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_radossnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['client_id'] = conf_get_string_scope(svc, conf, s, 'client_id')
    except ex.OptNotFound:
        pass

    try:
        kwargs['keyring'] = conf_get_string_scope(svc, conf, s, 'keyring')
    except ex.OptNotFound:
        pass

    try:
        kwargs['images'] = conf_get_string_scope(svc, conf, s, 'images').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have images set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncRados'+rcEnv.sysname)
    except:
        sc = __import__('resSyncRados')
    r = sc.syncRadosSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_symsnap(svc, conf, s):
    _add_syncs_symclone(svc, conf, s, "sync.symsnap")

def add_syncs_symclone(svc, conf, s):
    _add_syncs_symclone(svc, conf, s, "sync.symclone")

def _add_syncs_symclone(svc, conf, s, t):
    kwargs = {}
    kwargs['type'] = t
    try:
        kwargs['pairs'] = conf_get_string(svc, conf, s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    try:
        kwargs['symid'] = conf_get_string_scope(svc, conf, s, 'symid')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have sid set" % s)
        return

    try:
        kwargs['recreate_timeout'] = conf_get_int(svc, conf, s, 'recreate_timeout')
    except ex.OptNotFound:
        pass

    try:
        kwargs['consistent'] = conf_get_boolean(svc, conf, s, 'consistent')
    except ex.OptNotFound:
        pass

    try:
        kwargs['precopy'] = conf_get_boolean(svc, conf, s, 'precopy')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        sc = __import__('resSyncSymclone'+rcEnv.sysname)
    except:
        sc = __import__('resSyncSymclone')
    r = sc.syncSymclone(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_ibmdssnap(svc, conf, s):
    kwargs = {}

    try:
        kwargs['pairs'] = conf_get_string(svc, conf, s, 'pairs').split()
    except ex.OptNotFound:
        svc.log.error("config file section %s must have pairs set" % s)
        return

    try:
        kwargs['array'] = conf_get_string(svc, conf, s, 'array')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have array set" % s)
        return

    try:
        kwargs['bgcopy'] = conf_get_boolean(svc, conf, s, 'bgcopy')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have bgcopy set" % s)
        return

    try:
        kwargs['recording'] = conf_get_boolean(svc, conf, s, 'recording')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have recording set" % s)
        return

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))
    try:
        m = __import__('resSyncIbmdsSnap'+rcEnv.sysname)
    except:
        m = __import__('resSyncIbmdsSnap')
    r = m.syncIbmdsSnap(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_nexenta(svc, conf, s):
    kwargs = {}

    try:
        kwargs['name'] = conf_get_string(svc, conf, s, 'name')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have 'name' set" % s)
        return

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

    try:
        kwargs['reversible'] = conf_get_boolean_scope(svc, conf, s, "reversible")
    except:
        pass

    filers = {}
    if 'filer' in conf.options(s):
        for n in svc.nodes | svc.drpnodes:
            filers[n] = conf.get(s, 'filer')
    if 'filer@nodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@nodes')
    if 'filer@drpnodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@drpnodes')
    for o in conf.options(s):
        if 'filer@' not in o:
            continue
        (filer, node) = o.split('@')
        if node in ('nodes', 'drpnodes'):
            continue
        filers[node] = conf.get(s, o)
    if rcEnv.nodename not in filers:
        svc.log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    import resSyncNexenta
    r = resSyncNexenta.syncNexenta(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_netapp(svc, conf, s):
    kwargs = {}

    try:
        kwargs['path'] = conf_get_string_scope(svc, conf, s, 'path')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have path set" % s)
        return

    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have user set" % s)
        return

    filers = {}
    if 'filer' in conf.options(s):
        for n in svc.nodes | svc.drpnodes:
            filers[n] = conf.get(s, 'filer')
    if 'filer@nodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@nodes')
    if 'filer@drpnodes' in conf.options(s):
        for n in svc.nodes:
            filers[n] = conf.get(s, 'filer@drpnodes')
    for o in conf.options(s):
        if 'filer@' not in o:
            continue
        (filer, node) = o.split('@')
        if node in ('nodes', 'drpnodes'):
            continue
        filers[node] = conf.get(s, o)
    if rcEnv.nodename not in filers:
        svc.log.error("config file section %s must have filer@%s set" %(s, rcEnv.nodename))

    kwargs['filers'] = filers
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    import resSyncNetapp
    r = resSyncNetapp.syncNetapp(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_syncs_rsync(svc, conf, s):
    if s.startswith("sync#i"):
        # internal syncs have their own dedicated add function
        return

    options = []
    kwargs = {}
    kwargs['src'] = []
    try:
        _s = conf_get_string_scope(svc, conf, s, 'src')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have src set" % s)
        return

    for src in _s.split():
        kwargs['src'] += glob.glob(src)

    try:
        kwargs['dst'] = conf_get_string_scope(svc, conf, s, 'dst')
    except ex.OptNotFound:
        svc.log.error("config file section %s must have dst set" % s)
        return

    try:
        kwargs['dstfs'] = conf_get_string_scope(svc, conf, s, 'dstfs')
    except ex.OptNotFound:
        pass

    try:
        _s = conf_get_string_scope(svc, conf, s, 'options')
        options += _s.split()
    except ex.OptNotFound:
        pass

    try:
        # for backward compat (use options keyword now)
        _s = conf_get_string_scope(svc, conf, s, 'exclude')
        options += _s.split()
    except ex.OptNotFound:
        pass

    kwargs['options'] = options

    try:
        kwargs['snap'] = conf_get_boolean_scope(svc, conf, s, 'snap')
    except ex.OptNotFound:
        pass

    try:
        _s = conf_get_string_scope(svc, conf, s, 'target')
        target = _s.split()
    except ex.OptNotFound:
        target = []

    try:
        kwargs['bwlimit'] = conf_get_int_scope(svc, conf, s, 'bwlimit')
    except ex.OptNotFound:
        pass

    targethash = {}
    if 'nodes' in target: targethash['nodes'] = svc.nodes
    if 'drpnodes' in target: targethash['drpnodes'] = svc.drpnodes
    kwargs['target'] = targethash
    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs.update(get_sync_args(conf, s, svc))

    r = resSyncRsync.Rsync(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    svc += r

def add_task(svc, conf, s):
    kwargs = {}

    try:
        kwargs['command'] = conf_get_string_scope(svc, conf, s, 'command')
    except ex.OptNotFound:
        svc.log.error("'command' is not defined in config file section %s"%s)
        return

    try:
        kwargs['user'] = conf_get_string_scope(svc, conf, s, 'user')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    import resTask
    r = resTask.Task(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r

def add_app(svc, conf, s):
    resApp = ximport('resApp')
    kwargs = {}

    try:
        kwargs['script'] = conf_get_string_scope(svc, conf, s, 'script')
    except ex.OptNotFound:
        svc.log.error("'script' is not defined in config file section %s"%s)
        return

    try:
        kwargs['start'] = conf_get_int_scope(svc, conf, s, 'start')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'start'))
        return

    try:
        kwargs['stop'] = conf_get_int_scope(svc, conf, s, 'stop')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'stop'))
        return

    try:
        kwargs['check'] = conf_get_int_scope(svc, conf, s, 'check')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'check'))
        return

    try:
        kwargs['info'] = conf_get_int_scope(svc, conf, s, 'info')
    except ex.OptNotFound:
        pass
    except:
        svc.log.error("config file section %s param %s must be an integer" % (s, 'info'))
        return

    try:
        kwargs['timeout'] = conf_get_int_scope(svc, conf, s, 'timeout')
    except ex.OptNotFound:
        pass

    kwargs['rid'] = s
    kwargs['subset'] = get_subset(conf, s, svc)
    kwargs['tags'] = get_tags(conf, s, svc)
    kwargs['always_on'] = always_on_nodes_set(svc, conf, s)
    kwargs['disabled'] = get_disabled(conf, s, svc)
    kwargs['optional'] = get_optional(conf, s, svc)
    kwargs['monitor'] = get_monitor(conf, s, svc)
    kwargs['restart'] = get_restart(conf, s, svc)

    r = resApp.App(**kwargs)
    add_triggers_and_requires(svc, r, conf, s)
    r.pg_settings = get_pg_settings(svc, s)
    svc += r

def add_apps_sysv(svc, conf):
    """
    SysV-like launchers support.
    Translate to app#n resources.
    """
    resApp = ximport('resApp')

    s = "app"
    try:
        initd = conf_get_string_scope(svc, conf, s, 'dir')
    except ex.OptNotFound:
        initd = svc.initd

    disabled = get_disabled(conf, s, svc)
    #optional = get_optional(conf, s, svc)
    monitor = get_monitor(conf, s, svc)
    restart = get_restart(conf, s, svc)

    allocated = []

    def get_next_rid():
        rid_index = 0
        _rid = "app#%d" % rid_index
        while _rid in list(svc.resources_by_id.keys()) + allocated:
            rid_index += 1
            _rid = "app#%d" % rid_index
        allocated.append(_rid)
        return _rid

    def init_app(script):
        d = {
          'script': script,
          'rid': get_next_rid(),
          'info': 50,
          'optional': True,
          'disabled': disabled,
          'monitor': monitor,
          'restart': restart,
        }
        return d

    def find_app(script):
        for r in svc.get_resources("app", discard_disabled=False):
            if r.script.startswith(os.sep):
                rscript = r.script
            else:
                rscript = os.path.join(initd, r.script)
            rscript = os.path.realpath(rscript)
            if rscript == script:
                return r
        return

    def get_seq(s):
        s = s[1:]
        i = 0
        while s[i].isdigit() or i == len(s):
            i += 1
        s = int(s[:i])
        return s

    h = {}
    for f in glob.glob(os.path.join(initd, 'S[0-9]*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['start'] = get_seq(os.path.basename(f))

    for f in glob.glob(os.path.join(initd, 'K[0-9]*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['stop'] = get_seq(os.path.basename(f))

    for f in glob.glob(os.path.join(initd, 'C[0-9]*')):
        script = os.path.realpath(f)
        if find_app(script) is not None:
            continue
        if script not in h:
            h[script] = init_app(script)
        h[script]['check'] = get_seq(os.path.basename(f))

    if "app" not in svc.type2resSets:
        svc += resApp.RsetApps("app")

    for script, kwargs in h.items():
        r = resApp.App(**kwargs)
        svc += r

def get_pg_settings(svc, s):
    d = {}
    if s != "DEFAULT":
        conf = rcConfigParser.RawConfigParser()
        import codecs
        with codecs.open(svc.conf, "r", "utf8") as f:
            if sys.version_info[0] >= 3:
                conf.read_file(f)
            else:
                conf.readfp(f)
        import copy
        for o in copy.copy(conf.defaults()):
            conf.remove_option("DEFAULT", o)
    else:
        conf = svc.config
    try:
        d["cpus"] = conf_get_string_scope(svc, conf, s, "pg_cpus")
    except ex.OptNotFound:
        pass

    try:
        d["cpu_shares"] = conf_get_string_scope(svc, conf, s, "pg_cpu_shares")
    except ex.OptNotFound:
        pass

    try:
        d["cpu_quota"] = conf_get_string_scope(svc, conf, s, "pg_cpu_quota")
    except ex.OptNotFound:
        pass

    try:
        d["mems"] = conf_get_string_scope(svc, conf, s, "pg_mems")
    except ex.OptNotFound:
        pass

    try:
        d["mem_oom_control"] = conf_get_string_scope(svc, conf, s, "pg_mem_oom_control")
    except ex.OptNotFound:
        pass

    try:
        d["mem_limit"] = conf_get_string_scope(svc, conf, s, "pg_mem_limit")
    except ex.OptNotFound:
        pass

    try:
        d["mem_swappiness"] = conf_get_string_scope(svc, conf, s, "pg_mem_swappiness")
    except ex.OptNotFound:
        pass

    try:
        d["vmem_limit"] = conf_get_string_scope(svc, conf, s, "pg_vmem_limit")
    except ex.OptNotFound:
        pass

    try:
        d["blkio_weight"] = conf_get_string_scope(svc, conf, s, "pg_blkio_weight")
    except ex.OptNotFound:
        pass

    return d


def setup_logging(svcnames):
    """Setup logging to stream + logfile, and logfile rotation
    class Logger instance name: 'log'
    """
    global log
    max_svcname_len = 0

    # compute max svcname length to align logging stream output
    for svcname in svcnames:
        n = len(svcname)
        if n > max_svcname_len:
            max_svcname_len = n

    rcLogger.max_svcname_len = max_svcname_len
    log = rcLogger.initLogger('init')

def build(name, minimal=False, svcconf=None):
    """build(name) is in charge of Svc creation
    it return None if service Name is not managed by local node
    else it return new Svc instance
    """
    if svcconf is None:
        svcconf = os.path.join(rcEnv.pathetc, name) + '.conf'
    svcinitd = os.path.join(rcEnv.pathetc, name) + '.d'
    logfile = os.path.join(rcEnv.pathlog, name) + '.log'
    rcEnv.logfile = logfile

    #
    # node discovery is hidden in a separate module to
    # keep it separate from the framework stuff
    #
    discover_node()

    #
    # parse service configuration file
    # class RawConfigParser instance name: 'conf'
    #
    svcmode = "hosted"
    conf = None
    kwargs = {'svcname': name}
    if os.path.isfile(svcconf):
        conf = rcConfigParser.RawConfigParser()
        import codecs
        with codecs.open(svcconf, "r", "utf8") as f:
            if sys.version_info[0] >= 3:
                conf.read_file(f)
            else:
                conf.readfp(f)
        defaults = conf.defaults()
        if "mode" in defaults:
            svcmode = conf_get_string_scope({}, conf, 'DEFAULT', "mode")

        d_nodes = Storage()
        d_nodes.svcname = name

        if "encapnodes" in defaults:
            encapnodes = [n.lower() for n in conf_get_string_scope(d_nodes, conf, 'DEFAULT', "encapnodes").split() if n != ""]
        else:
            encapnodes = []
        d_nodes['encapnodes'] = set(encapnodes)

        if "nodes" in defaults:
            nodes = [n.lower() for n in conf_get_string_scope(d_nodes, conf, 'DEFAULT', "nodes").split() if n != ""]
        else:
            nodes = [rcEnv.nodename]
        d_nodes['nodes'] = set(nodes)

        if "drpnodes" in defaults:
            drpnodes = [n.lower() for n in conf_get_string_scope(d_nodes, conf, 'DEFAULT', "drpnodes").split() if n != ""]
        else:
            drpnodes = []

        if "drpnode" in defaults:
            drpnode = conf_get_string_scope(d_nodes, conf, 'DEFAULT', "drpnode").lower()
            if drpnode not in drpnodes and drpnode != "":
                drpnodes.append(drpnode)
        else:
            drpnode = ''
        d_nodes['drpnodes'] = set(drpnodes)

        if "flex_primary" in defaults:
            flex_primary = conf_get_string_scope(d_nodes, conf, 'DEFAULT', "flex_primary").lower()
        elif len(nodes) > 0:
            flex_primary = nodes[0]
        else:
            flex_primary = ''
        d_nodes['flex_primary'] = flex_primary

        if "drp_flex_primary" in defaults:
            drp_flex_primary = conf_get_string_scope(d_nodes, conf, 'DEFAULT', "drp_flex_primary").lower()
        elif len(drpnodes) > 0:
            drp_flex_primary = drpnodes[0]
        else:
            drp_flex_primary = ''
        d_nodes['drp_flex_primary'] = drp_flex_primary

        if "pkg_name" in defaults:
            if svcmode in ["sg", "rhcs", "vcs"]:
                kwargs['pkg_name'] = defaults["pkg_name"]

        kwargs['disabled'] = get_disabled(conf, "", "")


    #
    # dynamically import the module matching the service mode
    # and instanciate a service
    #
    mod , svc_class_name = svcmode_mod_name(svcmode)
    svcMod = __import__(mod)
    svc = getattr(svcMod, svc_class_name)(**kwargs)

    #
    # Store useful properties
    #
    svc.svcmode = svcmode
    svc.logfile = logfile
    svc.conf = svcconf
    svc.initd = svcinitd
    svc.config = conf

    if hasattr(svc, "builder"):
        builder_props = svc.builder_props
        svc.builder()
    else:
        builder_props = []

    #
    # Store and validate the service type
    #
    if "env" in defaults:
        svc.svc_env = defaults["env"]
    elif "service_type" in defaults:
        svc.svc_env = defaults["service_type"]

    #
    # Setup service properties from config file content
    #
    if "nodes" not in builder_props:
        svc.nodes = set(nodes)
    if "drpnodes" not in builder_props:
        svc.drpnodes = set(drpnodes)
    if "drpnode" not in builder_props:
        svc.drpnode = drpnode
    if "encapnodes" not in builder_props:
        svc.encapnodes = set(encapnodes)
    if "flex_primary" not in builder_props:
        svc.flex_primary = flex_primary
    if "drp_flex_primary" not in builder_props:
        svc.drp_flex_primary = drp_flex_primary

    try:
        svc.lock_timeout = conf_get_int_scope(svc, conf, 'DEFAULT', 'lock_timeout')
    except ex.OptNotFound:
        pass

    if conf.has_option('DEFAULT', 'disable'):
        svc.disabled = conf.getboolean("DEFAULT", "disable")
    else:
        svc.disabled = False

    if minimal:
        return svc

    try:
        svc.presnap_trigger = conf_get_string_scope(svc, conf, 'DEFAULT', 'presnap_trigger').split()
    except ex.OptNotFound:
        pass

    try:
        svc.postsnap_trigger = conf_get_string_scope(svc, conf, 'DEFAULT', 'postsnap_trigger').split()
    except ex.OptNotFound:
        pass

    try:
        svc.disable_rollback = not conf_get_boolean_scope(svc, conf, 'DEFAULT', "rollback")
    except ex.OptNotFound:
        pass

    if rcEnv.nodename in svc.encapnodes:
        svc.encap = True
    else:
        svc.encap = False

    #
    # amazon options
    #
    try:
        svc.aws = conf_get_string_scope(svc, conf, "DEFAULT", 'aws')
    except ex.OptNotFound:
        pass

    try:
        svc.aws_profile = conf_get_string_scope(svc, conf, "DEFAULT", 'aws_profile')
    except ex.OptNotFound:
        pass

    #
    # containerization options
    #
    try:
        svc.create_pg = conf_get_boolean_scope(svc, conf, "DEFAULT", 'create_pg')
    except ex.OptNotFound:
        pass
    svc.pg_settings = get_pg_settings(svc, "DEFAULT")

    try:
        svc.autostart_node = conf_get_string_scope(svc, conf, 'DEFAULT', 'autostart_node').split()
    except ex.OptNotFound:
        pass

    try:
        anti_affinity = conf_get_string_scope(svc, conf, 'DEFAULT', 'anti_affinity')
        svc.anti_affinity = set(conf_get_string_scope(svc, conf, 'DEFAULT', 'anti_affinity').split())
    except ex.OptNotFound:
        pass


    """ prune not managed service
    """
    if svc.svcmode not in rcEnv.vt_cloud and rcEnv.nodename not in svc.nodes | svc.drpnodes:
        raise ex.excInitError('service not managed by this node. hostname %s is not a member of DEFAULT.nodes, DEFAULT.drpnode nor DEFAULT.drpnodes' % rcEnv.nodename)

    if not hasattr(svc, "clustertype"):
        try:
            svc.clustertype = conf_get_string_scope(svc, conf, 'DEFAULT', 'cluster_type')
        except ex.OptNotFound:
            pass

    if 'flex' in svc.clustertype:
        svc.ha = True
    allowed_clustertype = ['failover', 'flex', 'autoflex']
    if svc.clustertype not in allowed_clustertype:
        raise ex.excInitError("invalid cluster type '%s'. allowed: %s"%(svc.clustertype, ', '.join(allowed_clustertype)))

    try:
        svc.flex_min_nodes = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_min_nodes')
    except ex.OptNotFound:
        svc.flex_min_nodes = 1
    if svc.flex_min_nodes < 0:
        raise ex.excInitError("invalid flex_min_nodes '%d' (<0)."%svc.flex_min_nodes)
    nb_nodes = len(svc.autostart_node)
    if nb_nodes == 0:
        nb_nodes = 1
    if nb_nodes > 0 and svc.flex_min_nodes > nb_nodes:
        raise ex.excInitError("invalid flex_min_nodes '%d' (>%d nb of nodes)."%(svc.flex_min_nodes, nb_nodes))

    try:
        svc.flex_max_nodes = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_max_nodes')
    except ex.OptNotFound:
        svc.flex_max_nodes = nb_nodes
    if svc.flex_max_nodes < 0:
        raise ex.excInitError("invalid flex_max_nodes '%d' (<0)."%svc.flex_max_nodes)
    if svc.flex_max_nodes < svc.flex_min_nodes:
        raise ex.excInitError("invalid flex_max_nodes '%d' (<flex_min_nodes)."%svc.flex_max_nodes)

    try:
        svc.flex_cpu_low_threshold = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_cpu_low_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_low_threshold = 10
    if svc.flex_cpu_low_threshold < 0:
        raise ex.excInitError("invalid flex_cpu_low_threshold '%d' (<0)."%svc.flex_cpu_low_threshold)
    if svc.flex_cpu_low_threshold > 100:
        raise ex.excInitError("invalid flex_cpu_low_threshold '%d' (>100)."%svc.flex_cpu_low_threshold)

    try:
        svc.flex_cpu_high_threshold = conf_get_int_scope(svc, conf, 'DEFAULT', 'flex_cpu_high_threshold')
    except ex.OptNotFound:
        svc.flex_cpu_high_threshold = 90
    if svc.flex_cpu_high_threshold < 0:
        raise ex.excInitError("invalid flex_cpu_high_threshold '%d' (<0)."%svc.flex_cpu_high_threshold)
    if svc.flex_cpu_high_threshold > 100:
        raise ex.excInitError("invalid flex_cpu_high_threshold '%d' (>100)."%svc.flex_cpu_high_threshold)

    try:
        svc.show_disabled = conf_get_boolean_scope(svc, conf, 'DEFAULT', 'show_disabled')
    except ex.OptNotFound:
        svc.show_disabled = True

    """ prune service whose service type does not match host mode
    """
    if svc.svc_env != 'PRD' and rcEnv.node_env == 'PRD':
        raise ex.excInitError('not allowed to run on this node (svc env=%s node env=%s)' % (svc.svc_env, rcEnv.node_env))

    try:
        svc.drp_type = conf_get_string_scope(svc, conf, 'DEFAULT', 'drp_type')
    except ex.OptNotFound:
        svc.drp_type = ''

    try:
        svc.comment = conf_get_string_scope(svc, conf, 'DEFAULT', 'comment')
    except ex.OptNotFound:
        svc.comment = ''

    try:
        svc.monitor_action = conf_get_string_scope(svc, conf, 'DEFAULT', "monitor_action")
    except ex.OptNotFound:
        pass

    try:
        svc.app = conf_get_string_scope(svc, conf, 'DEFAULT', "app")
    except ex.OptNotFound:
        svc.app = ''

    try:
        svc.drnoaction = conf_get_boolean_scope(svc, conf, 'DEFAULT', "drnoaction")
    except ex.OptNotFound:
        svc.drnoaction = False

    try:
        svc.bwlimit = conf_get_int_scope(svc, conf, 'DEFAULT', "bwlimit")
    except ex.OptNotFound:
        svc.bwlimit = None

    try:
        svc.clustername = conf_get_string_scope(svc, conf, 'DEFAULT', "cluster")
    except ex.OptNotFound:
        pass

    #
    # docker options
    #
    try:
        svc.docker_daemon_private = conf_get_boolean_scope(svc, conf, 'DEFAULT', 'docker_daemon_private')
    except ex.OptNotFound:
        svc.docker_daemon_private = True
    if rcEnv.sysname != "Linux":
        svc.docker_daemon_private = False

    try:
        svc.docker_exe = conf_get_string_scope(svc, conf, 'DEFAULT', 'docker_exe')
    except ex.OptNotFound:
        svc.docker_exe = None

    try:
        svc.docker_data_dir = conf_get_string_scope(svc, conf, 'DEFAULT', 'docker_data_dir')
    except ex.OptNotFound:
        svc.docker_data_dir = None

    try:
        svc.docker_daemon_args = conf_get_string_scope(svc, conf, 'DEFAULT', 'docker_daemon_args').split()
    except ex.OptNotFound:
        svc.docker_daemon_args = []

    if svc.docker_data_dir:
        from rcDocker import DockerLib
        if "--exec-opt" not in svc.docker_daemon_args and DockerLib(docker_exe=svc.docker_exe).docker_min_version("1.7"):
            svc.docker_daemon_args += ["--exec-opt", "native.cgroupdriver=cgroupfs"]

    #
    # instanciate resources
    #
    add_containers(svc, conf)
    add_resources('hb', svc, conf)
    add_resources('stonith', svc, conf)
    add_resources('ip', svc, conf)
    add_resources('disk', svc, conf)
    add_resources('fs', svc, conf)
    add_resources('share', svc, conf)
    add_resources('app', svc, conf)
    add_resources('task', svc, conf)

    # deprecated, folded into "disk"
    add_resources('vdisk', svc, conf)
    add_resources('vmdg', svc, conf)
    add_resources('loop', svc, conf)
    add_resources('drbd', svc, conf)
    add_resources('vg', svc, conf)
    add_resources('pool', svc, conf)

    # deprecated, folded into "app"
    add_apps_sysv(svc, conf)

    add_syncs(svc, conf)

    svc.post_build()
    return svc

def is_service(f):
    if os.name == 'nt':
        return True
    if os.path.realpath(f) != os.path.realpath(rcEnv.svcmgr):
        return False
    if not os.path.exists(f + '.conf'):
        return False
    return True

def list_services():
    if not os.path.exists(rcEnv.pathetc):
        print("create dir %s"%rcEnv.pathetc)
        os.makedirs(rcEnv.pathetc)

    s = glob.glob(os.path.join(rcEnv.pathetc, '*.conf'))
    s = list(map(lambda x: os.path.basename(x)[:-5], s))

    l = []
    for name in s:
        if len(s) == 0:
            continue
        if not is_service(os.path.join(rcEnv.pathetc, name)):
            continue
        l.append(name)
    return l

def build_services(status=None, svcnames=None,
                   onlyprimary=False, onlysecondary=False, minimal=False):
    """returns a list of all services of status matching the specified status.
    If no status is specified, returns all services
    """
    if svcnames is None:
        svcnames = []

    check_privs()

    errors = []
    services = {}
    if type(svcnames) == str:
        svcnames = [svcnames]

    if len(svcnames) == 0:
        svcnames = list_services()
    else:
        all_svcnames = list_services()
        missing_svcnames = sorted(list(set(svcnames) - set(all_svcnames)))
        for m in missing_svcnames:
            errors.append("%s: service does not exist" % m)
        svcnames = list(set(svcnames) & set(all_svcnames))

    setup_logging(svcnames)

    for name in svcnames:
        try:
            svc = build(name, minimal=minimal)
        except (ex.excError, ex.excInitError) as e:
            errors.append("%s: %s" % (name, str(e)))
            svclog = rcLogger.initLogger(name, handlers=["file", "syslog"])
            svclog.error(str(e))
            continue
        except ex.excAbortAction:
            continue
        except:
            import traceback
            traceback.print_exc()
            continue
        if status is not None and not svc.status() in status:
            continue
        if onlyprimary and rcEnv.nodename not in svc.autostart_node:
            continue
        if onlysecondary and rcEnv.nodename in svc.autostart_node:
            continue
        services[svc.svcname] = svc
    return [ s for n, s in sorted(services.items()) ], errors

def create(svcname, resources=[], interactive=False, provision=False):
    if not isinstance(svcname, list):
        print("ouch, svcname should be a list object", file=sys.stderr)
        return {"ret": 1}
    if len(svcname) != 1:
        print("you must specify a single service name with the 'create' action", file=sys.stderr)
        return {"ret": 1}
    svcname = svcname[0]
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return {"ret": 1}
    if svcname in list_services():
        print("service", svcname, "already exists", file=sys.stderr)
        return {"ret": 1}
    cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
    if os.path.exists(cf):
        print(cf, "already exists", file=sys.stderr)
        return {"ret": 1}
    try:
       f = open(cf, 'w')
    except:
        print("failed to open", cf, "for writing", file=sys.stderr)
        return {"ret": 1}

    defaults = {}
    sections = {}
    rtypes = {}

    import json
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print("can not parse resource:", r, file=sys.stderr)
            return {"ret": 1}
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            l = section.split('#')
            if len(l) != 2:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            rtype = l[1]
            if rtype in rtypes:
                rtypes[rtype] += 1
            else:
                rtypes[rtype] = 0
            del(d['rid'])
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        elif 'rtype' in d and d["rtype"] == "env":
            del(d["rtype"])
            if "env" in sections:
                sections["env"].update(d)
            else:
                sections["env"] = d
        elif 'rtype' in d and d["rtype"] != "DEFAULT":
            if 'rid' in d:
               del(d['rid'])
            rtype = d['rtype']
            if rtype in rtypes:
                section = '%s#%d'%(rtype, rtypes[rtype])
                rtypes[rtype] += 1
            else:
                section = '%s#0'%rtype
                rtypes[rtype] = 1
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
        else:
            if "rtype" in d:
                del(d["rtype"])
            defaults.update(d)

    from svcDict import KeyDict, MissKeyNoDefault, KeyInvalidValue
    try:
        keys = KeyDict(provision=provision)
        defaults.update(keys.update('DEFAULT', defaults))
        for section, d in sections.items():
            sections[section].update(keys.update(section, d))
    except (MissKeyNoDefault, KeyInvalidValue):
        if not interactive:
            return {"ret": 1}

    try:
        if interactive:
            defaults, sections = keys.form(defaults, sections)
    except KeyboardInterrupt:
        sys.stderr.write("Abort\n")
        return {"ret": 1}

    conf = rcConfigParser.RawConfigParser(defaults)
    for section, d in sections.items():
        conf.add_section(section)
        for key, val in d.items():
            if key == 'rtype':
                continue
            conf.set(section, key, val)

    conf.write(f)

    initdir = svcname+'.dir'
    svcinitdir = os.path.join(rcEnv.pathetc, initdir)
    if not os.path.exists(svcinitdir):
        os.makedirs(svcinitdir)
    fix_app_link(svcname)
    fix_exe_link(rcEnv.svcmgr, svcname)
    return {"ret": 0, "rid": sections.keys()}

def update(svcname, resources=[], interactive=False, provision=False):
    if not isinstance(svcname, list):
        print("ouch, svcname should be a list object", file=sys.stderr)
        return {"ret": 1}
    if len(svcname) != 1:
        print("you must specify a single service name with the 'update' action", file=sys.stderr)
        return {"ret": 1}
    svcname = svcname[0]
    if len(svcname) == 0:
        print("service name must not be empty", file=sys.stderr)
        return {"ret": 1}
    if svcname not in list_services():
        print("service", svcname, "does not exist", file=sys.stderr)
        return {"ret": 1}
    cf = os.path.join(rcEnv.pathetc, svcname+'.conf')
    sections = {}
    rtypes = {}
    conf = rcConfigParser.RawConfigParser()
    conf.read(cf)
    defaults = conf.defaults()
    for section in conf.sections():
        sections[section] = {}
        l = section.split('#')
        if len(l) == 2:
            rtype = l[0]
            ridx = l[1]
            if rtype not in rtypes:
                rtypes[rtype] = set([])
            rtypes[rtype].add(ridx)
        for o, v in conf.items(section):
            if o in defaults.keys() + ['rtype']:
                continue
            sections[section][o] = v

    from svcDict import KeyDict, MissKeyNoDefault, KeyInvalidValue
    keys = KeyDict(provision=provision)

    import json
    rid = []
    for r in resources:
        try:
            d = json.loads(r)
        except:
            print("can not parse resource:", r, file=sys.stderr)
            return {"ret": 1}
        is_resource = False
        if 'rid' in d:
            section = d['rid']
            if '#' not in section:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            l = section.split('#')
            if len(l) != 2:
                print(section, "must be formatted as 'rtype#n'", file=sys.stderr)
                return {"ret": 1}
            del(d['rid'])
            if section in sections:
                sections[section].update(d)
            else:
                sections[section] = d
            is_resource = True
        elif 'rtype' in d and d["rtype"] == "env":
            del(d["rtype"])
            if "env" in sections:
                sections["env"].update(d)
            else:
                sections["env"] = d
        elif 'rtype' in d and d["rtype"] != "DEFAULT":
            # new resource allocation, auto-allocated rid index
            if d['rtype'] in rtypes:
                ridx = 1
                while str(ridx) in rtypes[d['rtype']]:
                    ridx += 1
                ridx = str(ridx)
                rtypes[d['rtype']].add(ridx)
            else:
                ridx = '1'
                rtypes[d['rtype']] = set([ridx])
            section = '#'.join((d['rtype'], ridx))
            del(d['rtype'])
            sections[section] = d
            is_resource = True
        else:
            if "rtype" in d:
                del(d["rtype"])
            defaults.update(d)

        if is_resource:
            try:
                sections[section].update(keys.update(section, d))
            except (MissKeyNoDefault, KeyInvalidValue):
                if not interactive:
                    return {"ret": 1}
            rid.append(section)

    conf = rcConfigParser.RawConfigParser(defaults)
    for section, d in sections.items():
        conf.add_section(section)
        for key, val in d.items():
            conf.set(section, key, val)

    try:
        f = open(cf, 'w')
    except:
        print("failed to open", cf, "for writing", file=sys.stderr)
        return {"ret": 1}

    conf.write(f)

    fix_app_link(svcname)
    fix_exe_link(rcEnv.svcmgr, svcname)
    return {"ret": 0, "rid": rid}

def fix_app_link(svcname):
    os.chdir(rcEnv.pathetc)
    src = svcname+'.d'
    dst = svcname+'.dir'
    if os.name != 'posix':
        return
    try:
        p = os.readlink(src)
    except:
        if not os.path.exists(dst):
            os.makedirs(dst)
        os.symlink(dst, src)

def fix_exe_link(dst, src):
    if os.name != 'posix':
        return
    os.chdir(rcEnv.pathetc)
    try:
        p = os.readlink(src)
    except:
        os.symlink(dst, src)
        p = dst
    if p != dst:
        os.unlink(src)
        os.symlink(dst, src)

