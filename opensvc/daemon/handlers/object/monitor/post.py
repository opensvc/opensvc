import time

import daemon.handler
import daemon.shared as shared
import core.exceptions as ex
from utilities.storage import Storage
from utilities.naming import split_path, fmt_path, is_service

class Handler(daemon.handler.BaseHandler):
    """
    Set or unset properties of an object instance monitor.
    These properties are used by the monitor in the orchestration policies and object management by target state.
    """
    routes = (
        ("POST", "object_monitor"),
        (None, "set_service_monitor"),
    )
    prototype = [
        {
            "name": "path",
            "desc": "The object path.",
            "required": True,
            "format": "object_path",
        },
        {
            "name": "local_expect",
            "desc": "The expected object instance state on node. If 'started', the resource restart orchestration is active. A 'avail up' instance has local_expect set to 'started' automatically.",
            "required": False,
            "candidates": [
                "started",
                "unset",
            ],
            "format": "string",
            "strict_candidates": False,
        },
        {
            "name": "global_expect",
            "desc": "The expected object state clusterwide. This is the property used for object target state orchestration.",
            "required": False,
            "format": "string",
            "candidates": [
                "deleted",
                "purged",
                "provisioned",
                "unprovisioned",
                "thawed",
                "frozen",
                "started",
                "stopped",
                "aborted",
                "placed",
                "placed@.*",
                "shutdown",
                "scaled",
                "unset",
            ],
            "strict_candidates": False,
        },
        {
            "name": "status",
            "desc": "The current object instance monitor state on node. This is where the current running action, the last action failures are stored. The normal state is 'idle'.",
            "required": False,
            "format": "string",
        },
        {
            "name": "reset_retries",
            "desc": "If true, reset the resources retry counter. This rearms the resource restart orchestration.",
            "required": False,
            "format": "boolean",
            "default": False,
        },
        {
            "name": "stonith",
            "desc": "If 'unset' unarm the stonith trigger on cluster split. Any other value arms the trigger.",
            "required": False,
            "format": "string",
        },
    ]
    access = "custom"

    def rbac(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        name, namespace, kind = split_path(options.path)
        role = "admin"
        operator = (
            # (local_expect, global_expect, reset_retries)
            (None, None, True),
            (None, "thawed", False),
            (None, "frozen", False),
            (None, "started", False),
            (None, "stopped", False),
            (None, "aborted", False),
            (None, "placed", False),
            (None, "shutdown", False),
        )
        _global_expect = options.global_expect.split("@")[0] if options.global_expect else options.global_expect
        if (options.local_expect, _global_expect, options.reset_retries) in operator:
            role = "operator"
        thr.rbac_requires(roles=[role], namespaces=[namespace], **kwargs)

    def action(self, nodename, thr=None, **kwargs):
        options = self.parse_options(kwargs)
        paths = set([options.path])
        if options.global_expect != "scaled":
            paths |= self.get_service_slaves(options.path, thr=thr)
        errors = []
        info = []
        data = {"data": {}}
        for path in paths:
            try:
                self.validate_global_expect(path, options.global_expect, thr=thr)
                new_ge = self.validate_destination_node(path, options.global_expect, thr=thr)
            except ex.AbortAction as exc:
                info.append(str(exc))
            except ex.Error as exc:
                errors.append(str(exc))
            else:
                if new_ge:
                    options.global_expect = new_ge
                if options.global_expect:
                    data["data"]["global_expect"] = options.global_expect
                info.append("%s target state set to %s" % (path, options.global_expect))
                thr.set_smon(
                    path, status=options.status,
                    local_expect=options.local_expect,
                    global_expect=options.global_expect,
                    reset_retries=options.reset_retries,
                    stonith=options.stonith,
                )
        data["status"] = len(errors)
        if info:
            data["info"] = info
        if errors:
            data["error"] = errors
        return data

    def get_service_slaves(self, path, slaves=None, thr=None):
        """
        Recursive lookup of object slaves.
        """
        if slaves is None:
            slaves = set()
        _, namespace, _ = split_path(path)

        def set_ns(path, parent_ns):
            name, _namespace, kind = split_path(path)
            if _namespace:
                return path
            else:
                return fmt_path(name, parent_ns, kind)

        for nodename, data in thr.iter_service_instances(path):
            slaves.add(path)
            new_slaves = set(data.get("slaves", [])) | set(data.get("scaler_slaves", []))
            new_slaves = set([set_ns(slave, namespace) for slave in new_slaves])
            new_slaves -= slaves
            for slave in new_slaves:
                slaves |= self.get_service_slaves(slave, slaves, thr=thr)
        return slaves

    def validate_global_expect(self, path, global_expect, thr=None):
        if global_expect is None:
            return
        if global_expect in ("frozen", "aborted", "provisioned"):
            # allow provision target state on just-created service
            return

        # wait for object to appear
        for i in range(5):
            instances = thr.get_service_instances(path)
            if instances:
                break
            if not is_service(path):
                break
            time.sleep(1)
        if not instances:
            raise ex.Error("object does not exist")

        ges = set()
        for nodename, _data in instances.items():
            smon = _data.get("monitor", {})
            ge = smon.get("global_expect")
            ges.add(ge)
            if global_expect == ge:
                continue
            status = smon.get("status", "unknown")
            if status == "tocing" and global_expect == "placed":
                # Allow the "toc" action with the "switch" monitor_action
                # to change status from "tocing" to "start failed".
                pass
            elif status != "idle" and "failed" not in status and "wait" not in status:
                raise ex.Error("%s instance on node %s in %s state"
                                  "" % (path, nodename, status))

        if ges == set([global_expect]):
            raise ex.AbortAction("%s is already targeting %s" % (path, global_expect))

        if global_expect not in ("started", "stopped"):
            return
        agg = thr.get_service_agg(path)
        if global_expect == "started" and agg.avail == "up":
            raise ex.AbortAction("%s is already started" % path)
        elif global_expect == "stopped" and agg.avail in ("down", "stdby down", "stdby up"):
            raise ex.AbortAction("%s is already stopped" % path)
        if agg.avail in ("n/a", "undef"):
            raise ex.AbortAction()

    def validate_destination_node(self, path, global_expect, thr=None):
        """
        For a placed@<dst> <global_expect> (move action) on <path>,

        Raise an Error if
        * the object <path> does not exist
        * the object <path> topology is failover and more than 1
          destination node was specified
        * the specified destination is not a object candidate node
        * no destination node specified
        * an empty destination node is specified in a list of destination
          nodes

        Raise an AbortAction if
        * the avail status of the instance on the destination node is up
        """
        if global_expect is None:
            return
        try:
            global_expect, destination_nodes = global_expect.split("@", 1)
        except ValueError:
            return
        if global_expect != "placed":
            return
        instances = thr.get_service_instances(path)
        if not instances:
            raise ex.Error("object does not exist")
        if destination_nodes == "<peer>":
            instance = list(instances.values())[0]
            if instance.get("topology") == "flex":
                raise ex.Error("no destination node specified")
            else:
                nodes = [node for node, inst in instances.items() \
                              if inst.get("avail") not in ("up", "warn", "n/a") and \
                              inst.get("monitor", {}).get("status") != "started"]
                count = len(nodes)
                if count == 0:
                    raise ex.Error("no candidate destination node")
                svc = thr.get_service(path)
                return "placed@%s" % thr.placement_ranks(svc, nodes)[0]
        else:
            destination_nodes = destination_nodes.split(",")
            count = len(destination_nodes)
            if count == 0:
                raise ex.Error("no destination node specified")
            instance = list(instances.values())[0]
            if count > 1 and instance.get("topology") == "failover":
                raise ex.Error("only one destination node can be specified for "
                                  "a failover service")
            for destination_node in destination_nodes:
                if not destination_node:
                    raise ex.Error("empty destination node")
                if destination_node not in instances:
                    raise ex.Error("destination node %s has no %s instance" % \
                                      (destination_node, path))
                instance = instances[destination_node]
                if instance["avail"] == "up":
                    raise ex.AbortAction("instance on destination node %s is "
                                            "already up" % destination_node)

