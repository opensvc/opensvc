from __future__ import print_function
import os
import sys
import optparse
import time
import datetime
import threading

#
# add project lib to path
#
prog = "om mon"

import core.exceptions as ex
import utilities.render.color
from core.node import Node
from foreign.six.moves import queue
from utilities.journaled_data import JournaledData
from utilities.render.cluster import format_cluster

CLEAREOL = "\x1b[K"
CLEAREOLNEW = "\x1b[K\n"
CLEAREOS = "\x1b[J"
CURSORHOME = "\x1b[H"

PATCH_Q = queue.Queue()

def setup_parser(node):
    __ver = prog + " version " + node.agent_version
    __usage = prog + \
        " [ OPTIONS ]\n" \
        "\n" \
        "Flags:\n" \
        "  O  up\n" \
        "  S  stdby up\n" \
        "  X  down\n" \
        "  s  stdby down\n" \
        "  !  warn\n" \
        "  P  unprovisioned\n" \
        "  *  frozen\n" \
        "  ^  leader node or service placement non-optimal"
    parser = optparse.OptionParser(version=__ver, usage=__usage)
    parser.add_option("--color", default="auto",
                      action="store", dest="color",
                      help="colorize output. possible values are : auto=guess based "
                           "on tty presence, always|yes=always colorize, never|no="
                           "never colorize")
    parser.add_option("--format", default=None,
                      action="store", dest="format",
                      help="Specify a data formatter. Possible values are compact or "
                           "matrix. The compact mode is best for large cluster. If "
                           "not specified, the cluster.default_mon_format value is "
                           "used as the default. If cluster.default_mon_format is not "
                           "set, the default is the matrix renderer."),
    parser.add_option("--server", default="", action="store", dest="server",
                      help="The server uri to send a request to. If not "
                           "specified the local node is targeted. Supported "
                           "schemes are https and raw. The default scheme is "
                           "https. The default port is 1214 for the raw "
                           "scheme, and 1215 for https. The uri can be a "
                           "fullpath to a listener socket. In this case, "
                           "the scheme is deduced from the socket. "
                           "Examples: raw://1.2.3.4:1214, "
                           "https://relay.opensvc.com, "
                           "/var/lib/opensvc/lsnr/h2.sock."),
    parser.add_option("--node", action="store", dest="node",
                      help="The nodes to display information for. If not specified, "
                           "all nodes are displayed."),
    parser.add_option("--namespace", action="store", dest="namespace",
                      help="The namespace to switch to for the action. "
                           "Namespaces are cluster partitions. A default "
                           "namespace can be set for the session setting the "
                           "OSVC_NAMESPACE environment variable."),
    parser.add_option("--stats", default=False,
                      action="store_true", dest="stats",
                      help="refresh the information every --interval.")
    parser.add_option("-w", "--watch", default=False,
                      action="store_true", dest="watch",
                      help="refresh the information every --interval.")
    parser.add_option("-i", "--interval", default=0, action="store",
                      dest="interval", type="int",
                      help="with --watch, set the refresh interval. defaults "
                           "to 0, to refresh on event only.")
    parser.add_option("--sections", action="store",
                      dest="sections",
                      help="the comma-separated list of sections to display. "
                           "if not set, all sections are displayed. sections "
                           "names are: threads,arbitrators,nodes,services.")
    parser.add_option(
        "-s", "--service", default="*",
        action="store", dest="parm_svcs",
        help="An object selector expression ``[!]<expr>[<sep>[!]<expr>]`` where:\n\n"
             "- ``!`` is the expression negation operator\n\n"
             "- ``<sep>`` can be:\n\n"
             "  - ``,`` OR expressions\n\n"
             "  - ``+`` AND expressions\n\n"
             "- ``<expr>`` can be:\n\n"
             "  - a shell glob on service names\n\n"
             "  - ``<param><op><value>`` where:\n\n"
             "    - ``<param>`` can be:\n\n"
             "      - ``<rid>:``\n\n"
             "      - ``<group>:``\n\n"
             "      - ``<rid>.<key>``\n\n"
             "      - ``<group>.<key>``\n\n"
             "    - ``<op>`` can be:\n\n"
             "      - ``<``  ``>``  ``<=``  ``>=``  ``=``\n\n"
             "      - ``~`` with regexp value\n\n"
             "Examples:\n\n"
             "- ``*dns,ha*+app.timeout>1``\n\n"
             "- ``ip:+task:``\n\n"
             "- ``!*excluded``\n\n"
             "Note:\n\n"
             "- ``!`` usage requires single quoting the expression to prevent "
             "shell history expansion")
    return parser

def events(node, nodename, selector, namespace):
    global PATCH_Q
    for msg in node.daemon_events(nodename, selector=selector, namespace=namespace):
        if msg.get("kind") != "patch":
            continue
        PATCH_Q.put(msg)

def start_events_thread(node, nodename, selector, namespace):
    thr = threading.Thread(target=events, args=(node, nodename, selector, namespace))
    thr.daemon = True
    thr.start()
    return thr

def get_stats(options, node, paths):
    try:
        if options.stats:
            return node._daemon_stats(paths=paths, server=options.server, node=options.node)
        else:
            return None
    except Exception:
        return None

def nodes_info_from_cluster_data(data):
    info = {}
    for node in data.get("cluster", {}).get("nodes", []):
        info[node] = {}
    for node, _data in data.get("monitor", {}).get("nodes", {}).items():
        info[node] = {
            "labels": _data.get("labels", {}),
            "targets": _data.get("targets", {}),
        }
    return info

def _main(node, argv=None):
    parser = setup_parser(node)
    (options, args) = parser.parse_args(argv)
    node.check_privs(argv)
    svcmon(node, options)

def svcmon(node, options=None):
    global PATCH_Q
    utilities.render.color.use_color = options.color
    if not options.node:
        options.node = "*"
    chars = 0
    last_refresh = 0
    last_patch_id = None

    namespace = options.namespace if options.namespace else os.environ.get("OSVC_NAMESPACE")

    if options.stats and not options.interval:
        options.interval = 3
    if options.interval:
        options.watch = True
    nodes = []

    node.options.update({
        "color": options.color,
    })

    if options.parm_svcs is None:
        kind = os.environ.get("OSVC_KIND", "svc")
        options.parm_svcs = "*/%s/*" % kind
    dataset = JournaledData()
    result = node._daemon_status(server=options.server, selector=options.parm_svcs, namespace=namespace)
    if result is None or result.get("status", 0) != 0:
        status, error, info = node.parse_result(result)
        raise ex.Error(error)
    dataset.set([], result)
    status_data = dataset.get()
    nodes_info = nodes_info_from_cluster_data(status_data)
    expanded_svcs = [p for p in status_data.get("monitor", {}).get("services", {})]
    if options.format is None:
        options.format = node.oget("cluster", "default_mon_format")
    if options.format == "compact":
        nodes = []
    elif not nodes:
        nodes = node.nodes_selector(options.node, data=nodes_info)

    if options.watch:
        start_events_thread(node, options.server, options.parm_svcs, namespace)
        preamble = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats_data = get_stats(options, node, expanded_svcs)
        prev_stats_data = None
        outs = format_cluster(paths=expanded_svcs, node=nodes,
                              data=status_data, sections=options.sections,
                              selector=options.parm_svcs,
                              namespace=namespace)

        if outs is not None:
            print(CURSORHOME+preamble+CLEAREOLNEW+CLEAREOL)
            print(CLEAREOLNEW.join(outs.split("\n"))+CLEAREOS)

        while True:
            now = time.time()
            try:
                patch = PATCH_Q.get(False, 0.5)
                #for change in patch["data"]:
                #    print(change)
            except Exception as exc:
                # queue empty
                patch = None

            if patch:
                if last_patch_id and patch["id"] != last_patch_id + 1:
                    try:
                        dataset.set([], node._daemon_status(server=options.server, selector=options.parm_svcs, namespace=namespace))
                        status_data = dataset.get()
                        last_patch_id = patch["id"]
                    except Exception:
                        # seen on solaris under high load: decode_msg() raising on invalid json
                        pass
                else:
                    try:
                        dataset.patch([], patch["data"])
                        status_data = dataset.get()
                        last_patch_id = patch["id"]
                    except Exception as exc:
                        try:
                            dataset.set([], node._daemon_status(server=options.server, selector=options.parm_svcs, namespace=namespace))
                            status_data = dataset.get()
                            last_patch_id = patch["id"]
                        except Exception:
                            # seen on solaris under high load: decode_msg() raising on invalid json
                            pass

            stats_changed = options.interval and now - last_refresh >= options.interval
            if not patch and not stats_changed:
                time.sleep(0.2)
                continue
            if patch:
                if status_data is None:
                    # can happen when the secret is being reset on daemon join
                    time.sleep(0.2)
                    continue
                expanded_svcs = [p for p in status_data.get("monitor", {}).get("services", {})]
                nodes_info = nodes_info_from_cluster_data(status_data)
                if options.format != "compact":
                    nodes = node.nodes_selector(options.node, data=nodes_info)
            if stats_changed:
                prev_stats_data = stats_data
                stats_data = get_stats(options, node, expanded_svcs)
            if chars == 0:
                print(CURSORHOME+CLEAREOS)
                chars = 1
            preamble = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            outs = format_cluster(
                paths=expanded_svcs,
                node=nodes,
                data=status_data,
                prev_stats_data=prev_stats_data,
                stats_data=stats_data,
                sections=options.sections,
                selector=options.parm_svcs,
                namespace=namespace,
            )
            if outs is not None:
                print(CURSORHOME+preamble+CLEAREOLNEW+CLEAREOL)
                print(CLEAREOLNEW.join(outs.split("\n"))+CLEAREOS)
            # min delay
            last_refresh = now
            time.sleep(0.2)
    else:
        outs = format_cluster(paths=expanded_svcs, node=nodes,
                              data=status_data, sections=options.sections,
                              selector=options.parm_svcs,
                              namespace=namespace)
        if outs is not None:
            print(outs)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    else:
        argv.insert(0, __file__)

    try:
        node = Node()
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1

    try:
        _main(node, argv)
        return 0
    except ex.Error as e:
        if str(e):
            print(e, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("Keybord Interrupt\n")
        return 1
    finally:
        node.close()

    return 0

if __name__ == "__main__":
    ret = main()
    sys.exit(ret)

