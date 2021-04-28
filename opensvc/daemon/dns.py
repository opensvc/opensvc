"""
Listener Thread
"""
import errno
import os
import sys
import socket
import logging
import threading
import shutil
import json
import re
import time

import foreign.six as six
import daemon.shared as shared
from env import Env
from utilities.net.ipaddress import ip_address
from utilities.storage import Storage
from utilities.naming import split_path
from utilities.string import bdecode
from utilities.lazy import lazy

PTR_SUFFIX = ".in-addr.arpa."
PTR6_SUFFIX = ".ip6.arpa."

if six.PY2:
    MAKEFILE_KWARGS = {"bufsize": 0}
else:
    MAKEFILE_KWARGS = {"buffering": None}

class Dns(shared.OsvcThread):
    name = "dns"
    sock_tmo = 1.0

    def run(self):
        self.set_tid()
        self.log = logging.LoggerAdapter(logging.getLogger(Env.nodename+".osvcd.dns"), {"node": Env.nodename, "component": self.name})
        self.wait_monitor()
        self.cache = {}
        if not os.path.exists(Env.paths.dnsuxsockd):
            os.makedirs(Env.paths.dnsuxsockd)
        try:
            if os.path.isdir(Env.paths.dnsuxsock):
                shutil.rmtree(Env.paths.dnsuxsock)
            else:
                os.unlink(Env.paths.dnsuxsock)
        except Exception:
            pass
        try:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.bind(Env.paths.dnsuxsock)
            self.sock.listen(1)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.alert("error", "bind %s error: %s", Env.paths.dnsuxsock, exc)
            return

        self.log.info("listening on %s", Env.paths.dnsuxsock)

        self.zone = "%s." % self.cluster_name.strip(".")
        self.suffix = ".%s" % self.zone
        self.suffix_len = len(self.suffix)
        self.soa_data = {
            "origin": self.origin,
            "contact": self.contact,
            "serial": 1,
            "refresh": 7200,
            "retry": 3600,
            "expire": 432000,
            "minimum": 86400,
        }
        self.soa_content = "%(origin)s %(contact)s %(serial)d %(refresh)d " \
                           "%(retry)d %(expire)d %(minimum)d" % self.soa_data

        self.stats = Storage({
            "sessions": Storage({
                "accepted": 0,
                "tx": 0,
                "rx": 0,
            }),
        })

        while True:
            try:
                self.do()
                self.update_status()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                self.log.debug("stop event received (%d handler threads to join)", len(self.threads))
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def wait_monitor(self):
        while True:
            nmon_status = self.node_data.get(["monitor", "status"], default="init")
            if nmon_status != "init":
                break
            time.sleep(0.2)

    def cache_key(self):
        data = self.get_gen(inc=False)
        key = []
        for node in self.cluster_nodes:
            try:
                key.append(data[node])
            except KeyError:
                continue
            except AttributeError:
                break
        return tuple(key)

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        if hasattr(self, "stats"):
            data["stats"] = self.stats
        return data

    def do(self):
        self.reload_config()
        self.janitor_procs()
        self.janitor_threads()

        try:
            conn, addr = self.sock.accept()
            #self.log.info("accept connection")
            self.stats.sessions.accepted += 1
        except socket.timeout:
            return
        try:
            thr = threading.Thread(target=self.handle_client, args=(conn,))
            thr.start()
            self.threads.append(thr)
        except RuntimeError as exc:
            self.log.warning(exc)
            conn.close()

    def handle_client(self, conn):
        # todo: change implementation to avoid socket.makefile with non blocking mode
        conn.settimeout(self.sock_tmo)
        cr = conn.makefile("r", **MAKEFILE_KWARGS)
        cw = conn.makefile("w", **MAKEFILE_KWARGS)
        try:
            self._handle_client(conn, cr, cw)
        except Exception as exc:
            self.log.exception(exc)
        finally:
            try:
                cr.close()
            except socket.error:
                pass
            try:
                cw.close()
            except socket.error:
                pass
            try:
                conn.close()
            except socket.error:
                pass
            sys.exit(0)

    def _handle_client(self, conn, cr, cw):
        chunks = []
        buff_size = 4096
        while True:
            try:
                data = cr.readline()
            except socket.timeout as exc:
                break
            except socket.error as exc:
                self.log.info("%s", exc)
                break
            if len(data) == 0:
                #self.log.info("no more data")
                break

            self.log.debug("received %s", data)

            try:
                data = bdecode(data)
                data = json.loads(data)
            except Exception as exc:
                self.log.error(exc)
                data = None

            if self.stopped():
                self.log.info("stop event received (handler thread)")
                break

            if data is None or not isinstance(data, dict):
                continue

            try:
                result = self.router(data)
            except Exception as exc:
                self.log.error("dns request: %s => handler error: %s", data, exc)
                return {"error": "unexpected backend error", "result": False}
            if result is not None:
                message = json.dumps(result) + "\n"
                try:
                    cw.write(message)
                    cw.flush()
                except socket.error as exc:
                    if exc.errno != errno.EPIPE:
                        raise
                    self.log.info("client died (broken pipe)")
                    break
                self.log.debug("replied %s", message)
                message_len = len(message)
                self.stats.sessions.tx += message_len

    #########################################################################
    #
    # Methods
    #
    #########################################################################
    def router(self, data):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        if not isinstance(data, dict):
            return {"error": "invalid data format", "result": False}
        if "method" not in data:
            return {"error": "method not specified", "result": False}
        fname = "action_"+data["method"]
        if not hasattr(self, fname):
            return {"error": "action not supported", "result": False}
        result = getattr(self, fname)(data.get("parameters", {}))
        if result == []:
            return False
        return {"result": result}

    def action_initialize(self, parameters):
        return True

    def action_getDomainMetadata(self, parameters):
        """
        {
            "method": "getDomainMetadata",
            "parameters": {
                "kind": "ALLOW-AXFR-FROM",
                "name": "svcdevops-front.default."
            }
        }
        """
        kind = parameters.get("kind")
        if kind == "ALLOW-AXFR-FROM":
            return ["0.0.0.0/0", "AUTO-NS"]
        return []

    def action_lookup(self, parameters):
        qtype = parameters.get("qtype").upper()
        qname = parameters.get("qname").lower()
        #zone_id = parameters.get("zone-id")

        if qtype == "SOA":
            return self.soa_record(parameters)
        if qtype == "A":
            return self.a_record(parameters)
        if qtype == "AAAA":
            return self.aaaa_record(parameters)
        if qtype == "SRV":
            return self.srv_record(parameters)
        if qtype == "TXT":
            return self.txt_record(parameters)
        if qtype == "PTR":
            return self.ptr_record(parameters)
        if qtype == "PTR6":
            return self.ptr6_record(parameters)
        if qtype == "CNAME":
            return self.cname_record(parameters)
        if qtype == "NS":
            return self.ns_record(parameters)
        if qtype == "ANY":
            if PTR_SUFFIX in qname:
                return self.ptr_record(parameters)
            if PTR6_SUFFIX in qname:
                return self.ptr6_record(parameters)
            if parameters["qname"].startswith("*."):
                return []
            return self.ns_record(parameters) + \
                   self.a_record(parameters) + \
                   self.aaaa_record(parameters) + \
                   self.srv_record(parameters) + \
                   self.txt_record(parameters) + \
                   self.cname_record(parameters)

            return self._action_list(qname)
        return []

    def action_list(self, parameters):
        zonename = parameters.get("zonename").lower()
        return self._action_list(zonename)

    def lookup_pattern(self, suffix):
        data = []
        for qname, contents in self.a_records().items():
            if not qname.endswith(suffix):
                continue
            for content in contents:
                data.append({
                    "qtype": "A",
                    "qname": qname,
                    "content": content,
                    "ttl": 60
                })
        return data

    def _action_list(self, suffix):
        """
        Empty suffix is what "dns dump" uses.
        """
        data = []
        if suffix:
            data += self.soa_record({"qname": suffix})
            if len(data) == 0:
                return data
            # don't include NS record in dump, as those depend on suffix
            data += self.zone_ns_records(suffix)
        else:
            data += self.zone_ns_records(self.zone)
        for qname, contents in self.a_records().items():
            if suffix and not qname.endswith(suffix):
                continue
            for content in contents:
                qtype = "AAAA" if ":" in content else "A"
                data.append({
                    "qtype": qtype,
                    "qname": qname,
                    "content": content,
                    "ttl": 60
                })
        for qname, contents in self.srv_records().items():
            if suffix and not qname.endswith(suffix):
                continue
            for content in contents:
                data.append({
                    "qtype": "SRV",
                    "qname": qname,
                    "content": content,
                    "ttl": 60
                })
        for qname, contents in self.ptr_records().items():
            if suffix and not qname.endswith(suffix):
                continue
            for content in contents:
                qtype = "PTR6" if ":" in qname else "PTR"
                data.append({
                    "qtype": "PTR",
                    "qname": qname,
                    "content": content,
                    "ttl": 60
                })
        return data

    def remove_suffix(self, qname):
        return qname[:-self.suffix_len]

    @lazy
    def origin(self):
        return "dns.%s" % self.zone

    @lazy
    def contact(self):
        return "contact@opensvc.com"

    def ns_record(self, parameters):
        qname = parameters.get("qname").lower()
        if qname != self.zone:
            return []
        return self.zone_ns_records(self.zone)

    def zone_ns_records(self, zonename):
        data = []
        for i, dns in enumerate(shared.NODE.dns):
            dns = "ns%d.%s" % (i, zonename)
            data.append({
                "qtype": "NS",
                "qname": zonename,
                "content": dns,
                "ttl": 3600
            })
        return data

    def soa_records(self):
        return [self.zone]

    def soa_records_rev(self):
        addrs = set([PTR_SUFFIX[1:]])
        for addr in set(self.svc_ips()):
            z1 = ".".join(reversed(addr.split(".")[:-1]))+PTR_SUFFIX
            z2 = ".".join(reversed(addr.split(".")[:-2]))+PTR_SUFFIX
            z3 = ".".join(reversed(addr.split(".")[:-3]))+PTR_SUFFIX
            addrs.add(z1)
            addrs.add(z2)
            addrs.add(z3)
        return addrs

    def soa_record(self, parameters):
        qname = parameters.get("qname").lower()
        if qname.endswith(PTR_SUFFIX):
            if qname not in self.soa_records_rev():
                return []
        elif qname != self.zone:
            return []

        data = [
            {
                "qtype": "SOA",
                "qname": qname,
                "content": self.soa_content,
                "ttl": 60,
                "domain_id": -1
            }
        ]
        return data

    def cname_record(self, parameters):
        return []

    def txt_record(self, parameters):
        return []

    def srv_record(self, parameters):
        qname = parameters.get("qname").lower()
        if not qname.endswith(self.suffix):
            return []
        return [{
            "qtype": "SRV",
            "qname": qname,
            "content": content,
            "ttl": 60
        } for content in self.srv_records().get(qname, [])]

    def ptr_record(self, parameters):
        qname = parameters.get("qname").lower()
        return [{
            "qtype": "PTR",
            "qname": qname,
            "content": name,
            "ttl": 60
        } for name in self.ptr_records().get(qname, []) if "." in name]

    def ptr6_record(self, parameters):
        qname = parameters.get("qname").lower()
        return [{
            "qtype": "PTR6",
            "qname": qname,
            "content": name,
            "ttl": 60
        } for name in self.ptr_records().get(qname, []) if ":" in name]

    def a_record(self, parameters):
        qname = parameters.get("qname").lower()
        if not qname.endswith(self.suffix):
            return []
        return [{
            "qtype": "A",
            "qname": qname,
            "content": addr,
            "ttl": 60
        } for addr in self.a_records().get(qname, []) if "." in addr]

    def aaaa_record(self, parameters):
        qname = parameters.get("qname").lower()
        if not qname.endswith(self.suffix):
            return []
        return [{
            "qtype": "AAAA",
            "qname": qname,
            "content": addr,
            "ttl": 60
        } for addr in self.a_records().get(qname, []) if ":" in addr]

    def ptr_records(self):
        data = self.get_cache("ptr")
        if data is not None:
            return data
        names = {}
        key = self.cache_key()
        for path, nodename, status in self.iter_services_instances():
            name, namespace, kind = split_path(path)
            if kind != "svc":
                continue
            if not namespace:
                namespace = "root"
            for rid, resource in status.get("resources", {}).items():
                addr = resource.get("info", {}).get("ipaddr")
                if addr is None:
                    continue
                qname = ip_address(addr).reverse_pointer
                if qname not in names:
                    names[qname] = []
                try:
                    hostname = resource.get("info", {}).get("hostname").split(".")[0].lower()
                except Exception:
                    hostname = None
                gen_name = "%s.%s.%s.%s." % (name, namespace, kind, self.cluster_name)
                gen_name = gen_name.lower()
                if hostname and hostname != name:
                    target = "%s.%s" % (hostname, gen_name)
                else:
                    target = "%s" % gen_name
                if target in names[qname]:
                    continue
                names[qname].append(target)
        self.set_cache(key, "ptr", names)
        return names

    def set_cache(self, key, kind, data):
        if key not in self.cache:
            self.cache = {}
        self.cache[key] = {kind: data}

    def get_cache(self, kind):
        key = self.cache_key()
        if key not in self.cache:
            self.cache = {}
            return
        if kind not in self.cache[key]:
            return
        return self.cache[key].get(kind)

    @staticmethod
    def unique_name(addr):
        return addr.replace(".", "-").replace(":", "-")

    def a_records(self):
        data = self.get_cache("a")
        if data is not None:
            return data
        names = {}
        key = self.cache_key()
        for path, nodename, status in self.iter_services_instances():
            name, namespace, kind = split_path(path)
            if kind != "svc":
                continue
            if namespace:
                namespace = namespace.lower()
            else:
                namespace = "root"
            scaler_slave = status.get("scaler_slave")
            if scaler_slave:
                _name = name[name.index(".")+1:]
            else:
                _name = name

            zone = "%s.%s.%s." % (namespace, kind, self.cluster_name)
            qname = "%s.%s" % (_name, zone)
            if qname not in names:
                names[qname] = set()

            local_zone = "%s.%s.%s.node.%s." % (namespace, kind, nodename, self.cluster_name)
            local_qname = "%s.%s" % (_name, local_zone)
            if local_qname not in names:
                names[local_qname] = set()

            for rid, resource in status.get("resources", {}).items():
                addr = resource.get("info", {}).get("ipaddr")
                if addr is None:
                    continue
                hostname = resource.get("info", {}).get("hostname")
                names[qname].add(addr)
                names[local_qname].add(addr)
                rname = self.unique_name(addr) + "." + qname
                if rname not in names:
                    names[rname] = set()
                names[rname].add(addr)
                if hostname:
                    name = hostname.split(".")[0] + "." + qname
                    if name not in names:
                        names[name] = set()
                    names[name].add(addr)
        names.update(self.dns_a_records())
        self.set_cache(key, "a", names)
        return names

    def dns_a_records(self):
        names = {}
        for i, ip in enumerate(shared.NODE.dns):
            dns = "ns%d.%s." % (i, self.cluster_name)
            names[dns] = set([ip])
        return names

    def srv_records(self):
        data = self.get_cache("srv")
        if data is not None:
            return data
        names = {}
        key = self.cache_key()
        for path, nodename, status in self.iter_services_instances():
            weight = self.daemon_status_data.get(["monitor", "nodes", nodename, "stats", "score"], default=10)
            name, namespace, kind = split_path(path)
            if kind != "svc":
                continue
            if namespace:
                namespace = namespace.lower()
            else:
                namespace = "root"
            scaler_slave = status.get("scaler_slave")
            if scaler_slave:
                _name = name[name.index(".")+1:]
            else:
                _name = name
            for rid, resource in status.get("resources", {}).items():
                addr = resource.get("info", {}).get("ipaddr")
                if addr is None:
                    continue
                for expose in resource.get("info", {}).get("expose", []):
                    if "#" in expose:
                        # expose data by reference
                        expose_data = status.get("resources", {}).get(expose, {}).get("info")

                        try:
                            port = expose_data["port"]
                            proto = expose_data["protocol"]
                        except (KeyError, TypeError):
                            continue
                    else:
                        # expose data inline
                        try:
                            port, proto = re.split("[/-]", expose.split(":")[0])
                            port = int(port)
                        except Exception as exc:
                            continue
                    qnames = set()
                    qnames.add("_%s._%s.%s.%s.%s.%s." % (str(port), proto, _name, namespace, kind, self.cluster_name))
                    try:
                        serv = socket.getservbyport(port)
                        qnames.add("_%s._%s.%s.%s.%s.%s." % (serv, proto, _name, namespace, kind, self.cluster_name))
                    except (socket.error, OSError) as exc:
                        # port/proto not found
                        pass
                    except Exception as exc:
                        self.log.warning("port %d resolution failed: %s", port, exc)
                    target = "%s.%s.%s.%s.%s." % (self.unique_name(addr), _name, namespace, kind, self.cluster_name)
                    content = "%(prio)d %(weight)d %(port)d %(target)s" % {
                        "prio": 0,
                        "weight": weight,
                        "port": port,
                        "target": target,
                    }
                    for qname in qnames:
                        if qname not in names:
                            names[qname] = set()
                        uend = " %d %s" % (port, target)
                        if any([True for c in names[qname] if c.endswith(uend)]):
                            # avoid multiple SRV entries pointing to the same ip:port
                            continue
                        names[qname].add(content)
        self.set_cache(key, "srv", names)
        return names

    def svc_ips(self):
        addrs = []
        for path, nodename, status in self.iter_services_instances():
            for rid, resource in status.get("resources", {}).items():
                addr = resource.get("info", {}).get("ipaddr")
                if addr is None:
                    continue
                addrs.append(addr)
        return addrs


