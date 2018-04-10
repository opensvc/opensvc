"""
Listener Thread
"""
import os
import sys
import socket
import logging
import threading
import time
import shutil
import json
import re
import hashlib

import osvcd_shared as shared
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import lazy, bdecode
from comm import Crypt

PTR_SUFFIX = ".in-addr.arpa."
PTR_SUFFIX_LEN = 14
PTR6_SUFFIX = ".ip6.arpa."
PTR6_SUFFIX_LEN = 10

if sys.version_info[0] < 3:
    MAKEFILE_KWARGS = {"bufsize": 0}
else:
    MAKEFILE_KWARGS = {"buffering": None}

class Dns(shared.OsvcThread, Crypt):
    sock_tmo = 1.0

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.dns")
        if not os.path.exists(rcEnv.paths.dnsuxsockd):
            os.makedirs(rcEnv.paths.dnsuxsockd)
        try:
            if os.path.isdir(rcEnv.paths.dnsuxsock):
                shutil.rmtree(rcEnv.paths.dnsuxsock)
            else:
                os.unlink(rcEnv.paths.dnsuxsock)
        except Exception:
            pass
        try:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.bind(rcEnv.paths.dnsuxsock)
            self.sock.listen(1)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s error: %s", rcEnv.paths.dnsuxsock, exc)
            return

        self.log.info("listening on %s", rcEnv.paths.dnsuxsock)

        self.suffix = ".%s." % self.cluster_name
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
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                self.log.info("stop event received (%d handler threads to join)", len(self.threads))
                self.join_threads()
                self.sock.close()
                sys.exit(0)

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
        thr = threading.Thread(target=self.handle_client, args=(conn,))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, conn):
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

            result = self.router(data)
            if result is not None:
                message = json.dumps(result) + "\n"
                cw.write(message)
                cw.flush()
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
        if kind == "PRESIGNED":
            return ["1"]
        return ["0"]

    def action_lookup(self, parameters):
        qtype = parameters.get("qtype").upper()
        qname = parameters.get("qname").lower()
        #zone_id = parameters.get("zone-id")

        if qtype == "SOA":
            return self.soa_record(parameters)
        if qtype == "A":
            return self.a_record(parameters)
        if qtype == "SRV":
            return self.srv_record(parameters)
        if qtype == "TXT":
            return self.txt_record(parameters)
        if qtype == "PTR":
            return self.ptr_record(parameters)
        if qtype == "CNAME":
            return self.cname_record(parameters)
        if qtype == "ANY":
            if PTR_SUFFIX in qname:
                return self.ptr_record(parameters)
            return self.a_record(parameters) + \
                   self.srv_record(parameters) + \
                   self.txt_record(parameters) + \
                   self.cname_record(parameters)
        return []

    def remove_suffix(self, qname):
        return qname[:-self.suffix_len]

    @lazy
    def origin(self):
        return "dns"+self.suffix

    @lazy
    def contact(self):
        return "contact@opensvc.com"

    def soa_record(self, parameters):
        qname = parameters.get("qname").lower()
        if qname.endswith(PTR_SUFFIX):
            if qname not in self.soa_records_rev():
                return []
        elif qname.endswith(self.suffix):
            if qname not in self.soa_records():
                return []
        else:
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
        } for name in self.svc_ptr_record(qname)]

    def a_record(self, parameters):
        qname = parameters.get("qname").lower()
        if not qname.endswith(self.suffix):
            return []
        return [{
            "qtype": "A",
            "qname": qname,
            "content": addr,
            "ttl": 60
        } for addr in self.a_records().get(qname, [])]

    def svc_ptr_record(self, qname):
        if not qname.endswith(PTR_SUFFIX):
            return []
        names = []
        ref = ".".join(reversed(qname[:-PTR_SUFFIX_LEN].split(".")))
        for nodename, node in shared.CLUSTER_DATA.items():
            status = node.get("services", {}).get("status", {})
            for svcname, svc in status.items():
                app = svc.get("app", "default").lower()
                for rid, resource in status[svcname].get("resources", {}).items():
                    addr = resource.get("info", {}).get("ipaddr")
                    if addr is None:
                        continue
                    if addr != ref:
                        continue
                    if "hostname" in resource:
                        names.append("%s.%s.%s.svc.%s." % (hostname, svcname, app, self.cluster_name))
                    names.append("%s.%s.svc.%s." % (svcname, app, self.cluster_name))
        return names

    def soa_records(self):
        names = set([self.suffix])
        for nodename, node in shared.CLUSTER_DATA.items():
            status = node.get("services", {}).get("status", {})
            for svcname, svc in status.items():
                scaler_slave = svc.get("scaler_slave")
                if scaler_slave:
                    continue
                app = svc.get("app", "default").lower()
                names.add("%s.%s.svc.%s." % (svcname, app, self.cluster_name))
                names.add("%s.svc.%s." % (app, self.cluster_name))
        return names

    @staticmethod
    def unique_name(addr):
        return hashlib.sha1(addr.encode("ascii")).hexdigest()

    def a_records(self):
        names = {}
        for nodename, node in shared.CLUSTER_DATA.items():
            status = node.get("services", {}).get("status", {})
            for svcname, svc in status.items():
                app = svc.get("app", "default").lower()
                scaler_slave = svc.get("scaler_slave")
                if scaler_slave:
                    _svcname = svcname[svcname.index(".")+1:]
                else:
                    _svcname = svcname
                qname = "%s.%s.svc.%s." % (_svcname, app, self.cluster_name)
                if qname not in names:
                    names[qname] = set()
                for rid, resource in status[svcname].get("resources", {}).items():
                    addr = resource.get("info", {}).get("ipaddr")
                    if addr is None:
                        continue
                    hostname = resource.get("info", {}).get("hostname")
                    names[qname].add(addr)
                    rname = self.unique_name(addr) + "." + qname
                    if rname not in names:
                        names[rname] = set()
                    names[rname].add(addr)
                    if hostname:
                        name = hostname + "." + qname
                        if name not in names:
                            names[name] = set()
                        names[name].add(addr)
        return names

    def srv_records(self):
        names = {}
        for nodename, node in shared.CLUSTER_DATA.items():
            status = node.get("services", {}).get("status", {})
            weight = node.get("stats", {}).get("score", 10)
            for svcname, svc in status.items():
                app = svc.get("app", "default").lower()
                scaler_slave = svc.get("scaler_slave")
                if scaler_slave:
                    _svcname = svcname[svcname.index(".")+1:]
                else:
                    _svcname = svcname
                for rid, resource in status[svcname].get("resources", {}).items():
                    addr = resource.get("info", {}).get("ipaddr")
                    if addr is None:
                        continue
                    for expose in resource.get("info", {}).get("expose", []):
                        try:
                            port, proto = re.split("[/-]", expose.split(":")[0])
                            port = int(port)
                        except Exception as exc:
                            continue
                        try:
                            serv = socket.getservbyport(port)
                        except socket.error as exc:
                            continue
                        qname = "_%s._%s.%s.%s.svc.%s." % (serv, proto, _svcname, app, self.cluster_name)
                        target = "%s.%s.%s.svc.%s." % (self.unique_name(addr), _svcname, app, self.cluster_name)
                        if qname not in names:
                            names[qname] = set()
                        content = "%(prio)d %(weight)d %(port)d %(target)s" % {
                            "prio": 0,
                            "weight": weight,
                            "port": port,
                            "target": target,
                        }
                        names[qname].add(content)
        return names

    def svc_ips(self):
        addrs = []
        for nodename, node in shared.CLUSTER_DATA.items():
            status = node.get("services", {}).get("status", {})
            for svcname, svc in status.items():
                for rid, resource in status[svcname].get("resources", {}).items():
                    addr = resource.get("info", {}).get("ipaddr")
                    if addr is None:
                        continue
                    addrs.append(addr)
        return addrs

    def soa_records_rev(self):
        addrs = []
        for addr in set(self.svc_ips()):
            addrs.append(".".join(reversed(addr.split(".")[:-1]))+PTR_SUFFIX)
        return addrs

