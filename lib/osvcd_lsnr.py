"""
Listener Thread
"""
import os
import sys
import socket
import logging
import threading
import codecs
import time
from subprocess import Popen, PIPE

import osvcd_shared as shared
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import bdecode, drop_option
from converters import convert_size
from comm import Crypt

class Listener(shared.OsvcThread, Crypt):
    sock_tmo = 1.0

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener")
        try:
            self.port = self.config.getint("listener", "port")
        except Exception:
            self.port = rcEnv.listener_port
        try:
            self.addr = self.config.get("listener", "addr")
        except Exception:
            self.addr = "0.0.0.0"

        try:
            addrinfo = socket.getaddrinfo(self.addr, None)[0]
            self.addr = addrinfo[4][0]
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.addr, self.port))
            self.sock.listen(5)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.addr, self.port, exc)
            return

        self.log.info("listening on %s:%s", self.addr, self.port)

        self.stats = Storage({
            "sessions": Storage({
                "accepted": 0,
                "auth_validated": 0,
                "tx": 0,
                "rx": 0,
                "clients": Storage({
                })
            }),
        })

        while True:
            try:
                self.do()
            except Exception as exc:
                self.log.exception(exc)
            if self.stopped():
                self.join_threads()
                self.sock.close()
                sys.exit(0)

    def status(self, **kwargs):
        data = shared.OsvcThread.status(self, **kwargs)
        data["stats"] = self.stats
        data["config"] = {
            "port": self.port,
            "addr": self.addr,
        }
        return data

    def do(self):
        self.reload_config()
        self.janitor_procs()
        self.janitor_threads()

        try:
            conn, addr = self.sock.accept()
            self.stats.sessions.accepted += 1
            if addr[0] not in self.stats.sessions.clients:
                self.stats.sessions.clients[addr[0]] = Storage({
                    "accepted": 0,
                    "auth_validated": 0,
                    "tx": 0,
                    "rx": 0,
                })
            self.stats.sessions.clients[addr[0]].accepted += 1
            #self.log.info("accept %s", str(addr))
        except socket.timeout:
            return
        thr = threading.Thread(target=self.handle_client, args=(conn, addr))
        thr.start()
        self.threads.append(thr)

    def handle_client(self, conn, addr):
        try:
            self._handle_client(conn, addr)
        finally:
            conn.close()

    def _handle_client(self, conn, addr):
        chunks = []
        buff_size = 4096
        while True:
            chunk = conn.recv(buff_size)
            self.stats.sessions.rx += len(chunk)
            self.stats.sessions.clients[addr[0]].rx += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if sys.version_info[0] >= 3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        del chunks

        nodename, data = self.decrypt(data, sender_id=addr[0])
        #self.log.info("received %s from %s", str(data), nodename)
        self.stats.sessions.auth_validated += 1
        self.stats.sessions.clients[addr[0]].auth_validated += 1
        if data is None:
            cmd = [rcEnv.paths.nodemgr, 'dequeue_actions']
            p = Popen(cmd, stdout=None, stderr=None, stdin=None, close_fds=True)
            p.communicate()
        else:
            result = self.router(nodename, data, conn)
            if result:
                message = self.encrypt(result)
                conn.sendall(message)
                message_len = len(message)
                self.stats.sessions.tx += message_len
                self.stats.sessions.clients[addr[0]].tx += message_len

    #########################################################################
    #
    # Actions
    #
    #########################################################################
    def router(self, nodename, data, conn):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        if not isinstance(data, dict):
            return
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        fname = "action_"+data["action"]
        if not hasattr(self, fname):
            return {"error": "action not supported", "status": 1}
        # prepare options, sanitized for use as keywords
        options = {}
        for key, val in data.get("options", {}).items():
            options[str(key)] = val
        return getattr(self, fname)(nodename, conn=conn, **options)

    def action_relay_tx(self, nodename, **kwargs):
        if not hasattr(self, "relay_data"):
            self.relay_data = {}
        self.relay_data[nodename] = kwargs.get("msg")
        return {"status": 0}

    def action_relay_rx(self, nodename, **kwargs):
        if not hasattr(self, "relay_data"):
            return {"status": 1, "error": "no data"}
        _nodename = kwargs.get("slot")
        if _nodename not in self.relay_data:
            return {"status": 1, "error": "no data"}
        return {"status": 0, "data": self.relay_data[_nodename]}

    def action_daemon_blacklist_clear(self, nodename, **kwargs):
        """
        Clear the senders blacklist.
        """
        self.blacklist_clear()
        return {"status": 0}

    def action_daemon_blacklist_status(self, nodename, **kwargs):
        """
        Return the senders blacklist.
        """
        return {"status": 0, "data": self.get_blacklist()}

    @staticmethod
    def action_daemon_status(nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        data = {}
        with shared.THREADS_LOCK:
            for thr_id, thread in shared.THREADS.items():
                data[thr_id] = thread.status(**kwargs)
        return data

    def action_daemon_shutdown(self, nodename, **kwargs):
        self.log.info("shutdown daemon requested")
        with shared.THREADS_LOCK:
            shared.THREADS["monitor"].shutdown()

        while True:
            with shared.THREADS_LOCK:
                if not shared.THREADS["monitor"].is_alive():
                    break
        shared.DAEMON_STOP.set()
        return {"status": 0}

    def action_daemon_stop(self, nodename, **kwargs):
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            self.log.info("stop daemon requested")
            self.set_nmon(status="maintenance")
            self.log.info("announce maintenance state")
            shared.wake_monitor()
            time.sleep(5)
            shared.DAEMON_STOP.set()
            return {"status": 0}
        elif thr_id == "tx":
            thr_ids = [thr_id for thr_id in shared.THREADS.keys() if thr_id.endswith("tx")]
        else:
            thr_ids = [thr_id]
        for thr_id in thr_ids:
            with shared.THREADS_LOCK:
                has_thr = thr_id in shared.THREADS
            if not has_thr:
                self.log.info("stop thread requested on non-existing thread")
                return {"error": "thread does not exist"*50, "status": 1}
            self.log.info("stop thread %s requested", thr_id)
            with shared.THREADS_LOCK:
                shared.THREADS[thr_id].stop()
            if thr_id == "scheduler":
                shared.wake_scheduler()
            elif thr_id == "monitor":
                shared.wake_monitor()
            elif thr_id.endswith("tx"):
                shared.wake_heartbeat_tx()
            if kwargs.get("wait", False):
                with shared.THREADS_LOCK:
                    shared.THREADS[thr_id].join()
        return {"status": 0}

    def action_daemon_start(self, nodename, **kwargs):
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            return {"error": "no thread specified", "status": 1}
        with shared.THREADS_LOCK:
            has_thr = thr_id in shared.THREADS
        if not has_thr:
            self.log.info("start thread requested on non-existing thread")
            return {"error": "thread does not exist"*50, "status": 1}
        self.log.info("start thread requested")
        with shared.THREADS_LOCK:
            shared.THREADS[thr_id].unstop()
        return {"status": 0}

    def action_get_service_config(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if not svcname:
            return {"error": "no svcname specified", "status": 1}
        fpath = os.path.join(rcEnv.paths.pathetc, svcname+".conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 1}
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve service %s config to %s", svcname, nodename)
        return {"status": 0, "data": buff}

    def action_clear(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if svcname is None:
            return {"error": "no svcname specified", "status": 1}
        self.set_smon(svcname, status="idle", reset_retries=True)
        shared.wake_monitor()
        return {"status": 0}

    def get_service_slaves(self, svcname):
        slaves = set()
        for nodename in shared.CLUSTER_DATA:
            try:
                data = shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]
            except KeyError:
                continue
            if not data.get("enslave_children"):
                continue
            _slaves = set(data.get("children")) - slaves
            slaves |= _slaves
            for slave in _slaves:
                slaves |= self.get_service_slaves(slave)
        return slaves

    def action_set_service_monitor(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if svcname is None:
            return {"error": "no svcname specified", "status": 1}
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        reset_retries = kwargs.get("reset_retries", False)
        stonith = kwargs.get("stonith")
        svcnames = set([svcname]) | self.get_service_slaves(svcname)
        for svcname in svcnames:
            self.set_smon(
                svcname, status=status,
                local_expect=local_expect, global_expect=global_expect,
                reset_retries=reset_retries,
                stonith=stonith,
            )
        shared.wake_monitor()
        return {"status": 0}

    def action_set_node_monitor(self, nodename, **kwargs):
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        self.set_nmon(
            status=status,
            local_expect=local_expect, global_expect=global_expect,
        )
        shared.wake_monitor()
        return {"status": 0}

    def action_leave(self, nodename, **kwargs):
        if nodename not in self.cluster_nodes:
            self.log.info("node %s already left", nodename)
            return
        ret = self.remove_cluster_node(nodename)
        result = {
            "status": ret,
        }
        return result

    def action_collector_xmlrpc(self, nodename, **kwargs):
        args = kwargs.get("args", [])
        kwargs = kwargs.get("kwargs", {})
        shared.COLLECTOR_XMLRPC_QUEUE.insert(0, (args, kwargs))
        result = {
            "status": 0,
        }
        return result

    def action_join(self, nodename, **kwargs):
        if nodename in self.cluster_nodes:
            new_nodes = self.cluster_nodes
            self.log.info("node %s rejoins", nodename)
        else:
            new_nodes = self.cluster_nodes + [nodename]
            self.add_cluster_node(nodename)
        result = {
            "status": 0,
            "data": {
                "cluster": {
                    "nodes": new_nodes,
                    "name": self.cluster_name,
                    "quorum": self.quorum,
                },
            },
        }
        for section in self.config.sections():
            if section.startswith("hb#") or \
               section.startswith("stonith#") or \
               section.startswith("arbitrator#"):
                result["data"][section] = {}
                for key, val in self.config.items(section):
                    result["data"][section][key] = val
        return result

    def action_node_action(self, nodename, **kwargs):
        """
        Execute a nodemgr command on behalf of a peer node.
        kwargs:
        * cmd: list
        * sync: boolean
        """
        sync = kwargs.get("sync", True)
        action_mode = kwargs.get("action_mode", True)
        cmd = kwargs.get("cmd")
        if cmd is None or len(cmd) == 0:
            self.log.error("node %s requested a peer node action without "
                           "specifying the command", nodename)
            return {
                "status": 1,
            }

        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("--daemon", cmd)
        cmd = [rcEnv.paths.nodemgr] + cmd
        if action_mode and "--local" not in cmd:
            cmd += ["--local"]
        self.log.info("execute node action requested by node %s: %s",
                      nodename, " ".join(cmd))
        if sync:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
            result = {
                "status": 0,
                "data": {
                    "out": bdecode(out),
                    "err": bdecode(err),
                    "ret": proc.returncode,
                },
            }
        else:
            proc = Popen(cmd, stdin=None, close_fds=True)
            self.push_proc(proc)
            result = {
                "status": 0,
            }
        return result

    def action_service_action(self, nodename, **kwargs):
        """
        Execute a CRM command on behalf of a peer node.
        kwargs:
        * svcname: str
        * cmd: list
        * sync: boolean
        """
        sync = kwargs.get("sync", True)
        action_mode = kwargs.get("action_mode", True)
        svcname = kwargs.get("svcname")
        if svcname is None:
            self.log.error("node %s requested a service action without "
                           "specifying the service name", nodename)
            return {
                "status": 1,
            }
        cmd = kwargs.get("cmd")
        if self.get_service(svcname) is None and "create" not in cmd:
            self.log.warning("discard service action '%s' on a service "
                             "not installed: %s", " ".join(cmd), svcname)
            return {
                "err": "service not found",
                "status": 1,
            }
        if cmd is None or len(cmd) == 0:
            self.log.error("node %s requested a service action without "
                           "specifying the command", nodename)
            return {
                "status": 1,
            }

        cmd = drop_option("--node", cmd, drop_value=True)
        cmd = drop_option("--daemon", cmd)
        if action_mode and "--local" not in cmd:
            cmd.append("--local")
        cmd = [rcEnv.paths.svcmgr, "-s", svcname] + cmd
        self.log.info("execute service action requested by node %s: %s",
                      nodename, " ".join(cmd))
        if sync:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=None, close_fds=True)
            out, err = proc.communicate()
            result = {
                "status": 0,
                "data": {
                    "out": bdecode(out),
                    "err": bdecode(err),
                    "ret": proc.returncode,
                },
            }
        else:
            proc = Popen(cmd, stdin=None, close_fds=True)
            self.push_proc(proc)
            result = {
                "status": 0,
            }
        return result

    def action_service_logs(self, nodename, **kwargs):
        """
        Send service logs.
        kwargs:
        * svcname
        * conn: the connexion socket to the requester
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        conn = kwargs.get("conn")
        svcname = kwargs.get("svcname")
        backlog = kwargs.get("backlog")
        if backlog is None:
            backlog = 1024 * 10
        else:
            backlog = convert_size(backlog, _to='B')
        if svcname is None:
            return {
                "status": 1,
            }
        logfile = os.path.join(rcEnv.paths.pathlog, svcname+".log")
        if backlog > 0:
            fsize = os.path.getsize(logfile)
            if backlog > fsize:
                skip = 0
            else:
                skip = fsize - backlog

        with open(logfile, "r") as ofile:
            if backlog > 0:
                self.log.info("send %s log to node %s, backlog %d",
                              svcname, nodename, backlog)
                try:
                    ofile.seek(skip)
                except Exception as exc:
                    self.log.info(str(exc))
                    ofile.seek(0)
            elif backlog < 0:
                self.log.info("send %s log to node %s, whole file",
                              svcname, nodename)
                ofile.seek(0)
            else:
                self.log.info("follow %s log for node %s",
                              svcname, nodename)
                ofile.seek(0, 2)
            lines = []
            msg_size = 0
            conn.settimeout(1)
            loops = 0

            if skip:
                # drop first line (that is incomplete as the seek placed the
                # cursor in the middle
                line = ofile.readline()

            while True:
                line = ofile.readline()
                line_size = len(line)
                if line_size == 0:
                    if msg_size > 0:
                        message = self.encrypt(lines)
                        try:
                            conn.sendall(message)
                        except Exception as exc:
                            if hasattr(exc, "errno") and exc.errno == 32:
                                # Broken pipe (client has left)
                                break
                    if backlog != 0:
                        # don't follow file
                        break
                    else:
                        loops += 1
                        # follow
                        if loops > 10:
                            try:
                                conn.send(b"\0")
                                loops = 0
                            except Exception as exc:
                                self.log.info("stop following %s log for node %s: %s",
                                              svcname, nodename, exc)
                                break
                        time.sleep(0.1)
                        lines = []
                        msg_size = 0
                        continue
                lines.append(line)
                msg_size += line_size
                if msg_size > shared.MAX_MSG_SIZE:
                    message = self.encrypt(lines)
                    conn.sendall(message)
                    msg_size = 0
                    lines = []

