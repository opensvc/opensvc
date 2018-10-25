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
import select
import shutil
from subprocess import Popen, PIPE

import six
import osvcd_shared as shared
import rcExceptions as ex
from six.moves import queue
from rcGlobalEnv import rcEnv
from storage import Storage
from rcUtilities import bdecode, drop_option, chunker
from converters import convert_size

RELAY_DATA = {}
RELAY_LOCK = threading.RLock()

class Listener(shared.OsvcThread):
    sock_tmo = 1.0
    events_grace_period = True

    def setup_sock(self):
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
            self.sock.listen(128)
            self.sock.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s:%d error: %s", self.addr, self.port, exc)
            return
        self.log.info("listening on %s:%s", self.addr, self.port)
        self.sockmap[self.sock.fileno()] = self.sock

    def setup_sockux(self):
        if os.name == "nt":
            return
        if not os.path.exists(rcEnv.paths.lsnruxsockd):
            os.makedirs(rcEnv.paths.lsnruxsockd)
        try:
            if os.path.isdir(rcEnv.paths.lsnruxsock):
                shutil.rmtree(rcEnv.paths.lsnruxsock)
            else:
                os.unlink(rcEnv.paths.lsnruxsock)
        except Exception:
            pass
        try:
            self.sockux = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockux.bind(rcEnv.paths.lsnruxsock)
            self.sockux.listen(1)
            self.sockux.settimeout(self.sock_tmo)
        except socket.error as exc:
            self.log.error("bind %s error: %s", rcEnv.paths.lsnruxsock, exc)
            return
        self.log.info("listening on %s", rcEnv.paths.dnsuxsock)
        self.sockmap[self.sockux.fileno()] = self.sockux

    def run(self):
        self.log = logging.getLogger(rcEnv.nodename+".osvcd.listener")
        self.events_clients = []
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
        self.sockmap = {}
        self.setup_sock()
        self.setup_sockux()

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
        self.janitor_events()

        fds = select.select([fno for fno in self.sockmap], [], [], self.sock_tmo)
        if self.sock_tmo and fds == ([], [], []):
            return
        for fd in fds[0]:
            sock = self.sockmap[fd]
            try:
                conn, addr = sock.accept()
                self.stats.sessions.accepted += 1
                if len(addr) == 0:
                    addr = ["local"]
                    encrypted = False
                else:
                    encrypted = True
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
                continue
            try:
                thr = threading.Thread(target=self.handle_client, args=(conn, addr, encrypted))
                thr.start()
                self.threads.append(thr)
            except RuntimeError as exc:
                self.log.warning(exc)
                conn.close()

    def janitor_events(self):
        """
        Send queued events to all subscribed clients.

        Don't dequeue messages during the first 2 seconds of the listener lifetime,
        so clients have a chance to reconnect after a daemon restart and loose an
        event.
        """
        if self.events_grace_period:
            if time.time() > self.created + 2:
                self.events_grace_period = False
            else:
                return
        done = []
        while True:
            try:
                event = shared.EVENT_Q.get(False, 0)
            except queue.Empty:
                break
            emsg = self.encrypt(event)
            msg = self.msg_encode(event)
            to_remove = []
            for idx, (conn, encrypted) in enumerate(self.events_clients):
                if encrypted:
                    _msg = emsg
                else:
                    _msg = msg
                try:
                    conn.sendall(_msg)
                except socket.error as exc:
                    to_remove.append(idx)
            for idx in to_remove:
                try:
                    self.events_clients[idx][0].close()
                except Exception:
                    pass
                try:
                    del self.events_clients[idx]
                except IndexError:
                    pass

    def handle_client(self, conn, addr, encrypted):
        try:
            self._handle_client(conn, addr, encrypted)
        finally:
            conn.close()

    def _handle_client(self, conn, addr, encrypted):
        chunks = []
        buff_size = 4096
        conn.setblocking(0)
        while True:
            ready = select.select([conn], [], [conn], 6)
            if ready[0]:
                chunk = conn.recv(buff_size)
            else:
                self.log.warning("timeout waiting for data from client %s", addr[0])
                break
            if ready[2]:
                self.log.debug("exceptional condition on socket with client %s", addr[0])
                break
            self.stats.sessions.rx += len(chunk)
            self.stats.sessions.clients[addr[0]].rx += len(chunk)
            if chunk:
                chunks.append(chunk)
            if not chunk or chunk.endswith(b"\x00"):
                break
        if six.PY3:
            data = b"".join(chunks)
            dequ = data == b"dequeue_actions"
        else:
            data = "".join(chunks)
            dequ = data == "dequeue_actions"
        del chunks

        if dequ:
            p = Popen([rcEnv.paths.nodemgr, 'dequeue_actions'],
                      stdout=None, stderr=None, stdin=None,
                      close_fds=os.name!="nt")
            return

        if encrypted:
            nodename, data = self.decrypt(data, sender_id=addr[0])
        else:
            try:
                data = self.msg_decode(data)
            except ValueError:
                pass
            nodename = rcEnv.nodename
        #self.log.info("received %s from %s", str(data), nodename)
        self.stats.sessions.auth_validated += 1
        self.stats.sessions.clients[addr[0]].auth_validated += 1
        if data is None:
            return
        result = self.router(nodename, data, conn, encrypted)
        if result:
            conn.setblocking(1)
            if encrypted:
                message = self.encrypt(result)
            else:
                message = self.msg_encode(result)
            for chunk in chunker(message, 64*1024):
                try:
                    conn.sendall(chunk)
                except socket.error as exc:
                    if exc.errno == 32:
                        # broken pipe
                        self.log.info(exc)
                    else:
                        self.log.warning(exc)
                    break
            message_len = len(message)
            self.stats.sessions.tx += message_len
            self.stats.sessions.clients[addr[0]].tx += message_len

    #########################################################################
    #
    # Actions
    #
    #########################################################################
    def router(self, nodename, data, conn, encrypted):
        """
        For a request data, extract the requested action and options,
        translate into a method name, and execute this method with options
        passed as keyword args.
        """
        if not isinstance(data, dict):
            return {"error": "invalid data format", "status": 1}
        if "action" not in data:
            return {"error": "action not specified", "status": 1}
        fname = "action_"+data["action"]
        if not hasattr(self, fname):
            return {"error": "action not supported", "status": 1}
        # prepare options, sanitized for use as keywords
        options = {}
        for key, val in data.get("options", {}).items():
            options[str(key)] = val
        return getattr(self, fname)(nodename, conn=conn, encrypted=encrypted, **options)

    def action_run_done(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        action = kwargs.get("action")
        rids = kwargs.get("rids")
        if not rids is None:
            rids = ",".join(sorted(rids))
        if not action:
            return {"status": 0}
        sig = (action, svcname, rids)
        with shared.RUN_DONE_LOCK:
            shared.RUN_DONE.add(sig)
        return {"status": 0}

    def action_relay_tx(self, nodename, **kwargs):
        with RELAY_LOCK:
            RELAY_DATA[nodename] = {
                "msg": kwargs.get("msg"),
                "updated": time.time(),
            }
        return {"status": 0}

    def action_relay_rx(self, nodename, **kwargs):
        _nodename = kwargs.get("slot")
        with RELAY_LOCK:
            if _nodename not in RELAY_DATA:
                return {"status": 1, "error": "no data"}
            return {
                "status": 0,
                "data": RELAY_DATA[_nodename]["msg"],
                "updated": RELAY_DATA[_nodename]["updated"],
            }

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

    def action_daemon_status(self, nodename, **kwargs):
        """
        Return a hash indexed by thead id, containing the status data
        structure of each thread.
        """
        data = {
            "cluster": {
                "name": self.cluster_name,
                "id": self.cluster_id,
            }
        }
        with shared.THREADS_LOCK:
            for thr_id, thread in shared.THREADS.items():
                data[thr_id] = thread.status(**kwargs)
        return data

    def svc_shutdown(self):
        """
        Shutdown all local service instances. Do not freeze them, so they
        can be orchestrated after reboot (the CRM interprets the
        OSVC_ACTION_ORIGIN env var as "disable pre-shutdown freeze").

        Use a long waitlock to maximize chances to wait for user-submitted
        commands to finish
        """
        env = os.environ.copy()
        env["OSVC_ACTION_ORIGIN"] = "daemon"
        cmd = rcEnv.python_cmd + [
            os.path.join(rcEnv.paths.pathlib, "svcmgr.py"),
            "--service", "*", "shutdown", "--local",
            "--parallel", "--waitlock=5m"
        ]
        proc = Popen(cmd, stdin=None, stdout=None, stderr=None, close_fds=True,
                     env=env)
        proc.communicate()


    def action_daemon_shutdown(self, nodename, **kwargs):
        """
        Care with locks
        """
        self.log.info("shutdown daemon requested")
        with shared.THREADS_LOCK:
            shared.THREADS["scheduler"].stop()
            mon = shared.THREADS["monitor"]
        try:
            self.set_nmon("shutting")
            mon.kill_procs()
            self.svc_shutdown()

            # send a last status to peers so they can takeover asap
            mon.update_hb_data()

            mon._shutdown = True
            shared.wake_monitor("services shutdown done")
        except Exception as exc:
            self.log.exception(exc)

        self.log.info("services are now shutdown")
        while True:
            with shared.THREADS_LOCK:
                if not shared.THREADS["monitor"].is_alive():
                    break
            time.sleep(0.3)
        shared.DAEMON_STOP.set()
        return {"status": 0}

    def action_daemon_stop(self, nodename, **kwargs):
        thr_id = kwargs.get("thr_id")
        if not thr_id:
            self.log.info("stop daemon requested")
            if kwargs.get("upgrade"):
                self.set_nmon(status="upgrade")
                self.log.info("announce upgrade state")
            else:
                self.set_nmon(status="maintenance")
                self.log.info("announce maintenance state")
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
                shared.wake_monitor("shutdown")
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

    def action_get_node_config(self, nodename, **kwargs):
        fmt = kwargs.get("format")
        if fmt == "json":
            return self._action_get_node_config_json(nodename, **kwargs)
        else:
            return self._action_get_node_config_file(nodename, **kwargs)

    def _action_get_node_config_json(self, nodename, **kwargs):
        try:
            return shared.NODE.print_config_data()
        except Exception as exc:
            import traceback
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_node_config_file(self, nodename, **kwargs):
        fpath = os.path.join(rcEnv.paths.pathetc, "node.conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 3}
        mtime = os.path.getmtime(fpath)
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve node config to %s", nodename)
        return {"status": 0, "data": buff, "mtime": mtime}

    def action_get_service_config(self, nodename, **kwargs):
        fmt = kwargs.get("format")
        if fmt == "json":
            return self._action_get_service_config_json(nodename, **kwargs)
        else:
            return self._action_get_service_config_file(nodename, **kwargs)

    def _action_get_service_config_json(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        evaluate = kwargs.get("evaluate")
        impersonate = kwargs.get("impersonate")
        try:
            return shared.SERVICES[svcname].print_config_data(evaluate=evaluate, impersonate=impersonate)
        except Exception as exc:
            import traceback
            return {"status": "1", "error": str(exc), "traceback": traceback.format_exc()}

    def _action_get_service_config_file(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if not svcname:
            return {"error": "no svcname specified", "status": 1}
        if shared.SMON_DATA.get(svcname, {}).get("status") in ("purging", "deleting") or \
           shared.SMON_DATA.get(svcname, {}).get("global_expect") in ("purged", "deleted"):
            return {"error": "delete in progress", "status": 2}
        fpath = os.path.join(rcEnv.paths.pathetc, svcname+".conf")
        if not os.path.exists(fpath):
            return {"error": "%s does not exist" % fpath, "status": 3}
        mtime = os.path.getmtime(fpath)
        with codecs.open(fpath, "r", "utf8") as filep:
            buff = filep.read()
        self.log.info("serve service %s config to %s", svcname, nodename)
        return {"status": 0, "data": buff, "mtime": mtime}

    def action_wake_monitor(self, nodename, **kwargs):
        svcname = kwargs.get("svcname", "<unspecified>")
        shared.wake_monitor(reason="service %s notification" % svcname)
        return {"status": 0}

    def action_clear(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if svcname is None:
            return {"error": "no svcname specified", "status": 1}
        smon = self.get_service_monitor(svcname)
        if smon.status.endswith("ing"):
            return {"info": "skip clear on %s instance" % smon.status, "status": 0}
        self.set_smon(svcname, status="idle", reset_retries=True)
        return {"status": 0}

    def get_service_slaves(self, svcname, slaves=None):
        """
        Recursive lookup of service slaves.
        """
        if slaves is None:
            slaves = set()
        for nodename in shared.CLUSTER_DATA:
            try:
                data = shared.CLUSTER_DATA[nodename]["services"]["status"][svcname]
            except KeyError:
                continue
            new_slaves = set(data.get("slaves", [])) | set(data.get("scaler_slaves", []))
            new_slaves -= slaves
            slaves |= new_slaves
            for slave in new_slaves:
                slaves |= self.get_service_slaves(slave, slaves)
        return slaves

    def action_set_service_monitor(self, nodename, **kwargs):
        svcname = kwargs.get("svcname")
        if svcname is None:
            return {"error": ["no svcname specified"], "status": 1}
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        reset_retries = kwargs.get("reset_retries", False)
        stonith = kwargs.get("stonith")
        svcnames = set([svcname])
        if global_expect != "scaled":
            svcnames |= self.get_service_slaves(svcname)
        errors = []
        info = []
        for svcname in svcnames:
            try:
                self.validate_global_expect(svcname, global_expect)
                self.validate_destination_node(svcname, global_expect)
            except ex.excAbortAction as exc:
                info.append(str(exc))
            except ex.excError as exc:
                errors.append(str(exc))
            else:
                info.append("service %s target state set to %s" % (svcname, global_expect))
                self.set_smon(
                    svcname, status=status,
                    local_expect=local_expect, global_expect=global_expect,
                    reset_retries=reset_retries,
                    stonith=stonith,
                )
        ret = {"status": len(errors)}
        if info:
            ret["info"] = info
        if errors:
            ret["error"] = errors
        return ret

    def validate_destination_node(self, svcname, global_expect):
        """
        For a placed@<dst> <global_expect> (move action) on <svcname>,

        Raise an excError if
        * the service <svcname> does not exist
        * the service <svcname> topology is failover and more than 1
          destination node was specified
        * the specified destination is not a service candidate node
        * no destination node specified
        * an empty destination node is specified in a list of destination
          nodes

        Raise an excAbortAction if
        * the avail status of the instance on the destination node is up
        """
        if global_expect is None:
            return
        try:
            global_expect, destination_nodes = global_expect.split("@", 1)
        except ValueError:
            return
        instances = self.get_service_instances(svcname)
        if not instances:
            raise ex.excError("service does not exist")
        destination_nodes = destination_nodes.split(",")
        count = len(destination_nodes)
        if count == 0:
            raise ex.excError("no destination node specified")
        instance = list(instances.values())[0]
        if count > 1 and instance["topology"] == "failover":
            raise ex.excError("only one destination node can be specified for "
                              "a failover service")
        for destination_node in destination_nodes:
            if not destination_node:
                raise ex.excError("empty destination node")
            if destination_node not in instances:
                raise ex.excError("destination node %s has no service instance" % \
                                  destination_node)
            instance = instances[destination_node]
            if instance["avail"] == "up":
                raise ex.excAbortAction("instance on destination node %s is "
                                        "already up" % destination_node)

    def validate_global_expect(self, svcname, global_expect):
        if global_expect is None:
            return
        if global_expect in ("frozen", "aborted"):
            return
        instances = self.get_service_instances(svcname)
        if not instances:
            if global_expect == "provisioned":
                # allow provision target state on just-created service
                return
            else:
                raise ex.excError("service does not exist")
        for nodename, _data in instances.items():
            status = _data.get("monitor", {}).get("status", "unknown")
            if status != "idle" and "failed" not in status:
                raise ex.excError("%s instance on node %s in %s state"
                                  "" % (svcname, nodename, status))

        if global_expect not in ("started", "stopped"):
            return
        agg = Storage(shared.AGG.get(svcname, {}))
        if global_expect == "started" and agg.avail == "up":
            raise ex.excAbortAction("service %s is already started" % svcname)
        elif global_expect == "stopped" and agg.avail in ("down", "stdby down", "stdby up"):
            raise ex.excAbortAction("service %s is already stopped" % svcname)
        if agg.avail in ("n/a", "undef"):
            raise ex.excAbortAction()

    def action_set_node_monitor(self, nodename, **kwargs):
        status = kwargs.get("status")
        local_expect = kwargs.get("local_expect")
        global_expect = kwargs.get("global_expect")
        self.set_nmon(
            status=status,
            local_expect=local_expect, global_expect=global_expect,
        )
        return {"status": 0}

    def action_leave(self, nodename, **kwargs):
        if nodename not in self.cluster_nodes:
            self.log.info("node %s already left", nodename)
            return {"status": 0}
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
                "node": {
                    "env": rcEnv.node_env,
                },
                "cluster": {
                    "nodes": new_nodes,
                    "drpnodes": self.cluster_drpnodes,
                    "id": self.cluster_id,
                    "name": self.cluster_name,
                    "quorum": self.quorum,
                    "dns": shared.NODE.dns,
                },
            },
        }
        for section in self.config.sections():
            if section.startswith("hb#") or \
               section.startswith("stonith#") or \
               section.startswith("pool#") or \
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

    def action_events(self, nodename, **kwargs):
        encrypted = kwargs.get("encrypted")
        self.events_clients.append((kwargs.get("conn").dup(), encrypted))

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
        svcname = kwargs.get("svcname")
        if svcname is None:
            return {
                "status": 1,
            }
        logfile = os.path.join(rcEnv.paths.pathlog, svcname+".log")
        self._action_logs(nodename, logfile, "service %s" % svcname, **kwargs)

    def action_node_logs(self, nodename, **kwargs):
        """
        Send node logs.
        kwargs:
        * conn: the connexion socket to the requester
        * backlog: the number of bytes to send from the tail default is 10k.
                   A negative value means send the whole file.
                   The 0 value means follow the file.
        """
        logfile = os.path.join(rcEnv.paths.pathlog, "node.log")
        self._action_logs(nodename, logfile, "node", **kwargs)

    def _action_logs(self, nodename, logfile, obj, **kwargs):
        conn = kwargs.get("conn")
        encrypted = kwargs.get("encrypted")
        backlog = kwargs.get("backlog")
        if backlog is None:
            backlog = 1024 * 10
        else:
            backlog = convert_size(backlog, _to='B')
        skip = 0
        if backlog > 0:
            fsize = os.path.getsize(logfile)
            if backlog > fsize:
                skip = 0
            else:
                skip = fsize - backlog

        with open(logfile, "r") as ofile:
            if backlog > 0:
                self.log.debug("send %s log to node %s, backlog %d",
                               obj, nodename, backlog)
                try:
                    ofile.seek(skip)
                except Exception as exc:
                    self.log.info(str(exc))
                    ofile.seek(0)
            elif backlog < 0:
                self.log.info("send %s log to node %s, whole file",
                              obj, nodename)
                ofile.seek(0)
            else:
                self.log.info("follow %s log for node %s",
                              obj, nodename)
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
                if self.stopped():
                    break
                line = ofile.readline()
                line_size = len(line)
                if line_size == 0:
                    if msg_size > 0:
                        if encrypted:
                            message = self.encrypt(lines)
                        else:
                            message = self.msg_encode(lines)
                        try:
                            conn.sendall(message)
                        except Exception as exc:
                            if hasattr(exc, "errno") and getattr(exc, "errno") == 32:
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
                                              obj, nodename, exc)
                                break
                        time.sleep(0.1)
                        lines = []
                        msg_size = 0
                        continue
                lines.append(line)
                msg_size += line_size
                if msg_size > shared.MAX_MSG_SIZE:
                    if encrypted:
                        message = self.encrypt(lines)
                    else:
                        message = self.msg_encode(lines)
                    conn.sendall(message)
                    msg_size = 0
                    lines = []

