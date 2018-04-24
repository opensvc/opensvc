"""
The daemon encrypted communications primitives.
"""

import base64
import json
import os
import socket
import sys
import threading
import zlib
import time
import select

import pyaes
import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import lazy, bdecode

if sys.version_info[0] >= 3:
    to_bytes = lambda x: bytes(x, "utf-8")
else:
    to_bytes = lambda x: bytes(x)

# Number of received misencrypted data messages by senders
BLACKLIST = {}
BLACKLIST_LOCK = threading.RLock()

# The maximum number of misencrypted data messages received before refusing
# new messages
BLACKLIST_THRESHOLD = 5

try:
    from Crypto.Cipher import AES
    def _encrypt(message, key, _iv):
        """
        Low level encrypter.
        """
        message = pyaes.util.append_PKCS7_padding(zlib.compress(message.encode()))
        obj = AES.new(key, AES.MODE_CBC, _iv)
        ciphertext = obj.encrypt(message)
        return ciphertext
    def _decrypt(ciphertext, key, _iv):
        """
        Low level decrypter.
        """
        obj = AES.new(key, AES.MODE_CBC, _iv)
        message = obj.decrypt(ciphertext)
        return zlib.decompress(pyaes.util.strip_PKCS7_padding(message))
except ImportError:
    def _encrypt(message, key, _iv):
        """
        Low level encrypter.
        """
        obj = pyaes.Encrypter(pyaes.AESModeOfOperationCBC(key, iv=_iv))
        ciphertext = obj.feed(zlib.compress(message.encode()))
        ciphertext += obj.feed()
        return ciphertext
    def _decrypt(ciphertext, key, _iv):
        """
        Low level decrypter.
        """
        obj = pyaes.Decrypter(pyaes.AESModeOfOperationCBC(key, iv=_iv))
        message = obj.feed(ciphertext)
        message += obj.feed()
        return zlib.decompress(message)

class Crypt(object):
    """
    A class implement AES encrypt, decrypt and message padding.
    Used by child classes to authenticate senders on data receive.
    """
    @staticmethod
    def _encrypt(message, key, _iv):
        """
        A wrapper over the low level encrypter.
        """
        return _encrypt(message, key, _iv)

    @staticmethod
    def _decrypt(ciphertext, key, _iv):
        """
        A wrapper over the low level decrypter.
        """
        return _decrypt(ciphertext, key, _iv)

    @staticmethod
    def gen_iv(urandom=[], locker=threading.RLock()):
        """
        This is 4x faster than calling os.urandom(16) and prevents
        the "too many files open" issue with concurrent access to os.urandom()
        """
        try:
            return urandom.pop()
        except IndexError:
            try:
                locker.acquire()
                ur = os.urandom(1024)
                urandom += [ur[i:i + 16] for i in range(16, 1024, 16)]
                return ur[0:16]
            finally:
                locker.release()

    @lazy
    def sorted_cluster_nodes(self):
        return sorted(self.cluster_nodes)

    @lazy
    def arbitrators(self):
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        arbitrators = []
        for section in config.sections():
            data = {
                "id": section,
            }
            if not section.startswith("arbitrator#"):
                continue
            try:
                data["name"] = config.get(section, "name")
            except Exception:
                continue
            try:
                data["secret"] = config.get(section, "secret")
            except Exception:
                continue
            arbitrators.append(data)
        return arbitrators

    @lazy
    def cluster_nodes(self):
        """
        Return the cluster nodes, read from cluster.nodes in the node
        configuration. If not set, return a list with the local node as the
        only element.
        """
        nodes = None
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        try:
            nodes = config.get("cluster", "nodes").split()
        except Exception as exc:
            pass

        if nodes is not None:
            if rcEnv.nodename in nodes:
                return nodes
            else:
                nodes.append(rcEnv.nodename)
        else:
            nodes = [rcEnv.nodename]

        if hasattr(self, "get_node"):
            node = self.get_node()
        elif hasattr(self, "write_config"):
            node = self
        else:
            node = None
        if node is not None:
            if not node.config.has_section("cluster"):
                node.config.add_section("cluster")
            node.config.set("cluster", "nodes", " ".join(nodes))
            node.write_config()
        return nodes

    @lazy
    def cluster_drpnodes(self):
        """
        Return the cluster drp nodes, read from cluster.drpnodes in the node
        configuration. If not set, return an empty list.
        """
        nodes = []
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        try:
            nodes = config.get("cluster", "drpnodes").split()
        except Exception as exc:
            pass
        return nodes

    @lazy
    def cluster_name(self):
        """
        Return the cluster name, read from cluster.name in the node
        configuration. If not set, return "default".
        """
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        try:
            return config.get("cluster", "name").lower()
        except Exception as exc:
            pass
        if hasattr(self, "node"):
            node = self.node
        elif hasattr(self, "write_config"):
            node = self
        else:
            node = None
        name = "default"
        if node is not None:
            if not node.config.has_section("cluster"):
                node.config.add_section("cluster")
            node.config.set("cluster", "name", name)
            node.write_config()
        return name

    @lazy
    def cluster_key(self):
        """
        Return the key read from cluster.secret in the node configuration.
        If not already set generate and store a random one.
        """
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        try:
            key = config.get("cluster", "secret")
            return self.prepare_key(key)
        except Exception as exc:
            pass
        if hasattr(self, "node"):
            node = self.node
        elif hasattr(self, "write_config"):
            node = self
        else:
            return
        import uuid
        key = uuid.uuid1().hex
        if not config.has_section("cluster"):
            config.add_section("cluster")
        node.config.set("cluster", "secret", key)
        node.write_config()
        return self.prepare_key(key)

    @lazy
    def cluster_id(self):
        """
        Return the cluster id read from cluster.id in the node configuration.
        If not already set generate and store a random one.
        """
        if hasattr(self, "get_node"):
            # svc
            node = self.get_node()
        elif hasattr(self, "write_config"):
            # node
            node = self
        else:
            from osvcd_shared import NODE
            node = NODE
        try:
            return node.config.get("cluster", "id")
        except Exception as exc:
            pass
        import uuid
        cluster_id = str(uuid.uuid1())
        if not node.config.has_section("cluster"):
            node.config.add_section("cluster")
        node.config.set("cluster", "id", cluster_id)
        node.write_config()
        return cluster_id

    @staticmethod
    def prepare_key(key):
        """
        Return the key in a format expected by the encrypter and decrypter.
        """
        key = key.encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        return key

    def msg_encode(self, data):
        return (json.dumps(data)+'\0').encode()

    def msg_decode(self, message):
        message = bdecode(message).rstrip("\0\x00")
        if len(message) == 0:
            return
        return json.loads(message)

    def decrypt(self, message, cluster_name=None, secret=None, sender_id=None):
        """
        Validate the message meta, decrypt and return the data.
        """
        if cluster_name is None:
            cluster_name = self.cluster_name
        if secret is None:
            cluster_key = self.cluster_key
        else:
            cluster_key = secret
        message = bdecode(message).rstrip("\0\x00")
        try:
            message = json.loads(message)
        except ValueError:
            message_len = len(message)
            if message_len > 40:
                self.log.error("misformatted encrypted message from %s: %s",
                               sender_id, message[:30]+"..."+message[-10:])
            elif message_len > 0:
                self.log.error("misformatted encrypted message from %s",
                               sender_id)
            return None, None
        if cluster_name != "join" and \
           message.get("clustername") not in (cluster_name, "join"):
            self.log.warning("discard message from cluster %s, sender %s",
                             message.get("clustername"), sender_id)
            return None, None
        if cluster_key is None:
            return None, None
        nodename = message.get("nodename")
        if nodename is None:
            return None, None
        iv = message.get("iv")
        if iv is None:
            return None, None
        if self.blacklisted(sender_id):
            return None, None
        iv = base64.urlsafe_b64decode(to_bytes(iv))
        data = base64.urlsafe_b64decode(to_bytes(message["data"]))
        try:
            data = bdecode(self._decrypt(data, cluster_key, iv))
        except Exception as exc:
            self.log.error("decrypt message from %s: %s", nodename, str(exc))
            self.blacklist(sender_id)
            return None, None
        self.blacklist_clear(sender_id)
        try:
            return nodename, json.loads(data)
        except ValueError as exc:
            return nodename, data

    def encrypt(self, data, cluster_name=None, secret=None, encode=True):
        """
        Encrypt and return data in a wrapping structure.
        """
        if cluster_name is None:
            cluster_name = self.cluster_name
        if secret is None:
            cluster_key = self.cluster_key
        else:
            cluster_key = secret
        if cluster_key is None:
            return
        iv = self.gen_iv()
        message = {
            "clustername": cluster_name,
            "nodename": rcEnv.nodename,
            "iv": bdecode(base64.urlsafe_b64encode(iv)),
            "data": bdecode(
                base64.urlsafe_b64encode(
                    self._encrypt(json.dumps(data), cluster_key, iv)
                )
            ),
        }
        if encode:
            return (json.dumps(message)+'\0').encode()
        return json.dumps(message)

    def blacklisted(self, sender_id):
        """
        Return True if the sender's problem count is above threshold.
        Else, return False.
        """
        if sender_id is None:
            return False
        sender_id = str(sender_id)
        with BLACKLIST_LOCK:
            count = BLACKLIST.get(sender_id, 0)
        if count > BLACKLIST_THRESHOLD:
            self.log.warning("received a message from blacklisted sender %s",
                             sender_id)
            return True
        return False

    def blacklist(self, sender_id):
        """
        Increment the sender's problem count in the blacklist.
        """
        if sender_id is None:
            return
        sender_id = str(sender_id)
        with BLACKLIST_LOCK:
            count = BLACKLIST.get(sender_id, 0)
            if sender_id in BLACKLIST:
                BLACKLIST[sender_id] += 1
                if count == BLACKLIST_THRESHOLD:
                    self.log.warning("sender %s blacklisted", sender_id)
            else:
                BLACKLIST[sender_id] = 1

    def blacklist_clear(self, sender_id=None):
        """
        Clear the senders blacklist.
        """
        global BLACKLIST
        if sender_id is None and BLACKLIST == {}:
            return
        with BLACKLIST_LOCK:
            if sender_id is None:
                BLACKLIST = {}
                self.log.info("blacklist cleared")
            elif sender_id in BLACKLIST:
                del BLACKLIST[sender_id]
                self.log.info("sender %s removed from blacklist")

    @staticmethod
    def get_blacklist():
        """
        Return the senders blacklist.
        """
        with BLACKLIST_LOCK:
            return dict(BLACKLIST)

    def get_listener_info(self, nodename):
        """
        Get the listener address and port from node.conf.
        """
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        if nodename == rcEnv.nodename:
            if not config.has_section("listener"):
                return "127.0.0.1", rcEnv.listener_port
            if config.has_option("listener", "addr@"+nodename):
                addr = config.get("listener", "addr@"+nodename)
            elif config.has_option("listener", "addr"):
                addr = config.get("listener", "addr")
            else:
                addr = "127.0.0.1"
            if config.has_option("listener", "port@"+nodename):
                port = config.getint("listener", "port@"+nodename)
            elif config.has_option("listener", "port"):
                port = config.getint("listener", "port")
            else:
                port = rcEnv.listener_port
        else:
            if not config.has_section("listener"):
                return nodename, rcEnv.listener_port
            if config.has_option("listener", "addr@"+nodename):
                addr = config.get("listener", "addr@"+nodename)
            else:
                addr = nodename
            if config.has_option("listener", "port@"+nodename):
                port = config.getint("listener", "port@"+nodename)
            elif config.has_option("listener", "port"):
                port = config.getint("listener", "port")
            else:
                port = rcEnv.listener_port
        return addr, port

    def recv_message(self, *args, **kwargs):
        data = self.recv_messages(*args, **kwargs)
        if data is None:
            return
        return data[0]

    def recv_messages(self, sock, cluster_name=None, secret=None, use_select=True, encrypted=True, bufsize=65536):
        """
        Receive, decrypt and return a message from a socket.
        """
        sock.setblocking(0)
        messages = []
        chunks = []
        sep = b"\x00"
        while True:
            if use_select:
                ready = select.select([sock], [], [sock], 60)
                if ready[0]:
                    chunk = sock.recv(bufsize)
                else:
                    break
                if ready[2]:
                    break
            else:
                chunk = sock.recv(bufsize)
            if not chunk or chunk == sep:
                break
            chunks.append(chunk)
        if sys.version_info[0] >= 3:
            data = b"".join(chunks)
        else:
            data = "".join(chunks)
        if len(data) == 0:
            return
        for message in data.split(sep):
            if encrypted:
                nodename, message = self.decrypt(data, cluster_name=cluster_name,
                                                 secret=secret)
            else:
                message = self.msg_decode(data)
            messages.append(message)
        return messages

    def socket_parms(self, nodename):
        data = Storage()
        if nodename == rcEnv.nodename and os.name != "nt":
            data.af = socket.AF_UNIX
            data.to = rcEnv.paths.lsnruxsock
            data.to_s = rcEnv.paths.lsnruxsock
            data.encrypted = False
        else:
            addr, port = self.get_listener_info(nodename)
            data.af = socket.AF_INET
            data.to = (addr, port)
            data.to_s = "%s:%d" % (addr, port)
            data.encrypted = True
        return data

    def daemon_send(self, data, nodename=None, with_result=True, silent=False,
                    cluster_name=None, secret=None, timeout=0):
        """
        Send a request to the daemon running on nodename and return the result
        fetched if with_result is set.
        """
        if nodename is None or nodename == "":
            nodename = rcEnv.nodename
        sp = self.socket_parms(nodename)
        try:
            sock = socket.socket(sp.af, socket.SOCK_STREAM)
            sock.settimeout(0.2)
            sock.connect(sp.to)
            if sp.encrypted:
                message = self.encrypt(data, cluster_name=cluster_name,
                                       secret=secret)
            else:
                message = self.msg_encode(data)
            if message is None:
                return {"status": 1, "err": "failed to encrypt message"}
            sock.sendall(message)
            if with_result:
                elapsed = 0
                while True:
                    try:
                        return self.recv_message(sock, cluster_name=cluster_name, secret=secret, encrypted=sp.encrypted)
                    except socket.timeout:
                        if timeout > 0 and elapsed > timeout:
                            return {"status": 1, "err": "timeout"}
                        elapsed += 0.2
        except socket.error as exc:
            if not silent:
                self.log.error("daemon send to %s error: %s", sp.to_s, str(exc))
            return {"status": 1, "error": str(exc)}
        finally:
            sock.close()
        return {"status": 0}

    def daemon_get_stream(self, data, nodename=None, cluster_name=None,
                          secret=None):
        """
        Send a request to the daemon running on nodename and yield the results
        fetched if with_result is set.
        """
        if nodename is None:
            nodename = rcEnv.nodename
        sp = self.socket_parms(nodename)
        try:
            sock = socket.socket(sp.af, socket.SOCK_STREAM)
            sock.settimeout(6.2)
            sock.connect(sp.to)
            if sp.encrypted:
                message = self.encrypt(data, cluster_name=cluster_name,
                                       secret=secret)
            else:
                message = self.msg_encode(data)
            if message is None:
                return
            sock.sendall(message)
            while True:
                data = self.recv_messages(sock, cluster_name=cluster_name, secret=secret, encrypted=sp.encrypted, bufsize=1)
                if data is None:
                    return
                for message in data:
                    yield message
        except socket.error as exc:
            self.log.error("daemon send to %s error: %s", sp.to_s, str(exc))
        finally:
            sock.close()

    def daemon_get_streams(self, data, nodenames=None, cluster_name=None,
                           secret=None):
        """
        Send a request to the daemon running on nodename and yield the results
        fetched if with_result is set.
        """
        if nodenames is None:
            nodenames = [rcEnv.nodename]

        socks = {}
        for nodename in nodenames:
            sp = self.socket_parms(nodename)
            try:
                sock = socket.socket(sp.af, socket.SOCK_STREAM)
                sock.settimeout(6.2)
                sock.connect(sp.to)
                if sp.encrypted:
                    message = self.encrypt(data, cluster_name=cluster_name,
                                           secret=secret)
                else:
                    message = self.msg_encode(data)
                if message is None:
                    return
                sock.sendall(message)
                socks[sock] = sp
            except socket.error as exc:
                self.log.error("daemon send to %s error: %s", sp.to_s, str(exc))

        try:
            while True:
                ready_to_read, _, exceptionals = select.select(socks.keys(), [], socks, 1)
                for sock in ready_to_read:
                    data = self.recv_messages(sock, cluster_name=cluster_name, secret=secret, use_select=False, encrypted=socks[sock].encrypted, bufsize=1)
                    if data is None:
                        continue
                    for message in data:
                        yield message
                for sock in exceptionals:
                    del socks[sock]
                if len(socks) == 0:
                    break
        finally:
            for sock in socks:
                sock.close()

