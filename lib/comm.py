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

import pyaes
from rcGlobalEnv import rcEnv
from rcUtilities import lazy, bdecode

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
                ur = os.urandom(16 * 1024)
                urandom += [ur[i:i + 16] for i in range(16, 1024 * 16, 16)]
                return ur[0:16]
            finally:
                locker.release()

    @lazy
    def cluster_nodes(self):
        """
        Return the cluster nodes, read from cluster.nodes in the node
        configuration. If not set, return a list with the local node as the
        only element.
        """
        if hasattr(self, "get_node"):
            config = self.get_node().config
        else:
            config = self.config
        try:
            return config.get("cluster", "nodes").split()
        except Exception:
            return [rcEnv.nodename]

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
            return config.get("cluster", "name")
        except Exception:
            return "default"

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

    @staticmethod
    def prepare_key(key):
        """
        Return the key in a format expected by the encrypter and decrypter.
        """
        key = key.encode("utf-8")
        if len(key) > 32:
            key = key[:32]
        return key

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
            if len(message) > 0:
                self.log.error("misformatted encrypted message: %s",
                               repr(message))
            return None, None
        if cluster_name != "join" and \
           message.get("clustername") not in (cluster_name, "join"):
            self.log.warning("discard message from cluster %s",
                             message.get("clustername"))
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
        iv = base64.urlsafe_b64decode(str(iv))
        data = base64.urlsafe_b64decode(str(message["data"]))
        try:
            data = bdecode(self._decrypt(data, cluster_key, iv))
        except Exception as exc:
            self.log.error("decrypt message from %s: %s", nodename, str(exc))
            self.blacklist(sender_id)
            return None, None
        try:
            return nodename, json.loads(data)
        except ValueError as exc:
            return nodename, data

    def encrypt(self, data, cluster_name=None, secret=None):
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
        return (json.dumps(message)+'\0').encode()

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
                    self.log.warning("sender %s blacklisted")
            else:
                BLACKLIST[sender_id] = 1

    def blacklist_clear(self):
        """
        Clear the senders blacklist.
        """
        global BLACKLIST
        with BLACKLIST_LOCK:
            BLACKLIST = {}
        self.log.info("blacklist cleared")

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

    def daemon_send(self, data, nodename=None, with_result=True, silent=False,
                    cluster_name=None, secret=None):
        """
        Send a request to the daemon running on nodename and return the result
        fetched if with_result is set.
        """
        if nodename is None:
            nodename = rcEnv.nodename
        addr, port = self.get_listener_info(nodename)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(6.2)
            sock.connect((addr, port))
            message = self.encrypt(data, cluster_name=cluster_name,
                                   secret=secret)
            if message is None:
                return
            sock.sendall(message)
            if with_result:
                chunks = []
                while True:
                    chunk = sock.recv(4096)
                    if chunk:
                        chunks.append(chunk)
                    if not chunk or chunk.endswith(b"\x00"):
                        break
                if sys.version_info[0] >= 3:
                    data = b"".join(chunks)
                else:
                    data = "".join(chunks)
                nodename, data = self.decrypt(data, cluster_name=cluster_name,
                                              secret=secret)
                return data
        except socket.error as exc:
            if not silent:
                self.log.error("init error: %s", str(exc))
            return
        finally:
            sock.close()
