import os
import time
from subprocess import *
from rcGlobalEnv import rcEnv

pathsvc = os.path.join(os.path.dirname(__file__), '..')
nodeconf = os.path.join(pathsvc, "etc", "node.conf")

if rcEnv.sysname == "Windows":
    nodemgr = os.path.join(pathsvc, "nodemgr.cmd")
    svcmon = os.path.join(pathsvc, "svcmon.cmd")
    cron = os.path.join(pathsvc, "cron.cmd")
else:
    pathbin = os.path.join(pathsvc, "bin")
    nodemgr = os.path.join(pathbin, "nodemgr")
    svcmon = os.path.join(pathbin, "svcmon")
    cron = os.path.join(pathbin, "cron",  "cron")

import thread
import sys
from socket import *

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

try:
    config = ConfigParser.RawConfigParser()
    config.read(nodeconf)
    port = config.getint("listener", "port")
except:
    port = rcEnv.listener_port

def HandleClient(conn):
    data = conn.recv(1024)
    cmd = [nodemgr, 'dequeue_actions']
    p = Popen(cmd, stdout=None, stderr=None, stdin=None)
    p.communicate()
    conn.close()

class listener(object):
    def __init__(self):
        thread.start_new(self.do, tuple())
        while True:
            if getattr(sys, 'stop_listener', False):
                sys.exit(0)
            time.sleep(0.3)

    def do(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind((gethostname(), port))
        sock.listen(5)

        while True:
            conn, addr = sock.accept()
            thread.start_new(HandleClient, (conn,))

if __name__ == '__main__':
    a = listener()

