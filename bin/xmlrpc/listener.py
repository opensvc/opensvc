#!/usr/bin/env python

import os
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging
import logging.handlers
import _mysql

logfile = os.path.join(os.path.dirname(__file__), 'listener.log')
log = logging.Logger("listener")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
filehandler = logging.handlers.RotatingFileHandler(logfile, maxBytes=5000, backupCount=5)
streamhandler = logging.StreamHandler()
filehandler.setFormatter(formatter)
streamhandler.setFormatter(formatter)
log.addHandler(filehandler)
log.addHandler(streamhandler)
#log.setLevel(rcEnv.loglevel)

db = _mysql.connect(
    host='localhost',
    user='opensvc',
    passwd='opensvc',
    unix_socket='/tmp/mysql.sock.unxweb',
    db='opensvc'
)

def delete_services(hostid=None):
    if hostid is None:
        return 0
    sql="""delete from services where svc_hostid='%s'""" % hostid
    log.info(sql)
    db.query(sql)
    return 0

def update_service(vars, vals):
    if 'svc_hostid' not in vars:
        return 0
    sql="""insert into services (%s) values (%s)""" % (','.join(vars), ','.join(vals))
    log.info(sql)
    db.query(sql)
    return 0

port = 8000
host = "unxdevweb"

server = SimpleXMLRPCServer((host, port))
log.info("Listening on %s:%d" % (host, port))
server.register_function(delete_services, "delete_services")
server.register_function(update_service, "update_service")
server.serve_forever()

