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

class osvcdb:
    db = None

    def connect(self):
        self.db = _mysql.connect(
            host='localhost',
            user='opensvc',
            passwd='opensvc',
            unix_socket='/tmp/mysql.sock.unxweb',
            db='opensvc'
        )

    def query(self, sql):
        if self.db is None:
            try: self.connect()
            except:
                log.error('can not open connection to database')
                return
        self.db.ping(True)
        self.db.query(sql)

db = osvcdb()

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

def begin_action(vars, vals):
    sql="""insert delayed into SVCactions (%s) values (%s)""" % (','.join(vars), ','.join(vals))
    log.info(sql)
    db.query(sql)
    return 0

def end_action(vars, vals):
    upd = []
    for a, b in zip(vars, vals):
        upd.append("%s=%s" % (a, b))
    sql="""insert delayed into SVCactions (%s) values (%s) on duplicate key update %s""" % (','.join(vars), ','.join(vals), ','.join(upd))
    log.info(sql)
    db.query(sql)
    return 0

def svcmon_update(vars, vals):
    upd = []
    for a, b in zip(vars, vals):
        upd.append("%s=%s" % (a, b))
    sql="""insert delayed into svcmon (%s) values (%s) on duplicate key update %s""" % (','.join(vars), ','.join(vals), ','.join(upd))
    log.info(sql)
    db.query(sql)
    return 0

port = 8000
host = "unxdevweb"

server = SimpleXMLRPCServer((host, port))
log.info("Listening on %s:%d" % (host, port))
server.register_function(delete_services, "delete_services")
server.register_function(update_service, "update_service")
server.register_function(begin_action, "begin_action")
server.register_function(end_action, "end_action")
server.register_function(svcmon_update, "svcmon_update")
server.serve_forever()

