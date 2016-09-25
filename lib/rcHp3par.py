from __future__ import print_function
import os
import json
import rcExceptions as ex
from subprocess import *
import time
import urllib
import urllib2
from rcGlobalEnv import rcEnv
from rcUtilities import cache, clear_cache, justcall
import re
import datetime

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

if rcEnv.pathbin not in os.environ['PATH']:
    os.environ['PATH'] += ":"+rcEnv.pathbin

def reformat(s):
    lines = s.split('\n')
    for i, line in enumerate(lines):
        if '%' in line:
            # skip prompt
            x = line.index("%") + 2
            if x < len(line):
                line = line[x:]
            elif x == len(line):
                line = ""
        lines[i] = line
    s = '\n'.join(lines)
    s = s.replace("Pseudo-terminal will not be allocated because stdin is not a terminal.", "")
    return s.strip()

class Hp3pars(object):
    allowed_methods = ("ssh", "proxy", "cli")

    def __init__(self, objects=[], log=None):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.arrays = []
        self.index = 0
        if not os.path.exists(rcEnv.authconf):
            raise ex.excError("%s not found" % rcEnv.authconf)
        conf = ConfigParser.RawConfigParser()
        conf.read(rcEnv.authconf)
        m = {}

        for s in conf.sections():
            if not conf.has_option(s, "type") or \
               conf.get(s, "type") != "hp3par":
                continue

            if self.filtering and not s in self.objects:
                continue

            username = None
            manager = None
            key = None
            pwf = None

            try:
                method = conf.get(s, 'method')
            except:
                method = "ssh"

            if method not in self.allowed_methods:
                print("invalid method. allowed methods: %s" % ', '.join(self.allowed_methods))
                continue

            kwargs = {"log": log}
            try:
                manager = conf.get(s, 'manager')
                kwargs['manager'] = manager
            except Exception as e:
                kwargs['manager'] = s

            try:
                username = conf.get(s, 'username')
                key = conf.get(s, 'key')
                kwargs['username'] = username
                kwargs['key'] = key
            except Exception as e:
                if method in ("ssh"):
                    raise

            try:
                pwf = conf.get(s, 'pwf')
                kwargs['pwf'] = pwf
            except Exception as e:
                if method in ("cli"):
                    raise

            try:
                cli = conf.get(s, 'cli')
                kwargs['cli'] = cli
            except Exception as e:
                pass

            self.arrays.append(Hp3par(s, method, **kwargs))

        del(conf)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Hp3par(object):
    def __init__(self, name, method, manager=None, username=None, key=None, pwf=None, cli="cli", svcname="", log=None):
        self.name = name
        self.manager = manager
        self.method = method
        self.username = username
        self.pwf = pwf
        self.cli = cli
        self.svcname = svcname
        self.key = key
        self.keys = ['showvv', 'showsys', 'shownode', "showcpg", "showport", "showversion"]
        self.uuid = None
        self.remotecopy = None
        self.virtualcopy = None
        self.log = log
        self.cache_sig_prefix = "hp3par."+self.manager+"."

    def ssh_cmd(self, cmd, log=False):
        _cmd = ['ssh', '-i', self.key, '@'.join((self.username, self.manager))]
        cmd = 'setclienv csvtable 1 ; setclienv nohdtot 1 ; ' + cmd + ' ; exit'
        return self._rcmd(_cmd, cmd, log=log)

    def proxy_cmd(self, cmd, log=False):
        url = 'https://%s/api/cmd/' % self.manager
        user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
        header = { 'User-Agent' : user_agent }

        values = {
          'array' : self.name,
          'cmd' : cmd,
          'svcname' : self.svcname,
          'uuid' : self.uuid,
        }

        data = urllib.urlencode(values)
        req = urllib2.Request(url, data, header)

        try:
            f = urllib2.build_opener().open(req)
            response = f.read()
            #response = urllib2.urlopen(req)
        except Exception as e:
            return "", str(e)

        try:
            d = json.loads(response)
            ret = d['ret']
            out = d['out']
            err = d['err']
        except:
            ret = 1
            out = ""
            err = "unexpected proxy response format (not json)"

        if ret != 0:
            raise ex.excError("proxy error: %s" % err)

        return out, err

    def _rcmd(self, _cmd, cmd, log=False, retry=10):
        p = Popen(_cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
        p.stdin.write(cmd)
        out, err = p.communicate()
        out = reformat(out)
        err = reformat(err)

        if p.returncode != 0:
            if ("Connection closed by remote host" in err or "Too many local CLI connections." in err) and retry > 0:
                if log:
                    self.log.info("3par connection refused. try #%d" % retry)
                time.sleep(1)
                return self._rcmd(_cmd, cmd, log=log, retry=retry-1)
            if log:
                if len(out) > 0: self.log.info(out)
                if len(err) > 0: self.log.error(err)
            else:
                print(cmd)
                print(out)
            raise ex.excError("3par command execution error")

        return out, err

    def cli_cmd(self, cmd, log=False):
        os.environ["TPDPWFILE"] = self.pwf
        os.environ["TPDNOCERTPROMPT"] = "1"
        cmd = [self.cli, '-sys', self.name, '-nohdtot', '-csvtable'] + cmd.split()

        if log:
            s = " ".join(cmd)
            s = re.sub(r'password \w+', 'password xxxxx', s)
            self.log.info(s)

        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        out = reformat(out)
        err = reformat(err)

        if p.returncode != 0:
            if ("Connection closed by remote host" in err or "Too many local CLI connections." in err) and retry > 0:
                if log:
                    self.log.info("3par connection refused. try #%d" % retry)
                time.sleep(1)
                return self._rcmd(_cmd, cmd, log=log, retry=retry-1)
            if log:
                if len(out) > 0: self.log.info(out)
                if len(err) > 0: self.log.error(err)
            else:
                print(' '.join(cmd))
                print(out)
            raise ex.excError("3par command execution error")

        return out, err

    def get_uuid(self):
        if self.uuid is not None:
            return self.uuid
        config = ConfigParser.RawConfigParser()
        config.read(nodeconf)
        try:
            self.uuid = config.get("node", "uuid")
        except:
            pass
        return self.uuid

    def rcmd(self, cmd, log=False):
        if self.method == "ssh":
            return self.ssh_cmd(cmd, log=log)
        elif self.method == "cli":
            return self.cli_cmd(cmd, log=log)
        elif self.method == "proxy":
            self.get_uuid()
            return self.proxy_cmd(cmd, log=log)
        else:
            raise ex.excError("unsupported method %s set in auth.conf for array %s" % (self.method, self.name))

    def serialize(self, s, cols):
        json.dumps(self.csv_to_list_of_dict(s, cols))

    def csv_to_list_of_dict(self, s, cols):
        l = []
        for line in s.splitlines():
            v = line.strip().split(',')
            h = {}
            for a, b in zip(cols, v):
                h[a] = b
            if len(h) > 1:
                l.append(h)
        return l

    @cache("has_virtualcopy")
    def has_virtualcopy(self):
        if self.virtualcopy is not None:
            return self.virtualcopy
        cmd = 'showlicense'
        s = self.rcmd(cmd)[0].strip("\n")
        self.virtualcopy = False
        for line in s.split('\n'):
            if "Virtual Copy" in line:
                self.virtualcopy = True
        return self.virtualcopy

    @cache("has_remotecopy")
    def has_remotecopy(self):
        if self.remotecopy is not None:
            return self.remotecopy
        cmd = 'showlicense'
        s = self.rcmd(cmd)[0].strip("\n")
        self.remotecopy = False
        for line in s.split('\n'):
            if "Remote Copy" in line:
                self.remotecopy = True
        return self.remotecopy

    def get_showvv(self):
        if self.has_remotecopy():
            cols = ["Name", "VV_WWN", "Prov", "CopyOf", "Tot_Rsvd_MB", "VSize_MB", "UsrCPG", "CreationTime", "RcopyGroup", "RcopyStatus"]
        else:
            cols = ["Name", "VV_WWN", "Prov", "CopyOf", "Tot_Rsvd_MB", "VSize_MB", "UsrCPG", "CreationTime"]
        cmd = 'showvv -showcols ' + ','.join(cols)
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def updatevv(self, vvnames=None, log=False):
        cmd = 'updatevv -f'
        if vvnames is None or len(vvnames) == 0:
            raise ex.excError("updatevv: no vv names specified")
        if vvnames:
            cmd += ' ' + ' '.join(vvnames)
        s = self.rcmd(cmd, log=log)[0]

    def showvv(self, vvnames=None, vvprov=None, cols=None):
        fdata = []
        data = self._showvv()
        for d in data:
            if vvnames and d["Name"] not in vvnames:
                continue
            if vvprov and d["Prov"] != vvprov:
                continue
            fdata.append(d)
        return fdata

    @cache("showvv")
    def _showvv(self):
        cols = ["Name", "CreationTime", "Prov"]
        cmd = 'showvv -showcols ' + ','.join(cols)
        out, err = self.rcmd(cmd)
        return self.csv_to_list_of_dict(out, cols)

    def showrcopy(self, rcg):
        """
        Remote Copy System Information
        Status: Started, Normal

        Group Information

        Name        ,Target    ,Status  ,Role      ,Mode    ,Options
        RCG.SVCTEST1,baie-pra,Started,Primary,Periodic,"Last-Sync 2014-03-05 10:19:42 CET , Period 5m, auto_recover,over_per_alert"
         ,LocalVV     ,ID  ,RemoteVV    ,ID  ,SyncStatus   ,LastSyncTime
         ,LXC.SVCTEST1.DATA01,2706,LXC.SVCTEST1.DATA01,2718,Synced,2014-03-05 10:19:42 CET
         ,LXC.SVCTEST1.DATA02,2707,LXC.SVCTEST1.DATA02,2719,Synced,2014-03-05 10:19:42 CET

        """
        out, err = self._showrcopy()

        if len(out) == 0:
            raise ex.excError("unable to fetch rcg status")

        lines = []
        cols_rcg = ["Name", "Target", "Status", "Role", "Mode"]
        cols_vv = ["LocalVV", "ID", "RemoteVV", "ID", "SyncStatus", "LastSyncTime"]

        # extract rcg block
        in_block = False
        for line in out.splitlines():
            if not in_block:
                if not line.startswith(rcg+","):
                    continue
                lines.append(line)
                in_block = True
            else:
                if not line.startswith(" "):
                    break
                lines.append(line)

        if len(lines) == 0:
            raise ex.excError("rcg does not exist")

        # RCG status
        rcg_s = lines[0]
        options_start = rcg_s.index('"')
        rcg_options = rcg_s[options_start+1:-1].split(",")
        rcg_options = map(lambda x: x.strip(), rcg_options)
        rcg_v = rcg_s[:options_start].split(",")
        rcg_data = {}
        for a, b in zip(cols_rcg, rcg_v):
            rcg_data[a] = b
        rcg_data["Options"] = rcg_options

        # VV status
        vv_l = []
        for line in lines[1:]:
            v = line.strip().strip(",").split(",")
            if len(v) != len(cols_vv):
                continue
            vv_data = {}
            for a, b in zip(cols_vv, v):
                vv_data[a] = b
            vv_data['LastSyncTime'] = self.s_to_datetime(vv_data['LastSyncTime'])
            vv_l.append(vv_data)
        data = {'rcg': rcg_data, 'vv': vv_l}
        return data

    def s_to_datetime(self, s):
        out, err, ret = justcall(["date", "--utc", "--date=%s" % s, '+%Y-%m-%d %H:%M:%S'])
        d = datetime.datetime.strptime(out.strip(), "%Y-%m-%d %H:%M:%S")
        return d

    @cache("showrcopy_groups")
    def _showrcopy(self):
        cmd = 'showrcopy groups'
        out, err = self.rcmd(cmd)
        return out, err

    def clear_showrcopy_cache(self):
        clear_cache("showrcopy_groups", o=self)

    def clear_caches(self):
        clear_cache("showvv", o=self)

    def get_showsys(self):
        cols = ["ID", "Name", "Model", "Serial", "Nodes", "Master", "TotalCap", "AllocCap", "FreeCap", "FailedCap"]
        cmd = 'showsys'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_shownode(self):
        cols = ["Available_Cache", "Control_Mem", "Data_Mem", "InCluster", "LED", "Master", "Name", "Node", "State"]
        cmd = 'shownode -showcols ' + ','.join(cols)
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showcpg(self):
        cols = ["Id", "Name", "Warn%", "VVs", "TPVVs", "Usr", "Snp", "Total", "Used", "Total", "Used", "Total", "Used"]
        cmd = 'showcpg'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showport(self):
        cols = ["N:S:P", "Mode", "State", "Node_WWN", "Port_WWN", "Type", "Protocol", "Label", "Partner", "FailoverState"]
        cmd = 'showport'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0]
        return self.serialize(s, cols)

    def get_showversion(self):
        cmd = 'showversion -s'
        print("%s: %s"%(self.name, cmd))
        s = self.rcmd(cmd)[0].strip("\n")
        return json.dumps({"Version": s})

if __name__ == "__main__":
    o = Hp3pars()
    for hp3par in o:
        print(hp3par.get_showvv())
        print(hp3par.get_showsys())
        print(hp3par.get_shownode())
        print(hp3par.get_showcpg())
