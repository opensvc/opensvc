#
# Copyright (c) 2015 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os

from rcGlobalEnv import rcEnv
from rcUtilities import which, justcall
from subprocess import *
import rcExceptions as ex
import rcStatus
import time
import datetime
import resSync
import glob

class syncS3(resSync.Sync):
    def __init__(self,
                 rid=None,
                 src=[],
                 options=[],
                 bucket=None,
                 full_schedule="* sun",
                 sync_max_delay=None,
                 schedule=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 internal=False):
        resSync.Sync.__init__(self,
                              rid=rid, type="sync.s3",
                              sync_max_delay=sync_max_delay,
                              schedule=schedule,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

        self.label = "s3 backup"
        self.src = src
        self.bucket = bucket
        self.options = options
        self.full_schedule = full_schedule

    def on_add(self):
        self.prefix = "/" + self.svc.svcname + "/" + self.rid.replace("#",".")
        dst = "s3://"+self.bucket + self.prefix
        self.label += " to " + dst
        self.snar = os.path.join(rcEnv.pathvar, self.svc.svcname, self.rid.replace("#", "."))+".snar"

    def sync_basename(self, n):
        return os.path.basename(self.sync_fullname(n))

    def sync_fullname(self, n):
        s = self.prefix
        if n > 0:
            s += ".incr"+str(n)
        s += ".tar.gz"
        return s

    def sync_date(self, n):
        key = self.sync_basename(n)
        try:
            e = [ d for d in self.ls() if d["key"] == key ][0]
        except:
            raise ex.excError("key %s not found in bucket" % key)
        try:
            _d = datetime.datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S")
        except:
            raise ex.excError("undecodable date %s" % e["date"])
        return _d

    def _status(self, verbose=False):
        try:
            self.check_bin()
        except ex.excError as e:
            self.status_log(str(e))
            return rcStatus.WARN
        try:
            l = self.ls(refresh=True)
            n = self.get_n_incr()
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN
        
        if n is None:
            self.status_log("no backup found")
            return rcStatus.WARN
        
        if n > 0 and not os.path.exists(self.snar):
            self.status_log("snar file not found at %s" % self.snar)
            return rcStatus.WARN

        try:
            last = self.sync_date(n)
        except Exception as e:
            self.status_log(str(e))
            return rcStatus.WARN

        if self.sync_date(n) < datetime.datetime.now() - datetime.timedelta(minutes=self.sync_max_delay):
            self.status_log("last backup too old (%s)" % last.strftime("%Y-%m-%d %H:%M:%S"))
            return rcStatus.WARN

        self.status_log("last backup on %s" % last.strftime("%Y-%m-%d %H:%M:%S"))
        return rcStatus.UP

    def check_bin(self):
        if not which("gof3r"):
            raise ex.excError("could not find gof3r binary")
        if not which("tar"):
            raise ex.excError("could not find tar binary")

    def syncfullsync(self):
        self.check_bin()
        self.tar_full()

    def syncupdate(self):
        self.check_bin()
        self.tar()

    def ls(self, refresh=False):
        """
          list all saves in S3 for this resource
        """
        if not refresh and hasattr(self, "ls_cache"):
            return self.ls_cache
        cmd = ["aws", "s3", "ls", "s3://"+self.bucket+"/"+self.svc.svcname+"/"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return []
        l = []
        for line in out.split("\n"):
            v = line.split()
            if len(v) != 4:
                continue
            if v[2] == "PRE":
                continue
            if not v[-1].startswith(self.rid.replace("#", ".")):
                continue
            d = {
              'date': " ".join(v[:2]),
              'key': v[-1],
            }
            l.append(d)
        self.ls_cache = l
        return self.ls_cache

    def get_creds_from_aws(self):
        import ConfigParser
        aws_cf_f = "/root/.aws/config"
        try:
            aws_cf = ConfigParser.RawConfigParser()
            aws_cf.read(aws_cf_f)
        except:
            raise ex.excError("failed to load aws config at %s" % aws_cf_f)
        if hasattr(self.svc, "aws_profile"):
            profile = self.svc.aws_profile
        else:
            profile = "default"
        try:
            key = aws_cf.get(profile, "aws_access_key_id")
        except:
            raise ex.excError("aws_access_key_id not found in section %s of %s" % (profile, aws_cf_f))
        try:
            secret = aws_cf.get(profile, "aws_secret_access_key")
        except:
            raise ex.excError("aws_secret_access_key not found in section %s of %s" % (profile, aws_cf_f))
        return key, secret
        
    def set_creds(self):
        key, secret = self.get_creds_from_aws()
        os.environ["AWS_ACCESS_KEY_ID"] = key
        os.environ["AWS_SECRET_ACCESS_KEY"] = secret

    def unset_creds(self):
        if "AWS_ACCESS_KEY_ID" in os.environ:
            del(os.environ["AWS_ACCESS_KEY_ID"])
        if "AWS_SECRET_ACCESS_KEY" in os.environ:
            del(os.environ["AWS_SECRET_ACCESS_KEY"])

    def get_n_incr(self):
        l = self.ls()
        keys = sorted([d["key"] for d in l])
        n_incr = None
        full_found = False
        for i in range(len(keys)):
            last = keys[-(i+1)]
            if last == self.rid.replace("#", ".") + ".tar.gz":
                full_found = True
            v = last.split(".")
            if len(v) < 3:
                continue
            if v[-1] == "tar":
                incr = v[-2]
            elif v[-2] == "tar":
                incr = v[-3]
            else:
                continue
            if not incr.startswith("incr"):
                continue
            incr = incr.replace("incr", "")
            try:
                n_incr = int(incr)
                return n_incr
            except:
                continue
        if full_found:
            return 0
        return n_incr

    def remove_incr(self):
        cmd = ["aws", "s3", "rm"]
        keys = [ d["key"] for d in self.ls() ]
        for key in keys:
            if not key.startswith(os.path.basename(self.prefix) + ".incr"):
                continue
            self.vcall(cmd + ["s3://"+self.bucket+os.path.dirname(self.prefix)+"/"+key])

    def in_full_schedule(self):
        from rcScheduler import Scheduler, SchedNotAllowed, SchedSyntaxError
        sched = Scheduler()
        schedule = self.sched_get_schedule("dummy", "dummy", schedules=self.full_schedule)
        try:
            sched.in_schedule(schedule, now=datetime.datetime.now())
        except SchedNotAllowed:
            return False
        except SchedSyntaxError as e:
            raise ex.excError(str(e))
        return True

    def tar(self):
        n_incr = self.get_n_incr()
        if n_incr is None:
            self.log.info("first backup")
            self.tar_full()
        elif self.in_full_schedule():
            self.log.info("in schedule for a full backup")
            self.tar_full()
        else:
            self.tar_incr(n_incr+1)

    def tar_full(self):
        if os.path.exists(self.snar):
            self.log.info("full backup, removing snar file")
            self.log.info("rm " + self.snar)
            os.unlink(self.snar)
        else:
            self.log.info("full backup, no snar file found at %s" % self.snar)
        self.do_tar()
        self.remove_incr()

    def tar_incr(self, n):
        if os.path.exists(self.snar):
            self.log.info("incremental backup, using snar file")
        else:
            self.log.info("full backup, no snar file found at %s" % self.snar)
        self.do_tar(n=n)

    def do_tar(self, n=None):
        self.set_creds()
        paths = []
        for e in self.src:
            paths += glob.glob(e)
        cmd1 = ["tar", "czf", "-", "-g", self.snar] + self.options + paths
        p1 = Popen(cmd1, stdout=PIPE, stderr=PIPE)
        cmd2 = ["gof3r", "put", "-b", self.bucket, "-k", self.sync_fullname(n)]
        p2 = Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        self.log.info(" ".join(cmd1) + " | " + " ".join(cmd2))
        out, err = p2.communicate()
        self.unset_creds()
        if len(out) > 0:
            self.log.info(out)
        if p2.returncode != 0:
            if len(err) > 0:
                self.log.error(err)

    def __str__(self):
        return "%s src=%s bucket=%s" % (resSync.Sync.__str__(self), str(self.src), str(self.bucket))

