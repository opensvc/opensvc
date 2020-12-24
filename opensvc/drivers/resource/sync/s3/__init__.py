import datetime
import glob
import os

from foreign.six.moves import configparser as ConfigParser
from subprocess import *

import core.exceptions as ex
import core.status
from .. import Sync, notify
from core.objects.svcdict import KEYS
from utilities.proc import justcall, which

DRIVER_GROUP = "sync"
DRIVER_BASENAME = "s3"
KEYWORDS = [
    {
        "keyword": "snar",
        "at": True,
        "example": "/srv/mysvc/var/sync.1.snar",
        "text": "The GNU tar snar file full path. The snar file stored the GNU tar metadata needed to do an incremental tarball. If the service fails over shared disks the snar file should be stored there, so the failover node can continue the incremental cycle."
    },
    {
        "keyword": "src",
        "convert": "list",
        "at": True,
        "required": True,
        "example": "/srv/mysvc/tools /srv/mysvc/apps*",
        "text": "Source globs as passed as paths to archive to a tar command."
    },
    {
        "keyword": "options",
        "convert": "shlex",
        "at": True,
        "example": "--exclude *.pyc",
        "text": "Options passed to GNU tar for archiving."
    },
    {
        "keyword": "bucket",
        "at": True,
        "required": True,
        "example": "opensvc-myapp",
        "text": "The name of the S3 bucket to upload the backup to."
    },
    {
        "keyword": "full_schedule",
        "at": True,
        "example": "@1441 sun",
        "text": "The schedule of full backups. :c-action:`sync_update` actions are triggered according to the resource :kw:`schedule` parameter, and do a full backup if the current date matches the :kw:`full_schedule` parameter or an incremental backup otherwise."
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)

def driver_capabilities(node=None):
    from utilities.proc import which
    data = []
    if which("gof3r") and which("tar"):
        data.append("sync.s3")
    return data


class SyncS3(Sync):
    def __init__(self,
                 src=None,
                 options=None,
                 bucket=None,
                 snar=None,
                 full_schedule="@1441 sun",
                 **kwargs):
        super(SyncS3, self).__init__(type="sync.s3", **kwargs)
        if options is None:
            options = []
        if src is None:
            src = []
        self.label = "s3 backup"
        self.src = src
        self.bucket = bucket
        self.options = options
        self.full_schedule = full_schedule
        self.snar = snar

    def __str__(self):
        return "%s src=%s bucket=%s" % (
            super(SyncS3, self).__str__(),
            self.src,
            self.bucket
        )

    def on_add(self):
        if self.svc.namespace:
            self.prefix = "/".join(("", self.svc.namespace.lower(), self.svc.name, self.rid.replace("#",".")))
        else:
            self.prefix = "/".join(("", self.svc.name, self.rid.replace("#",".")))
        dst = "s3://"+self.bucket + self.prefix
        self.label += " to " + dst
        if self.snar is None:
            self.snar = os.path.join(self.var_d, self.rid.replace("#", "."))+".snar"

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
            raise ex.Error("key %s not found in bucket" % key)
        try:
            _d = datetime.datetime.strptime(e["date"], "%Y-%m-%d %H:%M:%S")
        except:
            raise ex.Error("undecodable date %s" % e["date"])
        return _d

    def _status(self, verbose=False):
        try:
            self.check_bin()
        except ex.Error as e:
            self.status_log(str(e))
            return core.status.WARN
        try:
            l = self.ls(refresh=True)
            n = self.get_n_incr()
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        if n is None:
            self.status_log("no backup found")
            return core.status.WARN

        if n > 0 and not os.path.exists(self.snar):
            self.status_log("snar file not found at %s" % self.snar)
            return core.status.WARN

        try:
            last = self.sync_date(n)
        except Exception as e:
            self.status_log(str(e))
            return core.status.WARN

        if self.sync_date(n) < datetime.datetime.now() - datetime.timedelta(seconds=self.sync_max_delay):
            self.status_log("last backup too old (%s)" % last.strftime("%Y-%m-%d %H:%M:%S"))
            return core.status.WARN

        self.status_log("last backup on %s" % last.strftime("%Y-%m-%d %H:%M:%S"))
        return core.status.UP

    def check_bin(self):
        if not which("gof3r"):
            raise ex.Error("could not find gof3r binary")
        if not which("tar"):
            raise ex.Error("could not find tar binary")

    def sync_full(self):
        self.check_bin()
        self.tar_full()

    @notify
    def sync_update(self):
        self.check_bin()
        self.tar()

    def ls(self, refresh=False):
        """
          list all saves in S3 for this resource
        """
        if not refresh and hasattr(self, "ls_cache"):
            return getattr(self, "ls_cache")
        cmd = ["aws", "s3", "ls", "s3://"+self.bucket+"/"+os.path.dirname(self.prefix)+"/"]
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
        aws_cf_f = "/root/.aws/config"
        try:
            aws_cf = ConfigParser.RawConfigParser()
            aws_cf.read(aws_cf_f)
        except:
            raise ex.Error("failed to load aws config at %s" % aws_cf_f)
        if hasattr(self.svc, "aws_profile"):
            profile = self.svc.aws_profile
        else:
            profile = "default"
        try:
            key = aws_cf.get(profile, "aws_access_key_id")
        except:
            raise ex.Error("aws_access_key_id not found in section %s of %s" % (profile, aws_cf_f))
        try:
            secret = aws_cf.get(profile, "aws_secret_access_key")
        except:
            raise ex.Error("aws_secret_access_key not found in section %s of %s" % (profile, aws_cf_f))
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
        from core.scheduler import Schedule, SchedNotAllowed, SchedSyntaxError
        now = datetime.datetime.now()
        try:
            sched = Schedule(self.full_schedule)
            return sched.validate(now)
        except SchedNotAllowed:
            return False
        except SchedSyntaxError as e:
            raise ex.Error(str(e))

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

    def do_tar_x(self, n=None):
        self.set_creds()
        paths = []
        cmd1 = ["gof3r", "get", "-b", self.bucket, "-k", self.sync_fullname(n)]
        p1 = Popen(cmd1, stdout=PIPE, stderr=PIPE)
        cmd2 = ["tar", "xzf", "-", "-g", self.snar, "-C", "/"]
        p2 = Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
        self.log.info(" ".join(cmd1) + " | " + " ".join(cmd2))
        out, err = p2.communicate()
        self.unset_creds()
        if len(out) > 0:
            self.log.info(out)
        if p2.returncode != 0:
            if len(err) > 0:
                self.log.error(err)

    def sync_restore(self):
        n = self.get_n_incr()
        for i in range(n):
            self.do_tar_x(i)
