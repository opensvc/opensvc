import os
import glob
import json
import lock
import resDisk
import rcExceptions as ex
from rcGlobalEnv import rcEnv
from rcUtilities import justcall, qcall, which, lazy, cache
from rcZfs import zpool_devs, zpool_getprop, zpool_setprop
from converters import convert_duration


class Disk(resDisk.Disk):
    """
    Zfs pool resource driver.
    """
    def __init__(self,
                 rid=None,
                 name=None,
                 multihost=None,
                 **kwargs):
        resDisk.Disk.__init__(self,
                              rid=rid,
                              name=name,
                              type='disk.zpool',
                              **kwargs)
        self.multihost = multihost
        self.label = 'zpool ' + name if name else "<undefined>"

    def _info(self):
        data = [
          ["name", self.name],
        ]
        return data

    @lazy
    def sub_devs_name(self):
        return os.path.join(self.var_d, 'sub_devs')

    def files_to_sync(self):
        return [self.sub_devs_name]

    def presync(self):
        """ this one is exported as a service command line arg
        """
        if not self.has_it():
            return
        dl = self._sub_devs()
        with open(self.sub_devs_name, 'w') as f:
            f.write(json.dumps(list(dl)))

    def has_it(self):
        """Returns True if the pool is present
        """
        if not which("zpool"):
            raise ex.excError("zpool command not found")
        ret = qcall(['zpool', 'list', self.name])
        if ret == 0 :
            return True
        return False

    @cache("zpool.health.{args[1]}")
    def zpool_health(self, name):
        cmd = ["zpool", "list", "-H", "-o", "health", name]
        out, err, ret = justcall(cmd)
        return out.strip()

    def is_up(self):
        """Returns True if the pool is present and activated
        """
        if not self.has_it():
            return False
        state = self.zpool_health(self.name)
        if state == "ONLINE":
            return True
        elif state in ("SUSPENDED", "DEGRADED"):
            self.status_log(state.lower())
            return True
        return False

    @lazy
    def zpool_cache(self):
        return os.path.join(rcEnv.paths.pathvar, 'zpool.cache')

    def import_pool_no_cachefile(self, verbose=True):
        cmd = ['zpool', 'import', '-f', '-o', 'cachefile='+self.zpool_cache,
               self.name]
        if verbose:
            return self.vcall(cmd, errlog=verbose)
        else:
            return self.call(cmd, errlog=verbose)

    def import_pool_cachefile(self, verbose=True):
        devzp = os.path.join(self.var_d, 'dev', 'dsk')
        if not os.path.isdir(devzp):
            return 1, "", ""
        cmd = ['zpool', 'import', '-f', '-o', 'cachefile='+self.zpool_cache,
               '-d', devzp, self.name]
        if verbose:
            return self.vcall(cmd, errlog=verbose)
        else:
            return self.call(cmd, errlog=verbose)

    def import_pool(self, verbose=True):
        self.lock()
        try:
            ret, _, _ = self.import_pool_cachefile(verbose=verbose)
            if ret == 0:
                return ret
            if verbose:
                self.log.info("import fallback without dev cache")
            ret, _, _ = self.import_pool_no_cachefile(verbose=verbose)
            self.can_rollback = True
        finally:
            self.unlock()
        return ret

    def do_start(self):
        if self.is_up():
            self.log.info("%s is already up" % self.name)
            return 0
        self.zgenhostid()
        ret = self.import_pool()
        if ret != 0:
            raise ex.excError("failed to import pool")
        self.can_rollback = True
        self.set_multihost()

    def do_stop(self):
        if not self.is_up():
            self.log.info("%s is already down" % self.name)
            return 0
        cmd = ["zpool", "export", self.name]
        ret, out, err = self.vcall(cmd, err_to_warn=True)
        if ret != 0:
            cmd = ["zpool", "export", "-f", self.name]
            ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def sub_devs(self):
        if self.is_up():
            self.log.debug("resource up ... refresh sub devs cache")
            self.presync()
        elif not os.path.exists(self.sub_devs_name):
            self.log.debug("no sub devs cache file and service not up ... unable to evaluate sub devs")
            return set()
        with open(self.sub_devs_name, 'r') as f:
            buff = f.read()
        try:
            dl = set(json.loads(buff))
        except:
            self.log.error("corrupted sub devs cache file %s"%self.sub_devs_name)
            raise ex.excError
        dl = self.remap_cached_sub_devs_controller(dl)
        return dl

    def remap_cached_sub_devs_controller(self, dl):
        if rcEnv.sysname != "SunOS":
            return dl
        mapping = self.get_wwn_map()
        vdl = []
        for d in dl:
            if os.path.exists(d):
                vdl.append(d)
                continue
            if len(d) < 36 or not d.endswith("s2"):
                self.log.debug("no remapping possible for disk %s. keep as is." % d)
                vdl.append(d)
                continue
            wwid = d[-36:-2]
            if len(wwid) in (18, 26, 34) and wwid.endswith("d0"):
                wwid = wwid[:-2]
                d0 = "d0"
            else:
                d0 = ""
            l = glob.glob("/dev/rdsk/*"+wwid+d0+"s2")
            if len(l) != 1:
                # may be the disk is a R2 mirror member, with a different wwn than R1
                if wwid in mapping:
                    wwid = mapping[wwid]
                    l = glob.glob("/dev/rdsk/*"+wwid+d0+"s2")
            if len(l) != 1:
                self.log.warning("discard disk %s from sub devs cache: "
                                 "not found", wwid)
                continue
            self.log.debug("remapped device %s to %s" % (d, l[0]))
            vdl.append(l[0])
        return set(vdl)

    def get_wwn_map(self):
        mapping = {}
        wwn_maps = glob.glob(os.path.join(self.svc.var_d, "*", "wwn_map"))
        for fpath in wwn_maps:
            try:
                with open(fpath, "r") as filep:
                    _mapping = json.load(filep)
            except ValueError:
                pass
            else:
                for (r1, r2) in _mapping:
                    mapping[r1] = r2
                    mapping[r2] = r1
        return mapping

    def _sub_devs(self):
        """
        Search zpool vdevs from the output of "zpool status poolname" if
        imported.
        """
        return set(zpool_devs(self.name, self.svc.node))

    def zgenhostid(self):
        if self.multihost and not os.path.exists("/etc/hostid"):
            try:
                justcall(["zgenhostid"])
            except Exception:
                self.log.warning("/etc/hostid does not exist and zgenhostid is not installed")

    def set_multihost(self):
        if self.multihost is None:
            # don't care
            return
        current = zpool_getprop(self.name, "multihost")
        if current is "":
            # not multihost capable (pre-0.7) or not imported
            return
        ret = 0
        if self.multihost is True:
            if current == "off":
                ret = zpool_setprop(self.name, "multihost", "on", log=self.log)
            else:
                self.log.info("multihost is already on")
        elif self.multihost is False:
            if current == "on":
                ret = zpool_setprop(self.name, "multihost", "off", log=self.log)
            else:
                self.log.info("multihost is already off")
        if ret != 0:
            raise ex.excError

    def lock(self):
        """
        Acquire the zpool disk lock
        """
        timeout = convert_duration(self.svc.options.waitlock)
        if timeout < 0:
            timeout = 120
        delay = 1
        lockfd = None
        action = "startdiskzpool"
        lockfile = os.path.join(rcEnv.paths.pathlock, action)
        details = "(timeout %d, delay %d, action %s, lockfile %s)" % \
                  (timeout, delay, action, lockfile)
        self.log.debug("acquire startdiskzpool lock %s", details)

        try:
            lockfd = lock.lock(timeout=timeout, delay=delay, lockfile=lockfile, intent="startdiskzpool")
        except lock.LockTimeout as exc:
            raise ex.excError("timed out waiting for lock %s: %s" % (details, str(exc)))
        except lock.LockNoLockFile:
            raise ex.excError("lock_nowait: set the 'lockfile' param %s" % details)
        except lock.LockCreateError:
            raise ex.excError("can not create lock file %s" % details)
        except lock.LockAcquire as exc:
            raise ex.excError("another action is currently running %s: %s" % (details, str(exc)))
        except ex.excSignal:
            raise ex.excError("interrupted by signal %s" % details)
        except Exception as exc:
            self.save_exc()
            raise ex.excError("unexpected locking error %s: %s" % (details, str(exc)))

        if lockfd is not None:
            self.lockfd = lockfd

    def unlock(self):
        """
        Release the zpool disk lock.
        """
        lock.unlock(self.lockfd)
        self.log.debug("release startdiskzpool lock")
