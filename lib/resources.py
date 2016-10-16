import os
import rcExceptions as exc
import rcStatus
import logging
import rcUtilities
import sys
import time
import shlex
from rcGlobalEnv import rcEnv

allow_action_with_noaction = [
  "presync",
]

class Resource(object):
    """Define basic resource
    properties: type, optional=optional, disabled=disabled, tags=tags
    a Resource should provide do_action(action):
    with action into (start/stop/status)
    """
    label = None

    def __init__(self,
                 rid=None,
                 type=None,
                 subset=None,
                 optional=False,
                 disabled=False,
                 monitor=False,
                 restart=0,
                 tags=set([]),
                 always_on=set([])):
        self.rid = rid
        self.tags = tags
        self.type = type
        self.subset = subset
        self.optional = optional
        self.disabled = disabled
        self.skip = False
        self.monitor = monitor
        self.nb_restart = restart
        self.log = logging.getLogger(self.log_label())
        self.rstatus = None
        self.always_on = always_on
        if self.label is None: self.label = type
        self.status_log_str = ""
        self.can_rollback = False

    def fmt_info(self, keys=[]):
        for i, e in enumerate(keys):
            if len(e) == 2:
                keys[i] = [self.svc.svcname, self.svc.node.nodename, self.svc.clustertype, self.rid] + e
            elif len(e) == 3:
                keys[i] = [self.svc.svcname, self.svc.node.nodename, self.svc.clustertype] + e
        return keys

    def log_label(self):
        s = ""
        if hasattr(self, "svc"):
            s += self.svc.svcname + '.'

        if self.rid is None:
            s += self.type
            return s

        if self.subset is None:
            s += self.rid
            return s

        v = self.rid.split('#')
        if len(v) != 2:
            s += rid
            return s

        s += "%s:%s#%s" % (self.type.split(".")[0], self.subset, v[1])
        return s

    def __str__(self):
        output="object=%s rid=%s type=%s" % (self.__class__.__name__,
                                             self.rid, self.type)
        if self.optional : output+=" opt="+str(self.optional)
        if self.disabled : output+=" disa="+str(self.disabled)
        return output

    def __cmp__(self, other):
        """resources needed to be started or stopped in a specific order
        should redefine that.
        """
        return cmp(self.rid, other.rid)

    def print_exc(self):
        import traceback
        try:
            self.log.error(traceback.format_exc())
        except:
            self.log.error("unexpected error")
            traceback.print_exc()

    def save_exc(self):
        import traceback
        try:
            import tempfile
            import datetime
            now = str(datetime.datetime.now()).replace(' ', '-')
            f = tempfile.NamedTemporaryFile(dir=rcEnv.pathtmp,
                                            prefix='exc-')
            f.close()
            f = open(f.name, 'w')
            traceback.print_exc(file=f)
            self.log.error("unexpected error. stack saved in %s"%f.name)
            f.close()
        except:
            self.log.error("unexpected error")
            traceback.print_exc()

    def setup_environ(self):
        """ setup environement variables for use by triggers and startup
            scripts. This method needs defining in each class with their
            class variable.
            Env vars names should, by convention, be prefixed by OPENSVC_
        """
        pass

    def is_optional(self): return self.optional
    def is_disabled(self): return self.disabled

    def set_optional(self): self.optional=True
    def unset_optional(self): self.optional=False

    def disable(self): self.disabled=True
    def enable(self):  self.disabled=False

    def action_triggers(self, type, action, blocking=False):
        attr = type+"_"+action
        if hasattr(self, attr):
            cmd = getattr(self, attr)
            cmdv = shlex.split(cmd)

            if self.svc.options.dry_run:
                self.log.info("exec trigger %s" % getattr(self, attr))
                return
            ret, out, err = self.vcall(cmdv)
            if blocking and ret != 0:
                raise exc.excError("%s trigger %s error" % (type, cmd))

    def action_main(self, action):
        if self.svc.options.dry_run:
            if self.rset.parallel:
                header = "+ "
            else:
                header = ""
            self.log.info("%s%s %s"%(header, action, self.label))
            return
        getattr(self, action)()

    def do_action(self, action):
        self.log.debug('do_action: action=%s res=%s'%(action, self.rid))
        if hasattr(self, action):
            if "stop" in action and rcEnv.nodename in self.always_on and not self.svc.force:
                standby_action = action+'standby'
                if hasattr(self, standby_action):
                    self.action_main(standby_action)
                    return
                else:
                    self.log.info("skip '%s' on standby resource (--force to override)"%action)
                    return
            self.setup_environ()
            self.action_triggers("pre", action)
            self.action_triggers("blocking_pre", action, blocking=True)
            self.action_main(action)
            self.action_triggers("post", action)
            self.action_triggers("blocking_post", action, blocking=True)
            if not self.svc.options.dry_run and \
               ("start" in action or "stop" in action or "rollback" in action or "sync" in action or action in ("provision", "install", "create", "switch", "migrate")):
                """ refresh resource status cache after changing actions
                """
                self.status(refresh=True, restart=False)
            return

        """Every class inheriting resource should define start() stop() status()
        Alert on these minimal implementation requirements
        """
        if action in ("start","stop","status") :
            raise exc.excUndefined(action,self.__class__.__name__,\
                                    "Resource.do_action")

    def action(self, action=None):
        """ action() try to call do_action() on self
        return if action is not None or if self is disabled
        return status depends on optional property value:
        if self is optional then return True
        else return do_action() return value
        """
        if not self.rid == "app" and not self.svc.encap and 'encap' in self.tags:
            self.log.debug('skip encap resource action: action=%s res=%s'%(action, self.rid))
            return

        if 'noaction' in self.tags and \
           not hasattr(self, "delayed_noaction") and \
           not action in allow_action_with_noaction:
            self.log.debug('skip resource action (noaction tag): action=%s res=%s'%(action, self.rid))
            return

        self.log.debug('action: action=%s res=%s'%(action, self.rid))
        if action == None:
            self.log.debug('action: action cannot be None')
            return True
        if self.skip and (\
             action.startswith("start") or \
             action.startswith("stop") or \
             action.startswith("sync") or \
             action.startswith("_pg_") or \
             action == "provision"
           ):
            self.log.debug('action: skip action on filtered-out resource')
            return True
        if self.disabled:
            self.log.debug('action: skip action on disabled resource')
            return True
        try :
            self.do_action(action)
        except exc.excUndefined as ex:
            print(ex)
            return False
        except exc.excError:
            if self.optional:
                pass
            else:
                raise

    def status_stdby(self, s):
        """ This function modifies the passed status according
            to this node inclusion in the always_on nodeset
        """
        if rcEnv.nodename not in self.always_on:
            return s
        if s == rcStatus.UP:
            return rcStatus.STDBY_UP
        elif s == rcStatus.DOWN:
            return rcStatus.STDBY_DOWN
        return s

    def _status(self, verbose=False):
        return rcStatus.UNDEF

    def force_status(self, s):
        self.rstatus = s
        self.status_log_str = "forced"
        self.write_status()

    def status(self, verbose=False, refresh=False, restart=True, ignore_nostatus=False):
        # refresh param: used by do_action() to force a res status re-eval
        # self.svc.options.refresh: used to purge disk cache
        if self.disabled:
            self.status_log("disabled")
            return rcStatus.NA

        if not ignore_nostatus and "nostatus" in self.tags:
            self.status_log("nostatus tag")
            return rcStatus.NA

        if self.rstatus is not None and not refresh:
            return self.rstatus

        last_status = self.load_status_last()

        if self.svc.options.refresh or refresh:
            self.purge_status_last()
        else:
            self.rstatus = last_status

        if self.rstatus is None or self.svc.options.refresh or refresh:
            self.status_log_str = ""
            self.rstatus = self._status(verbose)
            self.log.debug("refresh status: %s => %s" % (rcStatus.status_str(last_status), rcStatus.status_str(self.rstatus)))
            self.write_status()

        if restart:
            self.do_restart(last_status)

        return self.rstatus

    def do_restart(self, last_status):
        restart_last_status = (
          rcStatus.UP,
          rcStatus.STDBY_UP,
          rcStatus.STDBY_UP_WITH_UP,
          rcStatus.STDBY_UP_WITH_DOWN
        )
        no_restart_status = (
          rcStatus.UP,
          rcStatus.STDBY_UP,
          rcStatus.NA,
          rcStatus.UNDEF,
          rcStatus.STDBY_UP_WITH_UP,
          rcStatus.STDBY_UP_WITH_DOWN,
          rcStatus.TODO
        )
        if self.nb_restart == 0:
            return
        if self.rstatus in no_restart_status:
            return
        if last_status not in restart_last_status:
            self.status_log("not restarted because previous status is %s" % rcStatus.status_str(last_status))
            return

        if not hasattr(self, 'start'):
            self.log.error("resource restart configured on resource %s with no 'start' action support"%self.rid)
            return

        if self.svc.frozen():
            s = "resource restart skipped: service is frozen"
            self.log.info(s)
            self.status_log(s)
            return

        for i in range(self.nb_restart):
            try:
                self.log.info("restart resource %s. try number %d/%d"%(self.rid, i+1, self.nb_restart))
                self.action("start")
            except Exception as e:
                self.log.error("restart resource failed: " + str(e))
            self.rstatus = self._status()
            self.write_status()
            if self.rstatus == rcStatus.UP:
                self.log.info("monitored resource %s restarted."%self.rid)
                return
            if i + 1 < self.nb_restart:
                time.sleep(1)
        return

    def write_status(self):
        self.write_status_last()
        self.write_status_history()

    def fpath_status_last(self):
        dirname = os.path.join(rcEnv.pathvar, self.svc.svcname)
        fname = "resource.status.last." + self.rid
        fpath = os.path.join(dirname, fname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return fpath

    def fpath_status_history(self):
        dirname = os.path.join(rcEnv.pathvar, self.svc.svcname)
        fname = "resource.status.history." + self.rid
        fpath = os.path.join(dirname, fname)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return fpath

    def purge_status_last(self):
        try:
            os.unlink(self.fpath_status_last())
        except:
            pass

    def load_status_last(self):
        try:
            with open(self.fpath_status_last(), 'r') as f:
                lines = f.read().split("\n")
                status_str = lines[0]
                s = rcStatus.status_value(status_str)
                if len(lines) > 1:
                    self.status_log_str = '\n'.join(lines[1:])
        except:
            s = None
        return s

    def write_status_last(self):
        with open(self.fpath_status_last(), 'w') as f:
            s = rcStatus.status_str(self.rstatus)+'\n'
            if len(self.status_log_str) > 1:
                s += self.status_log_str+'\n'
            f.write(s)

    def write_status_history(self):
        fpath = self.fpath_status_history()
        try:
            with open(fpath, 'r') as f:
                lines = f.readlines()
                last = lines[-1].split(" | ")[-1].strip("\n")
        except:
            last = None
        current = rcStatus.status_str(self.rstatus)
        if current == last:
            return
        import logging
        log = logging.getLogger("status_history")
        logformatter = logging.Formatter("%(asctime)s | %(message)s")
        logfilehandler = logging.handlers.RotatingFileHandler(
          fpath,
          maxBytes=512000,
          backupCount=1,
        )
        logfilehandler.setFormatter(logformatter)
        log.addHandler(logfilehandler)
        log.error(current)
        logfilehandler.close()
        log.removeHandler(logfilehandler)

    def status_log(self, text):
        msg = "# " + text + "\n"
        if msg in self.status_log_str:
            return
        self.status_log_str += msg

    def status_quad(self):
        r = self.status(verbose=True)
        if 'encap' in self.tags:
            encap = True
        else:
            encap = False
        return (self.rid,
                self.type,
                rcStatus.status_str(r),
                self.label,
                self.status_log_str,
                self.monitor,
                self.disabled,
                self.optional,
                encap)

    def call(self, cmd=['/bin/false'], cache=False, info=False,
             errlog=True, err_to_warn=False, err_to_info=False,
             warn_to_info=False, outlog=False):
        """Use subprocess module functions to do a call
        """
        return rcUtilities.call(cmd, log=self.log,
                                cache=cache,
                                info=info, errlog=errlog,
                                err_to_warn=err_to_warn,
                                err_to_info=err_to_info,
                                warn_to_info=warn_to_info,
                                outlog=outlog)

    def vcall(self, cmd, err_to_warn=False, err_to_info=False,
              warn_to_info=False):
        """Use subprocess module functions to do a call and
        log the command line using the resource logger
        """
        return rcUtilities.vcall(cmd, log=self.log,
                                 err_to_warn=err_to_warn,
                                 err_to_info=err_to_info,
                                 warn_to_info=warn_to_info)

    def wait_for_fn(self, fn, tmo, delay, errmsg="Waited too long for startup"):
        for tick in range(tmo//delay):
            if fn():
                return
            time.sleep(delay)
        raise exc.excError(errmsg)

    def devlist(self):
        return self.disklist()

    def disklist(self):
        """List disks the resource holds. Some resource have none,
        and can leave this function as is.
        """
        return set()

    def presync(self):
        pass

    def postsync(self):
        pass

    def provision(self):
        pass

    def files_to_sync(self):
        return []

    def rollback(self):
        if self.can_rollback:
            self.stop()

    def stop(self):
        pass

    def startstandby(self):
        if rcEnv.nodename in self.always_on:
             self.start()

    def start(self):
        pass

    def shutdown(self):
        self.stop()

    def _pg_freeze(self):
        return self._pg_freezer("freeze")

    def _pg_thaw(self):
        return self._pg_freezer("thaw")

    def _pg_kill(self):
        return self._pg_freezer("kill")

    def _pg_freezer(self, a):
        if hasattr(self, "svc"):
            create_pg = self.svc.create_pg
        else:
            create_pg = self.create_pg
        if not create_pg:
            return
        try:
            pg = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.log.info("process group are not supported on this platform")
            return
        except Exception as e:
            print(e)
            raise
        if a == "freeze":
            pg.freeze(self)
        elif a == "thaw":
            pg.thaw(self)
        elif a == "kill":
            pg.kill(self)

    def pg_frozen(self):
        if not self.svc.create_pg:
            return False
        try:
            pg = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.status_log("process group are not supported on this platform")
            return False
        return pg.frozen(self)

    def create_pg(self):
        if not self.svc.create_pg:
            return
        try:
            pg = __import__('rcPg'+rcEnv.sysname)
        except ImportError:
            self.log.info("process group are not supported on this platform")
            return
        except Exception as e:
            print(e)
            raise
        pg.create_pg(self)


class ResourceSet(Resource):
    """ Define Set of same type resources
    Example 1: ResourceSet("fs",[m1,m2])
    Example 2: r=ResourceSet("fs",[ip1])
    It define the resource type
    """
    def __init__(self,
                 type=None,
                 resources=[],
                 parallel=False,
                 optional=False,
                 disabled=False,
                 tags=set([])):
        self.parallel = parallel
        self.resources = []
        Resource.__init__(self,
                          type=type,
                          optional=optional,
                          disabled=disabled,
                          tags=tags)
        for r in resources:
            self += r

    def __cmp__(self, other):
        return cmp(self.type, other.type)

    def __iadd__(self,r):
        """Example 1 iadd another ResourceSet: R+=ResSet ... R+=[m1,m2]
        Example 2 : iadd a single Resource : R+=ip1
        """
        if isinstance(r,ResourceSet) :
            self.resources.extend(r.resources)
        elif isinstance(r,Resource) :
            """ Setup a back pointer to the resource set
            """
            r.rset = self
            self.resources.append(r)
            if hasattr(r, 'sort_rset'):
                r.sort_rset(self)
        return (self)

    def __str__(self):
        output="resSet %s [" % ( Resource.__str__(self) )
        for r in self.resources:
            output+= " (%s)" % (r.__str__())
        return "%s]" % (output)

    def pre_action(self, rset=None, action=None):
        if len(self.resources) == 0:
            return
        types_done = []
        for r in self.resources:
            if r.type in types_done:
                continue
            types_done.append(r.type)
            if not hasattr(r, "pre_action"):
                continue
            r.pre_action(self, action)

    def post_action(self, rset=None, action=None):
        if len(self.resources) == 0:
            return
        types_done = []
        for r in self.resources:
            if r.type in types_done:
                continue
            types_done.append(r.type)
            if not hasattr(r, "post_action"):
                continue
            r.post_action(self, action)

    def purge_status_last(self):
        for r in self.resources:
            r.purge_status_last()

    def status(self, verbose=False):
        """aggregate status a ResourceSet
        """
        s = rcStatus.Status()
        for r in self.resources:
            if r.is_disabled():
                continue
            if not r.svc.encap and 'encap' in r.tags:
                # don't evaluate encap service resources
                continue
            try:
                status = r.status()
            except:
                import sys
                import traceback
                e = sys.exc_info()
                print(e[0], e[1], traceback.print_tb(e[2]))

                status = rcStatus.NA

            s += status
        return s.status

    def tag_match(self, rtags, keeptags):
        if len(keeptags) == 0:
            return True
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def has_resource_with_types(self, l, strict=False):
        for r in self.resources:
            if r.type in l:
                return True
            if not strict and "." in r.type and r.type.split(".")[0] in l:
                return True
        return False

    def has_encap_resources(self):
        resources = [r for r in self.resources if self.tag_match(r.tags, set(['encap']))]
        if len(resources) == 0:
            return False
        return True

    def action(self, action=None, tags=set([]), xtags=set([])):
        """Call action on each resource of the ResourceSet
        """
        if self.parallel:
            # verify we can actually do parallel processing, fallback to serialized
            try:
                from multiprocessing import Process
                if rcEnv.sysname == "Windows":
                    import sys
                    import os
                    from multiprocessing import set_executable
                    set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
            except:
                self.parallel = False

        if len(xtags) > 0 and not self.svc.command_is_scoped():
            resources = [r for r in self.resources if not self.tag_match(r.tags, xtags)]
        else:
            resources = self.resources

        resources = [r for r in resources if self.tag_match(r.tags, tags)]
        self.log.debug("resources after tags[%s] filter: %s"%(str(tags), ','.join([r.rid for r in resources])))

        resources = [r for r in resources if not r.disabled]
        self.log.debug("resources after 'disable' filter: %s"% ','.join([r.rid for r in resources]))

        if action == "startstandby":
            # filter out resource not in standby mode
            resources = [r for r in resources if rcEnv.nodename in r.always_on]

        if hasattr(self, "sort_resources"):
            resources = self.sort_resources(resources, action)
        elif action in ["fs", "start", "startstandby", "provision"]:
            # CODE TO KILL ASAP
            resources.sort()
        else:
            # CODE TO KILL ASAP
            resources.sort(reverse=True)

        if not self.svc.options.dry_run and self.parallel and len(resources) > 1 and action not in ["presync", "postsync"]:
            ps = {}
            for r in resources:
                if not r.can_rollback and action == "rollback":
                    continue
                if r.skip or r.disabled:
                    continue
                p = Process(target=self.action_job, args=(r, action,))
                p.start()
                r.log.info("action %s started in child process %d"%(action, p.pid))
                ps[r.rid] = p
            for p in ps.values():
                p.join()
            err = []
            for r in resources:
                if r.rid not in ps:
                    continue
                p = ps[r.rid]
                if p.exitcode == 1 and not r.optional:
                    err.append(r.rid)
                elif p.exitcode == 2:
                    # can_rollback resource property is lost with the thread
                    # the action_job tells us what to do with it through its exitcode
                    r.can_rollback = True
            if len(err) > 0:
                raise exc.excError("%s non-optional resources jobs returned with error" % ",".join(err))
        else:
            if self.svc.options.dry_run and self.parallel and len(resources) > 1 and action not in ["presync", "postsync"]:
                r.log.info("entering parallel subset")
            for r in resources:
                try:
                    r.action(action)
                except exc.excAbortAction:
                    break

    def action_job(self, r, action):
        try:
            getattr(r, 'action')(action)
        except Exception as e:
            self.log.error(str(e))
            sys.exit(1)
        if r.can_rollback:
            sys.exit(2)
        sys.exit(0)


if __name__ == "__main__":
    for c in (Resource,ResourceSet) :
        help(c)
    print("""m1=Resource("Type1")""")
    m1=Resource("Type1")
    print("m1=",m1)
    print("""m2=Resource("Type1")""")
    m2=Resource("Type1")
    print("m1=",m1)
    print("m2=",m2)
    print("""sets=ResourceSet("TypeRes2")""")
    sets=ResourceSet("TypeRes2")
    print("sets=", sets)
    print("""sets+=m1""")
    sets+=m1
    print("sets=", sets)
    print("""sets+=m2""")
    sets+=m2
    print("sets=", sets)


