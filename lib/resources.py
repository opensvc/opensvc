#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
# Copyright (c) 2010 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2010 Cyril Galibern <cyril.galibern@free.fr>'
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
# To change this template, choose Tools | Templates
# and open the template in the editor.

import rcExceptions as exc
import rcStatus
import logging
import rcUtilities
import sys
from rcGlobalEnv import rcEnv

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
        self.restart = restart
        self.log = logging.getLogger(self.log_label())
        self.rstatus = None
        self.always_on = always_on
        if self.label is None: self.label = type
        self.status_log_str = ""
        self.can_rollback = False

    def log_label(self):
        s = ""
        if hasattr(self, "svc"):
            s += self.svc.svcname + '.'

        if self.rid is None:
            s += self.type
            return s.upper()

        if self.subset is None:
            s += self.rid
            return s.upper()

        v = self.rid.split('#')
        if len(v) != 2:
            s += rid
            return s.upper()

        s += "%s:%s#%s" % (self.type, self.subset, v[1])
        return s.upper()
       
    def __str__(self):
        output="object=%s rid=%s type=%s" % (self.__class__.__name__,
                                             self.rid, self.type)
        if self.optional : output+=" opt="+str(self.optional)
        if self.disabled : output+=" disa="+str(self.disabled)
        return output

    def __cmp__(self, other):
        """resources needed to be started or stopped in a specific order
        should redefine that. For now consider all resources of a set equals
        """
        return 0

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

    def action_triggers(self, type, action):
        attr = type+"_"+action
        if hasattr(self, attr):
            self.vcall(getattr(self, attr))

    def do_action(self, action):
        self.log.debug('do_action: action=%s res=%s'%(action, self.rid))
        if hasattr(self, action):
            if "stop" in action and rcEnv.nodename in self.always_on and not self.svc.force:
                if hasattr(self, action+'standby'):
                    getattr(self, action+'standby')()
                    return
                else:
                    self.log.info("skip '%s' on standby resource (--force to override)"%action)
                    return
            self.setup_environ()
            self.action_triggers("pre", action)
            getattr(self, action)()
            self.action_triggers("post", action)
            if "start" in action or "stop" in action or "sync" in action or action == "provision":
                """ refresh resource status cache after changing actions
                """
                self.status(refresh=True)
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

        if 'noaction' in self.tags:
            self.log.debug('skip resource action (noaction tag): action=%s res=%s'%(action, self.rid))
            return

        self.log.debug('action: action=%s res=%s'%(action, self.rid))
        if action == None:
            self.log.debug('action: action cannot be None')
            return True
        if self.skip and (\
             action.startswith("start") or \
             action.startswith("stop") or \
             action.startswith("sync") \
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

    def status(self, verbose=False, refresh=False):
        if self.disabled:
            self.status_log("disabled")
            return rcStatus.NA
        if self.rstatus is None or refresh:
            self.status_log_str = ""
            self.rstatus = self._status(verbose)
        return self.rstatus

    def status_log(self, text):
        self.status_log_str += "# " + text + "\n"

    def status_quad(self):
        r = self.status(verbose=True)
        if 'encap' in self.tags:
            encap = True
        else:
            encap = False
        return (self.rid,
                rcStatus.status_str(r),
                self.label,
                self.status_log_str,
                self.monitor,
                self.disabled,
                self.optional,
                encap)

    def call(self, cmd=['/bin/false'], cache=False, info=False,
             errlog=True, err_to_warn=False, err_to_info=False,
             outlog=False):
        """Use subprocess module functions to do a call
        """
        return rcUtilities.call(cmd, log=self.log,
                                cache=cache,
                                info=info, errlog=errlog,
                                err_to_warn=err_to_warn,
                                err_to_info=err_to_info,
                                outlog=outlog)

    def vcall(self, cmd, err_to_warn=False, err_to_info=False):
        """Use subprocess module functions to do a call and
        log the command line using the resource logger
        """
        return rcUtilities.vcall(cmd, log=self.log,
                                 err_to_warn=err_to_warn,
                                 err_to_info=err_to_info)

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
            if hasattr(r, 'pre_action'):
                r.log.debug("install pre_action")
                self.pre_action = r.pre_action
            if hasattr(r, 'post_action'):
                r.log.debug("install post_action")
                self.post_action = r.post_action
            if hasattr(r, 'sort_rset'):
                r.sort_rset(self)
        return (self)

    def __str__(self):
        output="resSet %s [" % ( Resource.__str__(self) )
        for r in self.resources:
            output+= " (%s)" % (r.__str__())
        return "%s]" % (output)

    def pre_action(self, rset=None, action=None):
        pass

    def post_action(self, rset=None, action=None):
        pass

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

        if len(xtags) > 0:
            resources = [r for r in self.resources if not self.tag_match(r.tags, xtags)]
        else:
            resources = self.resources
        resources = [r for r in resources if self.tag_match(r.tags, tags)]
        self.log.debug("resources after tags[%s] filter: %s"%(str(tags), str(resources)))
        if hasattr(self, "sort_resources"):
            resources = self.sort_resources(resources, action)
        elif action in ["fs", "start", "startstandby", "provision"]:
            # CODE TO KILL ASAP
            resources.sort()
        else:
            # CODE TO KILL ASAP
            resources.sort(reverse=True)

        if self.parallel and len(resources) > 1:
            ps = {}
            for r in resources:
                p = Process(target=self.action_job, args=(r, action,))
                p.start()
                r.log.info("action %s started in child process %d"%(action, p.pid))
                ps[r.rid] = p
            for p in ps.values():
                p.join()
            err = 0
            for r in resources:
                p = ps[r.rid]
                if p.exitcode == 1 and not r.optional:
                    err += 1
                elif p.exitcode == 2:
                    # can_rollback resource property is lost with the thread
                    # the action_job tells us what to do with it through its exitcode
                    r.can_rollback = True
            if err > 0:
                raise exc.excError
        else:
            for r in resources:
                try:
                    r.action(action)
                except exc.excAbortAction:
                    break

    def action_job(self, r, action):
        try:
            getattr(r, action)()
        except:
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


