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

from textwrap import wrap
import rcExceptions as exc
import rcStatus
import logging
import rcUtilities
from rcGlobalEnv import rcEnv

class Resource(object):
    """Define basic resource
    properties: type, optional, disabled
    a Resource should provide do_action(action):
    with action into (start/stop/status)
    """
    label = None

    def __init__(self, rid=None, type=None, optional=False, disabled=False):
        self.rid = rid
        self.type = type
        self.optional = optional
        self.disabled = disabled
        self.log = logging.getLogger(str(rid).upper())
        self.rstatus = None
        if self.label is None: self.label = type
        self.status_log_str = ""

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
        if hasattr(self, action):
            self.setup_environ()
            self.action_triggers("pre", action)
            getattr(self, action)()
            self.action_triggers("post", action)
            if action in ["start", "stop"] or "sync" in action:
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
        if action == None: return True
        if self.disabled: return True
        try :
            self.do_action(action)
        except exc.excUndefined , ex:
            print ex
            return False
        except exc.excError:
            if self.optional:
                pass
            else:
                raise exc.excError

    def _status(self, verbose=False):
        return rcStatus.UNDEF

    def status(self, verbose=False, refresh=False):
        if self.disabled:
            self.status_log("disabled")
            return rcStatus.NA
        if self.rstatus is None or refresh:
            self.rstatus = self._status(verbose)
        return self.rstatus

    def status_log(self, text):
        self.status_log_str += "# " + text

    def print_status(self):
        self.status_log_str = ""
        r = self.status(verbose=True)
        print self.svc.print_status_fmt%(self.rid,
                                         rcStatus.status_str(r),
                                         self.label)
        if len(self.status_log_str) > 0:
            print '\n'.join(wrap(self.status_log_str,
                        initial_indent =    '                  ',
                        subsequent_indent = '                  ',
                        width=78
                       )
                     )

        return r

    def call(self, cmd=['/bin/false'], cache=False, info=False,
             errlog=True, err_to_warn=False, err_to_info=False):
        """Use subprocess module functions to do a call
        """
        return rcUtilities.call(cmd, log=self.log,
                                cache=cache,
                                info=info, errlog=errlog,
                                err_to_warn=err_to_warn,
                                err_to_info=err_to_info)

    def vcall(self, cmd, err_to_warn=False, err_to_info=False):
        """Use subprocess module functions to do a call and
        log the command line using the resource logger
        """
        return rcUtilities.vcall(cmd, log=self.log,
                                 err_to_warn=err_to_warn,
                                 err_to_info=err_to_info)

    def disklist(self):
        """List disks the resource holds. Some resource have none,
        and can leave this function as is.
        """
        return set()

    def presync(self):
        pass

    def postsync(self):
        pass

    def files_to_sync(self):
        return []

class ResourceSet(Resource):
    """ Define Set of same type resources
    Example 1: ResourceSet("fs",[m1,m2])
    Example 2: r=ResourceSet("fs",[ip1])
    It define the resource type
    """
    def __init__(self, type=None, resources=[], optional=False, disabled=False):
        self.resources=resources
        Resource.__init__(self, type=type, optional=optional, disabled=disabled)

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
                self.pre_action = r.pre_action
            if hasattr(r, 'post_action'):
                self.post_action = r.post_action
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
            try:
                status = r.status()
            except:
                import sys
                import traceback
                e = sys.exc_info()
                print e[0], e[1], traceback.print_tb(e[2])

                status = rcStatus.NA

            s += status
        return s.status

    def action(self,action=None):
        """Call action on each resource of the ResourceSet
        """
        if action in ["fs", "start", "startstandby"]:
            self.resources.sort()
        else:
            self.resources.sort(reverse=True)

        if action not in ["status", "print_status", "group_status"]:
            try:
                self.pre_action(self, action)
            except exc.excAbortAction:
                return

        for r in self.resources:
            try:
                r.action(action)
            except exc.excAbortAction:
                break

        if action != "status":
            self.post_action(self, action)


if __name__ == "__main__":
    for c in (Resource,ResourceSet) :
        help(c)
    print """m1=Resource("Type1")"""
    m1=Resource("Type1")
    print "m1=",m1
    print """m2=Resource("Type1")"""
    m2=Resource("Type1")
    print "m1=",m1
    print "m2=",m2
    print """sets=ResourceSet("TypeRes2")"""
    sets=ResourceSet("TypeRes2")
    print "sets=", sets
    print """sets+=m1"""
    sets+=m1
    print "sets=", sets
    print """sets+=m2"""
    sets+=m2
    print "sets=", sets


