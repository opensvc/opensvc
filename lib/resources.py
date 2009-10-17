#
# Copyright (c) 2009 Christophe Varoqui <christophe.varoqui@free.fr>'
# Copyright (c) 2009 Cyril Galibern <cyril.galibern@free.fr>'
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

import action as exc
import logging
import rcStatus

class Resource(object):
    """Define basic resource
    properties: type, optional, disabled
    a Resource should provide do_action(action):
    with action into (start/stop/status)
    """
    def __init__(self, type=None, optional=False, disabled=False):
        self.type = type
        self.optional = optional
        self.disabled = disabled
        self.log = logging.getLogger(type)
        self.id = type

    def __str__(self):
        output="object=%s type=%s" %   (self.__class__.__name__,self.type)
        if self.optional : output+=" opt="+str(self.optional)
        if self.disabled : output+=" disa="+str(self.disabled)
        return output

    def is_optional(self): return self.optional
    def is_disabled(self): return self.Disabled

    def set_optional(self): self.optional=True
    def unset_optional(self): self.optional=False

    def disable(self): self.disabled=True
    def enable(self):  self.disabled=False

    def do_action(self, action):
        "Every resource should define basic doAction: start() stop() status()"
        self.log.debug("call do_action on %s" % self.__class__.__name__)
        if hasattr(self, action):
            return getattr(self, action)()
        if action in ("start","stop","status") :
            raise exc.excUndefined(action,self.__class__.__name__,\
                                    "Resource.do_action")
    def action(self, action=None):
        """ action try to call do_action() on selft
        pass if action is not None or if self is disabled
        return status vary on optional property
        is do_action success then return True
        else return False if self selft not optional
        """
        if action == None : pass
        if self.is_disabled : pass
        try :
            self.do_action(action)
        except exc.excUndefined , ex :
            print ex
            return False
        except exc.excError :
            if self.is_optional() :  return True
            else :                  return False

    def status(self):
        """aggregate status a ResourceSet
        """
        s = rcStatus.Status()
        for r in self.resources:
                s.add(r.status())
        return s.status

    def print_status(self):
        return rcStatus.print_status(self.id, self.status())


class ResourceSet(Resource):
    """ Define Set of same type resources
    Example 1: ResourceSet("mount",[m1,m2])
    Example 2: r=ResourceSet("mount",[ip1])
    It define the resource type
    """
    def __init__(self,type=None,resources=[],optional=False,disabled=False):
        self.resources=resources
        Resource.__init__(self,type,optional,disabled)

    def __iadd__(self,r):
        """Example 1 iadd another ResourceSet: R+=ResSet ... R+=[m1,m2]
        Example 2 : iadd a single Resource : R+=ip1
        """
        if isinstance(r,ResourceSet) :
            self.resources.extend(r.resources)
        elif isinstance(r,Resource) :
            self.resources.append(r)
        return (self)

    def __str__(self):
        output="resSet %s [" % ( Resource.__str__(self) )
        for r in self.resources:
            output+= " (%s)" % (r.__str__())
        return "%s]" % (output)

    def action(self,action=None):
        """Call action on each resource of the ResourceSet"""
        for r in self.resources:
            r.action(action)


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


