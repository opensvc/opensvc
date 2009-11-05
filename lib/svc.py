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

import app
from resources import Resource, ResourceSet
from freezer import Freezer
import rcStatus
from rcGlobalEnv import rcEnv

class Svc(Resource, Freezer):
    """Service class define a Service Resource
    It contain list of ResourceSet where each ResourceSets contain same resource
    type
    """

    def __init__(self, svcname=None, type="hosted", optional=False, disabled=False):
        """usage : aSvc=Svc(type)"""
        if self.frozen():
            delattr(self.startip)
            delattr(self.stopip)
            delattr(self.mount)
            delattr(self.umount)
        self.svcname = svcname
        self.hostid = rcEnv.nodename
        self.resSets = []
        self.type2resSets = {}
        self += app.Apps(svcname)
        Resource.__init__(self, type, optional, disabled)
        Freezer.__init__(self, svcname)

    def __iadd__(self, r):
        """svc+=aResourceSet
        svc+=aResource
        """
        if r.type in self.type2resSets:
            self.type2resSets[r.type] += r

        elif isinstance(r, ResourceSet):
            self.resSets.append(r)
            self.type2resSets[r.type] = r

        elif isinstance(r, Resource):
            R = ResourceSet(r.type, [r])
            self.__iadd__(R)

        else:
            # Error
            pass

        return self

    def get_res_sets(self, type):
         return [ r for r in self.resSets if r.type == type ]

    def has_res_set(self, type):
        if len(get_res_sets(type)) > 0: return True
        else: return False

    def sub_set_action(self, type=None, action=None):
        """Call action on each member of the subset of specified type"""
        for r in self.get_res_sets(type):
            r.action(action)

    def __str__(self):
        output="Service %s available resources:" % (Resource.__str__(self))
        for k in self.type2resSets.keys() : output += " %s" % k
        output+="\n"
        for r in self.resSets:  output+= "  [%s]" % (r.__str__())
        return output

    def status(self, type_list):
        """aggregate status a service
        """
        s = rcStatus.Status()
        for t in type_list:
            for r in self.get_res_sets(t):
                s += r.status()
        return s.status

    def print_status(self, type_list):
        """print each resource status for a service
        """
        for t in type_list:
            for r in self.get_res_sets(t): r.action("print_status")
        rcStatus.print_status("overall", self.status())

    def disklist(self):
        """List all disks held by all resources of this service
        """
        disks = []
        for rs in self.resSets:
            for r in rs.resources:
                disks += r.disklist()
        # remove duplicate entries in disk list
        disks = list(set(disks)) 
        self.log.debug("found disks %s held by service" % disks)
        return disks

    def startapp(self):
        self.sub_set_action("app", "start")

    def stopapp(self):
        self.sub_set_action("app", "stop")

    def syncnodes(self):
        self.sub_set_action("rsync", "syncnodes")

    def syncdrp(self):
        self.sub_set_action("rsync", "syncdrp")

    def scsirelease(self):
        self.sub_set_action("vg", "scsirelease")
        self.sub_set_action("pool", "scsirelease")

    def scsireserv(self):
        self.sub_set_action("vg", "scsireserv")
        self.sub_set_action("pool", "scsireserv")

    def scsicheckreserv(self):
        self.sub_set_action("vg", "scsicheckreserv")
        self.sub_set_action("pool", "scsicheckreserv")

    def action(self, action):
        rcEnv.logfile = self.logfile
        import xmlrpcClient
        from datetime import datetime
        begin = datetime.now()
        xmlrpcClient.begin_action(self, action, begin)
        ret = getattr(self, action)()
        end = datetime.now()
        xmlrpcClient.end_action(self, action, begin, end, ret)
        return ret

    def restart(self):
	""" stop then start service"""
	# FIXME should test stop() status before start()
        self.stop()
        self.start()


if __name__ == "__main__" :
    for c in (Svc,) :
        help(c)
    print """s1=Svc("Zone")"""
    s1=Svc("Zone")
    print "s1=",s1
    print """s2=Svc("basic")"""
    s2=Svc("basic")
    print "s2=",s2
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print "s1=",s1
    print """s1+=Resource("ip")"""
    s1+=Resource("ip")
    print """s1+=Resource("disk.mount")"""
    s1+=Resource("disk.mount")
    print """s1+=Resource("disk.mount")"""
    s1+=Resource("disk.mount")
    print "s1=",s1

    print """s1.action("status")"""
    s1.action("status")
