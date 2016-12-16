import os
import svc
import socket
import rcExceptions as ex
from rcUtilities import justcall
from rcGlobalEnv import rcEnv
from xml.etree.ElementTree import ElementTree, SubElement
import rcIfconfigLinux as rcIfconfig

class SvcRhcs(svc.Svc):
    builder_props = [
      "nodes",
    ]

    def __init__(self, svcname, pkg_name=None):
        self.type = "rhcs"
        svc.Svc.__init__(self, svcname)
        self.cf = "/etc/cluster/cluster.conf"
        self.pkg_name = pkg_name
        ifconfig = rcIfconfig.ifconfig()
        self.node_ips = []
        self.member_to_nodename_h = {}
        for i in ifconfig.intf:
            self.node_ips += i.ipaddr

    def getaddr(self, ipname):
        a = socket.getaddrinfo(ipname, None)
        if len(a) == 0:
            raise ex.excError("unable to resolve %s ip address" % ipname)
        addr = a[0][4][0]
        return addr

    def member_to_nodename(self, member):
        if member in self.member_to_nodename_h:
            return self.member_to_nodename_h[member]

        try:
            addr = self.getaddr(member)
        except:
            self.member_to_nodename_h[member] = member
            return self.member_to_nodename_h[member]

        if addr in self.node_ips:
            self.member_to_nodename_h[member] = rcEnv.nodename
            return self.member_to_nodename_h[member]

        cmd = rcEnv.rsh.split() + [member, "hostname"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.member_to_nodename_h[member] = member
            return self.member_to_nodename_h[member]

        self.member_to_nodename_h[member] = out.strip()
        return self.member_to_nodename_h[member]

    def load_cluster_conf(self):
        self.tree = ElementTree()
        self.tree.parse(self.cf)
        e = self.tree.getiterator('cluster')
        if len(e) != 1:
            raise ex.excInitError()
        self.xml = e[0]

    def builder(self):
        if self.pkg_name is None:
            self.error("pkg_name is not set")
            raise ex.excInitError()
        self.load_cluster_conf()
        self.load_service()
        self.load_nodes()
        self.load_clustat()
        self.load_hb()

    def load_clustat(self):
        cmd = ['clustat']
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excInitError()
        self.clustat = out.split('\n')

    def load_hb(self):
        rid = 'hb#sg0'
        m = __import__("resHbRhcs")
        r = m.Hb(rid, self.pkg_name)
        self += r

    def load_vg(self, e):
        """
        r = m.Disk(rid, name)
        self += r
        r.monitor = True
        self.n_vg += 1
        """
        pass

    def load_ip(self, e):
        """
        <ip address="10.105.133.5" monitor_link="0">
        """
        if 'ref' in e.attrib:
            # load ref xml node and recurse
            return
        if not 'address' in e.attrib:
            return
        ipname = e.attrib['address']

        n = self.n_ip
        rid = 'ip#rhcs%d'%n
        m = __import__("resIpRhcs"+rcEnv.sysname)
        r = m.Ip(rid, "", ipname, "")
        r.monitor = True
        self += r
        self.n_ip += 1

    def load_fs(self, e):
        """
        <fs device="/dev/communvg/lv_test"
            force_fsck="0"
            force_unmount="1"
            fsid="13584"
            fstype="ext3"
            mountpoint="/mnt/hgfs"
            name="testfs"
            self_fence="0"/>
        """
        if 'device' not in e.attrib:
            return
        dev = e.attrib['device']

        if 'mountpoint' not in e.attrib:
            return
        mnt = e.attrib['mountpoint']

        if 'fstype' not in e.attrib:
            return
        fstype = e.attrib['fstype']

        mntopt = ""
        n = self.n_fs
        rid = 'fs#rhcs%d'%n
        m = __import__("resFsRhcs"+rcEnv.sysname)
        r = m.Mount(rid, mnt, dev, fstype, mntopt)
        r.monitor = True
        self += r
        self.n_fs += 1

    def load_nodes(self):
        nodes = set([])
        for m in self.xml.findall("clusternodes/clusternode"):
            member = m.attrib['name']
            nodename = self.member_to_nodename(member)
            nodes.add(nodename)
        self.nodes = nodes

    def get_ref(self, refname):
        for m in self.xml.findall("rm/resources/*"):
            if m.attrib.get('name') == refname or \
               m.attrib.get('address') == refname:
                return m
        return None

    def iter_rtype(self, head):
        for r in head.findall('*'):
            if r.tag == 'ip':
                rtype = 'ip'
            elif r.tag == 'fs':
                rtype = 'fs'
            else:
                rtype = None
            if rtype is not None:
                if 'ref' in r.attrib:
                    refname = r.attrib['ref']
                    ref = self.get_ref(refname)
                    if ref is not None:
                        getattr(self, "load_"+rtype)(ref)
                else:
                    getattr(self, "load_"+rtype)(r)
            self.iter_rtype(r)

    def load_service(self):
        self.n_ip = 0
        self.n_fs = 0
        self.n_vg = 0
        found = False
        for m in self.xml.findall("rm/service"):
            if m.attrib['name'] != self.pkg_name:
                continue
            found = True
            self.iter_rtype(m)
        if not found:
            raise ex.excInitError("service %s not found in cluster configuration"%self.pkg_name)

    def resource_monitor(self):
        pass

