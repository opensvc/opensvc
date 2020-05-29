"""
Top devices are bare disks or multipath paths.

Bottom devices are formatted devices or devices given to
applications like raw database devices.

A relation describes a parent-child link. A 'used' size can
be arbitrarily set on a relation : DevRelation.set_used()

A logical volume lv0 with segments on pv1 pv2 has two parent
relations : lv0-pv1 and lv0-pv2

"""
from utilities.render.forest import Forest
from utilities.converters import print_size
from env import Env
from utilities.render.color import color
from utilities.hash.md5 import hexdigest


class DevRelation(object):
    def __init__(self, parent, child, used=0):
        self.child = child
        self.parent = parent
        self.used = used
        self.used_set = False
        self.tree = None

    def set_used(self, used):
        self.used_set = True
        self.used = used

    def get_used(self, used):
        #
        # logical volumes and concatset need to set explicitly
        # the 'used' size the child consumes on the parent.
        #
        child = self.tree.get_dev(self.child)
        if used == 0:
            used = child.size
        if child.devtype is None:
            return used
        elif child.devtype in ("multipath", "linear", "partition", "extent"):
            return used
        elif child.devtype in ("raid0"):
            n = len(child.parents)
            return used/n
        elif child.devtype in ("raid1", "raid10"):
            n = len(child.parents)
            return used*2/n
        elif child.devtype in ("raid5"):
            n = len(child.parents)
            return used/(n-1)
        elif child.devtype in ("raid6"):
            n = len(child.parents)
            return used/(n-2)
        raise Exception("unknown devtype %s for %s"%(child.devtype, child.devname))

    def get_size(self, chain):
        if self.used_set:
            return self.used
        if len(chain) < 2:
            self.used = self.tree.get_dev(chain[-1].child).size
        else:
            self.used = chain[-2].used
        return self.used

class Dev(object):
    def __init__(self, devname, size, devtype):
        self.devname = devname
        self.devpath = []
        self.alias = devname
        self.size = size
        self.devtype = devtype
        self.tree = None
        self.dg = ""

        # list of relations
        self.parents = []
        self.children = []

        self.removed = False

    def __iadd__(self, o):
        pass

    def remove(self, r):
        # to implement for each os
        r.log.info("remove method not implemented for device %s"%self.alias)

    def set_alias(self, alias):
        self.alias = alias

    def get_dev(self, devname):
        return self.tree.get_dev(devname)

    def get_size(self):
        return self.size

    def set_devtype(self, devtype):
        self.devtype = devtype

    def set_devpath(self, devpath):
        if devpath not in self.devpath:
            self.devpath.append(devpath)

    def get_child(self, devname):
        for r in self.children:
            if r.parent == devname:
                return r
        return None

    def get_parent(self, devname):
        for r in self.parents:
            if r.child == devname:
                return r
        return None

    def add_child(self, devname, size=0, devtype=None):
        r = self.get_child(devname)
        if r is None:
            r = self.tree.get_relation(self.devname, devname)
            if r is None:
                r = DevRelation(parent=self.devname, child=devname, used=size)
                r.tree = self.tree
            self.children.append(r)
        self.tree.add_dev(devname, size, devtype)
        return r

    def add_parent(self, devname, size=0, devtype=None):
        r = self.get_parent(devname)
        if r is None:
            r = self.tree.get_relation(devname, self.devname)
            if r is None:
                r = DevRelation(parent=devname, child=self.devname, used=size)
                r.tree = self.tree
            else:
                r.used = size
            self.parents.append(r)
        self.tree.add_dev(devname, size, devtype)
        return r

    def is_parent(self, devname):
        for r in self.children:
            if r.child == devname:
                return True
            d = self.get_dev(r.child)
            if d.is_parent(devname):
                return True
        return False

    def get_top_devs(self):
        if len(self.parents) == 0 or self.devtype == "multipath":
            return set([self])
        d = set()
        for parent in self.parents:
            dev = self.get_dev(parent.parent)
            d |= dev.get_top_devs()
        return d

    def get_top_devs_chain(self, chain=None):
        if chain is None:
            chain = []
        if len(self.parents) == 0 or self.devtype == "multipath":
            return [[self, chain]]
        d = []
        for parent in self.parents:
            dev = self.get_dev(parent.parent)
            d += dev.get_top_devs_chain(chain+[parent])
        return d

    def print_dev(self, relation=None, node=None, highlight=None, verbose=False):
        if highlight is None:
            highlight = []
        if relation is None:
            parent_size = 0
        else:
            parent_size = self.get_dev(relation.parent).get_size()
        if parent_size == 0:
            pct = "-"
        else:
            pct = "%.2f%%" % (100*self.size//parent_size)

        node_dev = node.add_node()
        node_dev.add_column(self.alias, color.BROWN)
        node_dev.add_column(self.devtype)
        node_dev.add_column(print_size(self.size), align="right")
        node_dev.add_column(pct, align="right")
        if verbose:
            col = node_dev.add_column()
            for devpath in self.devpath:
                if highlight is not None and devpath in highlight:
                    textcolor = color.LIGHTBLUE
                else:
                    textcolor = None
                col.add_text(devpath, textcolor)

        for r in self.children:
            d = self.get_dev(r.child)
            if d is None:
                node_unk = node_dev.add_node()
                node_unk.add_column("%s (unknown)" % r.child)
            else:
                d.print_dev(relation=r, node=node_dev, highlight=highlight,
                            verbose=verbose)

    def print_dev_bottom_up(self, chain=None, node=None, highlight=None,
                            verbose=False):
        if highlight is None:
            highlight = []
        if chain is None:
            chain = []
        if len(chain) == 0:
            prev_size = 0
            used_s = "-"
        else:
            prev_size = self.tree.get_dev(chain[-1].child).size
            used = chain[-1].get_used(prev_size)
            used_s = print_size(used)
        if prev_size == 0:
            pct = "-"
        else:
            pct = "%.2f%%" % (100*used//self.size)

        node_dev = node.add_node()
        node_dev.add_column(self.alias, color.BROWN)
        node_dev.add_column(self.devtype)
        node_dev.add_column(used_s, align="right")
        node_dev.add_column(print_size(self.size), align="right")
        node_dev.add_column(pct, align="right")
        if verbose:
            col = node_dev.add_column()
            for devpath in self.devpath:
                if highlight is not None and devpath in highlight:
                    textcolor = color.LIGHTBLUE
                else:
                    textcolor = None
                col.add_text(devpath, textcolor)
        for r in self.parents:
            dev = self.get_dev(r.parent)
            dev.print_dev_bottom_up(chain+[r], node_dev, verbose=verbose)

    def get_parents_bottom_up(self, l=None):
        if l is None:
            l = []
        for parent in self.parents:
            dev = self.get_dev(parent.parent)
            l.append(dev)
            l = dev.get_parents_bottom_up(l)
        return l

    def get_children_bottom_up(self):
        l = self.get_children_top_down()
        l.reverse()
        return l

    def get_children_top_down(self):
        l = []
        for child in self.children:
            dev = self.get_dev(child.child)
            l.append(dev)
            l += dev.get_children_top_down()
        return l

class DevTree(object):
    dev_class = Dev

    def __init__(self):
        self.dev = {}

        # root node of the relation tree
        self.root = []

    def __iadd__(self, o):
        if isinstance(o, Dev):
            o.tree = self
            self.dev[o.devname] = o
            if not self.has_relations(o.devname):
                r = DevRelation(parent=None, child=o.devname, used=o.size)
                r.tree = self
                self.root.append(r)
        return self

    def __str__(self):
        s = ""
        for r in self.root:
            s += self.dev[r.child].print_dev()
        return s

    def print_tree(self, devices=None, verbose=False):
        ftree = Forest()
        node = ftree.add_node()
        node.add_column(Env.nodename, color.BOLD)
        node.add_column("Type", color.BOLD)
        node.add_column("Size", color.BOLD, align="right")
        node.add_column("Pct of Parent", color.BOLD, align="right")

        filtered = devices is not None and len(devices) > 0
        if filtered:
            devs = [self.get_dev_by_devpath(devpath) for devpath in devices]
        else:
            devs = [self.dev[r.child] for r in self.root]
        for dev in devs:
            if dev is None or (not filtered and dev.parents != []):
                continue
            dev.print_dev(node=node, highlight=devices, verbose=verbose)

        ftree.out()

    def print_tree_bottom_up(self, devices=None, verbose=False):
        ftree = Forest()
        node = ftree.add_node()
        node.add_column(Env.nodename, color.BOLD)
        node.add_column("Type", color.BOLD)
        node.add_column("Parent Use", color.BOLD, align="right")
        node.add_column("Size", color.BOLD, align="right")
        node.add_column("Ratio", color.BOLD, align="right")

        if devices is None:
            devices = set()
        else:
            devices = set(devices)
        for dev in self.get_bottom_devs():
            if len(devices) > 0 and len(set(dev.devpath)&devices) == 0:
                continue
            dev.print_dev_bottom_up(node=node, highlight=devices,
                                    verbose=verbose)

        ftree.out()

    def has_relations(self, devname):
        l = []
        for r in self.root:
            if r.child == devname:
                return True
            d = self.get_dev(r.child)
            if d.is_parent(devname):
                return True
        return False

    def get_dev(self, devname):
        if devname not in self.dev:
            return None
        return self.dev[devname]

    def get_devs_by_devpaths(self, devpaths):
        devs = set()
        for devpath in devpaths:
            dev = self.get_dev_by_devpath(devpath)
            if dev is None:
                continue
            devs.add(dev)
        return devs

    def get_dev_by_devpath(self, devpath):
        for dev in self.dev.values():
            if devpath in dev.devpath:
                return dev

    def blacklist(self, dev):
        """ overload this fn with os specific implementation
        """
        return False

    def add_dev(self, devname, size=0, devtype=None):
        if devname in self.dev:
            return self.dev[devname]
        if self.blacklist(devname):
            return
        d = self.dev_class(devname, size, devtype)
        self += d
        return d

    def set_relation_used(self, parent, child, used):
        for d in self.dev.values():
            for r in d.children + d.parents:
                if parent == r.parent and child == r.child:
                    r.set_used(used)

    def get_relation(self, parent, child):
        for d in self.dev.values():
            for r in d.children + d.parents:
                if parent == r.parent and child == r.child:
                    return r
        return None

    def get_bottom_devs(self):
        return [self.dev[devname] for devname in self.dev if len(self.dev[devname].children) == 0]

    def get_top_devs(self):
        d = set()
        for dev in self.get_bottom_devs():
            d |= dev.get_top_devs()
        return list(d)

    def get_used(self, chain):
        used = 0
        for rel in chain:
            used = rel.get_used(used)
        return used

    def get_top_devs_usage_for_devpath(self, devpath):
        dev = self.get_dev_by_devpath(devpath)
        if dev is None:
            return []
        l = []
        for d, chain in dev.get_top_devs_chain():
            if len(chain) == 0:
                used = d.size
                region = 0
            else:
                used = self.get_used(chain)
                ref = self.get_dev(chain[0].child).alias
                region = hexdigest(ref)
            l.append((d.devpath[0], used, region))
        return l

if __name__ == "__main__":
    tree = DevTree()
    d = tree.add_dev('/dev/sdb', 10000)
    d.add_child('/dev/sdb1', 8000)
    d.add_child('/dev/sdb2', 2000)
    d = tree.add_dev('/dev/sdc', 20000)
    d.add_child('/dev/mapper/vg01-foo', 1000)
    d = tree.get_dev('/dev/sdb2')
    d.add_child('/dev/mapper/vg01-foo', 1000)
    d = tree.get_dev('/dev/mapper/vg01-foo')
    d.add_child('foo.vmdk', 500)
    print(tree)
