class DevRelation(object):
    def __init__(self, parent, child, used=0):
        self.child = child
        self.parent = parent
        self.used = used
        self.used_set = False

    def set_used(self, used):
        self.used_set = True
        self.used = used
        
    def get_size(self, chain):
        if self.used_set:
            return self.used
        if len(chain) < 2:
            self.used = self.tree.get_dev(chain[-1].child).size
        else:
            self.used = chain[-2].used
        return self.used

class DevRelationRaid1(DevRelation):
     pass

class DevRelationRaid10(DevRelation):
    def get_size(self, chain):
        DevRelation.get_size(self, chain)
        d = self.tree.get_dev(self.child)
        n = len(d.parents)
        self.used = self.used*2/n
        return self.used

class DevRelationRaid0(DevRelation):
    def get_size(self, chain):
        DevRelation.get_size(self, chain)
        d = self.tree.get_dev(self.child)
        n = len(d.parents)
        self.used = self.used/n
        return self.used

class DevRelationRaid5(DevRelation):
    def get_size(self, chain):
        DevRelation.get_size(self, chain)
        d = self.tree.get_dev(self.child)
        n = len(d.parents)
        self.used = self.used/(n-1)
        return self.used

class DevRelationRaid6(DevRelation):
    def get_size(self, chain):
        DevRelation.get_size(self, chain)
        d = self.tree.get_dev(self.child)
        n = len(d.parents)
        self.used = self.used/(n-2)
        return self.used

class Dev(object):
    def __init__(self, devname, size, devtype):
        self.devname = devname
        self.devpath = devname
        self.alias = devname
        self.size = size
        self.devtype = devtype

        # list of relations
        self.parents = []
        self.children = []

    def __iadd__(self, o):
        pass

    def set_alias(self, alias):
        self.alias = alias

    def get_dev(self, devname):
        return self.tree.get_dev(devname)

    def get_size(self):
        return self.size

    def set_devpath(self, devpath):
        self.devpath = devpath

    def print_dev(self, level=0, relation=None):
        if relation is None:
            parent_size = self.size
        else:
            parent_size = self.get_dev(relation.parent).get_size()
        if parent_size == 0:
            pct = 0
        else:
            pct = 100*self.size//parent_size
        s = "%s %s %d %d%%\n" % (self.alias, self.devtype, self.size, pct)
        for r in self.children:
            d = self.get_dev(r.child)
            for i in range(level+1):
                s += " "
            if d is None:
                s += "unknown (dev %s not added)\n"%r.child
            else:
                s += d.print_dev(level=level+1, relation=r)
        return s

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
            r = DevRelation(parent=self.devname, child=devname, used=size)
            r.tree = self.tree
            self.children.append(r)
        self.tree.add_dev(devname, size, devtype)
        return r

    def add_parent(self, devname, size=0, devtype=None):
        r = self.get_parent(devname)
        if r is None:
            r = DevRelation(parent=devname, child=self.devname, used=size)
            r.tree = self.tree
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
        if len(self.parents) == 0:
            return set([self])
        d = set([])
        for parent in self.parents:
            dev = self.get_dev(parent.parent)
            d |= dev.get_top_devs()
        return d

    def print_dev_bottom_up(self, level=0, chain=[]):
        if len(chain) == 0:
            used = self.size
        else:
            used = chain[-1].get_size(chain)
        s = ""
        for i in range(level+1):
            s += " "
        s += "%s %s %d %d"%(self.alias, self.devtype, self.size, used)
        print s
        for parent in self.parents:
            dev = self.get_dev(parent.parent)
            #print map(lambda x: (x.parent, x.child, x.used, x.get_size(chain+[parent]), x.used), chain+[parent])
            dev.print_dev_bottom_up(level+1, chain+[parent])

class DevTree(object):
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

    def print_tree_bottom_up(self):
        for dev in self.get_bottom_devs():
            dev.print_dev_bottom_up()
        
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

    def blacklist(self, dev):
        """ overload this fn with os specific implementation
        """
        return False

    def add_dev(self, devname, size=0, devtype=None):
        if devname in self.dev:
            return self.dev[devname]
        if self.blacklist(devname):
            return
        d = Dev(devname, size, devtype)
        self += d
        return d

    def get_relation(self, parent, child):
        for d in self.dev.values():
            for r in d.children + d.parents:
                if parent == r.parent and child == r.child:
                    return r
        return None

    def get_bottom_devs(self):
        return [self.dev[devname] for devname in self.dev if len(self.dev[devname].children) == 0]

    def get_top_devs(self):
        d = set([])
        for dev in self.get_bottom_devs():
            d |= dev.get_top_devs()
        return list(d)

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
    print tree
