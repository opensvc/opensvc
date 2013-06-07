#!/opt/opensvc/bin/python

import os
import sys
import datetime
import shutil

class ParserError(Exception):
    pass

class Parser(object):
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.changed = False
        self.nocf = False
        self.load()
        self.bkp = path + '.' + str(datetime.datetime.now())

    def __str__(self):
        s = ""
        for k in sorted(self.data.keys()):
            v = self.data[k]
            s += k + " " + str(v) + '\n'
        return s

    def set(self, key, value):
        self.data[key] = value
        self.changed = True

    def unset(self, key):
        if key in self.data:
            del(self.data[key])
        self.changed = True

    def get(self, key):
        if key in self.data:
            return self.data[key]
        return

    def load(self):
        if not os.path.exists(self.path):
            raise ParserError("%s does not exist"%self.path)
            self.nocf = True
            return
        with open(self.path, 'r') as f:
            buff = f.read()
        self.parse(buff)

    def backup(self):
        if self.nocf:
            return
        try:
            shutil.copy(self.path, self.bkp)
        except Exception as e:
            print e
            raise ParserError("failed to backup %s"%self.path)
        print "%s backup up as %s" % (self.path, self.bkp)

    def restore(self):
        if self.nocf:
            return
        try:
            shutil.copy(self.bkp, self.path)
        except:
            raise ParserError("failed to restore %s"%self.path)
        print "%s restored from %s" % (self.path, self.bkp)


    def write(self):
        self.backup()
        try:
            with open(self.path, 'w') as f:
                f.write(str(self))
            print "%s rewritten"%self.path
        except:
            self.restore()
            raise ParserError()

    def parse(self, buff):
        for line in buff.split("\n"):
            line = line.strip()

            # discard comment line
            if line.startswith('#'): 
                continue

            # strip end-of-line comment
            try:
                i = line.index('#')
                line = line[:i]
                line = line.strip()
            except ValueError:
                pass

            # discard empty line
            if len(line) == 0:
                continue

            l = line.split()
            if len(l) < 2:
                 continue
            key = l[0]
            value = line[len(key):].strip()

            try:
                value = int(value)
            except:
                pass

            self.data[key] = value

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >>sys.stderr, "wrong number of arguments"
        sys.exit(1)
    o = Conf(sys.argv[1])
    o.get("Subsystem")
    o.set("Subsystem", "foo")
    o.unset("PermitRootLogin")
    o.backup()
    print o

