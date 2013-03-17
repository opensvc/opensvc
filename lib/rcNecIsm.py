from rcUtilities import which, justcall
import rcExceptions as ex
import os

class NecIsms(object):
    arrays = []
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0
        if which('iSMview') is None:
            print('Can not find iSMview programs in PATH')
            raise ex.excError
        out, err, ret = justcall(['iSMview', '-d'])
        if ret != 0:
            print(err)
            raise ex.excError

        """

--- Disk Array List ---
Product ID        Disk Array Name                   Resource State  Monitoring
D1-10             D1_10                             ready           running

        """
        lines = out.split('\n')
        for line in lines:
            if len(line) == 0:
                continue
            if '---' in line:
                continue
            if 'Product ID' in line:
                continue
            l = line.split()
            if len(l) != 4:
                continue
            if filtering and l[1] not in self.objects:
                continue
            self.arrays.append(NecIsm(l[1]))

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class NecIsm(object):
    def __init__(self, name):
        self.keys = ['all']
        self.name = name

    def _cmd(self, cmd):
        cmd = ['iSMview'] + cmd + [self.name]
        return justcall(cmd)

    def get_all(self):
        cmd = ['-all']
        out, err, ret = self._cmd(cmd)
        return out

if __name__ == "__main__":
    o = NecIsms()
    for necism in o:
        print(necism.all())

