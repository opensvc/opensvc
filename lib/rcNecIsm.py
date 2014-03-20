from rcUtilities import which, justcall
import rcExceptions as ex
import os

class Nec(object):
    arrays = []
    def get_bin(self):
        candidates = ['iSMcc_view', 'iSMview']
        for bin in candidates:
            if which(bin) is not None:
                self.bin = bin
                break
        if self.bin is None:
            raise ex.excError('Can not find %s program in PATH' % ' or '.join(candidates))

    def get_arrays(self):
        cmd = [self.bin, '-d']
        out, err, ret = justcall(cmd)
        if ret != 0:
            self.refresh_vollist()
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError(err)

        """

--- Disk Array List ---
Product ID        Disk Array Name                   Resource State  Monitoring
D1-10             D1_10                             ready           running


--- Disk Array List ---
Product ID        Disk Array Name                   Resource State
Optima3600        Optima7_LMW                       ready


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
            if len(l) < 3:
                continue
            if self.filtering and l[1] not in self.objects:
                continue
            self.arrays.append(NecIsm(l[1]))


class NecIsms(Nec):
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0
        self.bin = None
        self.get_bin()
        self.get_arrays()

    def __iter__(self):
        return self

    def refresh_vollist(self):
        if which('iSMvollist') is None:
            return
        cmd = ['iSMvollist', '-r']
        out, err, ret = justcall(cmd)
 
    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class NecIsm(Nec):
    def __init__(self, name):
        self.keys = ['all']
        self.name = name
        self.get_bin()

    def _cmd(self, cmd):
        cmd = [self.bin] + cmd + [self.name]
        return justcall(cmd)

    def get_all(self):
        cmd = ['-all']
        out, err, ret = self._cmd(cmd)
        return out

if __name__ == "__main__":
    o = NecIsms()
    for necism in o:
        print(necism.get_all())

