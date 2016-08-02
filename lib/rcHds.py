from rcUtilities import which, justcall
import rcExceptions as ex
import os
import ConfigParser

def _cmd(cmd, url, username, password, serial, bin):
    if which(bin) is None:
        print("Can not find %s"%bin)
        raise ex.excError
    l = [bin, url, cmd[0],
         "-u", username,
         "-p", password,
         "serialnum="+serial]
    if len(cmd) > 1:
        l += cmd[1:]
    out, err, ret = justcall(l)
    if ret != 0:
        raise ex.excError(err)
    return out, err, ret

class Hdss(object):
    arrays = []
    def __init__(self, objects=[]):
        self.objects = objects
        if len(objects) > 0:
            self.filtering = True
        else:
            self.filtering = False
        self.index = 0

        cf = rcEnv.authconf
        if not os.path.exists(cf):
            return
        conf = ConfigParser.RawConfigParser()
        conf.read(cf)
        m = []
        for s in conf.sections():
            try:
                stype = conf.get(s, 'type')
            except:
                continue
            if stype != "hds":
                continue
            try:
                bin = conf.get(s, 'bin')
            except:
                bin = None
            try:
                url = conf.get(s, 'url')
                arrays = conf.get(s, 'array').split()
                username = conf.get(s, 'username')
                password = conf.get(s, 'password')
                m += [(url, arrays, username, password, bin)]
            except:
                print("error parsing section", s)
                pass

        del(conf)
        done = []
        for url, arrays, username, password, bin in m:
            for name in arrays:
                if self.filtering and name not in self.objects:
                    continue
                if name in done:
                    continue
                self.arrays.append(Hds(name, url, username, password, bin))
                done.append(name)

    def __iter__(self):
        return self

    def next(self):
        if self.index == len(self.arrays):
            raise StopIteration
        self.index += 1
        return self.arrays[self.index-1]

class Hds(object):
    def __init__(self, serial, url, username, password, bin=None):
        self.keys = ['lu', 'arraygroup', 'port']
        self.name = serial
        self.serial = serial
        self.url = url
        self.username = username
        self.password = password
        if bin is None:
            self.bin = "HiCommandCLI"
        else:
            self.bin = bin

    def _cmd(self, cmd):
        return _cmd(cmd, self.url, self.username, self.password, self.serial, self.bin)

    def get_lu(self):
        cmd = ['GetStorageArray', 'subtarget=Logicalunit', 'lusubinfo=Path,LDEV,VolumeConnection']
        print(' '.join(cmd))
        out, err, ret = self._cmd(cmd)
        return out

    def get_arraygroup(self):
        cmd = ['GetStorageArray', 'subtarget=ArrayGroup']
        print(' '.join(cmd))
        out, err, ret = self._cmd(cmd)
        return out

    def get_port(self):
        cmd = ['GetStorageArray', 'subtarget=Port']
        print(' '.join(cmd))
        out, err, ret = self._cmd(cmd)
        return out

if __name__ == "__main__":
    o = Hdss()
    for hds in o:
        print(hds.get_lu())

