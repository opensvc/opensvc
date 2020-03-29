import drivers.check

from utilities.diskinfo import DiskInfo

class Check(drivers.check.Check):
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        devpath = '/dev/rdisk/'+dev
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.sub_devs()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.path
        return ''

    def do_check(self):
        di = DiskInfo()
        r = []
        for dev, data in di.h.items():
            r.append({"instance": data['wwid'],
                      "value": str(data['path_count']),
                      "path": self.find_svc(dev),
                     })
        return r
