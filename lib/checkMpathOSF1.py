import checks
from rcUtilities import justcall
from rcDiskInfoOSF1 import diskInfo

class check(checks.check):
    chk_type = "mpath"
    svcdevs = {}

    def find_svc(self, dev):
        devpath = '/dev/rdisk/'+dev
        for svc in self.svcs:
            if svc not in self.svcdevs:
                try:
                    devs = svc.disklist()
                except Exception as e:
                    devs = []
                self.svcdevs[svc] = devs
            if dev in self.svcdevs[svc]:
                return svc.svcname
        return ''

    def do_check(self):
        di = diskInfo()
        r = []
        for dev, data in di.h.items():
            r.append({'chk_instance': data['wwid'],
                      'chk_value': str(data['path_count']),
                      'chk_svcname': self.find_svc(dev),
                     })
        return r
