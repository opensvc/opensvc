import checks
from rcUtilitiesWindows import get_drives

class check(checks.check):
    chk_type = "fs_u"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if resource.mountPoint == mountpt:
                    return svc.svcname
        return ''

    def do_check(self):
        import win32api
        cmd = ['df', '-lP']
        r = []
        for drive in get_drives():
            try:
                n_free_user, n_total, n_free = win32api.GetDiskFreeSpaceEx(drive+':\\')
            except:
                continue
            pct = 100 * (n_total - n_free) // n_total
            r.append({
                      'chk_instance': drive,
                      'chk_value': str(pct),
                      'chk_svcname': self.find_svc(drive),
                     })
        return r

if __name__ == "__main__":
    o = check()
    print(o.do_check())
