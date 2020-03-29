import drivers.check
import utilities.devices.windows

class Check(drivers.check.Check):
    chk_type = "fs_u"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if not hasattr(resource, "mount_point"):
                    continue
                if resource.mount_point == mountpt:
                    return svc.path
        return ''

    def do_check(self):
        try:
            import win32api
        except ImportError:
            return []
        cmd = ['df', '-lP']
        r = []
        for drive in utilities.devices.windows.get_drives():
            try:
                n_free_user, n_total, n_free = win32api.GetDiskFreeSpaceEx(drive+':\\')
            except:
                continue
            pct = 100 * (n_total - n_free) // n_total
            r.append({
                      "instance": drive,
                      "value": str(pct),
                      "path": self.find_svc(drive),
                     })
        return r

if __name__ == "__main__":
    o = Check()
    print(o.do_check())
