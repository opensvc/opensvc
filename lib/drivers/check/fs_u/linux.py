import drivers.check

from utilities.proc import justcall

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
        cmd = ['df', '-lP']
        (out,err,ret) = justcall(cmd)
        if ret != 0:
            return self.undef
        lines = out.split('\n')
        if len(lines) < 2:
            return self.undef
        r = []
        for line in lines[1:]:
            l = line.split()
            if len(l) != 6:
                continue
            # discard bind mounts: we get metric from the source anyway
            if l[0].startswith('/') and not l[0].startswith('/dev') and not l[0].startswith('//'):
                continue
            if l[0] in ("overlay", "overlay2", "aufs"):
                continue
            if l[5].startswith('/Volumes'):
                continue
            if l[5].startswith('/media/'):
                continue
            if l[5].startswith('/run'):
                continue
            if l[5].startswith('/sys/'):
                continue
            if l[5].endswith('/shm'):
                continue
            if l[5].startswith('/snap/'):
                continue
            if "/overlay2/" in l[5]:
                continue
            if "/snapd/" in l[5]:
                continue
            if "/graph/" in l[5]:
                continue
            if "/aufs/mnt/" in l[5]:
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            path = self.find_svc(l[5])
            r.append({
                      "instance": l[5],
                      "value": l[4],
                      "path": path,
                     })
            r.append({
                      "instance": l[5]+".free",
                      "value": l[3],
                      "path": path,
                     })
            r.append({
                      "instance": l[5]+".size",
                      "value": l[1],
                      "path": path,
                     })
        return r
