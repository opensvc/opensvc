import checks
from rcUtilities import justcall

class check(checks.check):
    chk_type = "fs_i"

    def find_svc(self, mountpt):
        for svc in self.svcs:
            for resource in svc.get_resources('fs'):
                if resource.mount_point == mountpt:
                    return svc.svcname
        return ''

    def do_check(self):
        cmd = ['df', '-lPi']
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
            if "/graph/" in l[5]:
                continue
            if "/aufs/mnt/" in l[5]:
                continue
            if "osvc_sync_" in l[0]:
                # do not report osvc sync snapshots fs usage
                continue
            if l[4] == '-':
                # vfat, btrfs, ... have no inode counter in df -i
                continue
            r.append({
                      'chk_instance': l[5],
                      'chk_value': l[4],
                      'chk_svcname': self.find_svc(l[5]),
                     })
        return r
