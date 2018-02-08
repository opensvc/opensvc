import checks

SYNC_DRIVERS = [
    "sync.btrfs",
    "sync.dds",
    "sync.docker",
    "sync.rsync",
    "sync.zfs",
]

class check(checks.check):
    chk_type = "sync"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for resource in svc.get_resources('disk'):
                if not hasattr(resource, "name"):
                    continue
                if resource.name == vgname:
                    return svc.svcname
        return ''

    def do_check(self):
        print("here")
        data = []
        for svc in self.svcs:
            for resource in svc.get_resources(SYNC_DRIVERS):
                data += self.check_resource(svc, resource)
        return data

    def check_resource(self, svc, resource):
        data = []
        stats = resource.load_stats()
        if "bytes" in stats:
            data.append({
                'chk_instance': resource.rid + ".bytes",
                'chk_value': str(stats["bytes"]),
                'chk_svcname': svc.svcname,
             })
        if "speed" in stats:
            data.append({
                'chk_instance': resource.rid + ".speed",
                'chk_value': str(stats["speed"]),
                'chk_svcname': svc.svcname,
             })
        return data
