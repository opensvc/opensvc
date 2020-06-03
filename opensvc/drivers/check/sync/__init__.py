import drivers.check

SYNC_DRIVERS = [
    "sync.btrfs",
    "sync.dds",
    "sync.docker",
    "sync.rsync",
    "sync.zfs",
]

class Check(drivers.check.Check):
    chk_type = "sync"

    def find_svc(self, vgname):
        for svc in self.svcs:
            for resource in svc.get_resources('disk'):
                if not hasattr(resource, "name"):
                    continue
                if resource.name == vgname:
                    return svc.path
        return ''

    def do_check(self):
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
                "instance": resource.rid + ".bytes",
                "value": str(stats["bytes"]),
                "path": svc.path,
             })
        if "speed" in stats:
            data.append({
                "instance": resource.rid + ".speed",
                "value": str(stats["speed"]),
                "path": svc.path,
             })
        return data
