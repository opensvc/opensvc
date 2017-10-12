import provisioning
import rcExceptions as ex

class Prov(provisioning.Prov):
    def is_provisioned(self):
        return self.r.has_it()

    def start(self):
        image = self.r.conf_get("launch_image")
        try:
            options = self.r.conf_get("launch_options")
        except ex.OptNotFound as exc:
            options = exc.default
        cmd = ["/usr/bin/lxc", "launch", image] + options + [self.r.name]
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError
        self.r.can_rollback = True

        self.r.promote_zfs()


    def stop(self):
        cmd = ["/usr/bin/lxc", "delete", self.r.name] + options
        self.r.vcall(cmd)
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError

