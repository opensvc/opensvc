import provisioning
import os
import rcExceptions as ex
from rcUtilities import which
from converters import convert_size

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def provisioner(self):
        if which("mdadm") is None:
            raise ex.excError("mdadm is not installed")
        self.provisioner_md()
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def provisioner_md(self):
        if self.r.has_it():
            self.r.log.info("already provisioned")
            return
        try:
            level = self.r.svc.conf_get(self.r.rid, "level")
        except:
            raise ex.excError("'level' provisioning parameter not set")
        try:
            devs = self.r.svc.conf_get(self.r.rid, "devs").split()
        except:
            raise ex.excError("'devs' provisioning parameter not set")
        if len(devs) == 0:
            raise ex.excError("at least 2 devices must be set in the 'devs' provisioning parameter")
        try:
            spares = self.r.svc.conf_get(self.r.rid, 'spares')
        except:
            spares = 0
        try:
            chunk = self.r.svc.conf_get(self.r.rid, 'chunk')
        except:
            chunk = None
        try:
            layout = self.r.svc.conf_get(self.r.rid, 'layout')
        except:
            layout = None

        # long md names cause a buffer overflow in mdadm
        name = "/dev/md/"+self.r.svc.svcname.split(".")[0]+"."+self.r.rid.replace("#", ".")
        cmd = [self.r.mdadm, '--create', name, '--force', '--quiet',
               '--metadata=default']
        cmd += ['-n', str(len(devs)-spares)]
        if level:
            cmd += ["-l", level]
        if spares:
            cmd += ["-x", str(spares)]
        if chunk:
            cmd += ["-c", str(convert_size(chunk, _to="k", _round=4))]
        if layout:
            cmd += ["-p", layout]
        cmd += devs
        ret, out, err = self.r.vcall(cmd)
        if ret != 0:
            raise ex.excError(err)
        if len(out) > 0:
            self.r.log.info(out)
        if len(err) > 0:
            self.r.log.error(err)
        self.r.uuid = os.path.basename(name)
        uuid = self.get_real_uuid(name)
        self.r.uuid = uuid
        self.r.svc._set(self.r.rid, "uuid", uuid)

    def get_real_uuid(self, name):
        buff = self.r.detail()
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("UUID :"):
                return line.split(" : ")[-1]
        raise ex.excError("unable to determine md uuid")

    def unprovisioner(self):
        if self.r.uuid == "":
            return
        self.r.stop()
        for dev in self.r.sub_devs():
            self.r.vcall([self.r.mdadm, "--brief", "--zero-superblock", dev])
        self.r.svc._unset(self.r.rid, "uuid")
        self.r.svc._set(self.r.rid, "uuid", "")

