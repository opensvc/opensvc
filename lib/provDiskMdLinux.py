from provisioning import Provisioning
import os
import rcExceptions as ex
from svcBuilder import conf_get_string_scope, conf_get_int_scope
from rcUtilities import convert_size
from subprocess import *

class ProvisioningDisk(Provisioning):
    def __init__(self, r):
        Provisioning.__init__(self, r)

    def provisioner(self):
        self.provisioner_md()
        self.r.log.info("provisioned")
        self.r.start()
        return True

    def provisioner_md(self):
        if self.r.has_it():
            self.r.log.info("already provisioned")
            return
        try:
            level = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "level")
        except:
            raise ex.excError("'level' provisioning parameter not set")
        try:
            devs = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, "devs").split()
        except:
            raise ex.excError("'devs' provisioning parameter not set")
        if len(devs) == 0:
            raise ex.excError("at least 2 devices must be set in the 'devs' provisioning parameter")
        try:
            spares = conf_get_int_scope(self.r.svc, self.r.svc.config, self.r.rid, 'spares')
        except:
            spares = 0
        try:
            chunk = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, 'chunk')
        except:
            chunk = None
        try:
            layout = conf_get_string_scope(self.r.svc, self.r.svc.config, self.r.rid, 'layout')
        except:
            layout = None

        # long md names cause a buffer overflow in mdadm
        name = "/dev/md/"+self.r.svc.svcname.split(".")[0]+"."+self.r.rid.replace("#", ".")
        cmd = [self.r.mdadm, '--create', name]
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
        _cmd = "yes | " + " ".join(cmd)
        self.r.log.info(_cmd)
        p1 = Popen(["yes"], stdout=PIPE)
        p2 = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=p1.stdout)
        out, err = p2.communicate()
        if p2.returncode != 0:
            raise ex.excError(err)
        if len(out) > 0:
            self.r.log.info(out)
        if len(err) > 0:
            self.r.log.error(err)
        self.r.uuid = os.path.basename(name)
        uuid = self.get_real_uuid(name)
        self.r.uuid = uuid
        self.r.svc.config.set(self.r.rid, "uuid", uuid)
        self.r.svc.write_config()

    def get_real_uuid(self, name):
        buff = self.r.detail()
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("UUID :"):
                return line.split(" : ")[-1]
        raise ex.excError("unable to determine md uuid")




