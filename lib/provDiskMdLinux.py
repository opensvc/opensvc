import provisioning
import os
import rcExceptions as ex

from rcGlobalEnv import rcEnv
from rcUtilities import which, bdecode
from converters import convert_size

class Prov(provisioning.Prov):
    def __init__(self, r):
        provisioning.Prov.__init__(self, r)

    def is_provisioned(self):
        if which("mdadm") is None:
            return
        return self.r.has_it()

    def provisioner(self):
        if which("mdadm") is None:
            raise ex.excError("mdadm is not installed")

        level = self.r.oget("level")
        devs = self.r.oget("devs")
        spares = self.r.oget('spares')
        chunk = self.r.oget("chunk")
        layout = self.r.oget("layout")

        if len(devs) == 0:
            raise ex.excError("at least 2 devices must be set in the 'devs' provisioning parameter")

        # long md names cause a buffer overflow in mdadm
        name = self.r.devname()
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
        self.r.log.info(" ".join(cmd))
        from subprocess import Popen, PIPE
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
        out, err = proc.communicate(input=b'no\n')
        out, err = bdecode(out).strip(), bdecode(err).strip()
        self.r.log.info(out)
        if proc.returncode != 0:
            raise ex.excError(err)
        self.r.can_rollback = True
        if len(out) > 0:
            self.r.log.info(out)
        if len(err) > 0:
            self.r.log.error(err)
        self.r.uuid = os.path.basename(name)
        uuid = self.get_real_uuid(name)
        self.r.uuid = uuid
        if self.r.shared:
            self.r.log.info("set %s.uuid = %s", self.r.rid, uuid)
            self.r.svc._set(self.r.rid, "uuid", uuid)
        else:
            self.r.log.info("set %s.uuid@%s = %s", self.r.rid, rcEnv.nodename, uuid)
            self.r.svc._set(self.r.rid, "uuid@"+rcEnv.nodename, uuid)
        self.r.svc.node.unset_lazy("devtree")

    def get_real_uuid(self, name):
        buff = self.r.detail()
        for line in buff.split("\n"):
            line = line.strip()
            if line.startswith("UUID :"):
                return line.split(" : ")[-1]
        raise ex.excError("unable to determine md uuid")

    def unprovisioner(self):
        if self.r.uuid == "" or self.r.uuid is None:
            return
        for dev in self.r.sub_devs():
            self.r.vcall([self.r.mdadm, "--brief", "--zero-superblock", dev])
        if self.r.shared:
            self.r.log.info("reset %s.uuid", self.r.rid)
            self.r.svc._set(self.r.rid, "uuid", "")
        else:
            self.r.log.info("reset %s.uuid@%s", self.r.rid, rcEnv.nodename)
            self.r.svc._set(self.r.rid, "uuid@"+rcEnv.nodename, "")
        self.r.svc.node.unset_lazy("devtree")

