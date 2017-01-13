import os
from subprocess import Popen, PIPE

import rcStatus
from rcUtilities import justcall
from rcGlobalEnv import rcEnv
import resSync
import rcExceptions as ex

os.environ['LANG'] = 'C'

class SyncDocker(resSync.Sync):
    def __init__(self,
                 rid=None,
                 target=None,
                 **kwargs):
        resSync.Sync.__init__(self,
                              rid=rid,
                              type="sync.docker",
                              **kwargs)

        self.label = "docker img sync to %s" % ", ".join(target)
        self.target = target
        self.images = []
        self.image_id_name = {}
        self.dstfs = None
        self.dst = "docker images"
        self.targets = set()

    def get_docker_data_dir_svc_fs(self):
        l = []
        for r in self.svc.get_resources("fs"):
            l.append(r.mount_point)
        l = sorted(l)
        v = self.svc.dockerlib.docker_data_dir.split("/")
        while len(v) > 0:
            path = "/".join(v)
            if path in l:
                return path
            v = v[:-1]

    def get_images(self):
        for r in self.svc.get_resources("container.docker"):
            image = r.run_image
            image_id = self.svc.dockerlib.get_run_image_id(image)
            self.image_id_name[image_id] = r.run_image
            self.images.append(image_id)

    def get_remote_images(self, node):
        ruser = self.svc.node.get_ruser(node)
        cmd = rcEnv.rsh.split()+['-l', ruser, node, '--', rcEnv.svcmgr, '-s', self.svc.svcname, "docker", "images", "-a", "--no-trunc"]
        out, err, ret = justcall(cmd)
        images = []
        for line in out.split('\n'):
            l = line.split()
            if len(l) < 3:
                continue
            if l[0] == "REPOSITORY":
                continue
            images.append(l[2])
        return images

    def on_add(self):
        self.dstfs = self.get_docker_data_dir_svc_fs()

    def get_targets(self, action=None):
        self.targets = set()
        if 'nodes' in self.target and action in (None, 'sync_nodes'):
            self.targets |= self.svc.nodes
        if 'drpnodes' in self.target and action in (None, 'sync_drp'):
            self.targets |= self.svc.drpnodes
        self.targets -= set([rcEnv.nodename])
        for node in self.targets.copy():
            if node in self.svc.nodes:
                target = 'nodes'
            elif node in self.svc.drpnodes:
                target = 'drpnodes'
            else:
                continue
            if not self.remote_node_env(node, target):
                self.targets -= set([node])
                continue
            try:
                mounted = self.remote_fs_mounted(node)
            except ex.excError:
                mounted = False
            if not mounted:
                self.targets -= set([node])

    def can_sync(self, target=None):
        return True

    def sync_nodes(self):
        self._sync_update('sync_nodes')

    def sync_drp(self):
        self._sync_update('sync_drp')

    def sanity_checks(self):
        self.pre_sync_check_flex_primary()
        self.pre_sync_check_svc_not_up()

    def _sync_update(self, action):
        try:
            self.sanity_checks()
        except ex.excError:
            return
        self.get_targets(action)
        if len(self.targets) == 0:
            return
        self.get_images()
        for node in self.targets:
            remote_images = self.get_remote_images(node)
            missing = set(self.images) - set(remote_images)
            for image in missing:
                self.save_load(node, image)

    def save_load(self, node, image):
        ruser = self.svc.node.get_ruser(node)
        save_cmd = [rcEnv.svcmgr, "-s", self.svc.svcname, "docker", "save", self.image_id_name[image]]
        load_cmd = rcEnv.rsh.split(' ')+['-l', ruser, node, '--', rcEnv.svcmgr, "-s", self.svc.svcname, "docker", "load"]
        self.log.info(' '.join(save_cmd) + " | " + ' '.join(load_cmd))
        p1 = Popen(save_cmd, stdout=PIPE)
        p2 = Popen(load_cmd, stdin=p1.stdout, stdout=PIPE)
        out, err = p2.communicate()
        if p2.returncode != 0:
            if err is not None and len(err) > 0:
                self.log.error(err)
            raise ex.excError("sync update failed")
        if out is not None and len(out) > 0:
            self.log.info(out)

    def _status(self, verbose=False):
        self.get_targets()
        if len(self.targets) == 0:
            self.status_log("no target nodes")
            return rcStatus.NA
        self.get_images()
        total_missing = 0
        for node in self.targets:
            remote_images = self.get_remote_images(node)
            missing = set(self.images) - set(remote_images)
            n_missing = len(missing)
            total_missing += n_missing
            if n_missing > 0:
                if n_missing > 1:
                    plural = "s"
                else:
                    plural = ""
                self.status_log("target node %s miss image%s %s" % (node, plural, ','.join(missing)))
        if total_missing > 0:
            return rcStatus.WARN
        return rcStatus.UP


