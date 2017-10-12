import os

import resources as Res
from rcUtilities import which, justcall, lazy
from rcGlobalEnv import rcEnv, Storage
import resContainer
import rcExceptions as ex

lxc = "/usr/bin/lxc"
lxd = "/usr/bin/lxd"

class Container(resContainer.Container):
    def __init__(self,
                 rid,
                 name,
                 **kwargs):
        resContainer.Container.__init__(self,
                                        rid=rid,
                                        name=name,
                                        type="container.lxd",
                                        **kwargs)

        self.runmethod = ['lxc', 'exec', name, '--']
        self.getaddr = self.dummy

    def files_to_sync(self):
        return []

    def rcp_from(self, src, dst):
        cmd = [lxc, "file", "pull", self.name+":"+src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        cmd = [lxc, "file", "push", src, self.name+":"+dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.excError("'%s' execution error:\n%s"%(' '.join(cmd), err))
        return out, err, ret

    def lxc_info(self):
        cmd = [lxc, "info", self.name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return
        return self.parse(out)

    def lxc_start(self):
        cmd = [lxc, "start", self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def lxc_stop(self):
        cmd = [lxc, "stop", self.name]
        ret, out, err = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def set_cpuset_clone_children(self):
        ppath = "/sys/fs/cgroup/cpuset"
        if not os.path.exists(ppath):
            self.log.debug("set_clone_children: %s does not exist" % ppath)
            return
        path = "/sys/fs/cgroup/cpuset/lxc"
        val = "1"
        if not os.path.exists(path):
            self.log.info("mkdir %s" % path)
            os.makedirs(path)
        for parm in ("cpuset.mems", "cpuset.cpus"):
            current_val = self.get_sysfs(path, parm)
            if current_val is None:
                continue
            if current_val == "":
                parent_val = self.get_sysfs(ppath, parm)
                self.set_sysfs(path, parm, parent_val)
        parm = "cgroup.clone_children"
        current_val = self.get_sysfs(path, parm)
        if current_val is None:
            return
        if current_val == "1":
            self.log.debug("set_cpuset_clone_children: %s/%s already set to 1" % (path, parm))
            return
        self.set_sysfs(path, parm, "1")

    def get_sysfs(self, path, parm):
        fpath = os.sep.join([path, parm])
        if not os.path.exists(fpath):
            self.log.debug("get_sysfs: %s does not exist" % path)
            return
        with open(fpath, "r") as f:
            current_val = f.read().rstrip("\n")
        self.log.debug("get_sysfs: %s contains %s" % (fpath, repr(current_val)))
        return current_val

    def set_sysfs(self, path, parm, val):
        fpath = os.sep.join([path, parm])
        self.log.info("echo %s >%s" % (val, fpath))
        with open(fpath, "w") as f:
            f.write(val)

    def cleanup_cgroup(self, t="*"):
        import glob
        for p in glob.glob("/sys/fs/cgroup/%s/lxc/%s-[0-9]" % (t, self.name)) + \
                 glob.glob("/sys/fs/cgroup/%s/lxc/%s" % (t, self.name)):
            try:
                os.rmdir(p)
                self.log.info("removed leftover cgroup %s" % p)
            except Exception as e:
                self.log.debug("failed to remove leftover cgroup %s: %s" % (p, str(e)))

    def _migrate(self):
        cmd = [lxc, 'move', self.name, self.svc.options.destination_node+":"+self.name]
        (ret, buff, err) = self.vcall(cmd)
        if ret != 0:
            raise ex.excError

    def container_start(self):
        if not self.svc.create_pg:
            self.cleanup_cgroup()
        self.set_cpuset_clone_children()
        self.lxc_start()

    def container_stop(self):
        self.lxc_stop()

    def post_container_stop(self):
        self.cleanup_links()
        self.cleanup_cgroup()

    def container_forcestop(self):
        """ no harder way to stop a lxc container, raise to signal our
            helplessness
        """
        raise ex.excError

    def get_links(self):
        data = self.lxc_info()
        if data is None:
            return  []
        return list(data.get("Ips", {}).keys())

    def cleanup_link(self, link):
        cmd = ["ip", "link", "del", "dev", link]
        out, err, ret = justcall(cmd)
        if ret == 0:
            self.log.info(" ".join(cmd))
        else:
            self.log.debug(" ".join(cmd)+out+err)

    def cleanup_links(self):
        for link in self.get_links():
            self.cleanup_link(link)

    def is_up(self):
        data = self.lxc_info()
        if data is None:
            return False
        return data["Status"] == "Running"

    def get_container_info(self):
        cpu_set = self.get_cf_value("lxc.cgroup.cpuset.cpus")
        if cpu_set is None:
            vcpus = 0
        else:
            vcpus = len(cpu_set.split(','))
        return {'vcpus': str(vcpus), 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        if not which(lxc):
            self.log.debug("lxc is not in installed")
            return False
        return True

    def _status(self, verbose=False):
        return resContainer.Container._status(self, verbose=verbose)

    def dummy(self, cache_fallback=False):
        pass

    def operational(self):
        if not resContainer.Container.operational(self):
            return False

        cmd = self.runmethod + ['test', '-f', '/bin/systemctl']
        out, err, ret = justcall(cmd)
        if ret == 1:
            # not a systemd container. no more checking.
            self.log.debug("/bin/systemctl not found in container")
            return True

        # systemd on-demand loading will let us start the encap service before
        # the network is fully initialized, causing start issues with nfs mounts
        # and listening apps.
        # => wait for systemd default target to become active
        cmd = self.runmethod + ['systemctl', 'is-active', 'default.target']
        out, err, ret = justcall(cmd)
        if ret == 1:
            # if systemctl is-active fails, retry later
            self.log.debug("systemctl is-active failed")
            return False
        if out.strip() == "active":
            self.log.debug("systemctl is-active succeeded")
            return True

        # ok, wait some more
        self.log.debug("waiting for lxc to come up")
        return False

    def __str__(self):
        return "%s name=%s" % (Res.Resource.__str__(self), self.name)

    def parse(self, buff):
        data = Storage()
        pos = {}
        _pos = []
        indent = 2
        def dset(key, val, _pos, _data=None):
            if _data is None:
                _data = data
                val = val.strip()
            if len(_pos) == 0:
                if key in _data:
                    if not isinstance(_data[key], list):
                        _data[key] = [_data[key]]
                    _data[key].append(val)
                else:
                    _data[key] = val
                return _data
            _next = _pos[0]
            if _next not in _data:
                _data[_next] = Storage()
            _data[_next] = dset(key, val, _pos[1:], _data=_data[_next])
            return _data

        for line in buff.splitlines():
            stripped_line = line.lstrip()
            _lws = len(line) - len(stripped_line)
            _pos = pos.get(_lws, [])
            key, val = stripped_line.split(":", 1)
            if val == "":
                # new dict or list
                _pos = _pos + [key]
                pos[_lws+indent] = _pos
                continue
            dset(key, val, _pos)

        return data
