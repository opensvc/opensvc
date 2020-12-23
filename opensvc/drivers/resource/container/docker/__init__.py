"""
Docker container resource driver module.
"""
import os
import re
import shlex

from itertools import chain

import core.status
import utilities.subsystems.docker as dockerlib
import core.exceptions as ex
import utilities.ping

from .. import \
    KW_NO_PREEMPT_ABORT, \
    KW_OSVC_ROOT_PATH, \
    KW_GUESTOS, \
    KW_PROMOTE_RW, \
    KW_SCSIRESERV, \
    BaseContainer
from utilities.lazy import unset_lazy, lazy
from utilities.naming import factory, svc_pathvar
from utilities.files import makedirs
from core.resource import Resource
from utilities.converters import print_duration
from core.objects.svcdict import KEYS
from utilities.proc import justcall, drop_option, has_option, get_option, get_options

DRIVER_GROUP = "container"
DRIVER_BASENAME = "docker"
DRIVER_BASENAME_ALIASES = ["oci"]
KEYWORDS = [
    {
        "keyword": "hostname",
        "at": True,
        "text": "Set the container hostname. If not set, a unique id is used."
    },
    {
        "keyword": "detach",
        "at": True,
        "default": True,
        "convert": "boolean",
        "text": "Run container in background. Set to ``false`` only for init containers, alongside :kw:`start_timeout` and the :c-tag:`nostatus` tag.",
    },
    {
        "keyword": "entrypoint",
        "at": True,
        "text": "The script or binary executed in the container. Args must be set in :kw:`command`.",
        "example": "/bin/sh"
    },
    {
        "keyword": "rm",
        "at": True,
        "default": False,
        "convert": "boolean",
        "text": "If set to ``true``, add :opt:`--rm` to the docker run args and make sure the instance is removed on resource stop.",
        "example": False
    },
    {
        "keyword": "volume_mounts",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<volume name|local dir>:<containerized mount path>:<mount options>``. "
                "When the source is a local dir, the default <mount option> is rw. "
                "When the source is a volume name, the default <mount option> is taken from volume access.",
        "example": "myvol1:/vol1 myvol2:/vol2:rw /localdir:/data:ro"
    },
    {
        "keyword": "environment",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<var>=<value>``. A shell expression spliter is applied, so double quotes can be around values only or whole ``<var>=<value>``. Variables are uppercased.",
        "example": """AA="a a" "BB=c c" CC=d """
    },
    {
        "keyword": "secrets_environment",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "A whitespace separated list of ``<var>=<secret name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<secret name>/<key path>`` only or whole ``<var>=<secret name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "configs_environment",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<var>=<config name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<config name>/<key path>`` only or whole ``<var>=<config name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "devices",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<host devpath>:<containerized devpath>``, specifying the host devices the container should have access to.",
        "example": "myvol1:/dev/xvda myvol2:/dev/xvdb"
    },
    {
        "keyword": "netns",
        "at": True,
        "text": "Sets the :cmd:`docker run --net` argument. The default is ``none`` if :opt:`--net` is not specified in :kw:`run_args`, meaning the container will have a private netns other containers can share. A :c-res:`ip.netns` or :c-res:`ip.cni` resource can configure an ip address in this container. A container with ``netns=container#0`` will share the container#0 netns. In this case agent format a :opt:`--net=container:<name of container#0 docker instance>`. ``netns=host`` shares the host netns.",
        "example": "container#0"
    },
    {
        "keyword": "userns",
        "at": True,
        "candidates": ("host", None),
        "text": "Sets the :cmd:`docker run --userns` argument. If not set, the container will have a private userns other containers can share. A container with ``userns=host`` will share the host's userns.",
        "example": "container#0"
    },
    {
        "keyword": "pidns",
        "at": True,
        "text": "Sets the :cmd:`docker run --pid` argument. If not set, the container will have a private pidns other containers can share. Usually a pidns sharer will run a google/pause image to reap zombies. A container with ``pidns=container#0`` will share the container#0 pidns. In this case agent format a :opt:`--pid=container:<name of container#0 docker instance>`. Use ``pidns=host`` to share the host's pidns.",
        "example": "container#0"
    },
    {
        "keyword": "ipcns",
        "at": True,
        "text": "Sets the :cmd:`docker run --ipc` argument. If not set, the docker daemon's default value is used. ``ipcns=none`` does not mount /dev/shm. ``ipcns=private`` creates a ipcns other containers can not share. ``ipcns=shareable`` creates a netns other containers can share. ``ipcns=container#0`` will share the container#0 ipcns.",
        "example": "container#0"
    },
    {
        "keyword": "utsns",
        "at": True,
        "candidates": (None, "host"),
        "text": "Sets the :cmd:`docker run --uts` argument. If not set, the container will have a private utsns. A container with ``utsns=host`` will share the host's hostname.",
        "example": "container#0"
    },
    {
        "keyword": "privileged",
        "at": True,
        "convert": "tristate",
        "text": "Give extended privileges to the container.",
        "example": "container#0"
    },
    {
        "keyword": "interactive",
        "at": True,
        "convert": "tristate",
        "text": "Keep stdin open even if not attached. To use if the container entrypoint is a shell.",
    },
    {
        "keyword": "tty",
        "at": True,
        "convert": "tristate",
        "text": "Allocate a pseudo-tty.",
    },
    {
        "keyword": "name",
        "at": True,
        "default_text": "<autogenerated>",
        "text": "The name to assign to the container on docker run. If none is specified a ``<namespace>..<name>.container.<rid idx>`` name is automatically assigned.",
        "example": "osvcprd..rundeck.container.db"
    },
    {
        "keyword": "image",
        "at": True,
        "required": True,
        "text": "The docker image pull, and run the container with.",
        "example": "83f2a3dd2980 or ubuntu:latest"
    },
    {
        "keyword": "image_pull_policy",
        "at": True,
        "required": False,
        "default": "once",
        "candidates": ["once", "always"],
        "text": "The docker image pull policy. ``always`` pull upon each container start, ``once`` pull if not already pulled (default).",
        "example": "once"
    },
    {
        "keyword": "command",
        "protoname": "run_command",
        "at": True,
        "convert": "shlex",
        "text": "The command to execute in the docker container on run.",
        "example": "/opt/tomcat/bin/catalina.sh"
    },
    {
        "keyword": "run_args",
        "at": True,
        "convert": "expanded_shlex",
        "text": "Extra arguments to pass to the docker run command, like volume and port mappings.",
        "example": "-v /opt/docker.opensvc.com/vol1:/vol1:rw -p 37.59.71.25:8080:8080"
    },
    {
        "keyword": "pull_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the container action a failure.",
        "default": "2m",
        "example": "2m"
    },
    {
        "keyword": "start_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the container action a failure.",
        "default": "5",
        "example": "5"
    },
    {
        "keyword": "stop_timeout",
        "convert": "duration",
        "at": True,
        "text": "Wait for <duration> before declaring the container action a failure.",
        "default": "120",
        "example": "180"
    },
    {
        "keyword": "registry_creds",
        "at": True,
        "text": "The name of a secret in the same namespace having a config.json key which value is used to login to the container image registry. If not specified, the node-level registry credential store is used.",
        "example": "creds-registry-opensvc-com"
    },
    KW_NO_PREEMPT_ABORT,
    KW_OSVC_ROOT_PATH,
    KW_GUESTOS,
    KW_PROMOTE_RW,
    KW_SCSIRESERV,
]
DEPRECATED_KEYWORDS = {
    "container.docker.run_image": "image",
    "container.docker.run_command": "command",
    "container.docker.net": "netns",
    "container.oci.run_image": "image",
    "container.oci.run_command": "command",
    "container.oci.net": "netns",
}
REVERSE_DEPRECATED_KEYWORDS = {
    "container.docker.image": "run_image",
    "container.docker.command": "run_command",
    "container.docker.netns": "net",
    "container.oci.image": "run_image",
    "container.oci.command": "run_command",
    "container.oci.netns": "net",
}

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
    deprecated_keywords=DEPRECATED_KEYWORDS,
    reverse_deprecated_keywords=REVERSE_DEPRECATED_KEYWORDS,
    driver_basename_aliases=DRIVER_BASENAME_ALIASES,
)


ATTR_MAP = {
    "hostname": {
        "attr": "vm_hostname",
        "path": ["Config", "Hostname"],
    },
    "privileged": {
        "path": ["HostConfig", "Privileged"],
    },
    "interactive": {
        "path": ["Config", "OpenStdin"],
    },
    "entrypoint": {
        "path": ["Config", "Entrypoint"],
        "cmp": "cmp_entrypoint",
    },
    "tty": {
        "path": ["Config", "Tty"],
    },
    #    "rm": {
    #        "path": ["HostConfig", "AutoRemove"],
    #    },
    "netns": {
        "path": ["HostConfig", "NetworkMode"],
        "cmp": "cmp_ns",
    },
    "pidns": {
        "path": ["HostConfig", "PidMode"],
        "cmp": "cmp_ns",
    },
    "ipcns": {
        "path": ["HostConfig", "IpcMode"],
        "cmp": "cmp_ns",
    },
    "utsns": {
        "path": ["HostConfig", "UTSMode"],
        "cmp": "cmp_ns",
    },
    "userns": {
        "path": ["HostConfig", "UsernsMode"],
        "cmp": "cmp_ns",
    },
}

def driver_capabilities(node=None):
    data = []
    if dockerlib.has_docker(["docker", "docker.io"]):
        data += [
            "container.docker",
            "container.docker.registry_creds",
            "container.docker.signal",
        ]
    return data

def alarm_handler(signum, frame):
    raise KeyboardInterrupt


class ContainerDocker(BaseContainer):
    """
    Docker container resource driver.
    """
    default_net = "none"
    dns_option_option = "--dns-option"

    def __init__(self,
                 name="",
                 type="container.docker",
                 hostname=None,
                 image=None,
                 image_pull_policy="once",
                 run_command=None,
                 run_args=None,
                 detach=True,
                 entrypoint=None,
                 rm=None,
                 netns=None,
                 userns=None,
                 pidns=None,
                 utsns=None,
                 ipcns=None,
                 privileged=None,
                 interactive=None,
                 tty=None,
                 volume_mounts=None,
                 devices=None,
                 environment=None,
                 secrets_environment=None,
                 configs_environment=None,
                 registry_creds=None,
                 guestos="Linux",
                 pull_timeout=120,
                 **kwargs):
        super(ContainerDocker, self).__init__(
            name="",
            type=type,
            guestos=guestos,
            **kwargs
        )
        self.user_defined_name = name
        self.hostname = hostname
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.pull_timeout = pull_timeout
        self.run_command = run_command
        self.run_args = run_args
        self.detach = detach
        self.entrypoint = entrypoint
        self.rm = rm
        self.netns = netns
        self.userns = userns
        self.pidns = pidns
        self.utsns = utsns
        self.ipcns = ipcns
        self.privileged = privileged
        self.interactive = interactive
        self.tty = tty
        self.volume_mounts = volume_mounts if volume_mounts else []
        self.devices = devices if devices else []
        self.volumes = {}
        self.environment = environment
        self.secrets_environment = secrets_environment
        self.configs_environment = configs_environment
        self.registry_creds = registry_creds
        if not self.detach:
            self.rm = True

    @lazy
    def lib(self):
        """
        Lazy allocator for the dockerlib object.
        """
        try:
            return self.svc.dockerlib
        except AttributeError:
            self.svc.dockerlib = dockerlib.DockerLib(self.svc)
            return self.svc.dockerlib

    def on_add(self):
        try:
            self.volume_options()
        except ex.Error:
            # volume not created yet
            pass
        try:
            self.device_options()
        except ex.Error:
            # volume not created yet
            pass

    def abort_start_ping(self, *args, **kwargs):
        return False

    def getaddr(self, *args, **kwargs):
        return

    @lazy
    def container_name(self):
        """
        Format a docker container name
        """
        if self.user_defined_name:
            return self.user_defined_name
        if self.svc.namespace:
            container_name = self.svc.namespace + ".."
        else:
            container_name = ""
        container_name += self.svc.name + '.' + self.rid
        return container_name.replace('#', '.')

    @lazy
    def container_label_id(self):
        """
        Format a docker container name
        """
        return "com.opensvc.id=%s.%s" % (self.svc.id, self.rid)

    @lazy
    def name(self):  # pylint: disable=method-hidden
        return self.container_name

    @lazy
    def container_id(self):
        return self.lib.get_container_id(self)

    @lazy
    def label(self):  # pylint: disable=method-hidden
        return "docker " + self.lib.image_userfriendly_name(self)

    def __str__(self):
        return "%s name=%s" % (Resource.__str__(self), self.name)

    def rcmd(self, cmd):
        cmd = self.lib.docker_cmd + ['exec', '-t', self.container_name] + cmd
        return justcall(cmd)

    def enter(self):
        for cmd in [["/bin/bash"], ["/bin/sh"]]:
            try:
                self.execute(interactive=True, tty=True, cmd=cmd)
                return
            except ValueError:
                continue
            else:
                return

    def execute(self, interactive=False, tty=False, cmd=None):
        import subprocess
        xcmd = self.lib.docker_cmd + ["exec"]
        if interactive:
            xcmd.append("-i")
        if tty:
            xcmd.append("-t")
        xcmd.append(self.container_name)
        xcmd += cmd
        proc = subprocess.Popen(xcmd)
        proc.communicate()
        if proc.returncode in [126, 127]:
            # executable not found
            raise ValueError

    def rcp_from(self, src, dst):
        """
        Copy <src> from the container's rootfs to <dst> in the host's fs.
        """
        cmd = self.lib.docker_cmd + ["cp", self.container_name + ":" + src, dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s" % (" ".join(cmd), err))
        return out, err, ret

    def rcp(self, src, dst):
        """
        Copy <src> from the host's fs to the container's rootfs.
        """
        cmd = self.lib.docker_cmd + ["cp", src, self.container_name + ":" + dst]
        out, err, ret = justcall(cmd)
        if ret != 0:
            raise ex.Error("'%s' execution error:\n%s" % (" ".join(cmd), err))
        return out, err, ret

    def files_to_sync(self):
        """
        Files to contribute to sync#i0.
        """
        return []

    def operational(self):
        """
        Always return True for docker containers.
        """
        return True

    @lazy
    def vm_hostname(self):
        """
        The container hostname
        """
        try:
            hostname = self.hostname
        except ex.OptNotFound:
            hostname = ""
        return hostname

    def wait_for_startup(self):
        if not self.detach:
            return
        super(ContainerDocker, self).wait_for_startup()

    def wait_for_removed(self):
        def removed():
            self.is_up_clear_cache()
            if self.container_id:
                return False
            return True

        self.wait_for_fn(removed, 10, 1, errmsg="waited too long for container removal")

    def container_rm(self):
        """
        Remove the resource docker instance.
        """
        if not self.lib.docker_running():
            return
        cmd = self.lib.docker_cmd + ['rm', self.container_name]
        out, err, ret = justcall(cmd)
        if ret != 0:
            if "No such container" in err:
                pass
            elif "no such file" in err:
                pass
            elif "removal" in err and "already in progress" in err:
                self.wait_for_removed()
            else:
                self.log.info(" ".join(cmd))
                raise ex.Error(err)
        else:
            self.log.info(" ".join(cmd))
        self.is_up_clear_cache()

    def client_config(self):
        if not self.registry_creds:
            return []
        self.install_client_config()
        args = self.lib.client_config_args(self.registry_creds_path)
        return args

    def install_client_config(self):
        buff = self.registry_creds_sec.decode_key("config.json")
        makedirs(os.path.dirname(self.registry_creds_path), uid=0, gid=0, mode=0o600)
        self.registry_creds_sec.write_key(self.registry_creds_path, buff, uid=0, gid=0, mode=0o600)

    @lazy
    def registry_creds_path(self):
        if not self.registry_creds:
            return
        var_d = svc_pathvar(self.registry_creds_sec.path)
        return os.path.join(var_d, "registry_creds", "config.json")

    @lazy
    def registry_creds_sec(self):
        if not self.registry_creds:
            return
        return factory("sec")(self.registry_creds, namespace=self.svc.namespace, volatile=True)

    def docker(self, action):
        """
        Wrap docker commands to honor <action>.
        """
        if self.lib.docker_cmd is None:
            raise ex.Error("docker executable not found")
        sec_env = {}
        cfg_env = {}
        cmd = self.lib.docker_cmd + []
        if action == "start":
            if self.lib.config_args_position_head:
                cmd += self.client_config()
            if self.rm:
                self.container_rm()
            if self.container_id is None:
                self.is_up_clear_cache()
            if self.container_id is None:
                try:
                    image_id = self.lib.get_image_id(self.image)
                except ValueError as exc:
                    raise ex.Error(str(exc))
                if image_id is None:
                    if not self.registry_creds:
                        self.lib.docker_login(self.image)
                    if self.start_timeout:
                        self.log.info("push start timeout to %s (cached) + %s (pull)",
                                      print_duration(self.start_timeout),
                                      print_duration(self.pull_timeout))
                        self.start_timeout += self.pull_timeout
                sec_env = self.kind_environment_env("sec", self.secrets_environment)
                cfg_env = self.kind_environment_env("cfg", self.configs_environment)
                cmd += ["run"]
                if not self.lib.config_args_position_head:
                    cmd += self.client_config()
                cmd += self._add_run_args()
                for var in sec_env:
                    cmd += ["-e", var]
                for var in cfg_env:
                    cmd += ["-e", var]
                cmd += [self.image]
                if self.run_command:
                    cmd += self.run_command
            else:
                cmd += ["start", self.container_id]
        elif action == "stop":
            cmd += ["stop", self.container_id]
        elif action == "kill":
            cmd += ["kill", self.container_id]
        else:
            self.log.error("unsupported docker action: %s", action)
            return 1

        env = {}
        env.update(os.environ)
        env.update(sec_env)
        env.update(cfg_env)

        if action == "start":
            timeout = self.start_timeout or None
        else:
            timeout = None

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self.vcall, cmd, warn_to_info=True, env=env)
            try:
                ret = future.result(timeout=timeout)[0]
            except concurrent.futures.TimeoutError:
                self.log.error("%s timeout exceeded", print_duration(timeout))
                if action == "start":
                    cmd = self.lib.docker_cmd + ["kill", self.container_name]
                    self.vcall(cmd, warn_to_info=True, env=env)
                ret = 1

        if ret != 0:
            raise ex.Error

        if action == "start":
            self.is_up_clear_cache()
        elif action in ("stop", "kill"):
            if self.rm:
                self.container_rm()
            self.is_up_clear_cache()
            self.lib.docker_stop()

    def device_options(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args
        devices = []
        for arg in chain(get_options("--device", args), iter(self.devices)):
            elements = arg.split(":")
            if not elements or len(elements) not in (2, 3):
                continue
            if not elements[0].startswith(os.sep):
                # vol service
                elements[0], vol = self.replace_volname(elements[0], mode="blk", strict=False, errors=errors)
                if not elements[0]:
                    continue
                devices.append(":".join(elements))
            elif not os.path.exists(elements[0]):
                # host path
                raise ex.Error("source dir of mapping %s does not "
                                  "exist" % arg)
            else:
                devices.append(arg)
        return devices

    def volume_options(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args
        volumes = []
        dsts = []
        for volarg in chain(get_options("-v", args), get_options("--volume", args), iter(self.volume_mounts)):
            elements = volarg.split(":")
            if not elements or len(elements) not in (2, 3):
                continue
            if elements[1] in dsts:
                raise ex.Error("different volume mounts use the same destination "
                                  "mount point: %s" % elements[1])
            if not elements[0].startswith(os.sep):
                # vol service
                wants_ro = False
                elements[0], vol = self.replace_volname(elements[0], strict=False, errors=errors)
                if not elements[0]:
                    continue
                try:
                    options = elements[2].split(",")
                    if 'ro' in options:
                        wants_ro = True
                    options = drop_option("ro", options)
                    options = drop_option("rw", options)
                    del elements[2]
                except Exception:
                    options = []
                if wants_ro or (vol and vol.volsvc.access.startswith("ro")):
                    options.insert(0, "ro")
                else:
                    options.insert(0, "rw")
                elements.append(",".join(options))
                volumes.append(":".join(elements))
            elif not os.path.exists(elements[0]):
                # host path
                raise ex.Error("source dir of mapping %s does not "
                                  "exist" % volarg)
            else:
                volumes.append(volarg)
            dsts.append(elements[1])
        return volumes

    def environment_options(self):
        if self.environment is None:
            return []
        options = []
        for mapping in self.environment:
            try:
                var, val = mapping.split("=", 1)
            except Exception as exc:
                self.log.info("ignored environment mapping %s: %s", mapping, exc)
                continue
            var = var.upper()
            options += ["-e", "%s=%s" % (var, val)]
        return options

    def cgroup_options(self):
        if not self.lib.docker_min_version("1.7"):
            return []
        if self.lib.docker_info.get("CgroupDriver") != "cgroupfs":
            return []
        return ["--cgroup-parent", self.cgroup_dir]

    def _add_run_args(self, errors="raise"):
        if self.run_args is None:
            args = []
        else:
            args = [] + self.run_args

        args = drop_option("-d", args, drop_value=False)
        args = drop_option("--detach", args, drop_value=False)
        if self.detach:
            args += ["--detach"]

        # drop user specified --name. we set ours already
        args = drop_option("--name", args, drop_value=True)
        args = drop_option("-n", args, drop_value=True)
        args += ['--name=' + self.container_name]
        args += ['--label=' + self.container_label_id]
        args += ['--label=com.opensvc.path=' + self.svc.path]
        args += ['--label=com.opensvc.namespace=%s' % (self.svc.namespace if self.svc.namespace else "root")]
        args += ['--label=com.opensvc.name=' + self.svc.name]
        args += ['--label=com.opensvc.kind=' + self.svc.kind]
        args += ['--label=com.opensvc.rid=' + self.rid]

        args = drop_option("--hostname", args, drop_value=True)
        args = drop_option("-h", args, drop_value=True)
        if not self.netns or (self.netns != "host" and not self.netns.startswith("container#")):
            # only allow hostname setting if the container has a private netns
            if self.vm_hostname:
                args += ["--hostname", self.vm_hostname]
            elif not self.run_args:
                pass
            else:
                hostname = get_option("--hostname", self.run_args, boolean=False)
                if not hostname:
                    hostname = get_option("-h", self.run_args, boolean=False)
                if hostname:
                    args += ["--hostname", hostname]

        if self.entrypoint:
            args = drop_option("--entrypoint", args, drop_value=True)
            args += ["--entrypoint", self.entrypoint]

        if self.netns:
            args = drop_option("--net", args, drop_value=True)
            args = drop_option("--network", args, drop_value=True)
            if self.netns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--net=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.Error("resource %s, referenced in %s.netns, does not exist" % (self.netns, self.rid))
            else:
                args += ["--net=" + self.netns]
        elif not has_option("--net", args):
            args += ["--net=" + self.default_net]

        if self.pidns:
            args = drop_option("--pid", args, drop_value=True)
            if self.pidns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--pid=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.Error("resource %s, referenced in %s.pidns, does not exist" % (self.pidns, self.rid))
            else:
                args += ["--pid=" + self.pidns]

        if self.ipcns:
            args = drop_option("--ipc", args, drop_value=True)
            if self.ipcns.startswith("container#"):
                res = self.svc.get_resource(self.netns)
                if res is not None:
                    args += ["--ipc=container:" + res.container_name]
                elif errors == "raise":
                    raise ex.Error("resource %s, referenced in %s.ipcns, does not exist" % (self.ipcns, self.rid))
            else:
                args += ["--ipc=" + self.ipcns]

        if self.utsns == "host":
            args = drop_option("--uts", args, drop_value=True)
            args += ["--uts=host"]

        if self.userns is not None:
            args = drop_option("--userns", args, drop_value=True)
        if self.userns == "host":
            args += ["--userns=host"]

        if self.privileged is not None:
            args = drop_option("--privileged", args, drop_value=False)
        if self.privileged:
            args += ["--privileged"]

        if self.interactive is not None:
            args = drop_option("--interactive", args, drop_value=False)
            args = drop_option("-i", args, drop_value=False)
        if self.interactive:
            args += ["--interactive"]

        if self.tty is not None:
            args = drop_option("--tty", args, drop_value=False)
            args = drop_option("-t", args, drop_value=False)
        if self.tty:
            args += ["--tty"]

        drop_option("--rm", args, drop_value=False)

        drop_option("-v", args, drop_value=True)
        drop_option("--volume", args, drop_value=True)
        for vol in self.volume_options(errors=errors):
            args.append("--volume=%s" % vol)

        drop_option("--device", args, drop_value=True)
        for dev in self.device_options(errors=errors):
            args.append("--device=%s" % dev)

        args += self.cgroup_options()

        def dns_opts(args):
            if not self.svc.node.dns or "--dns" in args:
                return []
            net = get_option("--net", args, boolean=False)
            if net and net.startswith("container:"):
                return []
            if net == "host":
                return []
            l = []
            for dns in self.svc.node.dns:
                l += ["--dns", dns]
            for search in self.dns_search():
                l += ["--dns-search", search]
            dns_options = [o for o in get_options(self.dns_option_option, args)]
            args = drop_option(self.dns_option_option, args, drop_value=True)
            for o in self.dns_options(dns_options):
                l += [self.dns_option_option, o]
            return l

        args += dns_opts(args)
        args += self.environment_options()
        return args

    @lazy
    def cgroup_dir(self):
        return os.sep + self.svc.pg.get_cgroup_relpath(self)

    def image_pull(self):
        self.client_config()
        self.lib.image_pull(self.image, config=self.registry_creds_path)

    def container_start(self):
        self.docker('start')

    def _start(self):
        self.lib.docker_start()
        super(ContainerDocker, self).start()

    def provision(self):
        super(ContainerDocker, self).provision()
        self.svc.sub_set_action("ip", "provision", tags=set([self.rid]))

    def unprovision(self):
        self.svc.sub_set_action("ip", "unprovision", tags=set([self.rid]))
        super(ContainerDocker, self).unprovision()
        self.container_rm()

    def provisioner_shared_non_leader(self):
        if self.lib.docker_daemon_private:
            return
        try:
            image_id = self.lib.get_image_id(self.image)
            return
        except ValueError as exc:
            pass
        try:
            self.image_pull()
            return
        except ex.Error as exc1:
            self.log.warning("could not pull image '%s': %s", self.image, str(exc1).strip())

    def start(self):
        if self.image_pull_policy == "always":
            self.image_pull()
        try:
            self._start()
        except KeyboardInterrupt:
            if not self.detach:
                self.is_up_clear_cache()
                self.container_forcestop()
                self.container_rm()
                raise ex.Error("timeout")
            else:
                raise ex.AbortAction
        self.svc.sub_set_action("ip", "start", tags=set([self.rid]))

    def container_stop(self):
        self.docker('stop')

    def stop(self):
        self.svc.sub_set_action("ip", "stop", tags=set([self.rid]))
        self._stop()

    def _stop(self):
        if not self.lib.docker_running():
            return
        super(ContainerDocker, self).stop()
        if self.rm:
            self.container_rm()
        self.is_up_clear_cache()
        self.lib.docker_stop()

    def _info(self):
        """
        Return keys to contribute to resinfo.
        """
        data = self.lib._info()
        data.append([self.rid, "run_args", " ".join(self._add_run_args(errors="ignore"))])
        data.append([self.rid, "rm", str(self.rm)])
        data.append([self.rid, "container_name", str(self.container_name)])
        if self.netns:
            data.append([self.rid, "netns", str(self.netns)])
        if self.run_command:
            data.append([self.rid, "run_command", " ".join(self.run_command)])
        return data

    def _status_container_image(self, inspect_data):
        try:
            image_id = self.lib.get_image_id(self.image)
        except ValueError as exc:
            self.status_log(str(exc))
            return
        try:
            running_image_id = re.sub("^sha256:", "", inspect_data["Image"])
        except KeyError:
            return
        if image_id is None:
            self.status_log("image '%s' is not pulled yet." % self.image)
        elif image_id != running_image_id:
            self.status_log("the current container is based on image '%s' "
                            "instead of '%s'" % (running_image_id, image_id))

    def cmp_entrypoint(self, current, target, data):
        try:
            alt_target = shlex.split(target)
            if current == target or current == alt_target:
                return
            self.status_log("%s=%s, but %s=%s" % \
                            (".".join(data["path"]), current, data["attr"], target))
        except Exception:
            pass

    def cmp_ns(self, current, target, data):
        try:
            res = self.svc.get_resource(self.netns)
            target = "container:" + res.container_name
            if current == target:
                return
            target = "container:" + res.container_id
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, data["attr"], target))
        except Exception:
            pass

    def _status_inspect(self):
        try:
            inspect_data = self.lib.docker_inspect(self.container_id)
        except Exception:
            return
        self._status_container_image(inspect_data)

        def get(path, data=None):
            try:
                return get(path[1:], data[path[0]])
            except IndexError:
                return data[path[0]]

        def validate(attr, data):
            try:
                current = get(data["path"], inspect_data)
            except KeyError:
                return
            _attr = data.get("attr", attr)
            _fn = data.get("cmp")
            target = getattr(self, _attr)
            if not target:
                return
            if _fn:
                _fn = getattr(self, _fn)
                return _fn(current, target, data)
            if current != target:
                self.status_log("%s=%s, but %s=%s" % \
                                (".".join(data["path"]), current, attr, target))

        if get(["State", "Status"], inspect_data) != "running":
            return
        for attr, data in ATTR_MAP.items():
            validate(attr, data)

    def _status(self, verbose=False):
        if not self.detach:
            return core.status.NA
        try:
            self.lib.docker_exe
        except ex.InitError as exc:
            self.status_log(str(exc), "warn")
            return core.status.DOWN
        if not self.lib.docker_running():
            self.status_log("docker daemon is not running", "info")
            return core.status.DOWN
        sta = super(ContainerDocker, self)._status(verbose)
        self._status_inspect()
        return sta

    def send_signal(self, sig):
        if not self.is_up():
            return
        cmd = self.lib.docker_cmd + ["kill", "-s", str(sig), self.container_id]
        self.vcall(cmd)

    def container_forcestop(self):
        self.docker('kill')

    def _ping(self):
        return utilities.ping.check_ping(self.addr, timeout=1)

    def is_down(self):
        return not self.is_up()

    def is_up_clear_cache(self):
        self.unset_lazy("container_id")
        unset_lazy(self.lib, "container_ps")
        unset_lazy(self.lib, "running_instance_ids")
        unset_lazy(self.lib, "container_by_label")
        unset_lazy(self.lib, "container_by_name")
        unset_lazy(self.lib, "containers_inspect")
        unset_lazy(self.lib, "images")

    def is_up(self):
        if self.lib.docker_daemon_private and \
                self.lib.container_data_dir is None:
            self.status_log("DEFAULT.container_data_dir must be defined")

        if not self.lib.docker_running():
            return False

        if self.container_id is None:
            self.status_log("can not find container id", "info")
            return False
        if self.container_id in self.lib.get_running_instance_ids():
            return True
        return False

    def get_container_info(self):
        return {'vcpus': '0', 'vmem': '0'}

    def check_manual_boot(self):
        return True

    def check_capabilities(self):
        return True

    def container_pid(self):
        try:
            data = self.lib.docker_inspect(self.container_id)
            return data["State"]["Pid"]
        except (IndexError, KeyError):
            return

    def container_sandboxkey(self):
        try:
            data = self.lib.docker_inspect(self.container_id)
            return data["NetworkSettings"]["SandboxKey"]
        except (AttributeError, IndexError, KeyError):
            return

    def cni_containerid(self):
        """
        Used by ip.cni
        """
        return self.container_pid()

    def cni_netns(self):
        """
        Used by ip.cni
        """
        return self.container_sandboxkey()

    def is_provisioned(self):
        return True

    def post_provision_start(self):
        self._start()
        self.status(resfresh=True)

    def pre_provision_stop(self):
        self._stop()
        self.status(resfresh=True)
