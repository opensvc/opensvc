import os

import core.exceptions as ex

from .. import BaseTask, KEYWORDS as BASE_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS
from drivers.resource.app import run_as_popen_kwargs
from utilities.lazy import lazy

DRIVER_GROUP = "task"
DRIVER_BASENAME = "host"
KEYWORDS = BASE_KEYWORDS + [
    {
        "keyword": "command",
        "at": True, 
        "required": True,
        "text": "The command to execute on 'run' action and at scheduled interval. The default schedule for tasks is ``@0``.",
        "example": "/srv/{name}/data/scripts/backup.sh"
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
        "keyword": "environment",
        "at": True,
        "convert": "shlex",
        "default": [],
        "text": "The whitespace separated list of ``<var>=<config name>/<key path>``. A shell expression spliter is applied, so double quotes can be around ``<config name>/<key path>`` only or whole ``<var>=<config name>/<key path>``. Variables are uppercased.",
        "example": "CRT=cert1/server.crt PEM=cert1/server.pem"
    },
    {
        "keyword": "limit_as",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_cpu",
        "convert": "duration",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_core",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_data",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_fsize",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_memlock",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nofile",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_nproc",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_rss",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_stack",
        "convert": "size",
        "at": True,
        "text": ""
    },
    {
        "keyword": "limit_vmem",
        "convert": "size",
        "at": True,
        "text": ""
    },
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


class TaskHost(BaseTask):
    def __init__(self, *args, **kwargs):
        kwargs["type"] = "task.host"
        BaseTask.__init__(self, *args, **kwargs)

    @lazy
    def limits(self):
        data = {}
        try:
            import resource
        except ImportError:
            return data
        for key in ("as", "cpu", "core", "data", "fsize", "memlock", "nofile", "nproc", "rss", "stack", "vmem"):
            try:
                data[key] = self.conf_get("limit_"+key)
            except ex.OptNotFound:
                continue
            rlim = getattr(resource, "RLIMIT_"+key.upper())
            _vs, _vg = resource.getrlimit(rlim)
            if data[key] > _vs:
                if data[key] > _vg:
                    _vg = data[key]
                resource.setrlimit(rlim, (data[key], _vg))
        return data

    def _run_call(self):
        kwargs = {
            'timeout': self.timeout,
            'blocking': True,
        }
        kwargs.update(run_as_popen_kwargs(None, cwd=self.cwd, user=self.user, group=self.group, limits=self.limits, umask=self.umask))
        if self.configs_environment or self.secrets_environment:
            if "env" not in kwargs:
                kwargs["env"] = {}
            kwargs["env"].update(self.kind_environment_env("cfg", self.configs_environment))
            kwargs["env"].update(self.kind_environment_env("sec", self.secrets_environment))
        if self.environment:
            if "env" not in kwargs:
                kwargs["env"] = {}
            for mapping in self.environment:
                try:
                    var, val = mapping.split("=", 1)
                except Exception as exc:
                    self.log.info("ignored environment mapping %s: %s", mapping, exc)
                    continue
                var = var.upper()
                kwargs["env"][var] = val

        try:
            self.action_triggers("", "command", **kwargs)
            self.write_last_run_retcode(0)
        except ex.Error:
            self.write_last_run_retcode(1)
            if self.on_error:
                kwargs["blocking"] = False
                self.action_triggers("", "on_error", **kwargs)
            raise
