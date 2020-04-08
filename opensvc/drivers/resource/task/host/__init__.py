import os

import core.exceptions as ex

from .. import BaseTask, KEYWORDS as BASE_KEYWORDS
from env import Env
from core.objects.svcdict import KEYS

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
]

KEYS.register_driver(
    DRIVER_GROUP,
    DRIVER_BASENAME,
    name=__name__,
    keywords=KEYWORDS,
)


def run_as_popen_kwargs(user):
    if Env.sysname == "Windows":
        return {}
    if user is None:
        return {}
    cwd = Env.paths.pathtmp
    import pwd
    try:
        pw_record = pwd.getpwnam(user)
    except Exception as exc:
        raise ex.Error("user lookup failure: %s" % str(exc))
    user_name      = pw_record.pw_name
    user_home_dir  = pw_record.pw_dir
    user_uid  = pw_record.pw_uid
    user_gid  = pw_record.pw_gid
    env = os.environ.copy()
    env['HOME']  = user_home_dir
    env['LOGNAME']  = user_name
    env['PWD']  = cwd
    env['USER']  = user_name
    return {'preexec_fn': demote(user_uid, user_gid), 'cwd': cwd, 'env': env}

def demote(user_uid, user_gid):
    def result():
        os.setgid(user_gid)
        os.setuid(user_uid)
    return result

class TaskHost(BaseTask):
    def __init__(self, *args, **kwargs):
        kwargs["type"] = "task.host"
        BaseTask.__init__(self, *args, **kwargs)

    def _run_call(self):
        kwargs = {
            'timeout': self.timeout,
            'blocking': True,
        }
        kwargs.update(run_as_popen_kwargs(self.user))
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
