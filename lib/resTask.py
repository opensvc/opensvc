import os
import pwd
import resources as Res
import rcStatus
import rcExceptions as exc
from rcGlobalEnv import rcEnv

def run_as_popen_kwargs(user):
    if rcEnv.sysname == "Windows":
        return {}
    if user is None:
        return {}
    cwd = rcEnv.pathtmp
    import pwd
    try:
        pw_record = pwd.getpwnam(user)
    except Exception as e:
        raise ex.excError("user lookup failure: %s" % str(e))
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

class Task(Res.Resource):
    def __init__(self,
                 rid=None,
                 command=None,
                 user=None,
                 type="task",
                 always_on=set([]),
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 subset=None,
                 monitor=False,
                 restart=0):
        Res.Resource.__init__(self,
                              rid, type,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset,
                              monitor=monitor,
                              restart=restart,
                              always_on=always_on)
        self.command = command
        self.user = user

    def __str__(self):
        return "%s command=%s user=%s" % (Res.Resource.__str__(self), self.command, str(self.user))

    def info(self):
        data = [
          ["command", self.command],
        ]
        if self.user:
            data.append(["user", self.user])
        return self.fmt_info(data)

    def has_it(self): return False
    def is_up(self): return False

    def stop(self):
        pass

    def start(self):
        pass

    def run(self):
        kwargs = {
          'blocking': False,
        }
        kwargs.update(run_as_popen_kwargs(self.user))

        self.action_triggers("", "command", **kwargs)

    def _status(self, verbose=False):
        return rcStatus.NA

