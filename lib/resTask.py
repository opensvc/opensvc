import os
import pwd
import resources as Res
import rcStatus
import rcExceptions as ex
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
    except Exception as exc:
        raise ex.excError("user lookup failure: %s" % str(exc))
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
                 on_error=None,
                 **kwargs):
        Res.Resource.__init__(self, rid, type="task", **kwargs)
        self.command = command
        self.on_error = on_error
        self.user = user

    def __str__(self):
        return "%s command=%s user=%s" % (Res.Resource.__str__(self), self.command, str(self.user))

    def info(self):
        data = [
          ["command", self.command],
        ]
        if self.on_error:
            data.append(["on_error", self.on_error])
        if self.user:
            data.append(["user", self.user])
        return self.fmt_info(data)

    def has_it(self):
        return False

    def is_up(self):
        return False

    def stop(self):
        pass

    def start(self):
        pass

    def run(self):
        kwargs = {
          'blocking': True,
        }
        kwargs.update(run_as_popen_kwargs(self.user))

        try:
            self.action_triggers("", "command", **kwargs)
        except ex.excError:
            if self.on_error:
                kwargs["blocking"] = False
                self.action_triggers("", "on_error", **kwargs)
            raise ex.excError

    def _status(self, verbose=False):
        return rcStatus.NA

