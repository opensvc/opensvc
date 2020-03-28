def agent_version():
    try:
        import version
    except ImportError:
        pass

    try:
        reload(version)
        return version.version
    except (NameError, AttributeError):
        pass

    try:
        import imp
        imp.reload(version)
        return version.version
    except (AttributeError, UnboundLocalError):
        pass

    try:
        import importlib
        importlib.reload(version)
        return version.version
    except (ImportError, AttributeError, UnboundLocalError):
        pass
    import os
    from utilities.proc import which, justcall
    from env import Env
    if which("git"):
        cmd = ["git", "--git-dir", os.path.join(Env.paths.pathsvc, ".git"),
               "describe", "--tags", "--abbrev=0"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return "dev"
        _version = out.strip()
        cmd = ["git", "--git-dir", os.path.join(Env.paths.pathsvc, ".git"),
               "describe", "--tags"]
        out, err, ret = justcall(cmd)
        if ret != 0:
            return "dev"
        _release = out.strip().split("-")[1]
        return "-".join((_version, _release+"dev"))

    return "dev"


