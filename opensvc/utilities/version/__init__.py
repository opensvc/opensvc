def agent_version():
    try:
        from utilities.version import version
        return version.version
    except ImportError:
        pass

    try:
        import importlib
        importlib.reload(version)
        return version.version
    except (ImportError, AttributeError, UnboundLocalError):
        pass

    try:
        import imp
        imp.reload(version)
        return version.version
    except (AttributeError, UnboundLocalError):
        pass

    import os
    from utilities.proc import justcall
    from core.capabilities import capabilities
    from env import Env
    if "node.x.git" in capabilities:
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
        try:
            _release = out.strip().split("-")[1]
        except IndexError:
            _release = "0"
        return "-".join((_version, _release+"dev"))

    return "dev"


