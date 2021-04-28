import json
import os
import shutil
import time
from functools import wraps

import core.exceptions as ex
import utilities.lock
from env import Env
from utilities.files import makedirs


def cache_uuid():
    return os.environ.get("OSVC_CACHE_UUID") or Env.session_uuid


def get_cache_d(sid=None):
    return os.path.join(Env.paths.pathvar, "cache", sid or cache_uuid())


def cache(sig):
    def wrapper(fn):
        @wraps(fn)
        def decorator(*args, **kwargs):
            if len(args) > 0 and hasattr(args[0], "log"):
                log = args[0].log
            else:
                log = None

            if len(args) > 0 and hasattr(args[0], "cache_sig_prefix"):
                _sig = args[0].cache_sig_prefix + sig
            else:
                _sig = sig.format(args=args, kwargs=kwargs)

            fpath = cache_fpath(_sig)

            try:
                lfd = utilities.lock.lock(timeout=30, delay=0.1, lockfile=fpath + '.lock', intent="cache")
            except Exception as e:
                if log:
                    log.warning("cache locking error: %s. run command uncached." % str(e))
                return fn(*args, **kwargs)
            try:
                data = cache_get(fpath, log=log)
            except Exception as e:
                if log:
                    log.debug(str(e))
                data = fn(*args, **kwargs)
                cache_put(fpath, data, log=log)
            utilities.lock.unlock(lfd)
            return data

        return decorator

    return wrapper


def cache_fpath(sig):
    cache_d = get_cache_d()
    makedirs(cache_d)
    sig = sig.replace("/", "(slash)")
    fpath = os.path.join(cache_d, sig)
    return fpath


def cache_put(fpath, data, log=None):
    if log:
        log.debug("cache PUT: %s" % fpath)
    try:
        with open(fpath, "w") as f:
            json.dump(data, f)
    except Exception as e:
        try:
            os.unlink(fpath)
        except:
            pass
    return data


def cache_get(fpath, log=None):
    if not os.path.exists(fpath):
        raise Exception("cache MISS: %s" % fpath)
    if log:
        log.debug("cache GET: %s" % fpath)
    try:
        with open(fpath, "r") as f:
            data = json.load(f)
    except Exception as e:
        raise ex.Error("cache read error: %s" % str(e))
    return data


def clear_cache(sig, o=None):
    if o and hasattr(o, "cache_sig_prefix"):
        sig = o.cache_sig_prefix + sig
    fpath = cache_fpath(sig)
    if not os.path.exists(fpath):
        return
    if o and hasattr(o, "log"):
        o.log.debug("cache CLEAR: %s" % fpath)
    lfd = utilities.lock.lock(timeout=30, delay=0.1, lockfile=fpath + '.lock')
    try:
        os.unlink(fpath)
    except:
        pass
    utilities.lock.unlock(lfd)


def purge_cache():
    cache_d = get_cache_d()
    try:
        shutil.rmtree(cache_d)
    except:
        pass


def purge_cache_session(sid):
    cache_d = get_cache_d(sid)
    try:
        shutil.rmtree(cache_d)
    except:
        pass


def purge_cache_expired():
    cache_d = os.path.join(Env.paths.pathvar, "cache")
    if not os.path.exists(cache_d) or not os.path.isdir(cache_d):
        return
    for d in os.listdir(cache_d):
        d = os.path.join(cache_d, d)
        if not os.path.isdir(d) or not os.stat(d).st_ctime < time.time() - (21600):
            # session more recent than 6 hours
            continue
        try:
            shutil.rmtree(d)
        except:
            pass



