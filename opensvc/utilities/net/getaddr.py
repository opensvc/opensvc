import os
import datetime
import socket

from env import Env
from utilities.files import makedirs

def getaddr_cache_set(name, addr):
    cache_d = os.path.join(Env.paths.pathvar, "cache", "addrinfo")
    makedirs(cache_d)
    cache_f = os.path.join(cache_d, name)
    with open(cache_f, 'w') as f:
        f.write(addr)
    return addr


def getaddr_cache_get(name):
    cache_d = os.path.join(Env.paths.pathvar, "cache", "addrinfo")
    makedirs(cache_d)
    cache_f = os.path.join(cache_d, name)
    if not os.path.exists(cache_f):
        raise Exception("addrinfo cache empty for name %s" % name)
    cache_mtime = datetime.datetime.fromtimestamp(os.stat(cache_f).st_mtime)
    limit_mtime = datetime.datetime.now() - datetime.timedelta(minutes=16)
    if cache_mtime < limit_mtime:
        raise Exception("addrinfo cache expired for name %s (%s)" % (name, cache_mtime.strftime("%Y-%m-%d %H:%M:%S")))
    with open(cache_f, 'r') as f:
        addr = f.read()
    if addr.count(".") != 3 and ":" not in addr:
        raise Exception("addrinfo cache corrupted for name %s: %s" % (name, addr))
    return addr


def getaddr(name, cache_fallback, log=None):
    if cache_fallback:
        return getaddr_caching(name, log=log)
    else:
        return getaddr_non_caching(name)


def getaddr_non_caching(name, log=None):
    a = socket.getaddrinfo(name, None)
    if len(a) == 0:
        raise Exception("could not resolve name %s: empty dns request resultset" % name)
    addr = a[0][4][0]
    try:
        getaddr_cache_set(name, addr)
    except Exception as e:
        if log:
            log.warning("failed to cache name addr %s, %s: %s" % (name, addr, str(e)))
    return addr


def getaddr_caching(name, log=None):
    try:
        addr = getaddr_non_caching(name)
    except Exception as e:
        if log:
            log.warning("%s. fallback to cache." % str(e))
        addr = getaddr_cache_get(name)
    if log:
        log.info("fetched %s address for name %s from cache" % (addr, name))
    return addr
