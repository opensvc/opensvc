import os
import stat

PROTECTED_DIRS = [
    '/',
    '/bin',
    '/boot',
    '/dev',
    '/dev/pts',
    '/dev/shm',
    '/home',
    '/opt',
    '/proc',
    '/sys',
    '/tmp',
    '/usr',
    '/var',
]


def getmount(path):
    path = os.path.abspath(path)
    while path != os.path.sep:
        if not os.path.islink(path) and os.path.ismount(path):
            return path
        path = os.path.abspath(os.path.join(path, os.pardir))
    return path


def protected_dir(path):
    path = path.rstrip("/")
    if path in PROTECTED_DIRS:
        return True
    return False


def protected_mount(path):
    mount = getmount(path)
    if mount in PROTECTED_DIRS:
        return True
    return False


def fsum(fpath):
    """
    Return a file content checksum
    """
    import hashlib
    import codecs
    with codecs.open(fpath, "r", "utf-8") as filep:
        buff = filep.read()
    cksum = hashlib.md5(buff.encode("utf-8"))
    return cksum.hexdigest()


def makedirs(path, mode=None, uid=None, gid=None):
    """
    Wraps os.makedirs with a more restrictive 755 mode and ignore
    already exists errors.
    """
    try:
        mode = mode or 0o755
        os.makedirs(path, mode)
    except OSError as exc:
        if exc.errno == 17:
            pass
        else:
            raise
    uid = uid if uid is not None else -1
    gid = gid if gid is not None else -1
    os.chown(path, uid, gid)


def rmtree_busy(path):
    import shutil
    import errno

    def onerror(fn, path, exc):
        if exc[1].errno in (errno.EBUSY, errno.ENOTEMPTY):
            return
        raise exc[1]

    shutil.rmtree(path, onerror=onerror)


def create_protected_file(filepath, buff):
    import foreign.six
    def onfile(f):
        if os.name == 'posix':
            os.chmod(filepath, 0o0600)
        f.write(buff)

    if foreign.six.PY2:
        if isinstance(buff, foreign.six.text_type):
            import codecs
            with codecs.open(filepath, "w", "utf-8") as f:
                onfile(f)
        else:
            with open(filepath, "w") as f:
                onfile(f)
    else:
        with open(filepath, "w") as f:
            onfile(f)


def read_unicode_file(filepath):
    import foreign.six
    if foreign.six.PY2:
        import codecs
        with codecs.open(filepath, "r", "utf-8") as f:
            return f.read()
    else:
        with open(filepath, "r") as f:
            return f.read()


def assert_file_exists(filename):
    if not os.path.exists(filename):
        raise Exception("%s is not present" % filename)


def assert_file_is_root_only_writeable(filename):
    if not os.path.exists(filename):
        raise Exception("%s is not present." % filename)
    stat_info = os.stat(filename)
    if stat_info.st_uid != 0:
        raise Exception("%s does not belong to root" % filename)
    if stat_info.st_mode & stat.S_IWOTH:
        raise Exception("%s is world writable" % filename)
