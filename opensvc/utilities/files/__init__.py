import os


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


def makedirs(path, mode=0o755):
    """
    Wraps os.makedirs with a more restrictive 755 mode and ignore
    already exists errors.
    """
    try:
        os.makedirs(path, mode)
    except OSError as exc:
        if exc.errno == 17:
            pass
        else:
            raise


def create_protected_file(filepath, buff, mode):
    with open(filepath, mode) as f:
        if os.name == 'posix':
            os.chmod(filepath, 0o0600)
        f.write(buff)




