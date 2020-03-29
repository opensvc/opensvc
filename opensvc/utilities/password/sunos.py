import shutil
from subprocess import *

from utilities.proc import which

shadow = '/etc/shadow'
tmpshadow = '/etc/shadow.tmp'
policy = '/etc/security/policy.conf'


def get_polices():
    """
       1	crypt_bsdmd5(5)
       2a	crypt_bsdbf(5)
       md5	crypt_sunmd5(5)
       5	crypt_sha256(5)
       6	crypt_sha512(5)
       __unix__	crypt_unix(5)	(default)
    """
    l = ["__unix__"]
    with open(policy, 'r') as fd:
        buff = fd.read()
        for line in buff.split('\n'):
            if line.strip().startswith('CRYPT_ALGORITHMS_ALLOW='):
                v = line.strip().split('=')
                if len(v) != 2:
                    continue
                l = v[1].split(',')
    return l


def change_root_pw(pw):
    try:
        _change_root_pw(pw)
    except Exception as e:
        print(e)
        return 1
    return 0


def _change_root_pw(pw):
    allowed = get_polices()
    if '1' not in allowed:
        raise Exception('can not generate a compatible password')
    shutil.copy(shadow, tmpshadow)
    with open(tmpshadow, 'r') as fd:
        buff = fd.read()
        lines = []
        for line in buff.split('\n'):
            if line.strip().startswith('#'):
                lines.append(line)
                continue
            v = line.split(':')
            if len(v) < 2 or v[0] != "root":
                lines.append(line)
                continue
            v[1] = pw_crypt(pw)
            lines.append(':'.join(v))
        buff = '\n'.join(lines)
    with open(tmpshadow, 'w') as fd:
        fd.write(buff + '\n')
    shutil.copy(tmpshadow, shadow)


def pw_crypt(pw):
    if not which('openssl'):
        raise Exception('openssl is mandatory')
    cmd = ['openssl', 'passwd', '-1', '-stdin']
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate(input=pw)
    if p.returncode != 0:
        raise Exception()
    return out.strip('\n')


if __name__ == "__main__":
    change_root_pw('toto')
