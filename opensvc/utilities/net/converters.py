import re

def cidr_to_dotted(s):
    i = int(s)
    _in = ""
    _out = ""
    for i in range(i):
        _in += "1"
    for i in range(32 - i):
        _in += "0"
    _out += str(int(_in[0:8], 2)) + '.'
    _out += str(int(_in[8:16], 2)) + '.'
    _out += str(int(_in[16:24], 2)) + '.'
    _out += str(int(_in[24:32], 2))
    return _out


def to_dotted(s):
    s = str(s)
    if '.' in s:
        return s
    return cidr_to_dotted(s)


def hexmask_to_dotted(mask):
    mask = mask.replace('0x', '')
    s = [str(int(mask[i:i + 2], 16)) for i in range(0, len(mask), 2)]
    return '.'.join(s)


def dotted_to_cidr(mask):
    if mask is None:
        return ''
    cnt = 0
    l = mask.split(".")
    l = map(lambda x: int(x), l)
    for a in l:
        cnt += str(bin(a)).count("1")
    return str(cnt)


def to_cidr(s):
    if s is None:
        return s
    elif '.' in s:
        return dotted_to_cidr(s)
    elif re.match(r"^(0x)*[0-9a-f]{8}$", s):
        # example: 0xffffff00
        s = hexmask_to_dotted(s)
        return dotted_to_cidr(s)
    return s



