import foreign.six as six

def bencode(buff):
    """
    Try a bytes cast, which only work in python3.
    """
    try:
        return bytes(buff, "utf-8")
    except TypeError:
        return buff


def bdecode(buff):
    """
    On python, convert bytes to string using utf-8 and ascii as a fallback
    """
    if buff is None:
        return buff
    if six.PY2:
        return buff
    if type(buff) == str:
        return buff
    return buff.decode("utf-8", errors="ignore")


def try_decode(string, codecs=None):
    codecs = codecs or ['utf8', 'latin1']
    for i in codecs:
        try:
            return string.decode(i)
        except:
            pass
    return string


def empty_string(buff):
    b = buff.strip(' ').strip('\n')
    if len(b) == 0:
        return True
    return False


def is_string(s):
    """
    python[23] compatible string-type test
    """
    if isinstance(s, six.string_types):
        return True
    return False


def is_glob(text):
    if len(set(text) & set("?*[")) > 0:
        return True
    return False

