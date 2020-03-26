try:
    from hashlib import md5
    def hexdigest(s):
        o = md5()
        o.update(s.encode('utf-8'))
        return o.hexdigest()
except ImportError:
    from .md5 import md5
    def hexdigest(s):
        return md5(s).digest().encode('hex')
