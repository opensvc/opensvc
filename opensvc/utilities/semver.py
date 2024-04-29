import re

SEMVER_RE = r'^([0-9]+).([0-9]+).([0-9]+)$'


class Semver(object):
    def __init__(self, major=0, minor=0, patch=0):
        self.major = int(major)
        self.minor = int(minor)
        self.patch = int(patch)

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.patch)

    @classmethod
    def parse(cls, s):
        if not isinstance(s, str):
            return cls()
        l = re.findall(SEMVER_RE, s)
        if len(l) != 1 or len(l[0]) != 3:
            return cls()
        return cls(major=int(l[0][0]), minor=int(l[0][1]), patch=int(l[0][2]))

    def __eq__(self, other):
        if (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch):
            return True
        return False

    def __lt__(self, other):
        if self.major < other.major:
            return True
        elif self.major == other.major:
            if self.minor < other.minor:
                return True
            elif self.minor == other.minor:
                if self.patch < other.patch:
                    return True
        return False

    def __le__(self, other):
        return (self < other) or (self == other)
