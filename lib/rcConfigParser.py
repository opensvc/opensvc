from __future__ import print_function
import sys

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

if sys.version_info[0] < 3:
    class RawConfigParser(ConfigParser.RawConfigParser):
        def __init__(self, *args, **kwargs):
            ConfigParser.RawConfigParser.__init__(self, *args, **kwargs)

        def write(self, fp):
            """Write an .ini-format representation of the configuration state."""
            if self._defaults:
                fp.write("[%s]\n" % DEFAULTSECT)
                for (key, value) in self._defaults.items():
                    if type(value) != unicode:
                        value = value.decode(sys.stdin.encoding)
                    fp.write("%s = %s\n" % (key, value.replace('\n', '\n\t')))
                fp.write("\n")
            for section in self._sections:
                fp.write("[%s]\n" % section)
                for (key, value) in self._sections[section].items():
                    if key == "__name__":
                        continue
                    if type(value) != unicode:
                        value = value.decode(sys.stdin.encoding)
                    if (value is not None) or (self._optcre == self.OPTCRE):
                        key = " = ".join((key, value.replace('\n', '\n\t')))

                    fp.write("%s\n" % (key.encode("utf-8")))
                fp.write("\n")
else:
    RawConfigParser = ConfigParser.RawConfigParser
