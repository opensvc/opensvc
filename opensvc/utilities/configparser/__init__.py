from __future__ import print_function
import sys

import foreign.six as six
from foreign.six.moves import configparser as ConfigParser

Error = ConfigParser.Error
ParsingError = ConfigParser.ParsingError
NoOptionError = ConfigParser.NoOptionError

if six.PY2:
    class RawConfigParser(ConfigParser.RawConfigParser):
        def __init__(self, *args, **kwargs):
            ConfigParser.RawConfigParser.__init__(self, *args, **kwargs)

        def write(self, fp):
            """
            Write an .ini formatted representation of the configuration.
            """
            encoding = sys.stdin.encoding if sys.stdin.encoding else 'UTF-8'
            if self._defaults:
                fp.write("[%s]\n" % ConfigParser.DEFAULTSECT)
                for (key, value) in self._defaults.items():
                    if not isinstance(value, six.string_types):
                        value = str(value)
                    if not isinstance(value, six.text_type):
                        value = value.decode(encoding)
                    fp.write("%s = %s\n" % (key, value.replace('\n', '\n\t')))
                fp.write("\n")
            for section in self._sections:
                fp.write("[%s]\n" % section)
                for (key, value) in self._sections[section].items():
                    if key == "__name__":
                        continue
                    if not isinstance(value, six.string_types):
                        value = str(value)
                    if not isinstance(value, six.text_type):
                        value = value.decode(encoding)
                    if (value is not None) or (self._optcre == self.OPTCRE):
                        key = " = ".join((key, value.replace('\n', '\n\t')))

                    fp.write("%s\n" % (key.encode("utf-8")))
                fp.write("\n")
else:
    class RawConfigParser(ConfigParser.RawConfigParser):
        def __init__(self, *args, **kwargs):
            kwargs["strict"] = False
            ConfigParser.RawConfigParser.__init__(self, *args, **kwargs)
