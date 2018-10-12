# coding: utf-8

from __future__ import print_function

import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from converters import *

class TestConverters:
    def test_convert_shlex(self):
        """
        Converter, shlex
        """
        assert convert_shlex("/bin/true arg0 arg1 --flag --kwarg1=bar --kwargs2 foo") == ["/bin/true", "arg0", "arg1", "--flag", "--kwarg1=bar", "--kwargs2", "foo"]
        assert convert_shlex(None) == None
        assert convert_shlex(["foo"]) == ["foo"]

    def test_convert_set(self):
        """
        Converter, set, whitespace separated
        """
        assert convert_set(" 1 1 2 2  A  ") == set(["1", "2", "A"])
        assert convert_set("  1 2 A  ") == set(["1", "2", "A"])
        assert convert_set("1 2 A") == set(["1", "2", "A"])
        assert convert_set("1,2 a") == set(["1,2", "a"])
        assert convert_set(["1,2", "a"]) == set(["1,2", "a"])
        assert convert_set(set(["1,2", "a"])) == set(["1,2", "a"])
        assert convert_set(None) == set([])
        assert convert_set("") == set([])

    def test_convert_set_comma(self):
        """
        Converter, set, comma separated
        """
        assert convert_set_comma(" 1 1 2 2  A  ") == set(["1 1 2 2  A"])
        assert convert_set_comma("  1 2 A  ") == set(["1 2 A"])
        assert convert_set_comma("1 2 A") == set(["1 2 A"])
        assert convert_set_comma("1,2 a") == set(["1", "2 a"])
        assert convert_set_comma(["1,2", "a"]) == set(["1,2", "a"])
        assert convert_set_comma(set(["1,2", "a"])) == set(["1,2", "a"])
        assert convert_set_comma(None) == set([])
        assert convert_set_comma("") == set([])

    def test_convert_list(self):
        """
        Converter, list, whitespace separated
        """
        assert convert_list("  1 2 A  ") == ["1", "2", "A"]
        assert convert_list("1 2 A") == ["1", "2", "A"]
        assert convert_list("1,2 a") == ["1,2", "a"]
        assert convert_list(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list(None) == []
        assert convert_list("") == []

    def test_convert_comma(self):
        """
        Converter, list, comma separated
        """
        assert convert_list_comma(", ,1 2A , ") == ["1 2A"]
        assert convert_list_comma("1 2 A") == ["1 2 A"]
        assert convert_list_comma("1,2 a") == ["1" ,"2 a"]
        assert convert_list_comma(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list_comma(None) == []
        assert convert_list_comma("") == []

    def test_convert_list_lower(self):
        """
        Converter, list, whitespace separated, lower-cased
        """
        assert convert_list_lower(None) == []
        assert convert_list_lower(["1,2", "A"]) == ["1,2", "a"]
        assert convert_list_lower("1 2 A") == ["1", "2", "a"]

    def test_convert_integer(self):
        """
        Converter, integer
        """
        assert convert_integer("1") == 1
        assert convert_integer(1) == 1
        assert convert_integer(-1) == -1
        assert convert_integer("-1") == -1
        assert convert_integer(1.1) == 1
        assert convert_integer(1.9) == 1
        assert convert_integer("1.9") == 1
        assert convert_integer(None) == None

    def test_convert_boolean(self):
        """
        Converter, boolean
        """
        assert convert_boolean("tRue") == True
        assert convert_boolean("y") == True
        assert convert_boolean("Y") == True
        assert convert_boolean("1") == True
        assert convert_boolean(1) == True
        assert convert_boolean("FaLse") == False
        assert convert_boolean("no") == False
        assert convert_boolean("n") == False
        assert convert_boolean("0") == False
        assert convert_boolean(0) == False
        try:
            convert_boolean("foo")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False

    def test_convert_size(self):
        """
        Converter, size
        """
        assert convert_size("1k") == 1024
        assert convert_size("1K") == 1024
        assert convert_size("1KB") == 1024
        assert convert_size("1 K") == 1024
        assert convert_size("1 Ki") == 1000
        assert convert_size("1 KiB") == 1000
        assert convert_size("1.1 Ki") == 1100
        assert convert_size(1000000, _to="Ki") == 1000
        assert convert_size(1000, _to="B") == 1000
        assert convert_size(None) is None
        assert convert_size(1100) == 1100
        assert convert_size(1100.0) == 1100
        assert convert_size("50%FREE") == "50%FREE"
        assert convert_size("") == 0
        assert convert_size("0") == 0
        assert convert_size(10000, _round=4096) == 8192
        try:
            convert_size("1j")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False
        try:
            convert_size("1", _to="j")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False

    def test_print_size(self):
        """
        Converter, size
        """
        assert print_size(1024) == "1.00 GB"
        assert print_size(1024, compact=True) == "1.00g"
        assert print_size(10, unit="ei", compact=True) == "10.0ei"
        assert print_size(1000, unit="gi", compact=True) == "1.00ti"
        assert print_size(0, unit="gi", compact=True) == "0"
        assert print_size(1015, unit="ib", compact=True) == "1.01ki"
        assert print_size(1015000, unit="ib", compact=True) == "1.01mi"
        assert print_size(1015000000, unit="ib", compact=True) == "1.01gi"
        assert print_size(1015000000000, unit="ib", compact=True) == "1.01ti"
        assert print_size(1010000000000000, unit="ib", compact=True) == "1.01ei"
        assert print_size(1010000000000000000, unit="ib", compact=True) == "1.01zi"
        assert print_size(10100000000000000000, unit="ib", compact=True) == "10.1zi"
        assert print_size(101000000000000000000, unit="ib", compact=True) == "101zi"
        assert print_size(1010000000000000000000, unit="ib", compact=True) == "1.01yi"
        assert print_size(10100000000000000000000, unit="ib", compact=True) == "10.1yi"
        assert print_size(101000000000000000000000, unit="ib", compact=True) == "101yi"
        assert print_size(1010000000000000000000000, unit="ib", compact=True) == "1010yi"

        try:
            print_size(1000, unit="ji")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False

    def test_print_speed(self):
        """
        Converter, speed
        """
        assert convert_speed("1 kb/s") == 1024
        assert convert_speed_kps("1 kb/s") == 1

    def test_convert_duration(self):
        """
        Converter, duration
        """
        assert convert_duration("1") == 1
        assert convert_duration("-1m") == -60
        assert convert_duration("1s") == 1
        assert convert_duration("1s ") == 1
        assert convert_duration("   1s ") == 1
        assert convert_duration("1h1s") == 3601
        assert convert_duration("1h 1s") == 3601
        assert convert_duration("1h 1s", _to="m") == 60
        assert convert_duration(None, _to="m") is None
        assert convert_duration("", _to="m") == 0
        try:
            convert_duration("1z 1m")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False
            pass
        try:
            convert_duration("", _to="z")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False
            pass
        try:
            convert_duration("", _from="z")
            assert False
        except ValueError:
            pass
        except Exception:
            assert False
            pass
