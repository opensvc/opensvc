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

    def test_convert_list(self):
        """
        Converter, list
        """
        assert convert_list("  1 2 A  ") == ["1", "2", "A"]
        assert convert_list("1 2 A") == ["1", "2", "A"]
        assert convert_list("1,2 a") == ["1,2", "a"]
        assert convert_list(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list(None) == []
        assert convert_list("") == []

    def test_convert_comma(self):
        """
        Converter, list
        """
        assert convert_list_comma(", ,1 2A , ") == ["1 2A"]
        assert convert_list_comma("1 2 A") == ["1 2 A"]
        assert convert_list_comma("1,2 a") == ["1" ,"2 a"]
        assert convert_list_comma(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list_comma(None) == []
        assert convert_list_comma("") == []

    def test_convert_list_lower(self):
        """
        Converter, list lower-cased
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
