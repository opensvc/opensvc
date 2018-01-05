# coding: utf-8

from __future__ import print_function

from converters import *

class TestConverters:
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
