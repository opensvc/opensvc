# coding: utf-8

from utilities.converters import *
import pytest


@pytest.mark.ci
class TestConverters:
    @staticmethod
    def test_convert_shlex():
        """
        Converter, shlex
        """
        assert convert_shlex("/bin/true arg0 arg1 --flag --kwarg1=bar --kwargs2 foo") == \
               ["/bin/true", "arg0", "arg1", "--flag", "--kwarg1=bar", "--kwargs2", "foo"]
        assert convert_shlex(None) is None
        assert convert_shlex(["foo"]) == ["foo"]

    @staticmethod
    def test_convert_set():
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

    @staticmethod
    def test_convert_set_comma():
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

    @staticmethod
    def test_convert_list():
        """
        Converter, list, whitespace separated
        """
        assert convert_list("  1 2 A  ") == ["1", "2", "A"]
        assert convert_list("1 2 A") == ["1", "2", "A"]
        assert convert_list("1,2 a") == ["1,2", "a"]
        assert convert_list(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list(None) == []
        assert convert_list("") == []

    @staticmethod
    def test_convert_comma():
        """
        Converter, list, comma separated
        """
        assert convert_list_comma(", ,1 2A , ") == ["1 2A"]
        assert convert_list_comma("1 2 A") == ["1 2 A"]
        assert convert_list_comma("1,2 a") == ["1" ,"2 a"]
        assert convert_list_comma(["1,2", "a"]) == ["1,2", "a"]
        assert convert_list_comma(None) == []
        assert convert_list_comma("") == []

    @staticmethod
    def test_convert_list_lower():
        """
        Converter, list, whitespace separated, lower-cased
        """
        assert convert_list_lower(None) == []
        assert convert_list_lower(["1,2", "A"]) == ["1,2", "a"]
        assert convert_list_lower("1 2 A") == ["1", "2", "a"]

    @staticmethod
    def test_convert_integer():
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
        assert convert_integer(None) is None

    @staticmethod
    def test_convert_boolean():
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

    @staticmethod
    @pytest.mark.parametrize("size, expected_size, kwargs", [
        ["0B", 0, {}],
        ["10B", 10, {}],
        ["1k", 1024, {}],
        ["1mb", 1048576, {}],
        ["1MB", 1048576, {}],
        ["1GB", 1073741824, {}],
        ["1Gb", 1073741824, {}],
        ["1 GB", 1073741824, {}],
        ["1 G", 1073741824, {}],
        ["1K", 1024, {}],
        ["1KB", 1024, {}],
        ["1 k", 1024, {}],
        ["1 K", 1024, {}],
        ["1 Ki", 1000, {}],
        ["1 KiB", 1000, {}],
        ["1 KiB", 1000, {}],
        ["1.1 Ki", 1100, {}],
        ["1.1 ki", 1100, {}],
        ["1000000", 1000, {"_to": "Ki"}],
        ["1000", 1000, {"_to": "B"}],
        [None, None, {}],
        ["1100", 1100, {}],
        ["1100.0", 1100, {}],
        ["50%FREE", "50%FREE", {}],
        ["", 0, {}],
        ["0", 0, {}],
        ["10000", 8192, {"_round": 4096}],
    ])
    def test_convert_size_is_correct(size, expected_size, kwargs):
        assert convert_size(size, **kwargs) == expected_size

    @staticmethod
    @pytest.mark.parametrize("value", [
        "1j",
        "1d",
        "100y",
    ])
    def test_convert_size_raise_value_error_when_value_has_invalid_unit(value):
        with pytest.raises(ValueError):
            convert_size(value)

    @staticmethod
    @pytest.mark.parametrize("to", [
        "j",
        "d",
        "y",
        "a",
    ])
    def test_convert_size_raise_value_error_when_to_has_invalid_unit(to):
        with pytest.raises(ValueError):
            convert_size("1", _to=to)

    @staticmethod
    def test_print_size():
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

    @staticmethod
    def test_print_speed():
        """
        Converter, speed
        """
        assert convert_speed("1 kb/s") == 1024
        assert convert_speed_kps("1 kb/s") == 1

    @staticmethod
    def test_convert_duration():
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
