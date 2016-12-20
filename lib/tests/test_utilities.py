# coding: utf-8

from __future__ import print_function

from rcUtilities import *

def test_lazy():
    class Test(object):
        @lazy
        def foo(self):
            return 0
    testobj = Test()
    assert lazy_initialized(testobj, "foo") == False
    assert testobj.foo == 0
    assert lazy_initialized(testobj, "foo") == True

def test_is_string():
    assert is_string(1) == False
    assert is_string("a") == True
    assert is_string("bêh") == True

def test_empty_string():
    assert empty_string("") == True
    assert empty_string("foo") == False
    assert empty_string("fêo") == False

def test_mimport():
    mod = mimport("res", "fs", "linux")
    assert hasattr(mod, "Mount") == True
    mod = mimport("prov", "fs", "linux")
    assert hasattr(mod, "ProvisioningFs") == True

def test_ximport():
    mod = ximport("resFs")
    assert hasattr(mod, "Mount") == True

def test_is_exe():
    assert is_exe("/bin/ls") == True
    assert is_exe("/dev/null") == False
    assert is_exe("/tmp") == False
    assert is_exe("/etc/hosts") == False

def test_which():
    assert which("ls") == "/bin/ls"
    assert which("foo") == None

def test_justcall():
    out, err, ret = justcall(["ls", "/foo"])
    assert is_string(out) == True
    assert is_string(err) == True
    assert ret == 2

def test_vcall():
    ret, out, err = vcall(["ls", "/foo"])
    assert is_string(out) == True
    assert is_string(err) == True
    assert ret == 2

def test_call():
    ret, out, err = call(["ls", "/foo"])
    assert is_string(out) == True
    assert is_string(err) == True
    assert ret == 2

def test_qcall():
    ret = qcall(["ls", "/foo"])
    assert ret == 2

def test_getmount():
    assert getmount("/bin") == "/"
    assert getmount("/") == "/"

def test_protected_mount():
    assert protected_mount("/bin") == True
    assert protected_mount("/bin/") == True
    assert protected_mount("/mysvc") == True

def test_protected_dir():
    assert protected_dir("/bin") == True
    assert protected_dir("/bin/") == True
    assert protected_dir("/mysvc") == False

def test_convert_bool():
    assert convert_bool("tRue") == True
    assert convert_bool("y") == True
    assert convert_bool("Y") == True
    assert convert_bool("1") == True
    assert convert_bool(1) == True
    assert convert_bool("FaLse") == False
    assert convert_bool("no") == False
    assert convert_bool("n") == False
    assert convert_bool("0") == False
    assert convert_bool(0) == False

def test_convert_size():
    assert convert_size("1k") == 1024
    assert convert_size("1K") == 1024
    assert convert_size("1KB") == 1024
    assert convert_size("1 K") == 1024
    assert convert_size("1 Ki") == 1000
    assert convert_size("1 KiB") == 1000
    assert convert_size("1.1 Ki") == 1100

def test_cidr_to_dotted():
    assert cidr_to_dotted(22) == "255.255.252.0"

def test_to_dotted():
    assert to_dotted(22) == "255.255.252.0"
    assert to_dotted("22") == "255.255.252.0"
    assert to_dotted("255.255.252.0") == "255.255.252.0"

def test_hexmask_to_dotted():
    assert hexmask_to_dotted("ffffff00") == "255.255.255.0"

def test_dotted_to_cidr():
    assert dotted_to_cidr("255.255.252.0") == "22"

def test_term_width():
    assert term_width() > 0

