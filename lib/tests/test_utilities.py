# coding: utf-8

from __future__ import print_function

from rcUtilities import *

class TestUtilities:

    def test_lazy(self):
        """
        Lazy properties
        """
        class Test(object):
            @lazy
            def foo(self):
                return 0
        testobj = Test()
        assert lazy_initialized(testobj, "foo") == False
        assert testobj.foo == 0
        assert lazy_initialized(testobj, "foo") == True

    def test_is_string(self):
        """
        is_string()
        """
        assert is_string(1) == False
        assert is_string("a") == True
        assert is_string("bêh") == True

    def test_empty_string(self):
        """
        empty_string()
        """
        assert empty_string("") == True
        assert empty_string("foo") == False
        assert empty_string("fêo") == False

    def test_mimport(self):
        """
        mimport()
        """
        mod = mimport("res", "fs", "linux")
        assert hasattr(mod, "Mount") == True
        mod = mimport("prov", "fs", "linux")
        assert hasattr(mod, "Prov") == True

    def test_ximport(self):
        """
        ximport()
        """
        mod = ximport("resFs")
        assert hasattr(mod, "Mount") == True

    def test_is_exe(self):
        """
        is_exe()
        """
        assert is_exe("/bin/ls") == True
        assert is_exe("/dev/null") == False
        assert is_exe("/tmp") == False
        assert is_exe("/etc/hosts") == False

    def test_which(self):
        """
        which()
        """
        assert which("ls") == "/bin/ls"
        assert which("foo") == None

    def test_justcall(self):
        """
        justcall()
        """
        out, err, ret = justcall(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret == 2

    def test_vcall(self):
        """
        vcall()
        """
        ret, out, err = vcall(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret == 2

    def test_call(self):
        """
        call()
        """
        ret, out, err = call(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret == 2

    def test_qcall(self):
        """
        qcall()
        """
        ret = qcall(["ls", "/foo"])
        assert ret == 2

    def test_getmount(self):
        """
        Get mount points
        """
        assert getmount("/bin") == "/"
        assert getmount("/") == "/"

    def test_protected_mount(self):
        """
        Protected mount points
        """
        assert protected_mount("/bin") == True
        assert protected_mount("/bin/") == True
        assert protected_mount("/mysvc") == True

    def test_protected_dir(self):
        """
        Protected directories
        """
        assert protected_dir("/bin") == True
        assert protected_dir("/bin/") == True
        assert protected_dir("/mysvc") == False

    def test_cidr_to_dotted(self):
        """
        Convert netmask, cidr to dotted
        """
        assert cidr_to_dotted(22) == "255.255.252.0"

    def test_to_dotted(self):
        """
        Convert netmask, octal or dotted to dotted
        """
        assert to_dotted(22) == "255.255.252.0"
        assert to_dotted("22") == "255.255.252.0"
        assert to_dotted("255.255.252.0") == "255.255.252.0"

    def test_hexmask_to_dotted(self):
        """
        Convert netmask, hex to dotted
        """
        assert hexmask_to_dotted("ffffff00") == "255.255.255.0"

    def test_dotted_to_cidr(self):
        """
        Convert netmask, dotted to cidr
        """
        assert dotted_to_cidr("255.255.252.0") == "22"

    def test_term_width(self):
        """
        Compute term width
        """
        assert term_width() > 0

