# coding: utf-8

from __future__ import print_function

import sys
import os
import datetime
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from rcUtilities import *

class TestUtilities:

    def test_eval_expr(self):
        """
        eval_expr()
        """
        foo = 1
        assert eval_expr("0 & 1") == 0
        assert eval_expr("0 | 1") == 1
        assert eval_expr("0 ^ 1") == 1
        assert eval_expr("1 ^ 1") == 0
        assert eval_expr("-1 + 1") == 0
        assert eval_expr("True and False") == False
        assert eval_expr("True and True") == True
        assert eval_expr("True or False") == True
        assert eval_expr("True or True") == True
        assert eval_expr("2 > 1")
        assert eval_expr("1 == 1")
        assert eval_expr("0 != 1")
        assert eval_expr("'1' == '1'")
        assert eval_expr("foo != 1") # foo is not a safe name
        assert eval_expr("1 == True")
        assert eval_expr("True == 1")
        assert eval_expr("True != None")
        assert eval_expr("'a' in 'ab'")
        assert eval_expr("a in (a, b)")
        assert eval_expr("c in (a, b)") is False

        try:
            eval_expr("c.b")
            assert False
        except TypeError:
            pass
        except:
            assert False

    def test_cache(self):
        """
        Session cache
        """
        class Test(object):
            @cache("foo.{args[1]}")
            def foo(self, arg0, data=0):
                return data
        testobj = Test()
        testobj.foo("bar")
        testobj.foo("bar")
        testobj.foo("bar", data=datetime.datetime.now())
        clear_cache("foo.bar")
        purge_cache()

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
        unset_lazy(testobj, "foo")
        assert lazy_initialized(testobj, "foo") == False
        set_lazy(testobj, "foo", 1)
        assert lazy_initialized(testobj, "foo") == True
        assert testobj.foo == 1
        unset_all_lazy(testobj)
        assert lazy_initialized(testobj, "foo") == False

    def test_fcache(self):
        """
        Function cache
        """
        class Test(object):
            @fcache
            def foo(self):
                return 0
        testobj = Test()
        assert fcache_initialized(testobj, "foo") == False
        assert testobj.foo() == 0
        assert fcache_initialized(testobj, "foo") == True
        unset_fcache(testobj, "foo")
        assert fcache_initialized(testobj, "foo") == False

    def test_bencode(self):
        """
        bencode()
        """
        assert bencode("foo")

    def test_bdecode(self):
        """
        bencode()
        """
        assert bdecode("foo")
        assert bdecode(None) is None

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
        mod = mimport("prov", "fs", "linux", "")
        assert hasattr(mod, "Prov") == True

        try:
            mode = mimport("aa", "bb", "cc")
        except ImportError:
            pass
        else:
            assert False

    def test_ximport(self):
        """
        ximport()
        """
        mod = ximport("resFs")
        assert hasattr(mod, "Mount") == True

        try:
            mod = ximport("aa")
        except ImportError:
            pass
        else:
            assert False

    def test_is_exe(self):
        """
        is_exe()
        """
        assert is_exe("/bin/ls") == True
        assert is_exe("/dev/null") == False
        assert is_exe("/tmp") == False
        assert is_exe("/etc/hosts") == False
        assert is_exe("/etc/hosts", realpath=True) == False

    def test_which(self):
        """
        which()
        """
        assert which("ls") in ("/bin/ls", "/usr/bin/ls")
        assert which("/bin/ls") in ("/bin/ls", "/usr/bin/ls")
        assert which("foo") == None
        assert which(None) == None

    def test_justcall(self):
        """
        justcall()
        """
        out, err, ret = justcall(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret > 0
        out, err, ret = justcall(["/bin/ls2", "/foo"])
        assert ret != 0

    def test_lcall(self):
        """
        lcall()
        """
        log = logging.getLogger()
        ret = lcall(["ls"], log)
        assert ret == 0
        ret = lcall(["sleep", "2"], log, timeout=0.1)
        assert ret == -15

    def test_qcall(self):
        """
        qcall()
        """
        ret = qcall(["ls", "/foo"])
        assert ret > 0
        ret = qcall(None)
        assert ret == 0
        ret = qcall()
        assert ret == 1
        ret = qcall(["/bin/true"])
        assert ret == 0

    def test_vcall(self):
        """
        vcall()
        """
        ret, out, err = vcall([])
        assert ret == 0
        assert out == ""
        assert err == ""

        ret, out, err = vcall(["ls2"])
        assert ret == 1

        ret, out, err = vcall(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret > 0

        # redirects
        ret, out, err = vcall(["ls", "/foo"], err_to_warn=True)
        ret, out, err = vcall(["ls", "/foo"], err_to_info=True)
        ret, out, err = call(["ls"], info=False, outlog=False, outdebug=True)
        ret, out, err = vcall("ls /foo 2>/dev/stdout", shell=True, err_to_info=True)
        ret, out, err = vcall("ls /foo 2>/dev/stdout", shell=True, err_to_warn=True)
        ret, out, err = vcall("ls /foo 2>/dev/stdout", shell=True, warn_to_info=False)
        ret, out, err = vcall("ls /foo 2>/dev/stdout", shell=True, outlog=False, outdebug=True)
        ret, out, err = vcall("ls >/dev/stderr", shell=True, warn_to_info=True)
        ret, out, err = vcall("ls >/dev/stderr", shell=True, warn_to_info=False)
        ret, out, err = vcall("ls >/dev/stderr", shell=True, errlog=False, errdebug=True)

        # to cache
        ret, out, err = vcall("ls", shell=True, cache=True)
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret == 0

        # from cache
        ret, out2, err = vcall("ls", shell=True, cache=True)
        assert out == out2

        # cache discard
        ret, out, err = vcall("touch /tmp/foo", shell=True, cache=True)
        ret, out, err = vcall("test -f /tmp/foo", shell=True, cache=True)
        try:
            os.unlink("/tmp/foo")
        except:
            pass
        ret, out, err = vcall("test -f /tmp/foo", shell=True, cache=False)
        assert ret == 1

    def test_call(self):
        """
        call()
        """
        ret, out, err = call(["ls", "/foo"])
        assert is_string(out) == True
        assert is_string(err) == True
        assert ret > 0

    def test_getmount(self):
        """
        getmount()
        """
        assert getmount("/proc") == "/proc"
        assert getmount("/bin") == "/"
        assert getmount("/") == "/"

    def test_protected_mount(self):
        """
        protected_mount()
        """
        assert protected_mount("/bin") == True
        assert protected_mount("/bin/") == True
        assert protected_mount("/mysvc") == True
        call("mkdir -p /tmp/foo && sudo mount -t tmpfs none /tmp/foo", shell=True)
        assert protected_mount("/tmp/foo") == False
        assert protected_mount("/tmp/foo/") == False
        assert protected_mount("/tmp/foo/a") == False
        call("sudo umount /tmp/foo && rmdir /tmp/foo", shell=True)

    def test_protected_dir(self):
        """
        protected_dir()
        """
        assert protected_dir("/bin") == True
        assert protected_dir("/bin/") == True
        assert protected_dir("/mysvc") == False

    def test_cidr_to_dotted(self):
        """
        cidr_to_dotted()
        """
        assert cidr_to_dotted(22) == "255.255.252.0"

    def test_to_dotted(self):
        """
        to_dotted()
        """
        assert to_dotted(22) == "255.255.252.0"
        assert to_dotted("22") == "255.255.252.0"
        assert to_dotted("255.255.252.0") == "255.255.252.0"

    def test_hexmask_to_dotted(self):
        """
        hexmask_to_dotted()
        """
        assert hexmask_to_dotted("ffffff00") == "255.255.255.0"

    def test_dotted_to_cidr(self):
        """
        dotted_to_cidr()
        """
        assert dotted_to_cidr("255.255.252.0") == "22"

    def test_term_width(self):
        """
        term_width()
        """
        assert term_width() > 0

    def test_banner(self):
        """
        banner()
        """
        assert "foo" in banner("foo")

    def test_fsum(self):
        """
        fsum()
        """
        call("echo abc >/tmp/foo", shell=True)
        assert fsum("/tmp/foo") == "0bee89b07a248e27c83fc3d5951213c1"
        call("rm -f /tmp/foo", shell=True)

    def test_chunker(self):
        """
        chunker()
        """
        assert [chunk for chunk in chunker("aabbcc", 2)] == ["aa", "bb", "cc"]

    def test_is_service(self):
        """
        is_service()
        """
        assert is_service("magic123") is None

    def test_list_services(self):
        """
        list_services()
        """
        assert isinstance(list_services(), list)

    def test_read_cf(self):
        """
        read_cf()
        """
        call("echo '[aa]\nbb = cc\n' >/tmp/foo", shell=True)
        config = read_cf("/tmp/foo")
        assert config.get("aa", "bb") == "cc"
        call("rm -f /tmp/foo", shell=True)

        # cf not present
        config = read_cf("/tmp/foo")
        assert config.sections() == []

    def test_drop_option(self):
        """
        drop_option()
        """
        assert drop_option("--foo", ["/bin/true", "--foo=bar", "--baz"], drop_value=False) == ["/bin/true", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo=bar", "--baz"], drop_value=True) == ["/bin/true", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo", "bar", "--baz"], drop_value=False) == ["/bin/true", "bar", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo", "bar", "--baz"], drop_value=True) == ["/bin/true", "--baz"]

    def test_purge_cache(self):
        """
        purge_cache()
        """
        purge_cache()

    def test_purge_cache_expired(self):
        """
        purge_cache_expired()
        """
        purge_cache_expired()


