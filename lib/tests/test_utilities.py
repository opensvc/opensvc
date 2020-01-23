# coding: utf-8

from __future__ import print_function

import sys
import os
import datetime
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

from rcUtilities import *
import pytest


@pytest.mark.ci
class TestUtilities:
    @staticmethod
    def test_eval_expr():
        """
        eval_expr()
        """
        foo = 1
        assert eval_expr("0 & 1") == 0
        assert eval_expr("0 | 1") == 1
        assert eval_expr("0 ^ 1") == 1
        assert eval_expr("1 ^ 1") == 0
        assert eval_expr("-1 + 1") == 0
        assert eval_expr("True and False") is False
        assert eval_expr("True and True") is True
        assert eval_expr("True or False") is True
        assert eval_expr("True or True") is True
        assert eval_expr("2 > 1")
        assert eval_expr("1 == 1")
        assert eval_expr("0 != 1")
        assert eval_expr("'1' == '1'")
        assert eval_expr("foo != 1")  # foo is not a safe name
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

    @staticmethod
    def test_cache():
        """
        Session cache
        """
        class ObjTest(object):
            @cache("foo.{args[1]}")
            def foo(self, _, data=None):
                data = data or 0
                return data
        test_obj = ObjTest()
        test_obj.foo("bar")
        test_obj.foo("bar")
        test_obj.foo("bar", data=datetime.datetime.now())
        clear_cache("foo.bar")
        purge_cache()

    @staticmethod
    def test_lazy():
        """
        Lazy properties
        """
        class Test(object):
            @lazy
            def foo(self):
                return 0
        testobj = Test()
        assert lazy_initialized(testobj, "foo") is False
        assert testobj.foo == 0
        assert lazy_initialized(testobj, "foo") is True
        unset_lazy(testobj, "foo")
        assert lazy_initialized(testobj, "foo") is False
        set_lazy(testobj, "foo", 1)
        assert lazy_initialized(testobj, "foo") is True
        assert testobj.foo == 1
        unset_all_lazy(testobj)
        assert lazy_initialized(testobj, "foo") is False

    @staticmethod
    def test_fcache():
        """
        Function cache
        """
        class Test(object):
            @fcache
            def foo(self):
                return 0
        testobj = Test()
        assert fcache_initialized(testobj, "foo") is False
        assert testobj.foo() == 0
        assert fcache_initialized(testobj, "foo") is True
        unset_fcache(testobj, "foo")
        assert fcache_initialized(testobj, "foo") is False

    @staticmethod
    def test_bencode():
        """
        bencode()
        """
        assert bencode("foo")

    @staticmethod
    def test_bdecode():
        """
        bencode()
        """
        assert bdecode("foo")
        assert bdecode(None) is None

    @staticmethod
    def test_is_string():
        """
        is_string()
        """
        assert is_string(1) is False
        assert is_string("a") is True
        assert is_string("bêh") is True

    @staticmethod
    def test_empty_string():
        """
        empty_string()
        """
        assert empty_string("") is True
        assert empty_string("foo") is False
        assert empty_string("fêo") is False

    @staticmethod
    def test_mimport():
        """
        mimport()
        """
        mod = mimport("res", "fs", "linux")
        assert hasattr(mod, "Mount") is True
        mod = mimport("prov", "fs", "linux")
        assert hasattr(mod, "Prov") is True
        mod = mimport("prov", "fs", "linux", "")
        assert hasattr(mod, "Prov") is True

        try:
            _ = mimport("aa", "bb", "cc")
        except ImportError:
            pass
        else:
            assert False

    @staticmethod
    def test_ximport():
        """
        ximport()
        """
        mod = ximport("resFs")
        assert hasattr(mod, "Mount") is True

        try:
            _ = ximport("aa")
        except ImportError:
            pass
        else:
            assert False

    @staticmethod
    def test_is_exe():
        """
        is_exe()
        """
        assert is_exe("/bin/ls") is True
        assert is_exe("/dev/null") is False
        assert is_exe("/tmp") is False
        assert is_exe("/etc/hosts") is False
        assert is_exe("/etc/hosts", realpath=True) is False

    @staticmethod
    def test_which(non_existing_file):
        """
        which()
        """
        assert which("ls") in ("/bin/ls", "/usr/bin/ls")
        assert which("/bin/ls") in ("/bin/ls", "/usr/bin/ls")
        assert which(non_existing_file) is None
        assert which(None) is None

    @staticmethod
    def test_justcall(non_existing_file):
        """
        justcall()
        """
        out, err, ret = justcall(["ls", non_existing_file])
        assert is_string(out) is True
        assert is_string(err) is True
        assert ret > 0
        out, err, ret = justcall(["/bin/ls2", non_existing_file])
        assert ret != 0

    @staticmethod
    def test_lcall():
        """
        lcall()
        """
        log = logging.getLogger()
        ret = lcall(["ls"], log)
        assert ret == 0
        ret = lcall(["sleep", "2"], log, timeout=0.1)
        assert ret == -15

    @staticmethod
    def test_qcall(non_existing_file):
        """
        qcall()
        """
        ret = qcall(["ls", non_existing_file])
        assert ret > 0
        ret = qcall(None)
        assert ret == 1
        ret = qcall()
        assert ret == 1
        ret = qcall(rcEnv.syspaths.true)
        assert ret == 0

    @staticmethod
    def test_vcall(non_existing_file):
        """
        vcall()
        """
        ret, out, err = vcall([])
        assert ret == 0
        assert out == ""
        assert err == ""

        ret, out, err = vcall(["ls2"])
        assert ret == 1

        ret, out, err = vcall(["ls", non_existing_file])
        assert is_string(out) is True
        assert is_string(err) is True
        assert ret > 0

        # redirects
        cmd1 = 'ls'
        _, _, _ = vcall([cmd1, non_existing_file], err_to_warn=True)
        _, _, _ = vcall([cmd1, non_existing_file], err_to_info=True)
        _, _, _ = call([cmd1], info=False, outlog=False, outdebug=True)

        cmd2 = "ls " + non_existing_file + " 2>/dev/stdout"
        _, _, _ = vcall(cmd2, shell=True, err_to_info=True)
        _, _, _ = vcall(cmd2, shell=True, err_to_warn=True)
        _, _, _ = vcall(cmd2, shell=True, warn_to_info=False)
        _, _, _ = vcall(cmd2, shell=True, outlog=False, outdebug=True)

        cmd3 = "ls >/dev/stderr"
        _, _, _ = vcall(cmd3, shell=True, warn_to_info=True)
        _, _, _ = vcall(cmd3, shell=True, warn_to_info=False)
        _, _, _ = vcall(cmd3, shell=True, errlog=False, errdebug=True)

        # to cache
        ret, out, err = vcall(cmd1, shell=True, cache=True)
        assert is_string(out) is True
        assert is_string(err) is True
        assert ret == 0

        # from cache
        _, out2, _ = vcall(cmd1, shell=True, cache=True)
        assert out == out2

        # cache discard
        _, _, _ = vcall("touch " + non_existing_file, shell=True, cache=True)
        _, _, _ = vcall("test -f " + non_existing_file, shell=True, cache=True)
        try:
            os.unlink(non_existing_file)
        except:
            pass
        ret, _, _ = vcall("test -f " + non_existing_file, shell=True, cache=False)
        assert ret == 1

    @staticmethod
    def test_call(non_existing_file):
        """
        call()
        """
        ret, out, err = call(["ls", non_existing_file])
        assert is_string(out) is True
        assert is_string(err) is True
        assert ret > 0

    @staticmethod
    def test_getmount():
        """
        getmount()
        """
        assert getmount("/dev") == "/dev"
        assert getmount("/bin") == "/"
        assert getmount("/") == "/"

    @staticmethod
    def test_protected_mount(tmpdir):
        """
        protected_mount()
        """
        assert protected_mount("/bin") is True
        assert protected_mount("/bin/") is True
        assert protected_mount("/mysvc") is True
        if rcEnv.sysname == 'Darwin':
            location = "/Volumes/RAMDiskOpensvcTest"
            create_mount = 'diskutil erasevolume HFS+ "RAMDiskOpensvcTest" `hdiutil attach -nomount ram://524288`'
            delete_mount = "diskutil eject RAMDiskOpensvcTest"
        else:
            location = str(tmpdir)
            create_mount = "sudo mount -t tmpfs none %s" % location
            delete_mount = "sudo umount %s && rmdir %s" % (location, location)
        call(create_mount, shell=True)
        assert protected_mount("%s" % location) is False
        assert protected_mount("%s/" % location) is False
        assert protected_mount("%s/a" % location) is False
        call(delete_mount, shell=True)

    @staticmethod
    def test_protected_dir():
        """
        protected_dir()
        """
        assert protected_dir("/bin") is True
        assert protected_dir("/bin/") is True
        assert protected_dir("/mysvc") is False

    @staticmethod
    def test_cidr_to_dotted():
        """
        cidr_to_dotted()
        """
        assert cidr_to_dotted(22) == "255.255.252.0"

    @staticmethod
    def test_to_dotted():
        """
        to_dotted()
        """
        assert to_dotted(22) == "255.255.252.0"
        assert to_dotted("22") == "255.255.252.0"
        assert to_dotted("255.255.252.0") == "255.255.252.0"

    @staticmethod
    def test_hexmask_to_dotted():
        """
        hexmask_to_dotted()
        """
        assert hexmask_to_dotted("ffffff00") == "255.255.255.0"

    @staticmethod
    def test_dotted_to_cidr():
        """
        dotted_to_cidr()
        """
        assert dotted_to_cidr("255.255.252.0") == "22"

    @staticmethod
    def test_term_width():
        """
        term_width()
        """
        assert term_width() > 0

    @staticmethod
    def test_banner():
        """
        banner()
        """
        assert "foo" in banner("foo")

    @staticmethod
    def test_fsum(non_existing_file):
        """
        fsum()
        """
        filename = str(non_existing_file)
        call("echo abc > " + str(filename), shell=True)
        assert fsum(filename) == "0bee89b07a248e27c83fc3d5951213c1"
        call("rm -f " + filename, shell=True)

    @staticmethod
    def test_chunker():
        """
        chunker()
        """
        assert [chunk for chunk in chunker("aabbcc", 2)] == ["aa", "bb", "cc"]

    @staticmethod
    def test_is_service():
        """
        is_service()
        """
        assert is_service("magic123") is None

    @staticmethod
    def test_list_services():
        """
        list_services()
        """
        assert isinstance(list_services(), list)

    @staticmethod
    def test_read_cf(tmp_path):
        """
        read_cf()
        """
        tmp_file = os.path.join(str(tmp_path), 'foo')
        call("echo '[aa]\nbb = cc\n' >" + tmp_file, shell=True)
        config = read_cf(tmp_file)
        assert config.get("aa", "bb") == "cc"
        call("rm -f " + tmp_file, shell=True)

        # cf not present
        config = read_cf(tmp_file)
        assert config.sections() == []

    @staticmethod
    def test_drop_option():
        """
        drop_option()
        """
        assert drop_option("--foo", ["/bin/true", "--foo=bar", "--baz"],
                           drop_value=False) == ["/bin/true", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo=bar", "--baz"],
                           drop_value=True) == ["/bin/true", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo", "bar", "--baz"],
                           drop_value=False) == ["/bin/true", "bar", "--baz"]
        assert drop_option("--foo", ["/bin/true", "--foo", "bar", "--baz"],
                           drop_value=True) == ["/bin/true", "--baz"]

    @staticmethod
    def test_purge_cache():
        """
        purge_cache()
        """
        purge_cache()

    @staticmethod
    def test_purge_cache_expired():
        """
        purge_cache_expired()
        """
        purge_cache_expired()
