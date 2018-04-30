# coding: utf8

from __future__ import print_function

import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import platform
from nose.plugins.skip import Skip, SkipTest

from resourceset import ResourceSet
import svc
import resFsLinux
import rcExceptions as ex
import rcLogger

SVCNAME = "unittest"

if platform.uname()[0] != "Linux":
    raise SkipTest

class TestRset:
    def setUp(self):
        rcLogger.DEFAULT_HANDLERS = ["file"]
        self.expected = {}
        fs1 = resFsLinux.Mount(rid="fs#1", mount_point="/srv/"+SVCNAME, device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['1'] = 'fs#1'
        fs2 = resFsLinux.Mount(rid="fs#2", mount_point="/srv/"+SVCNAME+"/abc", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['3'] = 'fs#2'
        fs3 = resFsLinux.Mount(rid="fs#3", mount_point="/srv/"+SVCNAME+"/def", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['4'] = 'fs#3'
        fs4 = resFsLinux.Mount(rid="fs#4", mount_point="/srv/"+SVCNAME+"/ghi", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['5'] = 'fs#4'
        fs5 = resFsLinux.Mount(rid="fs#5", mount_point="/srv/"+SVCNAME+"/ghi/123", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['9'] = 'fs#5'
        fs6 = resFsLinux.Mount(rid="fs#6", mount_point="/srv/"+SVCNAME+"/ghi/456", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['10'] = 'fs#6'
        fs7 = resFsLinux.Mount(rid="fs#7", mount_point="/srv/"+SVCNAME+"/ghi/789", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['11'] = 'fs#7'
        fs8 = resFsLinux.Mount(rid="fs#8", mount_point="/srv/"+SVCNAME+"/ghi/456/abc", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['12'] = 'fs#8'
        fs9 = resFsLinux.Mount(rid="fs#9", mount_point="/srv/"+SVCNAME+"/ghi/456/def", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['13'] = 'fs#9'
        fs10 = resFsLinux.Mount(rid="fs#10", mount_point="/srv/"+SVCNAME+"/ghi/456/abc/123", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['14'] = 'fs#10'
        fs11 = resFsLinux.Mount(rid="fs#11", mount_point="/srv/"+SVCNAME+"/ghi/456/abc/456", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['15'] = 'fs#11'
        fs12 = resFsLinux.Mount(rid="fs#12", mount_point="/srv/"+SVCNAME+"/abc/123", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['6'] = 'fs#12'
        fs13 = resFsLinux.Mount(rid="fs#13", mount_point="/srv/"+SVCNAME+"/abc/123/abc", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['7'] = 'fs#13'
        fs14 = resFsLinux.Mount(rid="fs#14", mount_point="/srv/"+SVCNAME+"/abc/123/abc/123", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['8'] = 'fs#14'
        fs15 = resFsLinux.Mount(rid="fs#15", mount_point="/srv/"+SVCNAME+"/klm/123", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['16'] = 'fs#15'
        fs16 = resFsLinux.Mount(rid="fs#16", mount_point="/srv/"+SVCNAME+"/klm", device="/tmp", mount_options="bind,rw", fs_type="none") ; self.expected['2']='fs#16'
        self.rset = ResourceSet("fs", [fs16, fs15, fs14, fs13, fs12, fs11, fs10, fs9, fs8, fs7, fs6, fs5, fs4, fs3, fs2, fs1])

    def test_001_sort(self):
        """
        Sort Fs Resourceset
        """
        ret = 0
        cpt = 1
        for r in sorted(self.rset):
            if r.rid != self.expected[str(cpt)]:
                ret = 1
            cpt += 1
        assert ret == 0
