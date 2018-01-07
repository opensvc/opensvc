# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import sys
import json
import logging
from StringIO import StringIO

import nodemgr

UNICODE_STRING = "bêh"
logging.disable(logging.CRITICAL)

class TestNodemgr:
    def test_011_nodemgr_print_schedule(self):
        """
        Print node schedules
        """
        ret = nodemgr.main(argv=["print", "schedule"])
        assert ret == 0

    def test_012_nodemgr_print_schedule_json(self):
        """
        Print node schedules (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["print", "schedule", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), list)

    def test_0211_nodemgr_print_config(self):
        """
        Print node config
        """
        ret = nodemgr.main(argv=["print", "config"])
        assert ret == 0

    def test_0212_nodemgr_print_config(self):
        """
        Print node json config (compat)
        """
        ret = nodemgr.main(argv=["json", "config"])
        assert ret == 0

    def test_022_nodemgr_print_config_json(self):
        """
        Print node config (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["print", "config", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_031_nodemgr_print_authconfig(self):
        """
        Print node auth config
        """
        ret = nodemgr.main(argv=["print", "authconfig"])
        assert ret == 0

    def test_032_nodemgr_print_authconfig_json(self):
        """
        Print node auth config (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["print", "authconfig", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_041_set(self):
        """
        Set node env.comment to a unicode string
        """
        ret = nodemgr.main(argv=["set", "--param", "env.comment", "--value", UNICODE_STRING])
        assert ret == 0

    def test_042_get(self):
        """
        Get node env.comment
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["get", "--param", "env.comment"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        from rcUtilities import try_decode
        print(output)

        assert ret == 0
        assert try_decode(output) == UNICODE_STRING

    def test_043_unset(self):
        """
        Unset env.comment
        """
        ret = nodemgr.main(argv=["unset", "--param", "env.comment"])
        assert ret == 0

    def test_044_get_not_found(self):
        """
        Get an unset keyword
        """
        _stderr = sys.stdout

        try:
            err = StringIO()
            sys.stderr = err
            ret = nodemgr.main(argv=["get", "--param", "env.comment"])
            output = err.getvalue().strip()
        finally:
            sys.stderr = _stderr

        assert ret == 1

    def test_05_nodemgr_checks(self):
        """
        Run node checks
        """
        ret = nodemgr.main(argv=["checks"])
        assert ret == 0

    def test_06_nodemgr_sysreport(self):
        """
        Run node sysreport
        """
        ret = nodemgr.main(argv=["sysreport"])
        assert ret == 0

    def test_07_nodemgr_pushasset(self):
        """
        Run node pushasset
        """
        ret = nodemgr.main(argv=["pushasset"])
        assert ret == 0

    def test_08_nodemgr_collect_stats(self):
        """
        Run node collect stats
        """
        ret = nodemgr.main(argv=["collect_stats"])
        assert ret == 0

    def test_09_nodemgr_pushstats(self):
        """
        Run node pushstats
        """
        ret = nodemgr.main(argv=["pushstats"])
        assert ret == 0

    def test_10_nodemgr_pushpkg(self):
        """
        Run node pushpkg
        """
        ret = nodemgr.main(argv=["pushpkg"])
        assert ret == 0

    def test_11_nodemgr_pushpatch(self):
        """
        Run node pushpatch
        """
        ret = nodemgr.main(argv=["pushpatch"])
        assert ret == 0

    def test_12_nodemgr_pushdisks(self):
        """
        Run node pushdisks
        """
        ret = nodemgr.main(argv=["pushdisks"])
        assert ret == 0

    def test_131_nodemgr_schedule_reboot(self):
        """
        Run schedule reboot
        """
        ret = nodemgr.main(argv=["schedule", "reboot"])
        assert ret == 0

    def test_132_nodemgr_unschedule_reboot(self):
        """
        Run unschedule reboot
        """
        ret = nodemgr.main(argv=["unschedule", "reboot"])
        assert ret == 0

    def test_133_nodemgr_print_reboot_status(self):
        """
        Print reboot schedule status
        """
        ret = nodemgr.main(argv=["schedule", "reboot", "status"])
        assert ret == 0

    def test_14_nodemgr_logs(self):
        """
        Print node logs
        """
        ret = nodemgr.main(argv=["logs", "--no-pager"])
        assert ret == 0

    def test_151_nodemgr_network_ls(self):
        """
        List node networks
        """
        ret = nodemgr.main(argv=["network", "ls"])
        assert ret == 0

    def test_152_nodemgr_network_ls_json(self):
        """
        List node networks (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["network", "ls", "--format", "json"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_161_nodemgr_print_devs(self):
        """
        Print node device tree
        """
        ret = nodemgr.main(argv=["print", "devs"])
        assert ret == 0

    def test_162_nodemgr_prkey(self):
        """
        Print persistent resevation key
        """
        ret = nodemgr.main(argv=["prkey"])
        assert ret == 0

    def test_163_nodemgr_dequeue_actions(self):
        """
        Dequeue actions
        """
        ret = nodemgr.main(argv=["dequeue", "actions"])
        assert ret == 0

    def test_164_nodemgr_scan_scsi(self):
        """
        Scan scsi buses
        """
        ret = nodemgr.main(argv=["scanscsi"])
        assert ret == 0

    def test_164_nodemgr_collector_networks(self):
        """
        Collector networks
        """
        ret = nodemgr.main(argv=["collector", "networks"])
        assert ret == 0


