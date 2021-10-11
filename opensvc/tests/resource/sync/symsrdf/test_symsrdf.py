import json
import os

import pytest

from drivers.resource.sync.symsrdfs import SyncSymsrdfs


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestListPd:
    @staticmethod
    @pytest.mark.parametrize("tc", [
        "1",
        "mixed-dev_name-length"
    ])
    def test_returns_correct_dev_list_from_sym_output(mocker, tc):
        mocker.patch.object(SyncSymsrdfs, "call")
        r = SyncSymsrdfs()
        tc = "list_pd_%s" % tc
        fixture_dir = os.path.join(os.path.dirname(__file__), "fixtures")
        with open(os.path.join(fixture_dir, fixture_dir, "%s.syminq.xml" % tc), "r") as f:
            syminq_out = f.read()
        with open(os.path.join(fixture_dir, fixture_dir, "%s.symdg.xml" % tc), "r") as f:
            symdg_out = f.read()
        with open(os.path.join(fixture_dir, fixture_dir, "%s.expected-pd.json" % tc), "r") as f:
            expected_pd = json.load(f)
        r.call.side_effect = [(0, syminq_out, ""), (0, symdg_out, "")]

        assert r.list_pd() == expected_pd
