import pytest

from core.node.sunos import Node


@pytest.mark.ci
class TestStatsMeminfo:
    @staticmethod
    @pytest.mark.parametrize("swap_output, expected_result", [
        ("""swapfile                    dev            swaplo      blocks        free
/dev/zvol/dsk/rpool/swap 231,1               8     2097144     2082552""",
         {'mem_avail': 46, 'mem_total': 4095, 'swap_avail': 100, 'swap_total': 1023}),

        # With encrypted swap
        ("""swapfile                    dev            swaplo      blocks        free encrypted
/dev/zvol/dsk/rpool/swap 231,1               8     2097144     2082552  yes""",
         {'mem_avail': 46, 'mem_total': 4095, 'swap_avail': 100, 'swap_total': 1023}),
    ])
    def test_correct_compute(mocker, swap_output, expected_result):
        kstat_output = """unix:0:system_pages:availrmem   488703
unix:0:system_pages:class       pages
unix:0:system_pages:crtime      35,240997153
unix:0:system_pages:desfree     8191
unix:0:system_pages:desscan     25
unix:0:system_pages:econtig     18446744073646514176
unix:0:system_pages:fastscan    524231
unix:0:system_pages:freemem     334973
unix:0:system_pages:kernelbase  18446604435732824064
unix:0:system_pages:lotsfree    16382
unix:0:system_pages:minfree     4095
unix:0:system_pages:nalloc      18446744072078344455
unix:0:system_pages:nalloc_calls        488843
unix:0:system_pages:nfree       18446744072074150214
unix:0:system_pages:nfree_calls 478894
unix:0:system_pages:nscan       0
unix:0:system_pages:pagesfree   334973
unix:0:system_pages:pageslocked 521872
unix:0:system_pages:pagestotal  1048463
unix:0:system_pages:physmem     1048463
unix:0:system_pages:pp_kernel   559310
unix:0:system_pages:slowscan    100
unix:0:system_pages:snaptime    117957,358758642"""
        mocker.patch("core.node.sunos.justcall",
                     side_effect=[
                         (swap_output, "", 0),
                         (kstat_output, "", 0),
                     ])
        node = Node()
        assert node.stats_meminfo() == expected_result
