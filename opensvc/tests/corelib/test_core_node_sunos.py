import pytest

from core.node.sunos import Node


@pytest.mark.ci
@pytest.mark.parametrize(
    "desc, swap_cmd_out, swap_cmt_ret_code, kstat_out, kstat_ret_code, expected",
    [
        ("swap -l with no output, kstat with no output", "", 0, "", "", {
            "mem_avail": 0,
            "mem_total": 0,
            "swap_avail": 0,
            "swap_total": 0
        }),

        ("swap -l with errors, kstat with errors", "", 1, "", 1, {
            "mem_avail": 0,
            "mem_total": 0,
            "swap_avail": 0,
            "swap_total": 0
        }),

        ("swap -l with encrypted swap",
         """swapfile                    dev            swaplo      blocks        free encrypted
/dev/zvol/dsk/rpool/swap 231,1               8     2097144     1282552  yes""",
         0,

         """unix:0:system_pages:availrmem   488703
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
unix:0:system_pages:snaptime    117957,358758642""",
         0,
         {"mem_avail": 46, "mem_total": 4095, "swap_avail": 61, "swap_total": 1023}
         ),

        ("swap -l ok, kstat ok",
         """swapfile                  dev            swaplo      blocks        free
/dev/zvol/dsk/rpool/swap  231,1               8     2097144      699048
/dev/zvol/dsk/rpool/swap1 231,2               8     2097144      699048
/dev/zvol/dsk/rpool/swap2 231,3               8     4194296     1398098
""",
         0,
         """unix:0:system_pages:availrmem   349487
unix:0:system_pages:class       pages
unix:0:system_pages:crtime      11,455762078
unix:0:system_pages:desfree     8191
unix:0:system_pages:desscan     25
unix:0:system_pages:econtig     18446744073646514176
unix:0:system_pages:fastscan    524231
unix:0:system_pages:freemem     652681
unix:0:system_pages:kernelbase  18446604435732824064
unix:0:system_pages:lotsfree    16382
unix:0:system_pages:minfree     4095
unix:0:system_pages:nalloc      1753453451
unix:0:system_pages:nalloc_calls        70383
unix:0:system_pages:nfree       1749255091
unix:0:system_pages:nfree_calls 60425
unix:0:system_pages:nscan       0
unix:0:system_pages:pagesfree   652681
unix:0:system_pages:pageslocked 321233
unix:0:system_pages:pagestotal  1048463
unix:0:system_pages:physmem     1048463
unix:0:system_pages:pp_kernel   358715
unix:0:system_pages:slowscan    100
unix:0:system_pages:snaptime    13296,580766939""",
         0,
         {"mem_total": 4095, "swap_total": 4095, "mem_avail": 33, "swap_avail": 33}),
    ]
)
class TestStatsMeminfo(object):
    @staticmethod
    def test_stats_meminfo_result(
            mocker, desc, swap_cmd_out, swap_cmt_ret_code, kstat_out,
            kstat_ret_code, expected):
        mocker.patch("core.node.sunos.justcall",
                     side_effect=[[swap_cmd_out, "", swap_cmt_ret_code],
                                  [kstat_out, "", kstat_ret_code]])
        result = Node().stats_meminfo()
        assert result == expected, (
                "scenario: %s \n"
                "unexpected result: \n"
                "expected %s \n"
                "got      %s \n"
                "swap -l is \n%s\n\n"
                "kstat -n system_pages -p is \n%s"
                % (desc, expected, result, swap_cmd_out, kstat_out)
        )
