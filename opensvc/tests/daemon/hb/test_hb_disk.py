import io
import mmap
import os

import pytest

from daemon.hb.disk import HbDisk


def hb_with_dev(path):
    """
    A disk heartbeat opening path, without the node configuration a
    configured one reads.
    """
    hb = HbDisk.__new__(HbDisk)
    hb.dev = str(path)
    hb.flags = os.O_RDWR | os.O_DIRECT
    with open(hb.dev, "wb") as f:
        f.write(b"\0" * (2 * 1024 * 1024))
    return hb


@pytest.mark.ci
class TestHbDiskFileObject:
    """
    The heartbeat device is opened with O_DIRECT, which refuses a buffer that
    is not page aligned.

    The buffers this class reads into and writes from are mmap, so they are
    aligned, but a buffered file object does not hand them to the file
    descriptor: it reads and writes through an internal buffer of its own,
    which is not aligned, unless the caller buffer is at least as large as it.

    meta_slot_buff is two pages, which was exactly io.DEFAULT_BUFFER_SIZE
    until python 3.14 raised it from 8kB to 128kB. The meta reads then began
    going through the unaligned buffer, and every one of them failed with
    EINVAL.
    """

    @staticmethod
    def test_the_device_is_opened_unbuffered(tmp_path):
        """
        The one that holds on every python: a buffered object would only pass
        the caller buffer through while it happens to be large enough.
        """
        hb = hb_with_dev(tmp_path / "hbdisk")
        seen = []
        with hb.hb_fo() as fo:
            seen.append(fo)
        assert isinstance(seen[0], io.RawIOBase), "the device must be opened unbuffered"

    @staticmethod
    def test_a_two_page_buffer_reads_and_writes(tmp_path):
        """
        The size that broke, meta_slot_buff being two pages long.

        This one only fails where io.DEFAULT_BUFFER_SIZE is above two pages,
        which is python 3.14 and later.
        """
        hb = hb_with_dev(tmp_path / "hbdisk")
        buff = mmap.mmap(-1, 2 * mmap.PAGESIZE)
        buff[0:5] = b"hello"

        errs = []
        with hb.hb_fo() as fo:
            try:
                fo.seek(mmap.PAGESIZE, os.SEEK_SET)
                assert fo.write(buff) == 2 * mmap.PAGESIZE
            except Exception as exc:
                errs.append(exc)
        with hb.hb_fo() as fo:
            try:
                fo.seek(mmap.PAGESIZE, os.SEEK_SET)
                assert fo.readinto(buff) == 2 * mmap.PAGESIZE
            except Exception as exc:
                errs.append(exc)
        assert errs == []
        assert bytes(buff[0:5]) == b"hello"
