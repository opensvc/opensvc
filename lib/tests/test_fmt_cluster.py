import pytest

from utilities.render.cluster import bare_len


@pytest.mark.ci
class TestBarLen:
    @staticmethod
    @pytest.mark.parametrize('input,result_len',[
        (b'abc', 3),
        (b'', 0),
        (b'\x1b[1dABC', 7),
        (b'\x1b[1mABC', 3),
        (b'\x1b[32mABC', 3),
        (b'\x1b[32HABC', 3),
        (b'\x1b[32JABC', 3),
        (b'\x1b[32KABC', 3),
        (b'\x1b[32GABC', 3),
        (b'\x1b[32mABC\x1b[32mDE', 5),
        (b'\x1b[12mABC', 3),
        (b'\x1b[123mABC', 3),
        (b'\x1b[32;12mABC', 3),
        (b'\x1b[32;123mABC', 3),
    ])
    def test_len_is_correct(input, result_len):
        assert bare_len(input) == result_len
