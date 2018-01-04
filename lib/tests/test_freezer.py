from freezer import Freezer

def test_freeze_node():
    Freezer("node").freeze()
    Freezer("unittest").thaw()
    ret = Freezer("unittest").frozen()
    assert ret == 1
    ret = Freezer("unittest").frozen(strict=True)
    assert ret == 0
    Freezer("node").thaw()

