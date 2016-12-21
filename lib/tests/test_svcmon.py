import svcmon

def test_svcmon():
    ret = svcmon.main(argv=[])
    assert ret == 0

def test_svcmon_refresh():
    ret = svcmon.main(argv=["--refresh"])
    assert ret == 0

def test_svcmon_updatedb():
    ret = svcmon.main(argv=["--updatedb"])
    assert ret == 0

def test_svcmon_verbose():
    ret = svcmon.main(argv=["-v"])
    assert ret == 0

def test_svcmon_cluster_verbose():
    ret = svcmon.main(argv=["-v", "-c"])
    assert ret == 0

