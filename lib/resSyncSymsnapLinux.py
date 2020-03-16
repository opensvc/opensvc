import resSyncSymcloneLinux


def adder(svc, s):
    resSyncSymcloneLinux.adder(svc, s, drv=SyncSymsnap, t="sync.symsnap")


class SyncSymsnap(resSyncSymcloneLinux.SyncSymclone):
    pass
