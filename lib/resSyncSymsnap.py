import resSyncSymclone


def adder(svc, s):
    resSyncSymclone.adder(svc, s, drv=SyncSymsnap, t="sync.symsnap")


class SyncSymsnap(resSyncSymclone.SyncSymclone):
    pass
