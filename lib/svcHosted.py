import svc

class SvcHosted(svc.Svc):
    """Define hosted services
    """

    def __init__(self, svcname, optional=False, disabled=False, tags=set([])):
        svc.Svc.__init__(self, svcname, "Hosted", optional=optional, disabled=disabled, tags=tags)

if __name__ == "__main__":
    for c in (SvcHosted,) :
        help(c)
    import mountSunOS as mount
    import ipSunOS as ip
    print("""
    S=SvcHosted()
    S+=mount.Mount("/mnt1","/dev/sda")
    S+=mount.Mount("/mnt2","/dev/sdb")
    S+=ip.Ip("eth0","192.168.0.173")
    S+=ip.Ip("eth0","192.168.0.174")
    """)

    S=SvcSone()
    S+=mount.Mount("/mnt1","/dev/sda")
    S+=mount.Mount("/mnt2","/dev/sdb")
    S+=ip.Ip("eth0","192.168.0.173")
    S+=ip.Ip("eth0","192.168.0.174")

    print("Show S: ", S)
    print("start S:")
    S.start()

    print("stop S:")
    S.stop()

