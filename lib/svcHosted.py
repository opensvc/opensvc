import svc

class SvcHosted(svc.Svc):
    """
    Define a hosted service
    """

    def __init__(self, svcname):
        self.type = "hosted"
        svc.Svc.__init__(self, svcname)

