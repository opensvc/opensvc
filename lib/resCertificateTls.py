from resData import Data

class Certificate(Data):
    def __init__(self, rid, **kwargs):
        Data.__init__(self, rid, type="certificate.tls", **kwargs)
