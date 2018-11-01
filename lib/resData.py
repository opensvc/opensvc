import rcStatus
from resources import Resource
from storage import Storage

class Data(Resource):
    def __init__(self, rid, type="data", **kwargs):
        Resource.__init__(self, rid, type=type)
        self.options = Storage(kwargs)

    def status_info(self):
        data = {}
        for key, val in self.section_kwargs().items():
            if val not in (None, []):
                data[key] = val
        return data
        
    def _status(self, verbose=False):
        return rcStatus.NA

