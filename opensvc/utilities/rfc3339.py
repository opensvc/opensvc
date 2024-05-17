import time
import datetime


class RFC3339(object):
    """
    RFC3339 provides converters to RFC 3339 with timezone (process timezone)
    """
    def __init__(self):
        iso_format = "%Y-%m-%dT%H:%M:%S.%f"
        timezone_offset = time.strftime("%z")
        self.format = iso_format + timezone_offset[0:3]+":"+timezone_offset[3:]

    def from_epoch(self, t):
        return datetime.datetime.fromtimestamp(t).strftime(self.format)
