import logging
import time
from datetime import datetime


class RFC3339(object):
    """
    RFC3339 provides converters to RFC 3339 with timezone (process timezone)
    """
    def __init__(self):
        iso_format = "%Y-%m-%dT%H:%M:%S.%f"
        timezone_offset = time.strftime("%z")
        self.format = iso_format + timezone_offset[0:3]+":"+timezone_offset[3:]

    def from_epoch(self, t):
        return datetime.fromtimestamp(t).strftime(self.format)


class RFC3339Formatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        self.rfc3339 = RFC3339()

    def formatTime(self, record, datefmt=None):
        return self.rfc3339.from_epoch(record.created)
