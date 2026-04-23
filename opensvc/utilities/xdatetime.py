import time

def as_timestamp(dt):
    """
    Convert a datetime object to a Unix timestamp.

    Args:
        dt (datetime): The datetime object to convert.

    Returns:
        float: The Unix timestamp corresponding to the datetime object.
    """
    try:
        return dt.timestamp()
    except AttributeError:
        # Python 2.7 fallback
        return time.mktime(dt.timetuple())
