def fmt_listener(addr, port):
    if ":" in addr:
        return "[%s]:%d" % (addr, port)
    else:
        return "%s:%d" % (addr, port)
