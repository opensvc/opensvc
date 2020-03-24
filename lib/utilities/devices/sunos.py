from rcUtilities import justcall, cache

@cache("prtvtoc.{args[0]}")
def prtvtoc(dev):
    out, _, ret = justcall(["prtvtoc", dev])
    if ret != 0:
        return
    return out

