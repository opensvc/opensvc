try:
    from setproctitle import setproctitle
except ImportError:
    pass
else:
    setproctitle("forkserver")

