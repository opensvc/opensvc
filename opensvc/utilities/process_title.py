try:
    from setproctitle import setproctitle as set_process_title
except ImportError:
    def set_process_title(_):
        pass
