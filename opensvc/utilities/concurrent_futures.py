def get_concurrent_futures():
    """
    helper to get concurrent.futures module
    return concurrent.futures module from concurrent.futures or from
    foreign.concurrent.futures
    """
    try:
        import concurrent.futures as concurrent_futures
    except ImportError:
        import foreign.concurrent.futures as concurrent_futures
    return concurrent_futures
