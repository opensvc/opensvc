def chunker(buff, n):
    """
    Yield successive n-sized chunks from buff
    """
    for i in range(0, len(buff), n):
        yield buff[i:i + n]
