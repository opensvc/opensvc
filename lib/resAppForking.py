"""
The module defining the app.forking resource class.
"""

import resApp

class App(resApp.App):
    """
    The forking App resource driver class.
    """

    def __init__(self, rid, **kwargs):
        resApp.App.__init__(self, rid, type="app.forking", **kwargs)
