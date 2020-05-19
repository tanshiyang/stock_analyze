import threading
import sys


class Dispacher(threading.Thread):
    def __init__(self, fun, args):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.result = None
        self.error = None
        self.fun = fun
        self.args = args

        self.start()

    def run(self):
        try:
            self.result = self.fun(self.args)
        except:
            self.error = sys.exc_info()

