from popen2 import Popen4
import os
import signal

class Popenpgrp(Popen4):
    def __init__(self, cmd, bufsize=-1):
        Popen4.__init__(self,cmd,bufsize)

    def _run_child(self,cmd):
        os.setpgrp()
        Popen4._run_child(self,cmd)

    def kill(self,sig=signal.SIGTERM):
        os.kill(-self.pid,sig)
