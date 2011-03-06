import os
import signal
try:
    from pexpect import spawn
except ImportError, e:
    raise ImportError (str(e) + """
Pexpect module was not found. The module is used by SAM to start 
processes and obtain from them line-buffered output to stdout.
Normally the module should be shipped with SAM client and located in
client/lib/python.""")
class spawnpgrp(spawn):
    def __init__(self,command,timeout=30):
        self.sched_timeout = None # for catching global timeout from Schefuler.py  - [scheduler].default_timeout
        self.term_timeout = None  # for catching test timeout from 'same-run-test' - [submission].test_timeout
        spawn.__init__(self, command, args=[], timeout=timeout, maxread=2000, searchwindowsize=None, logfile=None, env=None)
        
    def __fork_pty(self):
        # make us the session leader
        os.setpgrp()
        spawn.__fork_pty(self)
        
    def kill(self,sig=signal.SIGTERM):
        if self.isalive():
            os.kill(-self.pid, sig)
