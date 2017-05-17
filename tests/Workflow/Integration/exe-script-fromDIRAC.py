#!/usr/bin/env python
'''Script to run Executable application'''

import os

print "This is the environment in which I am running"
print os.environ
print "Now I will try importing DIRAC"

import DIRAC
from DIRAC import gLogger

gLogger.always("Hello hello!")


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gConfig
setup = gConfig.getValue('/DIRAC/Setup')

gLogger.always("I am running on setup %s" % setup)
