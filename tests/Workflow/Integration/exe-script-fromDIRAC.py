#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
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
