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

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  """ usage
  
  Prints script usage 
  
  """
    
  print 'Usage: %s <WMS Job ID> [<WMS Job ID>]' % Script.scriptName
  DIRAC.exit( 2 )

if len(args)<1:
  usage()

jobIDs = []
diracProd = DiracProduction()
try:
  jobIDs = [ int(jobID) for jobID in args ]
except Exception,x:
  print 'ERROR WMS JobID(s) must be integers'
  DIRAC.exit( 2 )

exitCode = 0
errorList = []

for job in jobIDs:
  result = diracProd.getWMSProdJobID( job, printOutput = True )
  if result.has_key('Message'):
    errorList.append( (job, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (job, 'Null result for getWMSProdJobID() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
