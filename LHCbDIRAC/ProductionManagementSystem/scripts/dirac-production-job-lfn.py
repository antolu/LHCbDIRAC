#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-lfn.py
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve input and output LFNs for given job
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Job ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Job:      DIRAC Job Id' ] ) )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if len( args ) == 0:
  Script.showHelp()

from LHCbDIRAC.Core.Utilities.JobInfoFromXML import JobInfoFromXML

for job in args:

  jobinfo = JobInfoFromXML( job )
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue

  result = jobinfo.getOutputLFN()
  if result['OK']:
    lfns = result['Value']
    print 'OutputLFN:', lfns

  result = jobinfo.getInputLFN()
  if result['OK']:
    lfns = result['Value']
    print 'InputLFN:', lfns
