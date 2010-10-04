#! /usr/bin/env python

import DIRAC
from DIRAC.Core.Base import Script


def usage():
  print 'Usage: %s [Try -h,--help for more information] job [job2 [job3 [...]]]' %(Script.scriptName)
  DIRAC.exit(2)


Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len(args)==0:
  usage()

from LHCbDIRAC.Core.Utilities.JobInfoFromXML import JobInfoFromXML


for arg in args:
  try:
    job = int(arg)
  except:
    print "Wrong argument, job must be integer %s  "%arg
    continue
    
  jobinfo = JobInfoFromXML(job)
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue
    
  result = jobinfo.getOutputLFN()
  if result['OK']:
    lfns = result['Value']
    print 'OutputLFN:',lfns

  result = jobinfo.getInputLFN()
  if result['OK']:
    lfns = result['Value']
    print 'InputLFN:',lfns
