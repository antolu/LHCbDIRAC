#! /usr/bin/env python

__author__ = 'Greig A Cowan'
__date__ = 'Sept 2008'
__RCSID__ = '$Id$'
__VERSION__ = '$Revision: 1.1 $'

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac
from LHCbDIRAC.LHCbSystem.Utilities.JobInfoFromXML import JobInfoFromXML

def usage():
  print 'Usage: %s [Try -h,--help for more information] job [job2 [job3 [...]]]' %(Script.scriptName)
  DIRAC.exit(2)

#Script.initAsScript()
Script.addDefaultOptionValue( "LogLevel", "ALWAYS" )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len(args)==0:
  usage()

jobToLfns = {}

for arg in args:
  try:
    job = int(arg)
  except:
    print "Wrong argument, job must be integer %s  "%arg
    continue

  # Get job input LFNs
  jobinfo = JobInfoFromXML(job)
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue

  result = jobinfo.getInputLFN()

  if result['OK']:
    lfns = result['Value']
  else:
    print result['Message']
    DIRAC.exit(2)

  jobToLfns[ job] = lfns[0].split(';')

dirac = Dirac()
exitCode = 0
errorList = {}

for job, lfns in jobToLfns.iteritems():
  # Find the replica information for each LFN
  result = dirac.getReplicas( lfns, printOutput=False)
  if not result['OK']:
    errorList[ lfn] = result['Message']
    exitCode = 2
  else:
    replicas = result['Value']

  noReps = replicas['Failed']
  reps = replicas['Successful']

  for lfn in noReps.keys():
    print 'ERROR: %s has no replica' % lfn

  for lfn, surls in reps.iteritems():
    for se, surl in surls.iteritems():
      # Check the metadata for each file - should be cached
      meta = dirac.getPhysicalFileMetadata( surl, se, printOutput=False)
      if not meta['OK']:
        errorList[ lfn] = meta['Message']
        exitCode = 2
      elif meta['OK'] and meta['Value']['Successful'] == {}:
        errorList[ lfn] = meta['Value']['Failed'][ lfn]
        exitCode = 2

      # Check that you can get a TURL for each LFN
      pfn = dirac.getAccessURL( lfn, se, printOutput=False)
      if not pfn['OK']:
        errorList[ lfn] = pfn['Message']
        exitCode = 2
      elif pfn['OK'] and pfn['Value']['Successful'] == {}:
        errorList[ lfn] = pfn['Value']['Failed'][ lfn]
        exitCode = 2
      else:
        print lfn, meta['Value']['Successful'][ surl]['Cached'], \
              pfn['Value']['Successful'][ lfn].values()[0]

for lfn, error in errorList.iteritems():
  print 'ERROR %s: %s' % (lfn, error)

DIRAC.exit( exitCode)
