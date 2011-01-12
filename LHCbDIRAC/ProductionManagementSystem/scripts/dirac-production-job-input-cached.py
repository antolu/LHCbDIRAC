#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-input-cached.py
# Author :  Greig A Cowan
########################################################################
"""
  Cache input LFNs for given job
"""
__author__ = 'Greig A Cowan'
__date__ = 'Sept 2008'
__RCSID__ = '$Id$'

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

from DIRAC.Interfaces.API.Dirac import Dirac
from LHCbDIRAC.Core.Utilities.JobInfoFromXML import JobInfoFromXML

#ÊScript.addDefaultOptionValue( "LogLevel", "ALWAYS" )
jobToLfns = {}

for arg in args:
  # Get job input LFNs
  jobinfo = JobInfoFromXML( job )
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue

  result = jobinfo.getInputLFN()

  if result['OK']:
    lfns = result['Value']
  else:
    print result['Message']
    DIRAC.exit( 2 )

  jobToLfns[ job] = lfns[0].split( ';' )

dirac = Dirac()
exitCode = 0
errorList = {}

for job, lfns in jobToLfns.iteritems():
  # Find the replica information for each LFN
  result = dirac.getReplicas( lfns, printOutput = False )
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
      meta = dirac.getPhysicalFileMetadata( surl, se, printOutput = False )
      if not meta['OK']:
        errorList[ lfn] = meta['Message']
        exitCode = 2
      elif meta['OK'] and meta['Value']['Successful'] == {}:
        errorList[ lfn] = meta['Value']['Failed'][ lfn]
        exitCode = 2

      # Check that you can get a TURL for each LFN
      pfn = dirac.getAccessURL( lfn, se, printOutput = False )
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
  print 'ERROR %s: %s' % ( lfn, error )

DIRAC.exit( exitCode )
