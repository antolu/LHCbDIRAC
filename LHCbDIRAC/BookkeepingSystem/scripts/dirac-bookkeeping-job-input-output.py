#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-job-input-output
# Author :  Zoltan Mathe
########################################################################
"""
  It returns the input and output files of a given list of DIRAC Jobids
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import DIRAC
import os

if __name__ == "__main__":

  bkScript = DMScript()
  bkScript.registerJobsSwitches()
  Script.registerSwitch( '', 'InputFiles', '  Only input files' )
  Script.registerSwitch( '', 'OutputFiles', '  Only output files' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [DIRACJobid|File]' % Script.scriptName,
                                       'Arguments:',
                                       '  DIRACJobid:      DIRAC Jobids',
                                       '  File:     Name of the file with contains a list of DIRACJobids'] ) )



  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  inputFiles = False
  outputFiles = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'InputFiles':
      inputFiles = True
    if switch[0] == 'OutputFiles':
      outputFiles = True
  if not inputFiles and not outputFiles:
    inputFiles = True
    outputFiles = True
  jobidList = []
  for jobid in args:
    if os.path.exists( jobid ):
      bkScript.setJobidsFromFile( jobid )
    else:
      jobidList += jobid.split( ',' )
  jobidList += bkScript.getOption( 'JobIDs', [] )
  if not jobidList:
    print "No jobID provided!"
    Script.showHelp()
    DIRAC.exit( 0 )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  retVal = BookkeepingClient().getJobInputOutputFiles( jobidList )
  if retVal['OK']:
    success = retVal['Value']['Successful']
    for job in success:
      # Remove from input the files that are also output! This happens because the output of step 1 can be the input of step 2...
      # only worth if input files are requested though
      if inputFiles:
        success[job]['InputFiles'] = sorted( set( success[job]['InputFiles'] ) - set( success[job]['OutputFiles'] ) )

      if not inputFiles or not outputFiles:
        success[job].pop( 'InputFiles' if not inputFiles else 'OutputFiles' )

  printDMResult( retVal, empty = "File does not exists in the Bookkeeping" )
