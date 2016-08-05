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
  Script.registerSwitch( '', 'Input', '  Only input files' )
  Script.registerSwitch( '', 'Output', '  Only output files' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [DIRACJobid|File]' % Script.scriptName,
                                       'Arguments:',
                                       '  DIRACJobid:      DIRAC Jobids',
                                       '  File:     Name of the file with contains a list of DIRACJobids'] ) )



  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  input = False
  output = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Input':
      input = True
    if switch[0] == 'Output':
      output = True
  if not input and not output:
    input = True
    output = True
  jobidList = []
  for jobid in args:
    if os.path.exists( jobid ):
      bkScript.setJobidsFromFile( jobid )
    else:
      jobidList.append( jobid )
  jobidList += bkScript.getOption( 'JobIDs', [] )
  if not jobidList:
    print "No jobID provided!"
    Script.showHelp()
    DIRAC.exit( 0 )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  retVal = BookkeepingClient().getJobInputOutputFiles( jobidList )
  if retVal['OK'] and ( not input or not output ):
    success = retVal['Value']['Successful']
    for job in success:
      success[job].pop( 'InputFiles' if not input else 'OutputFiles' )

  if retVal['OK']:
    printDMResult( retVal, empty = "File does not exists in the Bookkeeping" )
